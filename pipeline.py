"""
pipeline.py — Main orchestrator for the Legal Tech Intelligence Digest.

Usage:
    python pipeline.py              # full run
    python pipeline.py --dry-run    # skip GitHub writes and Gmail draft
    python pipeline.py --skip-fetch # re-use existing feeds already in GitHub

Flow:
    1. Fetch priority + high_signal articles from InnoReader
    2. Save feeds to GitHub  /feeds/priority.json  and  /feeds/high_signal.json
    3. Fetch Standing View from GitHub
    4. Run Stage 1 → Event Ledger
    5. Run Stage 2 → Weekly Brief + Updated Standing View + Delta Log
    6. Save Weekly Brief to GitHub  /output/YYYYMMDD_WeeklyBrief.md
    7. Push Updated Standing View back to GitHub
    8. Convert brief to HTML and create Gmail draft
"""
import argparse
import json
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import markdown as md

import analysis
import config
import github_client as gh
import gmail_client
import inoreader_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── HTML wrapper ───────────────────────────────────────────────────────────────

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    body {{ font-family: Georgia, serif; max-width: 780px; margin: 40px auto;
           padding: 0 20px; color: #222; line-height: 1.7; }}
    h1 {{ color: #1a1a2e; border-bottom: 2px solid #1a1a2e; padding-bottom: 8px; }}
    h2 {{ color: #16213e; margin-top: 2em; }}
    h3 {{ color: #0f3460; }}
    a  {{ color: #0f3460; }}
    blockquote {{ border-left: 3px solid #ccc; margin-left: 0;
                  padding-left: 1em; color: #555; }}
    code {{ background: #f4f4f4; padding: 2px 5px; border-radius: 3px;
            font-size: 0.9em; }}
    pre  {{ background: #f4f4f4; padding: 1em; overflow-x: auto;
            border-radius: 4px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 8px 12px; text-align: left; }}
    th {{ background: #f0f0f0; }}
  </style>
</head>
<body>
{body}
</body>
</html>
"""


def markdown_to_html(md_text: str, title: str = "Legal Tech Intelligence Brief") -> str:
    body = md.markdown(
        md_text,
        extensions=["tables", "fenced_code", "toc", "nl2br"],
    )
    return _HTML_TEMPLATE.format(title=title, body=body)


# ── InnoReader token helper ────────────────────────────────────────────────────

def _get_inoreader_token() -> str:
    """
    Return a valid InnoReader auth token.
    Uses INOREADER_TOKEN if set; otherwise authenticates via ClientLogin
    using INOREADER_USERNAME + INOREADER_PASSWORD.
    """
    token = config._optional("INOREADER_TOKEN")
    if token:
        return token

    username = config._optional("INOREADER_USERNAME")
    password = config._optional("INOREADER_PASSWORD")
    if username and password:
        logger.info("INOREADER_TOKEN not set — authenticating via ClientLogin…")
        token = inoreader_client.get_token_via_clientlogin(username, password)
        logger.info("ClientLogin successful.")
        return token

    logger.error(
        "No InnoReader credentials found. Set INOREADER_TOKEN or "
        "INOREADER_USERNAME + INOREADER_PASSWORD in your .env file."
    )
    sys.exit(1)


# ── Pipeline steps ─────────────────────────────────────────────────────────────

def step_fetch_feeds(dry_run: bool) -> tuple[dict, dict]:
    """Fetch both feeds from InnoReader and save to GitHub."""
    logger.info("=== STEP 1: Fetch feeds from InnoReader ===")
    token = _get_inoreader_token()
    priority_feed, high_signal_feed = inoreader_client.get_both_feeds(token)

    if not dry_run:
        today_str = date.today().isoformat()
        gh.write_json(
            config.GITHUB_PATH_PRIORITY,
            priority_feed,
            f"chore: update priority feed [{today_str}]",
        )
        gh.write_json(
            config.GITHUB_PATH_HIGH_SIGNAL,
            high_signal_feed,
            f"chore: update high_signal feed [{today_str}]",
        )
        logger.info("Feeds saved to GitHub.")
    else:
        logger.info("[dry-run] Skipping GitHub feed writes.")

    return priority_feed, high_signal_feed


def step_load_existing_feeds() -> tuple[dict, dict]:
    """Load feeds from GitHub instead of fetching from InnoReader."""
    logger.info("=== STEP 1 (skip-fetch): Loading existing feeds from GitHub ===")
    priority_feed, _ = gh.read_json(config.GITHUB_PATH_PRIORITY)
    high_signal_feed, _ = gh.read_json(config.GITHUB_PATH_HIGH_SIGNAL)
    logger.info(
        "Loaded: priority=%d items, high_signal=%d items",
        priority_feed.get("item_count", len(priority_feed.get("items", []))),
        high_signal_feed.get("item_count", len(high_signal_feed.get("items", []))),
    )
    return priority_feed, high_signal_feed


def step_fetch_standing_view() -> str:
    """Fetch the current Standing View from GitHub."""
    logger.info("=== STEP 2: Fetch Standing View from GitHub ===")
    content, _ = gh.read_file(config.GITHUB_PATH_STANDING_VIEW)
    logger.info("Standing View: %d chars", len(content))
    return content


_LEDGER_CACHE = Path("event_ledger_cache.md")

# Max chars for summary text per article sent to Claude
_SUMMARY_MAX_CHARS = 400


def _trim_feed(feed: dict) -> dict:
    """
    Strip full HTML content and truncate summaries before sending to Claude.
    Keeps only the fields the Stage 1 prompt needs: title, source, url,
    published_at, and a short summary. This keeps token usage well under limits.
    """
    import re

    def strip_html(text: str) -> str:
        return re.sub(r"<[^>]+>", " ", text or "").strip()

    trimmed_items = []
    for item in feed.get("items", []):
        summary_clean = strip_html(item.get("summary", "") or item.get("content", ""))
        trimmed_items.append({
            "title": item.get("title", ""),
            "source": item.get("source", ""),
            "url": item.get("url", ""),
            "published_at": item.get("published_at", ""),
            "summary": summary_clean[:_SUMMARY_MAX_CHARS],
        })

    return {
        "feed_type": feed.get("feed_type", ""),
        "fetched_at": feed.get("fetched_at", ""),
        "item_count": len(trimmed_items),
        "items": trimmed_items,
    }


def step_stage1(standing_view: str, priority_feed: dict, high_signal_feed: dict) -> str:
    """Run Stage 1: Event Ledger generation. Saves result to disk as a cache."""
    logger.info("=== STEP 3: Stage 1 — Event Ledger ===")
    trimmed_priority = _trim_feed(priority_feed)
    trimmed_high_signal = _trim_feed(high_signal_feed)
    logger.info(
        "Feeds trimmed for prompt: priority=%d items, high_signal=%d items",
        trimmed_priority["item_count"], trimmed_high_signal["item_count"],
    )
    event_ledger = analysis.run_stage1(standing_view, trimmed_priority, trimmed_high_signal)
    _LEDGER_CACHE.write_text(event_ledger, encoding="utf-8")
    logger.info("Event Ledger: %d chars (cached to %s)", len(event_ledger), _LEDGER_CACHE)
    return event_ledger


def step_stage1_from_cache() -> str:
    """Load a previously generated Event Ledger from disk cache."""
    if not _LEDGER_CACHE.exists():
        logger.error("No cached Event Ledger found at '%s'. Run without --resume first.", _LEDGER_CACHE)
        sys.exit(1)
    event_ledger = _LEDGER_CACHE.read_text(encoding="utf-8")
    logger.info("=== STEP 3 (resumed): Loaded Event Ledger from cache (%d chars) ===", len(event_ledger))
    return event_ledger


def step_stage2(standing_view: str, event_ledger: str) -> analysis.Stage2Output:
    """Run Stage 2: Weekly Brief + Updated Standing View + Delta Log."""
    logger.info("=== STEP 4: Stage 2 — Weekly Brief ===")
    result = analysis.run_stage2(standing_view, event_ledger)
    logger.info("Weekly Brief: %d chars", len(result.weekly_brief))
    logger.info("Updated Standing View: %d chars", len(result.updated_standing_view))
    logger.info("Delta Log: %d chars", len(result.delta_log))
    return result


def step_save_outputs(result: analysis.Stage2Output, dry_run: bool) -> str:
    """Save Weekly Brief and Updated Standing View to GitHub. Returns dated filename."""
    today = date.today()
    date_str = today.strftime("%Y%m%d")
    brief_path = f"{config.GITHUB_OUTPUT_DIR}/{date_str}_WeeklyBrief.md"
    commit_date = today.isoformat()

    logger.info("=== STEP 5: Save outputs to GitHub ===")

    if not dry_run:
        gh.upsert_file(
            brief_path,
            result.weekly_brief,
            f"feat: add weekly brief [{commit_date}]",
        )
        logger.info("Brief saved: %s", brief_path)

        if result.updated_standing_view:
            gh.upsert_file(
                config.GITHUB_PATH_STANDING_VIEW,
                result.updated_standing_view,
                f"chore: update standing view [{commit_date}]",
            )
            logger.info("Standing View updated.")
        else:
            logger.warning("No updated Standing View returned by Stage 2; skipping write.")

        if result.delta_log:
            delta_path = f"{config.GITHUB_OUTPUT_DIR}/{date_str}_DeltaLog.md"
            gh.upsert_file(
                delta_path,
                result.delta_log,
                f"chore: add delta log [{commit_date}]",
            )
            logger.info("Delta Log saved: %s", delta_path)
    else:
        logger.info("[dry-run] Skipping GitHub output writes.")

    return brief_path


def step_email_draft(result: analysis.Stage2Output, dry_run: bool) -> None:
    """Convert Weekly Brief to HTML and create a Gmail draft."""
    logger.info("=== STEP 6: Create Gmail draft ===")

    week_str = date.today().strftime("%B %#d, %Y") if sys.platform == "win32" else date.today().strftime("%B %-d, %Y")
    subject = f"Legal Tech Intelligence Brief — Week of {week_str}"

    html_body = markdown_to_html(result.weekly_brief, title=subject)

    if not dry_run:
        draft_id = gmail_client.create_draft(
            subject=subject,
            html_body=html_body,
            plain_body=result.weekly_brief,
        )
        logger.info("Gmail draft created: id=%s  to=%s", draft_id, config.GMAIL_USER)
    else:
        logger.info("[dry-run] Skipping Gmail draft creation.")
        # Still show a snippet so you can inspect the HTML locally
        preview_path = "output_preview.html"
        with open(preview_path, "w", encoding="utf-8") as f:
            f.write(html_body)
        logger.info("[dry-run] HTML preview written to: %s", preview_path)


# ── Entry point ────────────────────────────────────────────────────────────────

def run(dry_run: bool = False, skip_fetch: bool = False, resume: bool = False) -> None:
    start = datetime.now(tz=timezone.utc)
    logger.info("Legal Tech Intelligence Pipeline starting at %s", start.isoformat())
    logger.info("Repo: %s  |  Model: %s  |  dry-run=%s  skip-fetch=%s  resume=%s",
                config.GITHUB_REPO, config.CLAUDE_MODEL, dry_run, skip_fetch, resume)

    # 1. Feeds (skip if resuming from cached Event Ledger)
    if resume:
        priority_feed, high_signal_feed = {}, {}
    elif skip_fetch:
        priority_feed, high_signal_feed = step_load_existing_feeds()
    else:
        priority_feed, high_signal_feed = step_fetch_feeds(dry_run)

    # 2. Standing View
    standing_view = step_fetch_standing_view()

    # 3. Stage 1 — Event Ledger (or load from cache)
    if resume:
        event_ledger = step_stage1_from_cache()
    else:
        event_ledger = step_stage1(standing_view, priority_feed, high_signal_feed)

    # 4. Stage 2 — Weekly Brief
    result = step_stage2(standing_view, event_ledger)

    # 5. Save to GitHub
    brief_path = step_save_outputs(result, dry_run)

    # 6. Gmail draft
    step_email_draft(result, dry_run)

    elapsed = (datetime.now(tz=timezone.utc) - start).total_seconds()
    logger.info("Pipeline complete in %.1fs. Brief saved to: %s", elapsed, brief_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Legal Tech Intelligence Digest Pipeline")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run analysis but skip GitHub writes and Gmail draft",
    )
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Re-use feeds already in GitHub instead of calling InnoReader",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip Stage 1 and resume from cached event_ledger_cache.md (useful after rate limit failures)",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run, skip_fetch=args.skip_fetch, resume=args.resume)


if __name__ == "__main__":
    main()
