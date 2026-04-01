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
    6. Save two brief versions to GitHub:
          /output/YYYYMMDD_WeeklyBrief_Operator.md    (all sections)
          /output/YYYYMMDD_WeeklyBrief_Stakeholder.md (Source Quality Feedback stripped)
    7. Push Updated Standing View + Delta Log back to GitHub
    8. Run Stage 3 QA evaluation (Operator version) → save /output/YYYYMMDD_QA_Report.md
    9. Create two Gmail drafts:
          Stakeholder — "Legal Tech Intelligence Brief — Week of [DATE]"
          Operator    — "Legal Tech Intelligence Brief — Week of [DATE] [OPERATOR VERSION]"
"""
import argparse
import difflib
import json
import logging
import string
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse, urlunparse

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

# ── Deduplication helpers ──────────────────────────────────────────────────────

_DEDUP_NOISE_WORDS = frozenset({
    "the", "a", "an", "in", "on", "at", "to", "for", "of", "and", "or",
    "is", "are", "was", "were", "has", "have", "with", "from", "that",
    "this", "how", "why", "what", "will", "can", "new", "says", "report",
})
_PUNCT_TABLE = str.maketrans("", "", string.punctuation)


def _normalize_url(url: str) -> str:
    """Strip query parameters and trailing slash for exact-match comparison."""
    try:
        p = urlparse(url)
        return urlunparse((p.scheme, p.netloc, p.path.rstrip("/"), "", "", "")).lower()
    except Exception:
        return url.lower().rstrip("/")


def _normalize_title(title: str) -> str:
    """Lowercase, strip punctuation, remove noise words."""
    t = title.lower().translate(_PUNCT_TABLE)
    return " ".join(w for w in t.split() if w not in _DEDUP_NOISE_WORDS)


def _dedup_by_url(
    p_items: list[dict],
    h_items: list[dict],
) -> tuple[list[dict], list[dict], int]:
    """
    Pass 1 — exact URL deduplication.
    Within each feed: keep first occurrence of each normalized URL.
    Across feeds: priority URL beats high_signal URL.
    Returns (deduped_priority, deduped_high_signal, n_removed).
    """
    removed = 0

    # Within priority
    seen: set[str] = set()
    deduped_p: list[dict] = []
    for item in p_items:
        norm = _normalize_url(item.get("url", ""))
        if norm and norm in seen:
            removed += 1
        else:
            seen.add(norm)
            deduped_p.append(item)

    # Within high_signal
    seen_h: set[str] = set()
    deduped_h: list[dict] = []
    for item in h_items:
        norm = _normalize_url(item.get("url", ""))
        if norm and norm in seen_h:
            removed += 1
        else:
            seen_h.add(norm)
            deduped_h.append(item)

    # Cross-feed: drop high_signal items whose URL already appears in priority
    priority_urls = {_normalize_url(item.get("url", "")) for item in deduped_p}
    final_h: list[dict] = []
    for item in deduped_h:
        norm = _normalize_url(item.get("url", ""))
        if norm and norm in priority_urls:
            removed += 1
        else:
            final_h.append(item)

    return deduped_p, final_h, removed


def _dedup_by_title(
    p_items: list[dict],
    h_items: list[dict],
) -> tuple[list[dict], list[dict], int]:
    """
    Pass 2 — title similarity deduplication using difflib.SequenceMatcher.
    Greedy clustering: once an article is absorbed into a cluster, skip it.
    Priority feed wins over high_signal; within same feed, keep longer summary.
    Returns (deduped_priority, deduped_high_signal, n_removed).
    """
    threshold = config.DEDUP_TITLE_SIMILARITY_THRESHOLD

    # Tag each item with its feed source
    tagged: list[tuple[dict, str]] = (
        [(item, "priority") for item in p_items] +
        [(item, "high_signal") for item in h_items]
    )
    n = len(tagged)
    keep = [True] * n
    removed = 0

    for i in range(n):
        if not keep[i]:
            continue
        item_i, feed_i = tagged[i]
        norm_i = _normalize_title(item_i.get("title", ""))

        for j in range(i + 1, n):
            if not keep[j]:
                continue
            item_j, feed_j = tagged[j]
            norm_j = _normalize_title(item_j.get("title", ""))

            ratio = difflib.SequenceMatcher(None, norm_i, norm_j).ratio()
            if ratio < threshold:
                continue

            # Determine winner
            if feed_i == "priority" and feed_j == "high_signal":
                loser_idx, winner, winner_feed, loser, loser_feed = j, item_i, feed_i, item_j, feed_j
            elif feed_j == "priority" and feed_i == "high_signal":
                loser_idx, winner, winner_feed, loser, loser_feed = i, item_j, feed_j, item_i, feed_i
            else:
                # Same feed — keep the one with the longer summary
                len_i = len(item_i.get("summary", "") or item_i.get("content", ""))
                len_j = len(item_j.get("summary", "") or item_j.get("content", ""))
                if len_j > len_i:
                    loser_idx, winner, winner_feed, loser, loser_feed = i, item_j, feed_j, item_i, feed_i
                else:
                    loser_idx, winner, winner_feed, loser, loser_feed = j, item_i, feed_i, item_j, feed_j

            keep[loser_idx] = False
            removed += 1
            logger.debug(
                "Duplicate removed: \"%s\" (kept, %s) vs \"%s\" (removed, %s, similarity=%.2f)",
                winner.get("title", "")[:80], winner_feed,
                loser.get("title", "")[:80], loser_feed,
                ratio,
            )

            if loser_idx == i:
                break  # item_i was removed; stop its outward comparisons

    final_p = [item for (item, feed), k in zip(tagged, keep) if k and feed == "priority"]
    final_h = [item for (item, feed), k in zip(tagged, keep) if k and feed == "high_signal"]
    return final_p, final_h, removed


def _dedup_feeds(
    priority_feed: dict,
    high_signal_feed: dict,
) -> tuple[dict, dict]:
    """
    Apply URL and title-similarity deduplication across both feeds.
    Preserves feed_type on each article (items stay in their original feed dict).
    No-ops if config.DEDUP_ENABLED is False.
    """
    if not config.DEDUP_ENABLED:
        logger.info("Deduplication disabled (DEDUP_ENABLED=False) — skipping.")
        return priority_feed, high_signal_feed

    p_items = list(priority_feed.get("items", []))
    h_items = list(high_signal_feed.get("items", []))
    before_p, before_h = len(p_items), len(h_items)

    # Pass 1: URL exact match
    p_items, h_items, url_removed = _dedup_by_url(p_items, h_items)

    # Pass 2: title similarity
    p_items, h_items, title_removed = _dedup_by_title(p_items, h_items)

    after_p, after_h = len(p_items), len(h_items)
    total_removed = url_removed + title_removed

    logger.info("Deduplication complete:")
    logger.info("  Priority feed:    %d articles before → %d after", before_p, after_p)
    logger.info("  High-signal feed: %d articles before → %d after", before_h, after_h)
    logger.info("  Combined:         %d articles before → %d after",
                before_p + before_h, after_p + after_h)
    logger.info("  Duplicates removed: %d (URL: %d, title similarity: %d)",
                total_removed, url_removed, title_removed)

    return (
        {**priority_feed,    "items": p_items, "item_count": len(p_items)},
        {**high_signal_feed, "items": h_items, "item_count": len(h_items)},
    )


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

    today = date.today()
    week_start = today - timedelta(days=today.weekday())  # most recent Monday
    fmt = "%B %#d, %Y" if sys.platform == "win32" else "%B %-d, %Y"
    coverage_period = f"Week of {week_start.strftime(fmt)} to {today.strftime(fmt)}"
    logger.info("Coverage period: %s", coverage_period)

    result = analysis.run_stage2(standing_view, event_ledger, coverage_period=coverage_period)
    logger.info("Weekly Brief: %d chars", len(result.weekly_brief))
    logger.info("Updated Standing View: %d chars", len(result.updated_standing_view))
    logger.info("Delta Log: %d chars", len(result.delta_log))
    return result


def _make_stakeholder_brief(brief: str) -> str:
    """
    Strip the Source Quality Feedback section from the brief to produce
    the stakeholder-facing version (Version A).  The section is always
    last, so we remove from its heading to end-of-string.
    """
    import re
    stripped = re.sub(
        r'\n#{1,4}\s+(?:\d+\)\s+)?Source Quality Feedback\b.*',
        '',
        brief,
        flags=re.DOTALL | re.IGNORECASE,
    )
    return stripped.rstrip()


def step_save_outputs(
    result: analysis.Stage2Output,
    stakeholder_brief: str,
    dry_run: bool,
) -> tuple[str, str]:
    """
    Save both brief versions and supporting outputs to GitHub.

    Returns (operator_path, stakeholder_path).
    """
    today = date.today()
    date_str = today.strftime("%Y%m%d")
    commit_date = today.isoformat()

    operator_path = f"{config.GITHUB_OUTPUT_DIR}/{date_str}_WeeklyBrief_Operator.md"
    stakeholder_path = f"{config.GITHUB_OUTPUT_DIR}/{date_str}_WeeklyBrief_Stakeholder.md"

    logger.info("=== STEP 5: Save outputs to GitHub ===")
    logger.info(
        "Stakeholder brief: %d chars (Source Quality Feedback stripped from %d chars)",
        len(stakeholder_brief), len(result.weekly_brief),
    )

    if not dry_run:
        gh.upsert_file(
            operator_path,
            result.weekly_brief,
            f"feat: add operator brief [{commit_date}]",
        )
        logger.info("Operator brief saved: %s", operator_path)

        gh.upsert_file(
            stakeholder_path,
            stakeholder_brief,
            f"feat: add stakeholder brief [{commit_date}]",
        )
        logger.info("Stakeholder brief saved: %s", stakeholder_path)

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

    return operator_path, stakeholder_path


def step_stage3_qa(
    result: analysis.Stage2Output,
    stakeholder_brief: str,
    dry_run: bool,
) -> analysis.Stage3QAOutput:
    """
    Run Stage 3 QA evaluation against both brief versions:
      - Stakeholder brief → Gates 1-6, 8, 9 (affects overall verdict)
      - Operator brief    → Gate 7 only (reported separately)
    """
    logger.info("=== STEP 6: Stage 3 — QA Evaluation ===")
    logger.info("  Stakeholder brief → Gates 1-6, 8, 9")
    logger.info("  Operator brief    → Gate 7 (Source Quality Feedback, separate)")

    qa = analysis.run_stage3_qa(stakeholder_brief, result.weekly_brief)

    # Always print the summary to the console
    print(qa.console_summary())

    # Save QA report
    date_str = date.today().strftime("%Y%m%d")
    qa_path = f"{config.GITHUB_OUTPUT_DIR}/{date_str}_QA_Report.md"

    if not dry_run:
        gh.upsert_file(
            qa_path,
            qa.raw,
            f"chore: add QA report [{date.today().isoformat()}]",
        )
        logger.info("QA Report saved: %s", qa_path)
    else:
        # Write locally for inspection
        local_qa_path = Path(f"{date_str}_QA_Report.md")
        local_qa_path.write_text(qa.raw, encoding="utf-8")
        logger.info("[dry-run] QA Report written locally: %s", local_qa_path)

    if qa.overall != "APPROVED":
        logger.warning(
            "QA: %d gate(s) failed — brief flagged for revision. "
            "Pipeline will continue and produce Gmail draft. "
            "Review QA report before sending.",
            qa.fail_count,
        )

    return qa


def step_email_draft(
    result: analysis.Stage2Output,
    stakeholder_brief: str,
    dry_run: bool,
) -> None:
    """
    Create two Gmail drafts:
      Version A — Stakeholder: Bottom Line, What Changed, Market Implications,
                  Watch Next. Subject: "Legal Tech Intelligence Brief — Week of [DATE]"
      Version B — Operator:    All sections including Source Quality Feedback.
                  Subject: "Legal Tech Intelligence Brief — Week of [DATE] [OPERATOR VERSION]"
    """
    logger.info("=== STEP 7: Create Gmail drafts (Stakeholder + Operator) ===")

    week_str = (
        date.today().strftime("%B %#d, %Y")
        if sys.platform == "win32"
        else date.today().strftime("%B %-d, %Y")
    )
    subject_stakeholder = f"Legal Tech Intelligence Brief — Week of {week_str}"
    subject_operator = f"Legal Tech Intelligence Brief — Week of {week_str} [OPERATOR VERSION]"

    html_stakeholder = markdown_to_html(stakeholder_brief, title=subject_stakeholder)
    html_operator = markdown_to_html(result.weekly_brief, title=subject_operator)

    if not dry_run:
        draft_a = gmail_client.create_draft(
            subject=subject_stakeholder,
            html_body=html_stakeholder,
            plain_body=stakeholder_brief,
        )
        logger.info("Stakeholder draft created: id=%s", draft_a)

        draft_b = gmail_client.create_draft(
            subject=subject_operator,
            html_body=html_operator,
            plain_body=result.weekly_brief,
        )
        logger.info("Operator draft created: id=%s", draft_b)
    else:
        logger.info("[dry-run] Skipping Gmail draft creation.")
        for name, html in (
            ("stakeholder", html_stakeholder),
            ("operator", html_operator),
        ):
            preview_path = f"output_preview_{name}.html"
            with open(preview_path, "w", encoding="utf-8") as f:
                f.write(html)
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
        priority_feed, high_signal_feed = _dedup_feeds(priority_feed, high_signal_feed)
    else:
        priority_feed, high_signal_feed = step_fetch_feeds(dry_run)
        priority_feed, high_signal_feed = _dedup_feeds(priority_feed, high_signal_feed)

    # 2. Standing View
    standing_view = step_fetch_standing_view()

    # 3. Stage 1 — Event Ledger (or load from cache)
    if resume:
        event_ledger = step_stage1_from_cache()
    else:
        event_ledger = step_stage1(standing_view, priority_feed, high_signal_feed)

    # 4. Stage 2 — Weekly Brief
    result = step_stage2(standing_view, event_ledger)

    # Derive stakeholder brief once; reused by save, QA, and email steps
    stakeholder_brief = _make_stakeholder_brief(result.weekly_brief)

    # 5. Save to GitHub (Operator + Stakeholder versions)
    operator_path, stakeholder_path = step_save_outputs(result, stakeholder_brief, dry_run)

    # 6. Stage 3 — QA: stakeholder brief (Gates 1-6,8,9) + operator brief (Gate 7)
    step_stage3_qa(result, stakeholder_brief, dry_run)

    # 7. Gmail drafts (Stakeholder + Operator)
    step_email_draft(result, stakeholder_brief, dry_run)

    elapsed = (datetime.now(tz=timezone.utc) - start).total_seconds()
    logger.info(
        "Pipeline complete in %.1fs. Operator: %s  Stakeholder: %s",
        elapsed, operator_path, stakeholder_path,
    )


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
