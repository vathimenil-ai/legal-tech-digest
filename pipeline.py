"""
pipeline.py — Main orchestrator for the Legal Tech Intelligence Digest.

Usage:
    python pipeline.py                       # daily run (default)
    python pipeline.py --mode daily          # daily run (explicit)
    python pipeline.py --mode weekly         # weekly synthesis run
    python pipeline.py --dry-run             # skip GitHub writes and Gmail draft
    python pipeline.py --skip-fetch          # re-use existing feeds already in GitHub
    python pipeline.py --resume              # skip Stage 1; resume from cached Event Ledger

Daily flow (Mon–Fri, 7:00 AM):
    1. Fetch feeds (2-day lookback window)
    2. Fetch Standing View from GitHub (read-only — not updated in daily mode)
    3. Fetch most recent EventLedger from GitHub (dedup context for Stage 1)
    4. Run Stage 1 → Event Ledger  (daily_stage1_prompt.txt or stage1_prompt.txt)
    5. Run Stage 2 → Daily Brief   (daily_stage2_prompt.txt or stage2_prompt.txt)
    6. Save to GitHub:
          /output/YYYYMMDD_DailyBrief_Operator.md
          /output/YYYYMMDD_DailyBrief_Stakeholder.md
          /output/YYYYMMDD_DailyBrief_Operator.html
          /output/YYYYMMDD_DailyBrief_Stakeholder.html
          /output/YYYYMMDD_EventLedger.md
    7. Run Stage 3 QA → /output/YYYYMMDD_DailyBrief_QA_Report.md
    8. Create two Gmail drafts:
          Stakeholder — "Legal Tech Intelligence — [Weekday, Month D, YYYY]"
          Operator    — same subject + [OPERATOR VERSION]

Weekly flow (Monday, 8:30 AM — covers prior Mon–Fri):
    1. Collect prior week's daily EventLedgers from GitHub
       (falls back to fresh Stage 1 with 7-day lookback if <3 found)
    2. Fetch Standing View from GitHub
    3. Run Stage 2 → Weekly Brief + Updated Standing View + Delta Log
       (weekly_stage2_prompt.txt or stage2_prompt.txt)
    4. Save to GitHub:
          /output/YYYYMMDD_WeeklyBrief_Operator.md
          /output/YYYYMMDD_WeeklyBrief_Stakeholder.md
          /output/YYYYMMDD_WeeklyBrief_Operator.html
          /output/YYYYMMDD_WeeklyBrief_Stakeholder.html
       + Update standing-view/standing_view.md
       + Save /output/YYYYMMDD_DeltaLog.md
    5. Run Stage 3 QA → /output/YYYYMMDD_QA_Report.md
    6. Create two Gmail drafts:
          Stakeholder — "Legal Tech Intelligence Brief — Week of [DATE]"
          Operator    — same subject + [OPERATOR VERSION]
"""
import argparse
import difflib
import json
import logging
import re
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


# ── Local editions output folder ───────────────────────────────────────────────
# HTML files are copied here after every live run so they sync to Google Drive
# without requiring a trip to GitHub.
_LOCAL_EDITIONS_DIR = Path(r"C:\Users\Nell\Desktop\LegalTechDigest_Editions")


def _write_local_editions(files: dict[str, str]) -> None:
    """Write HTML content to the local editions folder, creating it if needed.

    Args:
        files: mapping of filename (e.g. '20260404_DailyBrief_Stakeholder.html')
               to HTML content string.
    """
    try:
        _LOCAL_EDITIONS_DIR.mkdir(parents=True, exist_ok=True)
        for filename, content in files.items():
            dest = _LOCAL_EDITIONS_DIR / filename
            dest.write_text(content, encoding="utf-8")
            logger.info("Local edition written: %s", dest)
    except OSError as exc:
        logger.warning("Could not write local editions to %s: %s", _LOCAL_EDITIONS_DIR, exc)


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


def _post_process_html(html: str) -> str:
    """
    Enhance HTML output with citation rendering and Sources appendix styling.

    Component 1 — inline citation markers:
        [Sources: 1, 3] → small grey span linking to #sources anchor

    Component 2 — Sources appendix:
        ## Sources heading → styled with grey top border, id="sources"
        Numbered <li> entries with a trailing URL → description becomes
        a hyperlink; entries without a URL render as plain text.
    """

    # ── Component 1: Inline citation markers ─────────────────────────────────
    def _citation_span(m: re.Match) -> str:
        nums = m.group(1).strip()
        return (
            '<span style="font-size: 0.8em; color: #888; font-style: normal;">'
            f'[<a href="#sources" style="color: #888; text-decoration: none;">'
            f'Sources: {nums}</a>]</span>'
        )

    html = re.sub(r'\[Sources:\s*([\d,\s]+)\]', _citation_span, html)

    # ── Component 2a: Sources heading — add id + border styling ──────────────
    # Handles both <h2>Sources</h2> and <h2 id="sources">...</h2> (toc ext)
    html = re.sub(
        r'<h2[^>]*>\s*Sources\s*</h2>',
        (
            '<h2 id="sources" style="'
            'border-top: 1px solid #ddd; '
            'padding-top: 1em; '
            'margin-top: 2em; '
            'font-size: 1.1em; '
            'color: #555;'
            '">Sources</h2>'
        ),
        html,
        flags=re.IGNORECASE,
    )

    # ── Component 2b: Sources section — linkify entries and wrap ─────────────
    # Everything from the styled Sources heading to end of body is the appendix
    sources_match = re.search(r'<h2 id="sources"', html)
    if sources_match:
        before = html[:sources_match.start()]
        sources_section = html[sources_match.start():]

        def _linkify_source_entry(m: re.Match) -> str:
            content = m.group(1)
            # Look for a trailing URL (after the last " — ")
            url_match = re.search(
                r'\s*[—\-–]\s*(https?://[^\s<]+)\s*$', content
            )
            if not url_match:
                return f'<li>{content}</li>'
            url = url_match.group(1).rstrip('.,;)')
            before_url = content[:url_match.start()].strip()
            # Split description from "publication, date" on the first " — "
            parts = re.split(r'\s+[—\-–]\s+', before_url, maxsplit=1)
            if len(parts) == 2:
                description, pub_date = parts
                return (
                    f'<li>'
                    f'<a href="{url}" style="color: #0f3460;">{description}</a>'
                    f' — {pub_date}'
                    f'</li>'
                )
            # Fallback: link the whole pre-URL content
            return (
                f'<li>'
                f'<a href="{url}" style="color: #0f3460;">{before_url}</a>'
                f'</li>'
            )

        sources_section = re.sub(
            r'<li>(.*?)</li>',
            _linkify_source_entry,
            sources_section,
            flags=re.DOTALL,
        )

        # Wrap sources section in a styled div (smaller font, muted colour)
        sources_section = (
            '<div style="font-size: 0.9em; color: #555;">'
            + sources_section
            + '</div>'
        )

        html = before + sources_section

    return html


def markdown_to_html(md_text: str, title: str = "Legal Tech Intelligence Brief") -> str:
    body = md.markdown(
        md_text,
        extensions=["tables", "fenced_code", "toc", "nl2br"],
    )
    body = _post_process_html(body)
    return _HTML_TEMPLATE.format(title=title, body=body)


def _simplify_html_for_gmail(full_html: str, github_html_url: str) -> str:
    """
    Produce a Gmail-safe simplified HTML version from the full-fidelity HTML.

    Changes vs. full HTML:
    - Heading id= attributes removed (Gmail strips these, breaking anchor nav)
    - Citation spans de-linked: [Sources: X, Y] rendered as plain grey text
      rather than an <a href="#sources"> link (Gmail anchor nav unreliable)
    - id="sources" on the Sources heading removed (covered by heading strip)
    - Source URL hyperlinks in <li> items are kept (work fine in Gmail)
    - A 'View full brief' banner is injected immediately after <body> linking
      to the full-fidelity GitHub HTML file
    """
    html = full_html

    # Strip id= attributes from all heading tags
    html = re.sub(r'(<h[1-6])\s+id="[^"]*"', r'\1', html)

    # De-link citation spans: remove the <a href="#sources"> wrapper,
    # keep the Sources: X, Y text as plain grey span
    html = re.sub(
        r'<span style="font-size: 0\.8em; color: #888; font-style: normal;">'
        r'\[<a href="#sources" style="color: #888; text-decoration: none;">'
        r'(Sources: [\d,\s]+)'
        r'</a>\]</span>',
        r'<span style="font-size: 0.8em; color: #888; font-style: normal;">[\1]</span>',
        html,
    )

    # Inject "View full brief" link immediately after <body>
    view_link = (
        f'\n<p style="font-size: 0.9em; color: #555; border-bottom: 1px solid #eee; '
        f'padding-bottom: 0.75em; margin-bottom: 1.5em;">'
        f'<a href="{github_html_url}" style="color: #0f3460;">'
        f'View full brief with interactive source links \u2192</a></p>\n'
    )
    html = html.replace('<body>\n', f'<body>{view_link}', 1)

    return html


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

def step_fetch_feeds(dry_run: bool, lookback_days: int = 7) -> tuple[dict, dict]:
    """Fetch both feeds from InnoReader and save to GitHub.

    lookback_days: how far back to fetch articles (config.DAILY_LOOKBACK_DAYS
                   for daily runs, config.WEEKLY_LOOKBACK_DAYS for weekly fallback).
    """
    logger.info("=== STEP 1: Fetch feeds from InnoReader (lookback=%dd) ===", lookback_days)
    token = _get_inoreader_token()
    priority_feed, high_signal_feed = inoreader_client.get_both_feeds(token, lookback_days=lookback_days)

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


def step_fetch_prior_ledger() -> str:
    """
    Fetch the most recent daily EventLedger from GitHub to use as
    deduplication context for Stage 1.

    Returns the ledger content as a string, or an empty string if no
    prior ledger exists (e.g. first-ever run).
    """
    logger.info("=== STEP 2b: Fetch prior EventLedger from GitHub (dedup context) ===")
    try:
        entries = gh.list_dir(config.GITHUB_OUTPUT_DIR)
    except Exception as e:
        logger.warning("Could not list GitHub output dir: %s — skipping prior ledger.", e)
        return ""

    # Find all EventLedger files and sort to get most recent
    ledger_pattern = re.compile(r'^(\d{8})_EventLedger\.md$')
    ledger_files = sorted(
        (entry["name"] for entry in entries if ledger_pattern.match(entry.get("name", ""))),
        reverse=True,
    )

    if not ledger_files:
        logger.info("No prior EventLedger found in GitHub — Stage 1 will run without dedup context.")
        return ""

    latest = ledger_files[0]
    path = f"{config.GITHUB_OUTPUT_DIR}/{latest}"
    content, _ = gh.read_file_or_none(path)
    if content:
        logger.info("Prior EventLedger loaded: %s (%d chars)", latest, len(content))
        return content

    logger.info("Could not read prior EventLedger '%s' — proceeding without dedup context.", latest)
    return ""


def step_collect_weekly_ledgers() -> str | None:
    """
    Collect daily EventLedger files from the past 7 days in GitHub output dir.

    Returns the combined ledger content (as a single string) if 3 or more
    daily ledgers are found, otherwise returns None — signalling the weekly
    run to fall back to a fresh Stage 1 with a 7-day feed window.
    """
    logger.info("=== STEP 1 (weekly): Collecting prior daily EventLedgers from GitHub ===")

    today = date.today()
    cutoff = today - timedelta(days=7)

    try:
        entries = gh.list_dir(config.GITHUB_OUTPUT_DIR)
    except Exception as e:
        logger.warning("Could not list GitHub output dir: %s — falling back to fresh Stage 1.", e)
        return None

    ledger_pattern = re.compile(r'^(\d{8})_EventLedger\.md$')
    candidates: list[tuple[str, str]] = []  # (date_str, filename)

    for entry in entries:
        name = entry.get("name", "")
        m = ledger_pattern.match(name)
        if not m:
            continue
        date_str = m.group(1)
        try:
            file_date = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
        except ValueError:
            continue
        # Include ledgers from the past 7 days but exclude today
        if cutoff < file_date < today:
            candidates.append((date_str, name))

    candidates.sort()  # chronological order
    logger.info(
        "Found %d EventLedger file(s) in past 7 days: %s",
        len(candidates), [name for _, name in candidates],
    )

    if len(candidates) < 3:
        logger.info(
            "Fewer than 3 daily EventLedgers found (%d) — "
            "weekly run will fall back to fresh Stage 1 with %d-day feed window.",
            len(candidates), config.WEEKLY_LOOKBACK_DAYS,
        )
        return None

    # Download and combine all found ledgers
    combined_parts: list[str] = []
    for date_str, filename in candidates:
        path = f"{config.GITHUB_OUTPUT_DIR}/{filename}"
        content, _ = gh.read_file_or_none(path)
        if content:
            combined_parts.append(f"--- EventLedger: {filename} ---\n\n{content}")
            logger.info("Loaded EventLedger: %s (%d chars)", filename, len(content))
        else:
            logger.warning("Could not load %s — skipping.", filename)

    if not combined_parts:
        logger.warning("No EventLedger content loaded — falling back to fresh Stage 1.")
        return None

    combined = "\n\n".join(combined_parts)
    logger.info(
        "Combined weekly EventLedger: %d chars from %d file(s)",
        len(combined), len(combined_parts),
    )
    return combined


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


def step_stage1(
    standing_view: str,
    priority_feed: dict,
    high_signal_feed: dict,
    prompt_file: str = "stage1_prompt.txt",
    prior_ledger: str = "",
) -> str:
    """Run Stage 1: Event Ledger generation. Saves result to disk as a cache.

    prompt_file: prompt filename to use (daily_stage1_prompt.txt or stage1_prompt.txt).
    prior_ledger: most recent prior EventLedger content for dedup context.
    """
    logger.info("=== STEP 3: Stage 1 — Event Ledger (prompt=%s) ===", prompt_file)
    trimmed_priority = _trim_feed(priority_feed)
    trimmed_high_signal = _trim_feed(high_signal_feed)
    logger.info(
        "Feeds trimmed for prompt: priority=%d items, high_signal=%d items",
        trimmed_priority["item_count"], trimmed_high_signal["item_count"],
    )
    if prior_ledger:
        logger.info("Prior EventLedger injected as dedup context (%d chars).", len(prior_ledger))

    # Try the specified prompt; fall back to stage1_prompt.txt if not found
    try:
        event_ledger = analysis.run_stage1(
            standing_view, trimmed_priority, trimmed_high_signal,
            prompt_file=prompt_file, prior_ledger=prior_ledger,
        )
    except FileNotFoundError:
        if prompt_file != "stage1_prompt.txt":
            logger.warning("%s not found — falling back to stage1_prompt.txt", prompt_file)
            event_ledger = analysis.run_stage1(
                standing_view, trimmed_priority, trimmed_high_signal,
                prompt_file="stage1_prompt.txt", prior_ledger=prior_ledger,
            )
        else:
            raise

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


def step_stage2_daily(standing_view: str, event_ledger: str) -> analysis.Stage2DailyOutput:
    """Run Stage 2 in daily mode: Daily Brief only (no Standing View update)."""
    logger.info("=== STEP 4: Stage 2 — Daily Brief ===")

    today = date.today()
    fmt = "%B %#d, %Y" if sys.platform == "win32" else "%B %-d, %Y"
    coverage_period = today.strftime(fmt)
    logger.info("Coverage period: %s", coverage_period)

    result = analysis.run_stage2_daily(standing_view, event_ledger, coverage_period=coverage_period)
    logger.info("Daily Brief: %d chars", len(result.daily_brief))
    return result


def _make_stakeholder_brief(brief: str) -> str:
    """
    Strip the Source Quality Feedback section from the brief to produce
    the stakeholder-facing version (Version A).

    Removes from the Source Quality Feedback heading up to (but not
    including) the next top-level heading or end-of-string.  This
    preserves the ## Sources appendix, which appears after Source Quality
    Feedback and must be present in both stakeholder and operator versions.
    """
    stripped = re.sub(
        r'\n#{1,4}\s+(?:\d+\)\s+)?Source Quality Feedback\b.*?(?=\n##|\Z)',
        '',
        brief,
        flags=re.DOTALL | re.IGNORECASE,
    )
    return stripped.rstrip()


def step_save_outputs(
    result: analysis.Stage2Output,
    stakeholder_brief: str,
    dry_run: bool,
) -> tuple[str, str, str, str]:
    """
    Save both brief versions and supporting outputs to GitHub.

    Saves markdown (.md) and full-fidelity HTML (.html) for both versions.
    The HTML files preserve anchor links, styled Sources section, and
    clickable source hyperlinks — intended for browser viewing.

    Returns (operator_md_path, stakeholder_md_path,
             operator_html_path, stakeholder_html_path).
    """
    today = date.today()
    date_str = today.strftime("%Y%m%d")
    commit_date = today.isoformat()

    week_str = (
        today.strftime("%B %#d, %Y")
        if sys.platform == "win32"
        else today.strftime("%B %-d, %Y")
    )
    subject_stakeholder = f"Legal Tech Intelligence Brief — Week of {week_str}"
    subject_operator = f"Legal Tech Intelligence Brief — Week of {week_str} [OPERATOR VERSION]"

    operator_md_path = f"{config.GITHUB_OUTPUT_DIR}/{date_str}_WeeklyBrief_Operator.md"
    stakeholder_md_path = f"{config.GITHUB_OUTPUT_DIR}/{date_str}_WeeklyBrief_Stakeholder.md"
    operator_html_path = f"{config.GITHUB_OUTPUT_DIR}/{date_str}_WeeklyBrief_Operator.html"
    stakeholder_html_path = f"{config.GITHUB_OUTPUT_DIR}/{date_str}_WeeklyBrief_Stakeholder.html"

    html_operator = markdown_to_html(result.weekly_brief, title=subject_operator)
    html_stakeholder = markdown_to_html(stakeholder_brief, title=subject_stakeholder)

    logger.info("=== STEP 5: Save outputs to GitHub ===")
    logger.info(
        "Stakeholder brief: %d chars (Source Quality Feedback stripped from %d chars)",
        len(stakeholder_brief), len(result.weekly_brief),
    )

    if not dry_run:
        gh.upsert_file(
            operator_md_path,
            result.weekly_brief,
            f"feat: add operator brief [{commit_date}]",
        )
        logger.info("Operator brief saved: %s", operator_md_path)

        gh.upsert_file(
            stakeholder_md_path,
            stakeholder_brief,
            f"feat: add stakeholder brief [{commit_date}]",
        )
        logger.info("Stakeholder brief saved: %s", stakeholder_md_path)

        gh.upsert_file(
            operator_html_path,
            html_operator,
            f"feat: add operator HTML brief [{commit_date}]",
        )
        logger.info("Operator HTML brief saved: %s", operator_html_path)

        gh.upsert_file(
            stakeholder_html_path,
            html_stakeholder,
            f"feat: add stakeholder HTML brief [{commit_date}]",
        )
        logger.info("Stakeholder HTML brief saved: %s", stakeholder_html_path)

        _write_local_editions({
            f"{date_str}_WeeklyBrief_Operator.html": html_operator,
            f"{date_str}_WeeklyBrief_Stakeholder.html": html_stakeholder,
        })

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
        for name, html in (
            ("operator", html_operator),
            ("stakeholder", html_stakeholder),
        ):
            preview_path = f"output_preview_{name}.html"
            with open(preview_path, "w", encoding="utf-8") as f:
                f.write(html)
            logger.info("[dry-run] Full HTML preview written to: %s", preview_path)

    return operator_md_path, stakeholder_md_path, operator_html_path, stakeholder_html_path


def step_save_outputs_daily(
    result: analysis.Stage2DailyOutput,
    stakeholder_brief: str,
    event_ledger: str,
    dry_run: bool,
) -> tuple[str, str, str, str]:
    """
    Save daily brief versions and the Event Ledger to GitHub.

    Saves markdown (.md) and full-fidelity HTML (.html) for both versions,
    plus the raw Event Ledger for weekly synthesis use.

    Does NOT update the Standing View — that is weekly-only.

    Returns (operator_md_path, stakeholder_md_path,
             operator_html_path, stakeholder_html_path).
    """
    today = date.today()
    date_str = today.strftime("%Y%m%d")
    commit_date = today.isoformat()

    day_str = (
        today.strftime("%A, %B %#d, %Y")
        if sys.platform == "win32"
        else today.strftime("%A, %B %-d, %Y")
    )
    subject_stakeholder = f"Legal Tech Intelligence — {day_str}"
    subject_operator = f"Legal Tech Intelligence — {day_str} [OPERATOR VERSION]"

    operator_md_path = f"{config.GITHUB_OUTPUT_DIR}/{date_str}_DailyBrief_Operator.md"
    stakeholder_md_path = f"{config.GITHUB_OUTPUT_DIR}/{date_str}_DailyBrief_Stakeholder.md"
    operator_html_path = f"{config.GITHUB_OUTPUT_DIR}/{date_str}_DailyBrief_Operator.html"
    stakeholder_html_path = f"{config.GITHUB_OUTPUT_DIR}/{date_str}_DailyBrief_Stakeholder.html"
    ledger_path = f"{config.GITHUB_OUTPUT_DIR}/{date_str}_EventLedger.md"

    html_operator = markdown_to_html(result.daily_brief, title=subject_operator)
    html_stakeholder = markdown_to_html(stakeholder_brief, title=subject_stakeholder)

    logger.info("=== STEP 5: Save daily outputs to GitHub ===")
    logger.info(
        "Daily Brief stakeholder: %d chars (Source Quality Feedback stripped from %d chars)",
        len(stakeholder_brief), len(result.daily_brief),
    )

    if not dry_run:
        gh.upsert_file(
            operator_md_path,
            result.daily_brief,
            f"feat: add daily brief operator [{commit_date}]",
        )
        logger.info("Operator daily brief saved: %s", operator_md_path)

        gh.upsert_file(
            stakeholder_md_path,
            stakeholder_brief,
            f"feat: add daily brief stakeholder [{commit_date}]",
        )
        logger.info("Stakeholder daily brief saved: %s", stakeholder_md_path)

        gh.upsert_file(
            operator_html_path,
            html_operator,
            f"feat: add daily brief operator HTML [{commit_date}]",
        )
        logger.info("Operator daily HTML brief saved: %s", operator_html_path)

        gh.upsert_file(
            stakeholder_html_path,
            html_stakeholder,
            f"feat: add daily brief stakeholder HTML [{commit_date}]",
        )
        logger.info("Stakeholder daily HTML brief saved: %s", stakeholder_html_path)

        _write_local_editions({
            f"{date_str}_DailyBrief_Operator.html": html_operator,
            f"{date_str}_DailyBrief_Stakeholder.html": html_stakeholder,
        })

        if event_ledger:
            gh.upsert_file(
                ledger_path,
                event_ledger,
                f"chore: add event ledger [{commit_date}]",
            )
            logger.info("Event Ledger saved: %s", ledger_path)
    else:
        logger.info("[dry-run] Skipping GitHub daily output writes.")
        for name, html in (
            ("operator_daily", html_operator),
            ("stakeholder_daily", html_stakeholder),
        ):
            preview_path = f"output_preview_{name}.html"
            with open(preview_path, "w", encoding="utf-8") as f:
                f.write(html)
            logger.info("[dry-run] Daily HTML preview written to: %s", preview_path)

    return operator_md_path, stakeholder_md_path, operator_html_path, stakeholder_html_path


def step_stage3_qa(
    result,
    stakeholder_brief: str,
    dry_run: bool,
    mode: str = "weekly",
) -> analysis.Stage3QAOutput:
    """
    Run Stage 3 QA evaluation against both brief versions:
      - Stakeholder brief → Gates 1-6, 8, 9 (affects overall verdict)
      - Operator brief    → Gate 7 only (reported separately)

    mode: "daily" or "weekly" — controls QA report file naming.
          daily  → YYYYMMDD_DailyBrief_QA_Report.md
          weekly → YYYYMMDD_QA_Report.md  (unchanged, backward compat)
    """
    logger.info("=== STEP 6: Stage 3 — QA Evaluation (mode=%s) ===", mode)
    logger.info("  Stakeholder brief → Gates 1-6, 8, 9")
    logger.info("  Operator brief    → Gate 7 (Source Quality Feedback, separate)")

    qa = analysis.run_stage3_qa(stakeholder_brief, result.weekly_brief)

    # Always print the summary to the console
    print(qa.console_summary())

    # Determine QA report filename based on mode
    date_str = date.today().strftime("%Y%m%d")
    if mode == "daily":
        qa_filename = f"{date_str}_DailyBrief_QA_Report.md"
    else:
        qa_filename = f"{date_str}_QA_Report.md"

    qa_path = f"{config.GITHUB_OUTPUT_DIR}/{qa_filename}"

    # Always save locally for persistent run history
    local_qa_path = Path(qa_filename)
    local_qa_path.write_text(qa.raw, encoding="utf-8")
    logger.info("QA Report written locally: %s", local_qa_path)

    if not dry_run:
        gh.upsert_file(
            qa_path,
            qa.raw,
            f"chore: add QA report [{date.today().isoformat()}]",
        )
        logger.info("QA Report saved to GitHub: %s", qa_path)

    if qa.overall != "APPROVED":
        logger.warning(
            "QA: %d gate(s) failed — brief flagged for revision. "
            "Pipeline will continue and produce Gmail draft. "
            "Review QA report before sending.",
            qa.fail_count,
        )

    return qa


def step_email_draft(
    result,
    stakeholder_brief: str,
    operator_html_path: str,
    stakeholder_html_path: str,
    dry_run: bool,
    mode: str = "weekly",
) -> None:
    """
    Create two Gmail drafts using Gmail-simplified HTML.

    The Gmail HTML is derived from the full-fidelity HTML saved to GitHub,
    with heading anchor IDs removed and citation hrefs de-linked (Gmail
    strips these). A 'View full brief' link at the top points readers to
    the full-fidelity GitHub HTML file with working source navigation.

    mode: "daily" or "weekly" — controls email subject line format.
          daily  → "Legal Tech Intelligence — [Weekday, Month D, YYYY]"
          weekly → "Legal Tech Intelligence Brief — Week of [Month D, YYYY]"
    """
    logger.info("=== STEP 7: Create Gmail drafts (Stakeholder + Operator, mode=%s) ===", mode)

    today = date.today()

    if mode == "daily":
        day_str = (
            today.strftime("%A, %B %#d, %Y")
            if sys.platform == "win32"
            else today.strftime("%A, %B %-d, %Y")
        )
        subject_stakeholder = f"Legal Tech Intelligence — {day_str}"
        subject_operator = f"Legal Tech Intelligence — {day_str} [OPERATOR VERSION]"
    else:
        week_str = (
            today.strftime("%B %#d, %Y")
            if sys.platform == "win32"
            else today.strftime("%B %-d, %Y")
        )
        subject_stakeholder = f"Legal Tech Intelligence Brief — Week of {week_str}"
        subject_operator = f"Legal Tech Intelligence Brief — Week of {week_str} [OPERATOR VERSION]"

    # GitHub blob URLs for the full-fidelity HTML files
    github_base = f"https://github.com/{config.GITHUB_REPO}/blob/main"
    github_url_stakeholder = f"{github_base}/{stakeholder_html_path}"
    github_url_operator = f"{github_base}/{operator_html_path}"

    # Generate full HTML then simplify for Gmail
    full_html_stakeholder = markdown_to_html(stakeholder_brief, title=subject_stakeholder)
    full_html_operator = markdown_to_html(result.weekly_brief, title=subject_operator)

    gmail_html_stakeholder = _simplify_html_for_gmail(full_html_stakeholder, github_url_stakeholder)
    gmail_html_operator = _simplify_html_for_gmail(full_html_operator, github_url_operator)

    if not dry_run:
        draft_a = gmail_client.create_draft(
            subject=subject_stakeholder,
            html_body=gmail_html_stakeholder,
            plain_body=stakeholder_brief,
        )
        logger.info("Stakeholder draft created: id=%s", draft_a)

        draft_b = gmail_client.create_draft(
            subject=subject_operator,
            html_body=gmail_html_operator,
            plain_body=result.weekly_brief,
        )
        logger.info("Operator draft created: id=%s", draft_b)
    else:
        logger.info("[dry-run] Skipping Gmail draft creation.")
        for name, html in (
            ("stakeholder", gmail_html_stakeholder),
            ("operator", gmail_html_operator),
        ):
            preview_path = f"output_preview_{name}_gmail.html"
            with open(preview_path, "w", encoding="utf-8") as f:
                f.write(html)
            logger.info("[dry-run] Gmail HTML preview written to: %s", preview_path)


# ── Entry points ───────────────────────────────────────────────────────────────

def run_daily(dry_run: bool = False, skip_fetch: bool = False, resume: bool = False) -> None:
    """Daily pipeline: fresh signal brief + Event Ledger. No Standing View update."""
    start = datetime.now(tz=timezone.utc)
    logger.info("Legal Tech Intelligence Pipeline — DAILY MODE starting at %s", start.isoformat())
    logger.info("Repo: %s  |  Model: %s  |  dry-run=%s  skip-fetch=%s  resume=%s",
                config.GITHUB_REPO, config.CLAUDE_MODEL, dry_run, skip_fetch, resume)
    logger.info("Feed lookback: %d days", config.DAILY_LOOKBACK_DAYS)

    # 1. Feeds
    if resume:
        priority_feed, high_signal_feed = {}, {}
    elif skip_fetch:
        priority_feed, high_signal_feed = step_load_existing_feeds()
        priority_feed, high_signal_feed = _dedup_feeds(priority_feed, high_signal_feed)
    else:
        priority_feed, high_signal_feed = step_fetch_feeds(dry_run, lookback_days=config.DAILY_LOOKBACK_DAYS)
        priority_feed, high_signal_feed = _dedup_feeds(priority_feed, high_signal_feed)

    # 2. Standing View (read-only — not updated in daily mode)
    standing_view = step_fetch_standing_view()

    # 3. Prior EventLedger for dedup context
    prior_ledger = step_fetch_prior_ledger() if not resume else ""

    # 4. Stage 1 — Event Ledger (or load from cache)
    if resume:
        event_ledger = step_stage1_from_cache()
    else:
        event_ledger = step_stage1(
            standing_view, priority_feed, high_signal_feed,
            prompt_file="daily_stage1_prompt.txt",
            prior_ledger=prior_ledger,
        )

    # 5. Stage 2 — Daily Brief
    result = step_stage2_daily(standing_view, event_ledger)

    # Derive stakeholder brief (Source Quality Feedback stripped)
    stakeholder_brief = _make_stakeholder_brief(result.daily_brief)

    # 6. Save to GitHub (DailyBrief Operator + Stakeholder — markdown + HTML + EventLedger)
    operator_path, stakeholder_path, operator_html_path, stakeholder_html_path = (
        step_save_outputs_daily(result, stakeholder_brief, event_ledger, dry_run)
    )

    # 7. Stage 3 — QA
    step_stage3_qa(result, stakeholder_brief, dry_run, mode="daily")

    # 8. Gmail drafts
    step_email_draft(result, stakeholder_brief, operator_html_path, stakeholder_html_path,
                     dry_run, mode="daily")

    elapsed = (datetime.now(tz=timezone.utc) - start).total_seconds()
    logger.info(
        "Daily pipeline complete in %.1fs. Operator: %s  Stakeholder: %s",
        elapsed, operator_path, stakeholder_path,
    )


def run_weekly(dry_run: bool = False, skip_fetch: bool = False, resume: bool = False) -> None:
    """Weekly synthesis pipeline: synthesise prior week, update Standing View."""
    start = datetime.now(tz=timezone.utc)
    logger.info("Legal Tech Intelligence Pipeline — WEEKLY MODE starting at %s", start.isoformat())
    logger.info("Repo: %s  |  Model: %s  |  dry-run=%s  skip-fetch=%s  resume=%s",
                config.GITHUB_REPO, config.CLAUDE_MODEL, dry_run, skip_fetch, resume)

    if resume:
        # Resume from cached Event Ledger — skip everything up to Stage 2
        standing_view = step_fetch_standing_view()
        event_ledger = step_stage1_from_cache()
    else:
        # 1. Try to collect prior daily EventLedgers (Mon–Fri of prior week)
        combined_ledger = step_collect_weekly_ledgers() if not skip_fetch else None

        # 2. Standing View
        standing_view = step_fetch_standing_view()

        if combined_ledger:
            # Use the aggregated daily EventLedgers — no fresh Stage 1 needed
            event_ledger = combined_ledger
            logger.info("Using %d chars of combined daily EventLedgers for weekly synthesis.", len(event_ledger))
        else:
            # Fall back to fresh Stage 1 with 7-day feed window
            logger.info("Falling back to fresh Stage 1 with %d-day lookback.", config.WEEKLY_LOOKBACK_DAYS)
            if skip_fetch:
                priority_feed, high_signal_feed = step_load_existing_feeds()
            else:
                priority_feed, high_signal_feed = step_fetch_feeds(dry_run, lookback_days=config.WEEKLY_LOOKBACK_DAYS)
            priority_feed, high_signal_feed = _dedup_feeds(priority_feed, high_signal_feed)
            event_ledger = step_stage1(standing_view, priority_feed, high_signal_feed)

    # 3. Stage 2 — Weekly Brief + Updated Standing View + Delta Log
    result = step_stage2(standing_view, event_ledger)

    # Derive stakeholder brief once; reused by save, QA, and email steps
    stakeholder_brief = _make_stakeholder_brief(result.weekly_brief)

    # 4. Save to GitHub (Operator + Stakeholder — markdown + HTML + Standing View update)
    operator_path, stakeholder_path, operator_html_path, stakeholder_html_path = (
        step_save_outputs(result, stakeholder_brief, dry_run)
    )

    # 5. Stage 3 — QA: stakeholder brief (Gates 1-6,8,9) + operator brief (Gate 7)
    step_stage3_qa(result, stakeholder_brief, dry_run, mode="weekly")

    # 6. Gmail drafts (Stakeholder + Operator) — Gmail-simplified HTML
    step_email_draft(result, stakeholder_brief, operator_html_path, stakeholder_html_path,
                     dry_run, mode="weekly")

    elapsed = (datetime.now(tz=timezone.utc) - start).total_seconds()
    logger.info(
        "Weekly pipeline complete in %.1fs. Operator: %s  Stakeholder: %s",
        elapsed, operator_path, stakeholder_path,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Legal Tech Intelligence Digest Pipeline")
    parser.add_argument(
        "--mode",
        choices=["daily", "weekly"],
        default="daily",
        help="Run mode: 'daily' (default) for Mon–Fri fresh signal, 'weekly' for Monday synthesis",
    )
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

    # Append all log output to live_run_log.txt for persistent run history
    log_path = Path(__file__).parent / "live_run_log.txt"
    with open(log_path, "a", encoding="utf-8") as f:
        run_label = (
            f"mode={args.mode}  dry-run={args.dry_run}  "
            f"skip-fetch={args.skip_fetch}  resume={args.resume}"
        )
        f.write(f"\n{'=' * 72}\n")
        f.write(f"RUN: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  {run_label}\n")
        f.write(f"{'=' * 72}\n")
    file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    logging.getLogger().addHandler(file_handler)

    if args.mode == "weekly":
        run_weekly(dry_run=args.dry_run, skip_fetch=args.skip_fetch, resume=args.resume)
    else:
        run_daily(dry_run=args.dry_run, skip_fetch=args.skip_fetch, resume=args.resume)


if __name__ == "__main__":
    main()
