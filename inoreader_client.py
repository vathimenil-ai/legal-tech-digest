"""
inoreader_client.py — Fetch articles from InnoReader and transform to pipeline schema.

InnoReader API docs: https://www.inoreader.com/developers
Stream contents endpoint: GET /reader/api/0/stream/contents/{streamId}
Label stream ID format:   user/-/label/{label_name}
"""
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

import config

logger = logging.getLogger(__name__)

BASE_URL = "https://www.inoreader.com"
CLIENT_LOGIN_URL = f"{BASE_URL}/accounts/ClientLogin"
STREAM_URL = f"{BASE_URL}/reader/api/0/stream/contents"


# ── Token management ──────────────────────────────────────────────────────────

def get_token_via_clientlogin(username: str, password: str) -> str:
    """Authenticate via ClientLogin and return the Auth token."""
    resp = requests.post(
        CLIENT_LOGIN_URL,
        data={"Email": username, "Passwd": password},
        headers={"AppId": config.INOREADER_APP_ID, "AppKey": config.INOREADER_APP_KEY},
        timeout=30,
    )
    resp.raise_for_status()
    for line in resp.text.splitlines():
        if line.startswith("Auth="):
            return line.split("=", 1)[1].strip()
    raise ValueError(f"Auth token not found in ClientLogin response: {resp.text}")


def _auth_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"GoogleLogin auth={token}",
        "AppId": config.INOREADER_APP_ID,
        "AppKey": config.INOREADER_APP_KEY,
    }


# ── Fetch articles ─────────────────────────────────────────────────────────────

def fetch_label(
    label: str,
    access_token: str,
    max_items: int = 100,
    ot: int | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch all articles for an InnoReader label, handling pagination automatically.

    ot: optional Unix timestamp for the Layer 1 API-level date filter.
        InnoReader will only return articles crawled after this timestamp.

    Returns raw InnoReader item dicts.
    """
    stream_id = f"user/-/label/{label}"
    url = f"{STREAM_URL}/{requests.utils.quote(stream_id, safe='')}"
    headers = _auth_headers(access_token)

    items: list[dict[str, Any]] = []
    continuation: str | None = None

    while len(items) < max_items:
        params: dict[str, Any] = {
            "n": min(50, max_items - len(items)),  # page size (max 50 per InnoReader)
            "output": "json",
        }
        if ot is not None:
            params["ot"] = ot
        if continuation:
            params["c"] = continuation

        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 401:
            raise PermissionError("InnoReader token expired or invalid. Run setup_inoreader.py.")
        resp.raise_for_status()

        data = resp.json()
        batch = data.get("items", [])
        items.extend(batch)
        logger.debug("Fetched %d items (total %d) for label '%s'", len(batch), len(items), label)

        continuation = data.get("continuation")
        if not continuation or not batch:
            break

    return items


def _filter_by_published_date(
    items: list[dict[str, Any]],
    cutoff_dt: datetime,
    label: str,
) -> list[dict[str, Any]]:
    """
    Layer 2 pipeline-level date filter: discard articles published before cutoff_dt.

    For each article:
    - Uses the `published` Unix timestamp as the primary date signal.
    - Falls back to `crawlTimeMsec` (ms → s) if `published` is missing or zero.
    - If neither is available, keeps the article (do not discard on missing data).

    Logs a per-article line for every discarded item so the operator can see
    exactly what was filtered and why.
    """
    kept: list[dict[str, Any]] = []
    discarded: list[tuple[str, str]] = []

    for item in items:
        published_ts = item.get("published") or 0

        if not published_ts:
            # Fall back to crawlTimeMsec (string or int, in milliseconds)
            crawl_ms = item.get("crawlTimeMsec", 0)
            if crawl_ms:
                published_ts = int(str(crawl_ms)) // 1000

        if not published_ts:
            # No date at all — keep to avoid silently discarding valid content
            kept.append(item)
            continue

        pub_dt = datetime.fromtimestamp(published_ts, tz=timezone.utc)
        if pub_dt >= cutoff_dt:
            kept.append(item)
        else:
            raw_title = item.get("title", "")
            title = raw_title.get("content", "") if isinstance(raw_title, dict) else str(raw_title)
            discarded.append((title[:100], pub_dt.strftime("%Y-%m-%d")))

    # Summary log
    logger.info(
        "Date filter [%s]: %d fetched from API → %d kept, %d discarded (7-day cutoff: %s)",
        label, len(items), len(kept), len(discarded), cutoff_dt.date().isoformat(),
    )
    # Per-article discard log
    for title, pub_date in discarded:
        logger.info("  DISCARDED: [%s] '%s'", pub_date, title)


# ── Schema transformation ──────────────────────────────────────────────────────

def _extract_text(field: Any) -> str:
    """Extract string content from an InnoReader content dict or raw string."""
    if isinstance(field, dict):
        return field.get("content", "")
    return str(field) if field else ""


def _to_iso(ts: int | None) -> str:
    """Convert a Unix timestamp to ISO-8601 UTC string."""
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def transform_items(raw_items: list[dict[str, Any]], feed_type: str) -> dict[str, Any]:
    """
    Transform raw InnoReader items into the pipeline JSON schema.

    Output schema:
    {
        "feed_type": "priority" | "high_signal",
        "fetched_at": "<ISO-8601>",
        "item_count": N,
        "items": [
            {
                "id": str,
                "title": str,
                "url": str,
                "author": str,
                "source": str,           # feed/publication name
                "published_at": str,     # ISO-8601
                "updated_at": str,       # ISO-8601
                "summary": str,          # plain-text excerpt (HTML stripped)
                "content": str,          # full HTML content if available
                "categories": [str],     # InnoReader tags/labels
            }
        ]
    }
    """
    processed = []
    for item in raw_items:
        canonical = item.get("canonical", [])
        url = canonical[0].get("href", "") if canonical else ""

        origin = item.get("origin", {})
        source = origin.get("title", "")

        summary_html = _extract_text(item.get("summary", ""))
        content_html = _extract_text(item.get("content", ""))

        categories = [
            c.split("/")[-1]  # strip InnoReader prefix like "user/-/label/"
            for c in item.get("categories", [])
            if isinstance(c, str)
        ]

        processed.append({
            "id": item.get("id", ""),
            "title": _extract_text(item.get("title", "")),
            "url": url,
            "author": item.get("author", ""),
            "source": source,
            "published_at": _to_iso(item.get("published")),
            "updated_at": _to_iso(item.get("updated")),
            "summary": summary_html,
            "content": content_html,
            "categories": categories,
        })

    return {
        "feed_type": feed_type,
        "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
        "item_count": len(processed),
        "items": processed,
    }


# ── Public API ─────────────────────────────────────────────────────────────────

def fetch_and_transform(label: str, feed_type: str, access_token: str) -> dict[str, Any]:
    """
    Fetch articles for a label and return the transformed pipeline schema dict.

    Applies a two-layer date filter:
      Layer 1 (API): `ot` param limits InnoReader to articles crawled in the
                     last 10 days, reducing payload before any data is transferred.
      Layer 2 (pipeline): discard any article whose publication date (or crawl
                          date as fallback) is older than 7 days from run date.
    """
    now = datetime.now(tz=timezone.utc)

    # Layer 1: API-level crawl-date filter (10-day window)
    ot = int((now - timedelta(days=10)).timestamp())

    # Layer 2: pipeline-level publication-date cutoff (7-day window)
    cutoff_7d = now - timedelta(days=7)

    logger.info("Fetching InnoReader label '%s' (feed_type=%s)…", label, feed_type)
    raw = fetch_label(label, access_token, ot=ot)
    logger.info("Fetched %d raw items for '%s' (API filter: last 10 days).", len(raw), label)

    filtered = _filter_by_published_date(raw, cutoff_7d, label)
    return transform_items(filtered, feed_type)


def get_both_feeds(access_token: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Fetch and transform both priority and high_signal feeds.
    Returns (priority_feed, high_signal_feed).
    """
    priority = fetch_and_transform(
        config.INOREADER_LABEL_PRIORITY,
        "priority",
        access_token,
    )
    high_signal = fetch_and_transform(
        config.INOREADER_LABEL_HIGH_SIGNAL,
        "high_signal",
        access_token,
    )
    return priority, high_signal


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    token = config.INOREADER_ACCESS_TOKEN
    if not token:
        print("INOREADER_ACCESS_TOKEN not set. Run setup_inoreader.py first.")
    else:
        p, h = get_both_feeds(token)
        print(f"Priority: {p['item_count']} items")
        print(f"High signal: {h['item_count']} items")
        print(json.dumps(p["items"][0] if p["items"] else {}, indent=2))
