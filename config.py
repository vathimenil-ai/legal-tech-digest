"""
config.py — centralised environment variable loading and validation.
All other modules import from here rather than reading os.environ directly.
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / "legaltech_weeklydigest.env", override=True)


def _require(key: str) -> str:
    val = os.getenv(key, "").strip()
    if not val:
        print(f"[config] ERROR: Required environment variable '{key}' is not set.", file=sys.stderr)
        sys.exit(1)
    return val


def _optional(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


# ── Anthropic ──────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY: str = _require("ANTHROPIC_API_KEY")

# ── InnoReader ─────────────────────────────────────────────────────────────────
INOREADER_APP_ID: str = _require("INOREADER_APP_ID")
INOREADER_APP_KEY: str = _require("INOREADER_APP_KEY")
INOREADER_TOKEN: str = _optional("INOREADER_TOKEN")           # from ClientLogin
INOREADER_USERNAME: str = _optional("INOREADER_USERNAME")     # fallback auth
INOREADER_PASSWORD: str = _optional("INOREADER_PASSWORD")     # fallback auth

# Label names exactly as they appear in InnoReader
INOREADER_LABEL_PRIORITY: str = _optional("INOREADER_LABEL_PRIORITY", "Priority")
INOREADER_LABEL_HIGH_SIGNAL: str = _optional("INOREADER_LABEL_HIGH_SIGNAL", "High-Signal")

# ── GitHub ─────────────────────────────────────────────────────────────────────
GITHUB_TOKEN: str = _require("GITHUB_TOKEN")
GITHUB_REPO: str = _require("GITHUB_REPO")          # "owner/repo"

# Paths within the repo
GITHUB_PATH_PRIORITY: str = "feeds/priority.json"
GITHUB_PATH_HIGH_SIGNAL: str = "feeds/high_signal.json"
GITHUB_PATH_STANDING_VIEW: str = "standing-view/standing_view.md"
GITHUB_OUTPUT_DIR: str = "output"

# ── Gmail ──────────────────────────────────────────────────────────────────────
GMAIL_USER: str = _optional("GMAIL_USER", "vathimenil@gmail.com")
GMAIL_CLIENT_ID: str = _optional("GMAIL_CLIENT_ID")
GMAIL_CLIENT_SECRET: str = _optional("GMAIL_CLIENT_SECRET")
GMAIL_REFRESH_TOKEN: str = _optional("GMAIL_REFRESH_TOKEN")

# ── Claude model to use for analysis ──────────────────────────────────────────
CLAUDE_MODEL: str = _optional("CLAUDE_MODEL", "claude-opus-4-6")

# ── Deduplication ──────────────────────────────────────────────────────────────
# Title similarity threshold for cross-feed deduplication (0.0–1.0).
# 0.80 means 80% similar titles are treated as the same development.
# Raise this if distinct articles are being incorrectly collapsed.
# Set DEDUP_ENABLED=False to bypass deduplication entirely for debugging.
DEDUP_TITLE_SIMILARITY_THRESHOLD: float = float(_optional("DEDUP_TITLE_SIMILARITY_THRESHOLD", "0.80"))
DEDUP_ENABLED: bool = _optional("DEDUP_ENABLED", "true").lower() not in ("false", "0", "no")
