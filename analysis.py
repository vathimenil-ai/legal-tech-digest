"""
analysis.py — Two-stage Claude API analysis pipeline.

Stage 1: Event Ledger Generation
  Inputs : Standing View + priority feed + high_signal feed
  Output : Event Ledger (structured analysis of new events/trends)

Stage 2: Weekly Brief + Standing View Update + Delta Log
  Inputs : Standing View + Event Ledger
  Outputs: Updated Standing View, Weekly Brief, Delta Log

Prompts are loaded from:
  prompts/stage1_prompt.txt
  prompts/stage2_prompt.txt
Replace the placeholder text in those files with your actual prompts.
"""
import json
import logging
import re
from pathlib import Path
from typing import Any

import anthropic

import config

logger = logging.getLogger(__name__)

_client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
_PROMPTS_DIR = Path(__file__).parent / "prompts"


# ── Prompt loading ─────────────────────────────────────────────────────────────

def _load_prompt(filename: str) -> str:
    path = _PROMPTS_DIR / filename
    if not path.exists():
        raise FileNotFoundError(
            f"Prompt file not found: {path}\n"
            "Please add your prompt text to the prompts/ directory."
        )
    text = path.read_text(encoding="utf-8").strip()
    if text.startswith("PLACEHOLDER"):
        raise ValueError(
            f"Prompt file '{filename}' still contains placeholder text. "
            "Please replace it with your actual prompt before running the pipeline."
        )
    return text


# ── Claude helper ──────────────────────────────────────────────────────────────

def _call_claude(system_prompt: str, user_message: str, label: str = "") -> str:
    """Send a single-turn message to Claude and return the text response."""
    logger.info("Calling Claude for: %s (model=%s)", label or "analysis", config.CLAUDE_MODEL)
    response = _client.messages.create(
        model=config.CLAUDE_MODEL,
        max_tokens=8096,
        system=system_prompt,
        messages=[{"role": "user", "content": user_message}],
    )
    text = response.content[0].text
    logger.info("Claude response: %d chars for '%s'", len(text), label or "analysis")
    return text


# ── Stage 1: Event Ledger ──────────────────────────────────────────────────────

def run_stage1(
    standing_view: str,
    priority_feed: dict[str, Any],
    high_signal_feed: dict[str, Any],
) -> str:
    """
    Generate the Event Ledger from the Standing View + both feeds.
    Returns the Event Ledger as a markdown string.
    """
    system_prompt = _load_prompt("stage1_prompt.txt")

    priority_json = json.dumps(priority_feed, indent=2, ensure_ascii=False)
    high_signal_json = json.dumps(high_signal_feed, indent=2, ensure_ascii=False)

    user_message = f"""## Current Standing View

{standing_view}

---

## Priority Feed (JSON)

```json
{priority_json}
```

---

## High-Signal Feed (JSON)

```json
{high_signal_json}
```
"""
    return _call_claude(system_prompt, user_message, label="Stage 1 — Event Ledger")


# ── Stage 2: Weekly Brief + Updated Standing View + Delta Log ──────────────────

class Stage2Output:
    """Parsed outputs from the Stage 2 Claude response."""

    def __init__(self, raw: str):
        self.raw = raw
        self.weekly_brief = _extract_section(raw, "WEEKLY_BRIEF")
        self.updated_standing_view = _extract_section(raw, "UPDATED_STANDING_VIEW")
        self.delta_log = _extract_section(raw, "DELTA_LOG")

        # Validate all three sections were returned
        missing = [
            name
            for name, val in [
                ("WEEKLY_BRIEF", self.weekly_brief),
                ("UPDATED_STANDING_VIEW", self.updated_standing_view),
                ("DELTA_LOG", self.delta_log),
            ]
            if not val
        ]
        if missing:
            logger.warning(
                "Stage 2 response missing sections: %s. "
                "Check that your stage2_prompt.txt instructs Claude to use the "
                "<!-- BEGIN/END SECTION --> delimiters.",
                missing,
            )


def _extract_section(text: str, section_name: str) -> str:
    """
    Extract a named section delimited by:
        <!-- BEGIN SECTION_NAME -->
        ...content...
        <!-- END SECTION_NAME -->
    Falls back to returning the full text if no delimiters are found.
    """
    pattern = rf"<!--\s*BEGIN\s+{re.escape(section_name)}\s*-->(.*?)<!--\s*END\s+{re.escape(section_name)}\s*-->"
    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return ""


def run_stage2(standing_view: str, event_ledger: str) -> Stage2Output:
    """
    Generate the Weekly Brief, Updated Standing View, and Delta Log.
    Returns a Stage2Output with all three sections parsed.

    Your stage2_prompt.txt should instruct Claude to wrap each output in:
      <!-- BEGIN WEEKLY_BRIEF --> ... <!-- END WEEKLY_BRIEF -->
      <!-- BEGIN UPDATED_STANDING_VIEW --> ... <!-- END UPDATED_STANDING_VIEW -->
      <!-- BEGIN DELTA_LOG --> ... <!-- END DELTA_LOG -->
    """
    system_prompt = _load_prompt("stage2_prompt.txt")

    user_message = f"""## Current Standing View

{standing_view}

---

## Event Ledger

{event_ledger}
"""
    raw = _call_claude(system_prompt, user_message, label="Stage 2 — Weekly Brief")
    return Stage2Output(raw)
