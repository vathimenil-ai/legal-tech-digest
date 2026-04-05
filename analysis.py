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
import time
from pathlib import Path
from typing import Any

import anthropic

import config

logger = logging.getLogger(__name__)

_client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY, max_retries=5)
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

def _call_claude(system_prompt: str, user_message: str, label: str = "", inter_stage_delay: int = 65) -> str:
    """Send a single-turn message to Claude and return the text response.

    inter_stage_delay: seconds to wait before this call (used between stages to
    avoid hitting the tokens-per-minute rate limit).
    """
    if inter_stage_delay > 0:
        logger.info("Waiting %ds for rate limit window to reset before '%s'…", inter_stage_delay, label or "call")
        time.sleep(inter_stage_delay)
    logger.info("Calling Claude for: %s (model=%s)", label or "analysis", config.CLAUDE_MODEL)
    response = _client.messages.create(
        model=config.CLAUDE_MODEL,
        max_tokens=16000,
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
    prompt_file: str = "stage1_prompt.txt",
    prior_ledger: str = "",
) -> str:
    """
    Generate the Event Ledger from the Standing View + both feeds.
    Returns the Event Ledger as a markdown string.

    prompt_file: prompt filename to load from prompts/. Defaults to stage1_prompt.txt.
                 Pass "daily_stage1_prompt.txt" for daily-specific instructions when
                 that file exists.

    prior_ledger: content of the most recent prior EventLedger, injected as a
                  deduplication context section so Claude avoids re-surfacing
                  developments already captured in the previous run.
    """
    system_prompt = _load_prompt(prompt_file)

    priority_json = json.dumps(priority_feed, indent=2, ensure_ascii=False)
    high_signal_json = json.dumps(high_signal_feed, indent=2, ensure_ascii=False)

    prior_ledger_section = ""
    if prior_ledger:
        prior_ledger_section = f"""---

## Prior Event Ledger (Deduplication Context)

The following developments were captured in the most recent prior run.
Do NOT re-include these as new developments unless something materially
new has occurred since then.

{prior_ledger}

---

"""

    user_message = f"""{prior_ledger_section}## Current Standing View

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
    return _call_claude(system_prompt, user_message, label="Stage 1 — Event Ledger", inter_stage_delay=0)


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


def run_stage2(standing_view: str, event_ledger: str, coverage_period: str = "") -> Stage2Output:
    """
    Generate the Weekly Brief, Updated Standing View, and Delta Log.
    Returns a Stage2Output with all three sections parsed.

    coverage_period: explicit date range string injected into the prompt header,
    e.g. "Week of March 24, 2026 to March 31, 2026". When provided, Claude uses
    this as the authoritative coverage period rather than deriving it from article dates.

    Your stage2_prompt.txt should instruct Claude to wrap each output in:
      <!-- BEGIN WEEKLY_BRIEF --> ... <!-- END WEEKLY_BRIEF -->
      <!-- BEGIN UPDATED_STANDING_VIEW --> ... <!-- END UPDATED_STANDING_VIEW -->
      <!-- BEGIN DELTA_LOG --> ... <!-- END DELTA_LOG -->
    """
    system_prompt = _load_prompt("stage2_prompt.txt")

    coverage_line = f"**Coverage period:** {coverage_period}\n\n---\n\n" if coverage_period else ""

    user_message = f"""{coverage_line}## Current Standing View

{standing_view}

---

## Event Ledger

{event_ledger}
"""
    raw = _call_claude(system_prompt, user_message, label="Stage 2 — Weekly Brief", inter_stage_delay=0)
    return Stage2Output(raw)


# ── Stage 2 Daily: Daily Brief ────────────────────────────────────────────────

class Stage2DailyOutput:
    """Parsed outputs from the Stage 2 Daily Claude response."""

    def __init__(self, raw: str):
        self.raw = raw
        # Try DAILY_BRIEF delimiter first; fall back to WEEKLY_BRIEF for
        # compatibility when daily_stage2_prompt.txt doesn't exist yet and
        # the pipeline falls back to stage2_prompt.txt.
        self.daily_brief = (
            _extract_section(raw, "DAILY_BRIEF")
            or _extract_section(raw, "WEEKLY_BRIEF")
        )

        if not self.daily_brief:
            logger.warning(
                "Stage 2 daily response missing DAILY_BRIEF section. "
                "Check that daily_stage2_prompt.txt instructs Claude to use the "
                "<!-- BEGIN/END DAILY_BRIEF --> delimiter."
            )

    @property
    def weekly_brief(self) -> str:
        """Alias for daily_brief — lets shared pipeline steps (QA, email) work
        with either Stage2Output or Stage2DailyOutput without modification."""
        return self.daily_brief


def run_stage2_daily(
    standing_view: str,
    event_ledger: str,
    coverage_period: str = "",
) -> Stage2DailyOutput:
    """
    Generate the Daily Brief.
    Returns a Stage2DailyOutput with the daily_brief section parsed.

    Uses daily_stage2_prompt.txt if it exists; falls back to stage2_prompt.txt
    until the daily-specific prompt is created in Part 2.

    Your daily_stage2_prompt.txt should instruct Claude to wrap output in:
      <!-- BEGIN DAILY_BRIEF --> ... <!-- END DAILY_BRIEF -->
    """
    # Try daily-specific prompt; fall back to standard stage2 prompt
    try:
        system_prompt = _load_prompt("daily_stage2_prompt.txt")
        logger.info("Using daily_stage2_prompt.txt")
    except FileNotFoundError:
        logger.warning("daily_stage2_prompt.txt not found — falling back to stage2_prompt.txt")
        system_prompt = _load_prompt("stage2_prompt.txt")

    coverage_line = f"**Coverage period:** {coverage_period}\n\n---\n\n" if coverage_period else ""

    user_message = f"""{coverage_line}## Current Standing View

{standing_view}

---

## Event Ledger

{event_ledger}
"""
    raw = _call_claude(system_prompt, user_message, label="Stage 2 — Daily Brief", inter_stage_delay=0)
    return Stage2DailyOutput(raw)


# ── Stage 3: QA Evaluation ─────────────────────────────────────────────────────

class Stage3QAOutput:
    """Parsed output from the Stage 3 QA evaluation."""

    # Gates evaluated against the Stakeholder Brief (affect overall verdict)
    STAKEHOLDER_GATE_NAMES = [
        "Gate 1: Monday Morning Test",
        "Gate 2: Strategic Depth",
        "Gate 3: Market Implications Coverage",
        "Gate 4: Newsletter Test",
        "Gate 5: Watch Next Specificity",
        "Gate 6: Change Discipline",
        "Gate 8: Concision",
        "Gate 9: Recency Discipline",
    ]

    # Gate evaluated against the Operator Brief (reported separately)
    OPERATOR_GATE_NAME = "Gate 7: Source Quality Feedback"

    def __init__(self, raw: str):
        self.raw = raw
        all_verdicts = _parse_qa_verdicts(raw)

        # Separate Gate 7 (operator) from stakeholder gates
        gate7_key = next(
            (k for k in all_verdicts if "7" in k and "source quality" in k.lower()),
            None,
        )
        self.gate7_verdict = all_verdicts.pop(gate7_key, "UNKNOWN") if gate7_key else "UNKNOWN"

        # Remaining verdicts are stakeholder gates only
        self.verdicts = all_verdicts
        self.overall = _parse_overall_verdict(raw)
        self.pass_count = sum(1 for v in self.verdicts.values() if v == "PASS")
        self.fail_count = sum(1 for v in self.verdicts.values() if v == "FAIL")
        self.na_count  = sum(1 for v in self.verdicts.values() if v == "N/A")

    def console_summary(self) -> str:
        """Return a formatted summary string for printing to the console."""
        na_suffix = f"  |  {self.na_count} N/A" if self.na_count else ""
        lines = [
            "",
            "=" * 60,
            f"  QA REPORT — {'APPROVED' if self.overall == 'APPROVED' else 'NEEDS REVISION'}",
            f"  {self.pass_count} PASS  |  {self.fail_count} FAIL{na_suffix}  (Stakeholder Brief)",
            "=" * 60,
        ]
        for gate in self.STAKEHOLDER_GATE_NAMES:
            verdict = self.verdicts.get(gate, "UNKNOWN")
            if verdict == "PASS":
                icon = "[PASS]"
            elif verdict == "FAIL":
                icon = "[FAIL]"
            elif verdict == "N/A":
                icon = "[N/A] "
            else:
                icon = "[?]  "
            lines.append(f"  {icon} {gate}: {verdict}")
        lines.append("-" * 60)
        g7_icon = "[PASS]" if self.gate7_verdict == "PASS" else "[FAIL]" if self.gate7_verdict == "FAIL" else "[?]  "
        lines.append(f"  {g7_icon} Operator QA: {self.OPERATOR_GATE_NAME}: {self.gate7_verdict}")
        lines.append("=" * 60)
        if self.fail_count > 0:
            lines.append("  See QA Report for fix instructions.")
        lines.append("")
        return "\n".join(lines)


def _parse_qa_verdicts(text: str) -> dict[str, str]:
    """Extract PASS/FAIL/N/A verdicts from the QA table.

    Handles three verdict values:
      PASS  — gate evaluated and passed
      FAIL  — gate evaluated and failed
      N/A   — gate skipped (e.g. Gate 3 for daily briefs that have no
               Market Implications section)

    N/A is matched loosely: any cell that starts with "N/A" (with optional
    dash and trailing text) is normalised to "N/A" so Claude's explanatory
    prose (e.g. "N/A — Daily briefs do not include...") doesn't prevent
    the verdict from being captured.
    """
    verdicts: dict[str, str] = {}
    # Match table rows: | Gate N: Name | PASS / FAIL / N/A[...] | ... |
    pattern = r"\|\s*(Gate\s+\d+:[^|]+?)\s*\|\s*\*{0,2}(PASS|FAIL|N/A[^|]*?)\*{0,2}\s*\|"
    for match in re.finditer(pattern, text, re.IGNORECASE):
        gate_name = match.group(1).strip()
        raw_verdict = match.group(2).strip().upper()
        # Normalise any "N/A — ..." variant to plain "N/A"
        verdict = "N/A" if raw_verdict.startswith("N/A") else raw_verdict
        verdicts[gate_name] = verdict
    return verdicts


def _parse_overall_verdict(text: str) -> str:
    """Extract the overall APPROVED / NEEDS REVISION verdict."""
    match = re.search(r"###\s*Overall Verdict\s*\n+\s*\*{0,2}(APPROVED|NEEDS REVISION)\*{0,2}", text, re.IGNORECASE)
    if match:
        return match.group(1).strip().upper()
    # Fallback: scan for standalone verdict
    if re.search(r"\bAPPROVED\b", text):
        return "APPROVED"
    return "NEEDS REVISION"


def run_stage3_qa(stakeholder_brief: str, operator_brief: str) -> Stage3QAOutput:
    """
    Run QA evaluation against two brief versions:
      - Gates 1-6, 8, 9 evaluate the Stakeholder Brief (affects overall verdict)
      - Gate 7 evaluates the Operator Brief (reported separately, does not affect verdict)

    Returns a Stage3QAOutput with split verdicts and console summary.
    """
    system_prompt = _load_prompt("stage3_qa_prompt.txt")
    user_message = f"""## STAKEHOLDER BRIEF
(Use for Gates 1, 2, 3, 4, 5, 6, 8, 9)

{stakeholder_brief}

---

## OPERATOR BRIEF
(Use for Gate 7: Source Quality Feedback only)

{operator_brief}
"""
    raw = _call_claude(system_prompt, user_message, label="Stage 3 — QA Evaluation", inter_stage_delay=0)
    return Stage3QAOutput(raw)
