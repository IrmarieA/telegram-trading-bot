"""Prompt templates for the analyst."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

SYSTEM_PROMPT = """You are a professional forex trading analyst and mentor.
Your trader focuses on EUR/USD and XAU/USD (Gold) on the H1 timeframe.
They are intermediate level — they understand support/resistance and trends
but need help filtering setups and managing emotions, especially FOMO.
Always protect capital first. If conditions are unclear, say confidence 2 or below.
Never force a trade. Respond only with valid JSON matching the schema provided."""

USER_PROMPT_JSON_SUFFIX = """Return a JSON object with keys: pair, bias, confidence, summary, avoid_reason, scenarios, risk_warning, one_line_advice.
scenarios is a list of objects each with: label, trigger, entry_zone, stop_loss, target, risk_reward."""


def _format_block(title: str, payload: Any) -> str:
    if payload is None:
        body = "(none / unavailable)"
    elif isinstance(payload, (dict, list)):
        body = json.dumps(payload, indent=2, default=str)
    else:
        body = str(payload)
    return f"{title}\n{body}"


def build_prompt(
    pair: str,
    indicators: Optional[Dict[str, Any]],
    levels: Optional[Dict[str, Any]],
    events: Optional[List[Dict[str, Any]]],
) -> Tuple[str, str]:
    """
    Build (system_prompt, user_prompt) for the analyst LLM.

    ``pair`` is ``EURUSD`` or ``XAUUSD``. ``indicators``, ``levels``, and ``events``
    come from ``compute_indicators``, ``compute_levels``, and
    ``fetch_todays_economic_events`` respectively (any may be ``None`` or empty).
    """

    system_prompt = SYSTEM_PROMPT

    parts = [
        f"Instrument: {pair}",
        "",
        _format_block("Indicators (H1-derived)", indicators),
        "",
        _format_block("Key levels", levels),
        "",
        _format_block(
            "Today's filtered economic events (USD/EUR, high/medium impact)",
            events if events is not None else None,
        ),
        "",
        USER_PROMPT_JSON_SUFFIX,
    ]
    user_prompt = "\n".join(parts)
    return system_prompt, user_prompt
