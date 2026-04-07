"""LLM-backed market analysis orchestration."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Dict, List, Optional, cast

from openai import AsyncOpenAI
from pydantic import ValidationError

from ai.prompts import build_prompt
from config import OPENAI_API_KEY
from models.schemas import MarketBrief

logger = logging.getLogger(__name__)
ALLOWED_BIASES = {"bullish", "bearish", "ranging"}


def _strip_json_fences(raw: str) -> str:
    """Remove optional ``` or ```json wrappers from model output."""

    s = raw.strip()
    if not s.startswith("```"):
        return s
    s = re.sub(r"^```(?:json)?\s*\n?", "", s, count=1)
    s = re.sub(r"\n?```\s*$", "", s)
    return s.strip()


def _normalize_bias(parsed: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize model bias to protect validation and downstream logic."""

    normalized = dict(parsed)
    raw_bias = normalized.get("bias")

    if isinstance(raw_bias, str):
        bias = raw_bias.strip().lower()
    else:
        bias = ""

    if bias == "neutral":
        logger.info("Converted bias from neutral to ranging")
        normalized["bias"] = "ranging"
        return normalized

    if bias in ALLOWED_BIASES:
        # Ensure canonical lowercase value for stable downstream behavior.
        normalized["bias"] = bias
        return normalized

    logger.warning(
        "Unexpected bias value '%s'; defaulting to ranging",
        raw_bias,
    )
    normalized["bias"] = "ranging"
    return normalized


async def analyze_market(
    pair: str,
    indicators: Optional[Dict[str, Any]],
    levels: Optional[Dict[str, Any]],
    events: Optional[List[Dict[str, Any]]],
) -> MarketBrief | None:
    """
    Call OpenAI with the analyst prompts and return a validated ``MarketBrief``.

    Returns ``None`` on API, JSON, or validation failure (errors are logged).
    """

    try:
        if not OPENAI_API_KEY:
            logger.error("OPENAI_API_KEY is not set in environment / config")
            return None

        system_prompt, user_prompt = build_prompt(pair, indicators, levels, events)
        client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            max_tokens=800,
        )

        raw = response.choices[0].message.content
        if raw is None:
            logger.error("OpenAI returned empty message content")
            return None

        cleaned = _strip_json_fences(raw)
        parsed = json.loads(cleaned)
        if not isinstance(parsed, dict):
            logger.error("OpenAI response JSON is not an object")
            return None

        normalized = _normalize_bias(cast(Dict[str, Any], parsed))
        return MarketBrief(**normalized)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse OpenAI JSON: %s", e, exc_info=True)
        return None
    except ValidationError as e:
        logger.error("MarketBrief validation failed: %s", e, exc_info=True)
        return None
    except Exception as e:
        logger.error("analyze_market failed: %s", e, exc_info=True)
        return None


async def analyze_all_pairs(
    indicators_map: Dict[str, Any],
    levels_map: Dict[str, Any],
    events: Optional[List[Dict[str, Any]]],
) -> Dict[str, MarketBrief | None]:
    """
    Run ``analyze_market`` for EURUSD and XAUUSD concurrently.

    ``indicators_map`` / ``levels_map`` are shaped like ``{'EURUSD': {...}, 'XAUUSD': {...}}``.
    """

    eur_task = analyze_market(
        "EURUSD",
        indicators_map.get("EURUSD"),
        levels_map.get("EURUSD"),
        events,
    )
    xau_task = analyze_market(
        "XAUUSD",
        indicators_map.get("XAUUSD"),
        levels_map.get("XAUUSD"),
        events,
    )
    eur, xau = await asyncio.gather(eur_task, xau_task)
    return {"EURUSD": eur, "XAUUSD": xau}
