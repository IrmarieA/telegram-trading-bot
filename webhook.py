"""Flask webhook receiver for external trade alerts → Telegram."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import re
import threading
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from aiogram import Bot
from flask import Flask, jsonify, request

logger = logging.getLogger(__name__)

app = Flask(__name__)

BOT: Optional[Bot] = None
CHAT_ID: Optional[str | int] = None
EVENT_LOOP: Optional[asyncio.AbstractEventLoop] = None

# Initial send + 2 retries; delay between attempts (seconds).
_MAX_SEND_ATTEMPTS = 3
_RETRY_DELAY_SEC = 1.5

# Duplicate alert suppression (same content within window); cache TTL for cleanup.
_DEDUP_WINDOW_SEC = 60
_CACHE_TTL_SEC = 120
_dedup_lock = threading.Lock()
_dedup_cache: dict[str, float] = {}  # sha256 key -> last accepted unix time

# Dedup price canonicalization: whole → 2 decimals; fractional → up to 5 decimals, strip trailing zeros.
_D_QUANT_FRAC = Decimal("0.00001")
_D_QUANT_WHOLE = Decimal("0.01")

# Characters allowed in a Decimal literal after stripping noise (digits, sign, dot, scientific notation).
_DEDUP_DECIMAL_CHARS = frozenset("0123456789.+-eE")


def _sanitize_price_string(s: str) -> str:
    """Strip, remove commas/currency/symbols, keep only valid Decimal characters."""
    t = s.strip()
    t = t.replace(",", "").replace("\u00a0", "")
    for sym in ("$", "€", "£", "¥"):
        t = t.replace(sym, "")
    t = re.sub(r"\s+", "", t)
    return "".join(c for c in t if c in _DEDUP_DECIMAL_CHARS)


def _norm_dedup_field(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _norm_dedup_price_field(value: Any) -> str:
    """
    Normalize entry/sl/tp for dedup: whole numbers as two decimals; fractional values
    aligned to up to five decimal places (Decimal), then strip trailing zeros so
    equivalent forms (1.0823 vs 1.08230) match. If parsing fails, return original string.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).strip().lower()

    raw: str
    fallback_stripped: str | None = None
    if isinstance(value, int):
        raw = str(value)
    elif isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return str(value).strip()
        raw = format(value, ".15g")
    elif isinstance(value, str):
        fallback_stripped = value.strip()
        if not fallback_stripped:
            return ""
        sanitized = _sanitize_price_string(fallback_stripped)
        raw = sanitized if sanitized else fallback_stripped
    else:
        raw = str(value).strip()
        if not raw:
            return ""

    try:
        d = Decimal(raw)
    except InvalidOperation:
        if fallback_stripped is not None:
            return fallback_stripped
        return raw

    # Scientific inputs (e.g. "1e-3"): collapse to fixed-point Decimal before quantizing.
    if "e" in raw.lower():
        try:
            d = Decimal(format(d, "f"))
        except InvalidOperation:
            if fallback_stripped is not None:
                return fallback_stripped
            return raw

    if d.is_nan() or d.is_infinite():
        return fallback_stripped if fallback_stripped is not None else raw

    try:
        if d == d.to_integral_value():
            return format(d.quantize(_D_QUANT_WHOLE), "f")
    except InvalidOperation:
        return fallback_stripped if fallback_stripped is not None else raw

    try:
        q = d.quantize(_D_QUANT_FRAC)
    except InvalidOperation:
        return fallback_stripped if fallback_stripped is not None else raw

    s_out = format(q, "f")
    if "." in s_out:
        s_out = s_out.rstrip("0").rstrip(".")
    return s_out or "0"


def _dedup_key(payload: dict[str, Any]) -> str:
    """Stable hash over alert fields (excludes timestamps so identical payloads dedupe)."""
    parts = (
        _norm_dedup_field(payload.get("pair")),
        _norm_dedup_field(payload.get("direction")),
        _norm_dedup_price_field(payload.get("entry")),
        _norm_dedup_price_field(payload.get("sl")),
        _norm_dedup_price_field(payload.get("tp")),
        _norm_dedup_field(payload.get("reason")),
    )
    canonical = "|".join(parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _clean_dedup_cache_unlocked() -> None:
    now = time.time()
    cutoff = now - _CACHE_TTL_SEC
    stale = [k for k, ts in _dedup_cache.items() if ts < cutoff]
    for k in stale:
        del _dedup_cache[k]


def _dedup_should_skip(payload: dict[str, Any]) -> bool:
    """
    If this payload matches a recent alert, return True (caller skips Telegram).
    Otherwise record the key and return False. Must be fast; runs under lock only.
    """
    key = _dedup_key(payload)
    with _dedup_lock:
        _clean_dedup_cache_unlocked()
        now = time.time()
        last = _dedup_cache.get(key)
        if last is not None and (now - last) < _DEDUP_WINDOW_SEC:
            return True
        _dedup_cache[key] = now
        return False


def _safe_str(value: Any, default: str = "—") -> str:
    if value is None:
        return default
    s = str(value).strip()
    return s if s else default


def _format_direction(direction: Any) -> str:
    raw = _safe_str(direction, "—")
    if raw == "—":
        return raw
    upper = raw.upper()
    if upper == "BUY":
        return "🟢 BUY"
    if upper == "SELL":
        return "🔴 SELL"
    return raw


def _build_message(data: dict[str, Any]) -> str:
    pair = _safe_str(data.get("pair"))
    direction = _format_direction(data.get("direction"))
    entry = _safe_str(data.get("entry"))
    sl = _safe_str(data.get("sl"))
    tp = _safe_str(data.get("tp"))
    reason = _safe_str(data.get("reason"))

    return (
        f"🚨 {pair} {direction}\n\n"
        f"Entry: {entry}\n"
        f"Stop Loss: {sl}\n"
        f"Take Profit: {tp}\n\n"
        f"🧠 Reason: {reason}"
    )


async def _send_message_async(message: str) -> None:
    if BOT is None or CHAT_ID is None:
        return
    chat = int(CHAT_ID) if not isinstance(CHAT_ID, int) else CHAT_ID
    await BOT.send_message(chat, message)


async def _send_telegram_with_retries(message: str) -> None:
    """Send with up to 2 additional attempts after failures; delay between attempts."""
    for attempt in range(1, _MAX_SEND_ATTEMPTS + 1):
        logger.info("Telegram send attempt %s/%s", attempt, _MAX_SEND_ATTEMPTS)
        try:
            await _send_message_async(message)
            if attempt > 1:
                logger.info(
                    "Telegram send succeeded on attempt %s/%s",
                    attempt,
                    _MAX_SEND_ATTEMPTS,
                )
            return
        except Exception as exc:
            logger.warning(
                "Telegram send attempt %s/%s failed: %s",
                attempt,
                _MAX_SEND_ATTEMPTS,
                exc,
            )
            if attempt < _MAX_SEND_ATTEMPTS:
                logger.info(
                    "Retrying Telegram send in %.1f seconds (attempt %s → %s)",
                    _RETRY_DELAY_SEC,
                    attempt,
                    attempt + 1,
                )
                await asyncio.sleep(_RETRY_DELAY_SEC)
    logger.error(
        "All %s Telegram send attempts failed for webhook message; giving up",
        _MAX_SEND_ATTEMPTS,
    )


def _schedule_telegram_send(message: str) -> None:
    """
    Schedule Telegram send on the bot's main asyncio loop (set from main.py as EVENT_LOOP).
    Flask runs in worker threads; enqueue work onto the loop thread with call_soon_threadsafe,
    then create_task there so all awaits run on that loop.
    """
    if BOT is None or CHAT_ID is None or EVENT_LOOP is None:
        logger.warning(
            "Webhook received but BOT, CHAT_ID, or EVENT_LOOP is not configured; skipping send"
        )
        return

    def _enqueue_on_loop() -> None:
        task = asyncio.create_task(_send_telegram_with_retries(message))

        def _on_done(t: asyncio.Task[None]) -> None:
            try:
                t.result()
            except Exception:
                logger.exception("Telegram send task failed on event loop")

        task.add_done_callback(_on_done)

    try:
        EVENT_LOOP.call_soon_threadsafe(_enqueue_on_loop)
    except Exception:
        logger.exception("Failed to schedule Telegram send on event loop")


@app.route("/webhook", methods=["POST"])
def webhook() -> tuple[Any, int]:
    try:
        payload = request.get_json(silent=True)
        if payload is None:
            if request.data:
                logger.warning("Invalid JSON payload received; continuing with empty payload")
            payload = {}

        if not isinstance(payload, dict):
            payload = {}

        if _dedup_should_skip(payload):
            logger.info(
                "Duplicate webhook alert skipped (same pair/direction/entry/sl/tp/reason within %s s)",
                _DEDUP_WINDOW_SEC,
            )
            return jsonify({"status": "ok"}), 200

        message = _build_message(payload)

        if BOT is not None and CHAT_ID is not None:
            try:
                _schedule_telegram_send(message)
            except Exception:
                logger.exception("Failed to queue Telegram send from webhook")
        else:
            logger.warning("Webhook received but BOT or CHAT_ID is not configured; skipping send")

        return jsonify({"status": "ok"}), 200
    except Exception:
        logger.exception("Webhook handler failed")
        return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=5000, debug=False)
