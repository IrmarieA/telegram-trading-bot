"""Telegram command and message handlers (aiogram)."""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from aiogram import F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from ai.analyst import analyze_all_pairs, analyze_market
from keyboards import TRADE_REASON_LABELS, main_reply_keyboard, reason_keyboard
from market.calendar import fetch_todays_economic_events
from market.data_fetcher import fetch_eurusd_x_and_gc_ohlcv
from market.indicators import compute_indicators
from market.levels import compute_levels
from models.schemas import MarketBrief
from storage.db import (
    close_trade,
    get_edge_report,
    get_open_trades,
    get_stats,
    get_user_setting,
    save_trade,
    save_user_setting,
)

logger = logging.getLogger(__name__)

router = Router()

FRIENDLY_DATA_ERROR = (
    "Having trouble getting market data right now — try again in a few minutes 🙏"
)

NY_TZ = ZoneInfo("America/New_York")

BUTTON_TODAY = "📊 Today's Brief"
BUTTON_LEVELS = "📍 Key Levels"
BUTTON_SHOULD_TRADE = "🎯 Should I Trade?"
BUTTON_NEWS_TODAY = "📰 News Today"
BUTTON_LOG_TRADE = "📓 Log Trade"
BUTTON_MY_STATS = "📈 My Stats"
BUTTON_CLOSE_TRADE = "🔒 Close Trade"
BUTTON_HELP = "❓ Help"


def _edge_wr_emoji(wr: float) -> str:
    if wr > 60.0:
        return "✅"
    if wr >= 40.0:
        return "⚠️"
    return "❌"


def _edge_insight_bullets(report_items: List[tuple[str, Dict[str, Any]]]) -> List[str]:
    bullets: List[str] = []
    nonempty = [(r, m) for r, m in report_items if m.get("trade_count", 0) > 0]
    if not nonempty:
        return bullets
    high = [(r, m) for r, m in nonempty if float(m.get("win_rate", 0)) > 65.0]
    if high:
        best_r, _best_m = max(high, key=lambda x: float(x[1].get("win_rate", 0)))
        bullets.append(f"Your edge is in {best_r}. Keep focusing there.")
    lows = [
        (r, m)
        for r, m in nonempty
        if float(m.get("win_rate", 0)) < 35.0 and int(m.get("trade_count", 0)) > 2
    ]
    for r, _m in lows:
        bullets.append(f"Consider avoiding {r} — it is costing you pips.")
    if not bullets and all(40.0 <= float(m.get("win_rate", 0)) <= 60.0 for _, m in nonempty):
        bullets.append("Your edge is still forming — keep logging every trade.")
    return bullets


def _display_pair(pair: str) -> str:
    if pair == "EURUSD":
        return "EUR/USD"
    if pair == "XAUUSD":
        return "Gold (XAU/USD)"
    return pair


def _pair_short(pair: str) -> str:
    if pair == "EURUSD":
        return "EUR/USD"
    if pair == "XAUUSD":
        return "Gold"
    return pair


def _bias_label(bias: str) -> str:
    return bias.upper()


def _format_price_or_none(x: Any) -> str:
    if x is None:
        return "—"
    try:
        return str(x)
    except Exception:
        return "—"


def _format_brief_message(pair: str, brief: MarketBrief) -> str:
    """
    Format the LLM MarketBrief into the app's trader-facing "FOREX INTEL" layout.
    """

    # Use the first scenario as the active plan.
    scenario = brief.scenarios[0] if brief.scenarios else None
    entry_zone = getattr(scenario, "entry_zone", "—") if scenario else "—"
    stop_loss = getattr(scenario, "stop_loss", "—") if scenario else "—"
    target = getattr(scenario, "target", "—") if scenario else "—"

    # Spec says "invalidation from scenario trigger".
    invalidation = getattr(scenario, "trigger", "—") if scenario else "—"

    lines: List[str] = [
        f"🧠 FOREX INTEL — {_pair_short(pair)}",
        "",
        f"📊 BIAS: {_bias_label(brief.bias)} ({brief.confidence}/5 confidence)",
        "",
        f"💰 ENTRY ZONE: {entry_zone}",
        f"🎯 TARGET: {target}",
        f"🛑 STOP LOSS: {stop_loss}",
        f"⚠️ INVALIDATION: {invalidation}",
        "",
        f"📝 {brief.summary}",
        "",
        "💡 TRADE PLAN:",
        "- Take partial profits at first target",
        "- Move stop to breakeven once +20 pips in profit",
        "- Exit immediately if invalidation level breaches",
    ]

    if brief.risk_warning:
        lines.extend(["", f"🔕 {brief.risk_warning}"])

    return "\n".join(lines)


def _format_levels_message(pair: str, levels: Dict[str, Any]) -> str:
    title = _display_pair(pair)
    res = levels.get("nearest_resistance")
    sup = levels.get("nearest_support")
    lines = [
        f"📍 {title}",
        "",
        "Today’s session",
        f"  High: {levels.get('session_high')}",
        f"  Low: {levels.get('session_low')}",
        "",
        "Previous session",
        f"  High: {levels.get('prev_session_high')}",
        f"  Low: {levels.get('prev_session_low')}",
        "",
        "Swings (recent)",
        "  Highs: "
        + (", ".join(str(x) for x in levels.get("swing_highs") or []) or "—"),
        "  Lows: "
        + (", ".join(str(x) for x in levels.get("swing_lows") or []) or "—"),
        "",
        "Support / resistance",
        f"  Resistance (nearest above price): {res if res is not None else '—'}",
        f"  Support (nearest below price): {sup if sup is not None else '—'}",
    ]
    return "\n".join(lines)


def _load_market_pipeline_sync() -> Optional[Dict[str, Any]]:
    """Fetch OHLCV, indicators, levels, and calendar (blocking)."""

    try:
        eur_df, gc_df = fetch_eurusd_x_and_gc_ohlcv()
        if eur_df.empty or gc_df.empty:
            return None

        ind_eur = compute_indicators(eur_df)
        ind_gc = compute_indicators(gc_df)
        lev_eur = compute_levels(eur_df)
        lev_gc = compute_levels(gc_df)
        if ind_eur is None or ind_gc is None or lev_eur is None or lev_gc is None:
            return None

        events = fetch_todays_economic_events()
        return {
            "indicators_map": {"EURUSD": ind_eur, "XAUUSD": ind_gc},
            "levels_map": {"EURUSD": lev_eur, "XAUUSD": lev_gc},
            "events": events,
        }
    except Exception:
        logger.exception("Market pipeline load failed")
        return None


async def _load_market_pipeline() -> Optional[Dict[str, Any]]:
    return await asyncio.to_thread(_load_market_pipeline_sync)


def _format_news_event_line(e: Dict[str, Any]) -> str:
    icon = "🔴" if e.get("impact") == "high" else "🟡"
    fc = e.get("forecast")
    pv = e.get("previous")
    fc_text = str(fc) if fc is not None else "—"
    pv_text = str(pv) if pv is not None else "—"
    return (
        f"{icon} {e.get('time')} — {e.get('title')} ({e.get('currency')})"
        f" | Forecast: {fc_text} | Prev: {pv_text}"
    )


def _parse_time_to_est_today(time_text: str, *, now_est: datetime) -> Optional[datetime]:
    """
    Parse ForexFactory-ish time strings like '8:30am' / '10:00 AM' into a datetime
    in America/New_York for today.
    """

    if not time_text:
        return None
    s = time_text.strip().lower().replace(" ", "")
    # Expected: H[:MM](am|pm)
    m = re.match(r"^(\d{1,2})(?::(\d{2}))?(am|pm)$", s)
    if not m:
        return None
    hour = int(m.group(1))
    minute = int(m.group(2) or "0")
    suffix = m.group(3)
    if hour == 12:
        hour = 0
    if suffix == "pm":
        hour += 12
    dt = datetime(now_est.year, now_est.month, now_est.day, hour, minute, tzinfo=NY_TZ)
    return dt


def _minutes_until_event(event_dt: datetime, *, now_est: datetime) -> int:
    return int(round((event_dt - now_est).total_seconds() / 60.0))


async def _send_today_brief(message: Message) -> None:
    data = await _load_market_pipeline()
    if data is None:
        await message.answer(FRIENDLY_DATA_ERROR)
        return

    try:
        results = await analyze_all_pairs(
            data["indicators_map"],
            data["levels_map"],
            data["events"],
        )
    except Exception:
        logger.exception("analyze_all_pairs failed")
        await message.answer(FRIENDLY_DATA_ERROR)
        return

    for key in ("EURUSD", "XAUUSD"):
        brief = results.get(key)
        if brief is None:
            await message.answer(
                f"{_display_pair(key)} — brief unavailable right now. Please try again in a few minutes 🙏"
            )
            continue
        await message.answer(_format_brief_message(key, brief))


async def _send_levels(message: Message) -> None:
    data = await _load_market_pipeline()
    if data is None:
        await message.answer(FRIENDLY_DATA_ERROR)
        return
    lev = data["levels_map"]
    text = "\n\n".join(
        [
            _format_levels_message("EURUSD", lev["EURUSD"]),
            _format_levels_message("XAUUSD", lev["XAUUSD"]),
        ]
    )
    await message.answer(text)


async def _news_today(message: Message) -> None:
    try:
        events = fetch_todays_economic_events()
    except Exception:
        logger.exception("fetch_todays_economic_events failed")
        await message.answer(FRIENDLY_DATA_ERROR)
        return

    if not events:
        await message.answer("✅ No major news today — clean conditions.")
        return

    # Sort by parsed time when possible; otherwise keep original order.
    now_est = datetime.now(NY_TZ)

    def sort_key(e: Dict[str, Any]) -> Tuple[int, str]:
        dt = _parse_time_to_est_today(str(e.get("time") or ""), now_est=now_est)
        if dt is None:
            return (99999, "")
        mins = (dt - datetime(now_est.year, now_est.month, now_est.day, 0, 0, tzinfo=NY_TZ)).seconds // 60
        return (mins, str(e.get("time") or ""))

    events_sorted = sorted(events, key=sort_key)
    lines = ["📰 Economic Events Today (EST)", ""]
    for e in events_sorted:
        lines.append(_format_news_event_line(e))
    lines.extend(["", "💡 Avoid trading 30 mins before and after red events."])
    await message.answer("\n".join(lines))


async def _should_i_trade(message: Message) -> None:
    data = await _load_market_pipeline()
    if data is None:
        await message.answer(FRIENDLY_DATA_ERROR)
        return

    events = data.get("events") or []
    now_est = datetime.now(NY_TZ)
    high_events = [e for e in events if e.get("impact") == "high"]

    soonest_high: Optional[Tuple[Dict[str, Any], int]] = None
    for e in high_events:
        dt = _parse_time_to_est_today(str(e.get("time") or ""), now_est=now_est)
        if dt is None:
            continue
        mins = _minutes_until_event(dt, now_est=now_est)
        if 0 <= mins <= 60:
            if soonest_high is None or mins < soonest_high[1]:
                soonest_high = (e, mins)

    wait_trigger = soonest_high is not None
    wait_event = soonest_high[0] if soonest_high else None
    wait_mins = soonest_high[1] if soonest_high else None

    # Run AI analysis for both pairs concurrently (for confidence).
    try:
        eur_task = analyze_market(
            "EURUSD",
            data["indicators_map"].get("EURUSD"),
            data["levels_map"].get("EURUSD"),
            events,
        )
        xau_task = analyze_market(
            "XAUUSD",
            data["indicators_map"].get("XAUUSD"),
            data["levels_map"].get("XAUUSD"),
            events,
        )
        brief_eur, brief_xau = await asyncio.gather(eur_task, xau_task)
    except Exception:
        logger.exception("analyze_market failed in _should_i_trade")
        await message.answer(FRIENDLY_DATA_ERROR)
        return

    verdicts: Dict[str, str] = {}
    detail_lines: Dict[str, List[str]] = {"EURUSD": [], "XAUUSD": []}

    for pair in ("EURUSD", "XAUUSD"):
        indicators = (data["indicators_map"] or {}).get(pair) or {}
        levels = (data["levels_map"] or {}).get(pair) or {}
        brief = brief_eur if pair == "EURUSD" else brief_xau

        is_volatile = bool(indicators.get("is_volatile"))
        trend = indicators.get("trend")
        confidence = brief.confidence if brief is not None else 1

        if wait_trigger:
            # ⚠️ WAIT if: high impact news within 60 minutes
            verdicts[pair] = "⚠️ WAIT"
            title = str(wait_event.get("title") or "").strip() if wait_event else "High impact news"
            short_title = title.split()[0] if title else "News"
            detail_lines[pair] = [
                f"→ High impact news in {wait_mins} mins ({short_title})",
            ]
            continue

        # ✅ CONDITIONS GOOD if: trend is not ranging AND is_volatile is True
        if trend != "ranging" and is_volatile:
            verdicts[pair] = "✅ CONDITIONS GOOD"
            detail_lines[pair] = [
                f"→ Trending {trend}, volatility active",
            ]
            if trend == "bearish":
                detail_lines[pair].append(f"→ Nearest support: {_format_price_or_none(levels.get('nearest_support'))}")
            elif trend == "bullish":
                detail_lines[pair].append(
                    f"→ Nearest resistance: {_format_price_or_none(levels.get('nearest_resistance'))}"
                )
            else:
                detail_lines[pair].append(
                    f"→ Nearest support: {_format_price_or_none(levels.get('nearest_support'))}"
                )
            continue

        # ❌ NO TRADE if: is_volatile is False AND confidence from AI is below 3
        if (not is_volatile) and confidence < 3:
            verdicts[pair] = "❌ NO TRADE"
            detail_lines[pair] = [
                "→ Volatility inactive and AI confidence is low",
                f"→ AI confidence: {confidence}/5",
            ]
            continue

        # If none of the explicit conditions matched, default to patience.
        verdicts[pair] = "❌ NO TRADE"
        detail_lines[pair] = ["→ Conditions unclear — no setup, so no trade."]

    lines = ["🎯 Should You Trade Right Now?", ""]
    # The sample uses EUR/USD first, then Gold.
    for pair in ("EURUSD", "XAUUSD"):
        lines.append(f"{_pair_short(pair)}: {verdicts.get(pair)}")
        lines.extend(detail_lines.get(pair) or [])
        lines.append("")

    lines.append("💡 Reminder: no setup = no trade. Patience is the edge.")
    await message.answer("\n".join([ln for ln in lines if ln is not None]).rstrip())


class TradeLogFlow(StatesGroup):
    choose_pair = State()
    choose_direction = State()
    choose_reason = State()
    enter_prices = State()


class OnboardingFlow(StatesGroup):
    account_size = State()
    timezone = State()


class CloseTradeFlow(StatesGroup):
    choose_trade = State()
    enter_exit = State()
    choose_result = State()


def _trade_pair_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="EUR/USD", callback_data="trade:pair:EURUSD"),
                InlineKeyboardButton(text="Gold (XAU/USD)", callback_data="trade:pair:XAUUSD"),
            ]
        ]
    )


def _trade_direction_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="BUY", callback_data="trade:dir:BUY"),
                InlineKeyboardButton(text="SELL", callback_data="trade:dir:SELL"),
            ]
        ]
    )


def _pip_size_for_pair(pair: str) -> float:
    # Spec: EURUSD pip = 0.0001, Gold: multiply by 10 (pip size 0.1).
    if pair == "XAUUSD":
        return 0.1
    return 0.0001


def _compute_pips(pair: str, direction: str, entry: float, stop_loss: float, target: float) -> Tuple[float, float]:
    pip_size = _pip_size_for_pair(pair)
    if direction == "BUY":
        pips_sl = (stop_loss - entry) / pip_size
        pips_tp = (target - entry) / pip_size
    else:
        pips_sl = (entry - stop_loss) / pip_size
        pips_tp = (entry - target) / pip_size
    return pips_sl, pips_tp


@router.message(F.text == BUTTON_TODAY)
@router.message(Command("today"))
async def btn_today(message: Message) -> None:
    await _send_today_brief(message)


@router.message(F.text == BUTTON_LEVELS)
@router.message(Command("levels"))
async def btn_levels(message: Message) -> None:
    await _send_levels(message)


@router.message(F.text == BUTTON_NEWS_TODAY)
async def btn_news_today(message: Message) -> None:
    await _news_today(message)


@router.message(F.text == BUTTON_SHOULD_TRADE)
async def btn_should_i_trade(message: Message) -> None:
    await _should_i_trade(message)


@router.message(F.text == BUTTON_LOG_TRADE)
async def btn_log_trade(message: Message, state: FSMContext) -> None:
    await state.set_state(TradeLogFlow.choose_pair)
    await message.answer("Which pair? EUR/USD or Gold?", reply_markup=_trade_pair_keyboard())


@router.message(F.text == BUTTON_MY_STATS)
async def btn_my_stats(message: Message) -> None:
    if not message.from_user:
        return
    await _my_stats(message, message.from_user.id)


@router.message(Command("alert"))
async def cmd_alert(message: Message) -> None:
    if not message.from_user:
        return
    uid = message.from_user.id
    alerts_on = await get_user_setting(uid, "alerts_on")
    on = bool(alerts_on) if alerts_on is not None else False
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔕 Turn Alerts Off" if on else "🔔 Turn Alerts On",
                    callback_data="alert:toggle",
                )
            ]
        ]
    )
    await message.answer(
        "High-impact news alerts are used for the scheduled heads-ups. Toggle them when you want.",
        reply_markup=keyboard,
    )


@router.callback_query(F.data == "alert:toggle")
async def cb_alert_toggle(callback: CallbackQuery) -> None:
    if not callback.from_user:
        await callback.answer()
        return
    uid = callback.from_user.id
    current = await get_user_setting(uid, "alerts_on")
    new_val = 0 if int(current or 0) == 1 else 1
    await save_user_setting(uid, "alerts_on", new_val)
    await callback.answer("Updated.")
    await callback.message.answer("Alerts are now ON." if new_val == 1 else "Alerts are now OFF.")


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    name = (message.from_user.first_name if message.from_user else None) or "there"
    text = (
        f"Hi {name} 👋 I'm your forex trading partner. I watch EUR/USD and Gold 24/7 "
        f"so you don't have to. Here's what I can do for you:"
    )
    await message.answer(text)

    if not message.from_user:
        await message.answer("Here are your tools:", reply_markup=main_reply_keyboard())
        return

    onboarded = await get_user_setting(message.from_user.id, "onboarded")
    if onboarded is None or int(onboarded or 0) == 0:
        await state.set_state(OnboardingFlow.account_size)
        await message.answer(
            "Before we start — what is your trading account size? This helps me give you exact risk amounts. "
            "Reply with a number, e.g. 500"
        )
        return

    await message.answer("Here are your tools:", reply_markup=main_reply_keyboard())


@router.message(OnboardingFlow.account_size)
async def st_onboard_account_size(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        await state.clear()
        return
    try:
        account_size = float((message.text or "").strip())
    except Exception:
        await message.answer("Please reply with a number, e.g. 500")
        return

    await save_user_setting(message.from_user.id, "account_size", account_size)
    await state.set_state(OnboardingFlow.timezone)

    tz_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🌎 Americas (EST)", callback_data="onboard:tz:America/New_York"),
                InlineKeyboardButton(text="🌍 Europe (GMT)", callback_data="onboard:tz:Europe/London"),
                InlineKeyboardButton(text="🌏 Asia (SGT)", callback_data="onboard:tz:Asia/Singapore"),
            ]
        ]
    )
    await message.answer("Choose your timezone:", reply_markup=tz_keyboard)


@router.callback_query(F.data.startswith("onboard:tz:"))
async def cb_onboard_timezone(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.from_user:
        await callback.answer()
        return
    await callback.answer()
    tz = callback.data.split(":", 2)[2]
    await save_user_setting(callback.from_user.id, "timezone", tz)
    await save_user_setting(callback.from_user.id, "onboarded", 1)
    await state.clear()
    await callback.message.answer("Perfect! You are all set 🎯 Here are your tools:", reply_markup=main_reply_keyboard())


@router.message(Command("risk"))
async def cmd_risk(message: Message) -> None:
    if not message.from_user:
        return

    account_size = await get_user_setting(message.from_user.id, "account_size")
    if account_size is None:
        # Generic message (as before) + tip
        generic = (
            "📚 Position sizing & risk\n\n"
            "• Never risk more than 1–2% of your account on a single trade. "
            "That way a string of losses cannot wipe you out.\n\n"
            "• Simple lot check: decide your stop distance in pips (or dollars for gold), "
            "then size so that if the stop hits, you lose only your chosen percent of the account. "
            "If unsure, use your broker’s position-size calculator.\n\n"
            "• Capital first: small, consistent survival beats hero trades. "
            "Protecting what you have is the job; profits are a bonus.\n\n"
            "💡 Tip: tell me your account size by running /start again for personalized risk amounts."
        )
        await message.answer(generic)
        return

    try:
        acct = float(account_size)
    except Exception:
        await message.answer(
            "💡 Tip: tell me your account size by running /start again for personalized risk amounts."
        )
        return

    r1 = acct * 0.01
    r2 = acct * 0.02
    text = "\n".join(
        [
            "💰 Risk Calculator",
            "",
            f"Account size: ${acct:,.0f}",
            "",
            f"Max risk per trade (1%): ${r1:,.2f}",
            f"Max risk per trade (2%): ${r2:,.2f}",
            "",
            "💡 Size your position so that if your stop loss hits,",
            "you lose no more than the amounts above.",
            "Never risk more than 2% on a single trade.",
            "Capital protection is the job.",
        ]
    )
    await message.answer(text)


@router.message(F.text == BUTTON_CLOSE_TRADE)
@router.message(Command("close_trade"))
async def cmd_close_trade(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return

    trades = await get_open_trades(message.from_user.id)
    if not trades:
        await message.answer("No open trades to close. Log one first with 📓 Log Trade!")
        return

    buttons: List[List[InlineKeyboardButton]] = []
    for t in trades:
        pair = "EUR/USD" if t.get("pair") == "EURUSD" else "Gold"
        direction = str(t.get("direction") or "")
        entry = t.get("entry")
        ts = str(t.get("timestamp") or "")
        label = f"{pair} {direction} @ {entry} — {ts[:10] if ts else ''}".strip()
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"close:pick:{t.get('id')}")])

    await state.set_state(CloseTradeFlow.choose_trade)
    await message.answer("Select a trade to close:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("close:pick:"))
async def cb_close_pick_trade(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.from_user:
        await callback.answer()
        return
    await callback.answer()
    trade_id = int(callback.data.split(":", 2)[2])
    await state.update_data(trade_id=trade_id)
    await state.set_state(CloseTradeFlow.enter_exit)
    await callback.message.answer("What was your exit price?")


@router.message(CloseTradeFlow.enter_exit)
async def st_close_enter_exit(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        await state.clear()
        return
    try:
        exit_price = float((message.text or "").strip())
    except Exception:
        await message.answer("Please reply with a number for your exit price.")
        return

    await state.update_data(exit_price=exit_price)
    await state.set_state(CloseTradeFlow.choose_result)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Winner", callback_data="close:res:WIN"),
                InlineKeyboardButton(text="❌ Loser", callback_data="close:res:LOSS"),
                InlineKeyboardButton(text="➡️ Breakeven", callback_data="close:res:BE"),
            ]
        ]
    )
    await message.answer("Result?", reply_markup=kb)


@router.callback_query(F.data.startswith("close:res:"))
async def cb_close_result(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.from_user:
        await callback.answer()
        return
    await callback.answer()
    data = await state.get_data()
    trade_id = int(data.get("trade_id"))
    exit_price = float(data.get("exit_price"))
    result_raw = callback.data.split(":", 2)[2]
    result_db = {"WIN": "Winner", "LOSS": "Loser", "BE": "Breakeven"}.get(result_raw, result_raw)

    closed = await close_trade(trade_id, exit_price, result_db, ai_bias="")
    await state.clear()

    pair = "EUR/USD" if closed.get("pair") == "EURUSD" else "Gold"
    direction = closed.get("direction")
    entry = closed.get("entry")
    pips = float(closed.get("pips_result") or 0.0)

    if result_raw == "WIN":
        res_line = f"✅ Winner (+{int(round(pips))} pips)"
        tip = "💡 Good discipline. Log every trade — the data builds your edge."
    elif result_raw == "LOSS":
        res_line = f"❌ Loss (-{abs(int(round(pips)))} pips)"
        tip = "💡 Losses are part of the game. Review what happened and move on."
    else:
        res_line = "➡️ Breakeven (0 pips)"
        tip = "💡 Good job managing risk. Breakeven is a win for discipline."

    await callback.message.answer(
        "\n".join(
            [
                "🔒 Trade Closed",
                "",
                f"{pair} | {direction}",
                f"Entry: {entry} → Exit: {exit_price}",
                f"Result: {res_line}",
                "",
                tip,
            ]
        )
    )


@router.message(F.text == BUTTON_HELP)
@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    await message.answer(
        "👋 Here's what every button does:\n\n"
        "📊 Today's Brief\n"
        "Full AI market analysis for EUR/USD and Gold.\n"
        "Your most important button — check it every morning.\n\n"
        "📍 Key Levels\n"
        "Current support and resistance prices.\n"
        "These are the lines the market respects.\n\n"
        "📰 News Today\n"
        "Today's high-impact economic events.\n"
        "Avoid trading 30 mins before and after red events.\n\n"
        "🎯 Should I Trade?\n"
        "Honest verdict on current market conditions.\n"
        "If it says wait — wait.\n\n"
        "📓 Log Trade\n"
        "Record every trade you take.\n"
        "The journal builds your edge over time.\n\n"
        "📈 My Stats\n"
        "Your personal win rate and performance.\n"
        "Honest numbers — no guessing.\n\n"
        "🔒 Close Trade\n"
        "Mark an open trade as won, lost or breakeven.\n"
        "Always close your logged trades so stats stay accurate.\n\n"
        "💡 Golden rule: no setup = no trade.\n"
        "Patience is your biggest edge."
    )


@router.callback_query(F.data.startswith("trade:pair:"))
async def cb_trade_pair(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.from_user:
        await callback.answer()
        return
    await callback.answer()
    pair = callback.data.split(":", 2)[2]
    if pair not in ("EURUSD", "XAUUSD"):
        return
    await state.update_data(pair=pair)
    await state.set_state(TradeLogFlow.choose_direction)
    await callback.message.answer("Buy or Sell?", reply_markup=_trade_direction_keyboard())


@router.callback_query(F.data.startswith("trade:dir:"))
async def cb_trade_dir(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.from_user:
        await callback.answer()
        return
    await callback.answer()
    direction = callback.data.split(":", 2)[2]
    if direction not in ("BUY", "SELL"):
        return
    await state.update_data(direction=direction)
    await state.set_state(TradeLogFlow.choose_reason)
    await callback.message.answer(
        "🧠 Why are you taking this trade?\n"
        "Pick the main reason — be honest with yourself.",
        reply_markup=reason_keyboard(),
    )


@router.callback_query(F.data.in_(TRADE_REASON_LABELS))
async def cb_trade_reason(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.from_user:
        await callback.answer()
        return
    await callback.answer()
    reason = callback.data
    if reason not in TRADE_REASON_LABELS:
        return
    await state.update_data(reason=reason)
    await state.set_state(TradeLogFlow.enter_prices)
    await callback.message.answer(
        "Enter your entry price, stop loss and target like this: 1.1559 / 1.1540 / 1.1600"
    )


def _parse_three_prices(text: str) -> Optional[Tuple[float, float, float]]:
    parts = [p.strip() for p in text.split("/") if p.strip()]
    if len(parts) != 3:
        return None
    try:
        entry = float(parts[0])
        stop_loss = float(parts[1])
        target = float(parts[2])
        return entry, stop_loss, target
    except ValueError:
        return None


@router.message(TradeLogFlow.enter_prices)
async def st_trade_enter_prices(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        await state.clear()
        return

    data = await state.get_data()
    pair = data.get("pair")
    direction = data.get("direction")
    reason = data.get("reason")
    if (
        pair not in ("EURUSD", "XAUUSD")
        or direction not in ("BUY", "SELL")
        or reason not in TRADE_REASON_LABELS
    ):
        await state.clear()
        return

    parsed = _parse_three_prices(message.text or "")
    if parsed is None:
        await message.answer(
            "Sorry — I couldn’t read that. Use the format: entry / stop_loss / target (e.g. 1.1559 / 1.1540 / 1.1600)"
        )
        return

    entry, stop_loss, target = parsed
    pips_sl, pips_tp = _compute_pips(pair, direction, entry, stop_loss, target)

    # Risk/Reward uses absolute risk.
    risk = abs(pips_sl)
    reward = abs(pips_tp)
    ratio = (reward / risk) if risk > 0 else 0.0
    risk_reward = f"1:{ratio:.1f}"

    # Display signed pips for clarity.
    pips_sl_disp = int(round(pips_sl))
    pips_tp_disp = int(round(pips_tp))
    sl_pips_text = f"{pips_sl_disp}"
    tp_pips_text = f"+{pips_tp_disp}" if pips_tp_disp >= 0 else f"{pips_tp_disp}"

    await save_trade(
        {
            "user_id": message.from_user.id,
            "pair": pair,
            "direction": direction,
            "entry": entry,
            "stop_loss": stop_loss,
            "target": target,
            "pips_sl": pips_sl,
            "pips_tp": pips_tp,
            "risk_reward": risk_reward,
            "timestamp": datetime.now().isoformat(),
            "reason": reason,
        }
    )

    pair_label = "EUR/USD" if pair == "EURUSD" else "Gold"
    await message.answer(
        "\n".join(
            [
                "📓 Trade Logged ✅",
                "",
                f"{pair_label} | {direction} | {reason}",
                f"Entry: {entry}",
                f"SL: {stop_loss} ({sl_pips_text} pips)",
                f"TP: {target} ({tp_pips_text} pips)",
                f"R:R {risk_reward}",
                "",
                "💡 Stick to the plan. Let the trade breathe.",
            ]
        )
    )
    await state.clear()


async def _my_stats(message: Message, user_id: int) -> None:
    try:
        stats = await get_stats(user_id)
    except Exception:
        logger.exception("get_stats failed")
        await message.answer(FRIENDLY_DATA_ERROR)
        return

    if stats is None:
        await message.answer("No stats yet — log your first trade with 📓 Log Trade!")
        return

    total_closed = int(stats["total_trades"])
    open_count = int(stats["open_count"])

    winners = int(stats["winners"])
    losers = int(stats["losers"])
    breakeven = int(stats["breakeven"])
    win_rate = float(stats["win_rate"])

    best_trade_line = "—"
    worst_trade_line = "—"
    if stats.get("best_trade") is not None:
        bp = stats.get("best_pair") or ""
        bi = int(round(float(stats["best_trade"])))
        best_trade_line = f"{bi:+d} pips ({_pair_short(bp)})"
    if stats.get("worst_trade") is not None:
        wp = stats.get("worst_pair") or ""
        wi = int(round(float(stats["worst_trade"])))
        worst_trade_line = f"{wi:+d} pips ({_pair_short(wp)})"

    avg_rr_line = "—"
    if stats.get("avg_rr") is not None:
        avg_rr_line = f"1:{float(stats['avg_rr']):.1f}"

    lines = [
        "📈 Your Trading Stats",
        "",
        f"Total closed: {total_closed}",
        f"⏳ Open: {open_count}",
        f"✅ Winners: {winners}",
        f"❌ Losers: {losers}",
        f"➖ Breakeven: {breakeven}",
        f"Win rate: {win_rate:.1f}%",
        "",
        f"Best trade: {best_trade_line}",
        f"Worst trade: {worst_trade_line}",
        f"Avg R:R taken: {avg_rr_line}",
        "",
        "💡 You're doing well. Keep following the plan.",
    ]

    if total_closed >= 3:
        try:
            edge_report = await get_edge_report(user_id)
        except Exception:
            logger.exception("get_edge_report failed")
            edge_report = None
        if edge_report:
            lines.extend(["", "🧬 Your Edge Report", ""])
            for reason_key, m in edge_report.items():
                wr = float(m["win_rate"])
                emoji = _edge_wr_emoji(wr)
                lines.append(
                    f"{reason_key} → {int(m['trade_count'])} trades → {wr:.1f}% win rate {emoji}"
                )
            insight_bullets = _edge_insight_bullets(list(edge_report.items()))
            if insight_bullets:
                lines.append("")
                lines.append("💡 " + " ".join(insight_bullets))
    else:
        lines.extend(
            [
                "",
                "🧬 Edge Report unlocks after 3 closed trades.",
                "   Keep logging — the data builds your edge.",
            ]
        )

    await message.answer("\n".join(lines))

