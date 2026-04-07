"""Scheduled jobs (e.g. daily brief, calendar reminders)."""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from aiogram import Bot
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from ai.analyst import analyze_all_pairs
from market.calendar import fetch_todays_economic_events, get_high_impact_events

import aiosqlite

from handlers import FRIENDLY_DATA_ERROR, _display_pair, _format_brief_message, _load_market_pipeline
from storage.db import DB_PATH

logger = logging.getLogger(__name__)

NY_TZ = ZoneInfo("America/New_York")


def _parse_time_to_est_today(time_text: str, *, now_est: datetime) -> Optional[datetime]:
    if not time_text:
        return None
    s = str(time_text).strip().lower().replace(" ", "")
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
    return datetime(now_est.year, now_est.month, now_est.day, hour, minute, tzinfo=NY_TZ)


def _minutes_until_event(event_dt: datetime, *, now_est: datetime) -> int:
    return int(round((event_dt - now_est).total_seconds() / 60.0))


def _trader_chat_int(trader_chat_id: str) -> Optional[int]:
    s = (trader_chat_id or "").strip()
    if not s:
        return None
    try:
        return int(s)
    except (TypeError, ValueError):
        logger.warning("Invalid TRADER_CHAT_ID %r; cannot send scheduled DM", trader_chat_id)
        return None


async def _broadcast_alerts(bot: Bot, text: str, trader_chat_id: str) -> None:
    """
    Always send to ``TRADER_CHAT_ID`` when configured.
    Additionally send to SQLite users with ``alerts_on = 1`` (optional).
    """

    trader_cid = _trader_chat_int(trader_chat_id)
    if trader_cid is not None:
        try:
            await bot.send_message(trader_cid, text)
            logger.info("Scheduled message sent to TRADER_CHAT_ID=%s", trader_cid)
        except Exception:
            logger.exception("Scheduled message failed for TRADER_CHAT_ID=%s", trader_cid)
    else:
        logger.warning(
            "TRADER_CHAT_ID is empty or invalid; skipping primary alert destination "
            "(SQLite users may still receive messages)"
        )

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT user_id FROM user_settings WHERE alerts_on = 1"
            )
            rows = await cursor.fetchall()
    except Exception:
        logger.exception("Failed to query alert users from SQLite")
        return

    if not rows:
        logger.debug("No SQLite users with alerts_on=1; TRADER_CHAT_ID-only delivery applied")
        return

    for (uid,) in rows:
        try:
            uid_int = int(uid)
        except (TypeError, ValueError):
            logger.warning("Skipping invalid user_id from DB: %r", uid)
            continue
        if trader_cid is not None and uid_int == trader_cid:
            logger.debug("Skipping duplicate send: user_id matches TRADER_CHAT_ID")
            continue
        try:
            await bot.send_message(uid_int, text)
            logger.info("Scheduled message sent to user_id=%s", uid_int)
        except Exception:
            logger.exception("Scheduled message failed for user_id=%s", uid_int)


def _midday_snapshot(pair: str, ind: Dict[str, Any], lev: Dict[str, Any]) -> str:
    """Short line: price vs nearest support/resistance (no AI)."""

    title = _display_pair(pair)
    price = ind.get("current_price")
    sup = lev.get("nearest_support")
    res = lev.get("nearest_resistance")
    lines = [f"🕛 Midday check — {title}"]
    if price is None:
        lines.append("Price unavailable right now.")
        return "\n".join(lines)
    lines.append(f"Price: {price}")
    if sup is None and res is None:
        lines.append("Support/resistance levels unavailable.")
        return "\n".join(lines)
    try:
        px = float(price)
    except (TypeError, ValueError):
        lines.append("Could not compare price to levels.")
        return "\n".join(lines)

    ds: Optional[float] = None if sup is None else abs(px - float(sup))
    dr: Optional[float] = None if res is None else abs(px - float(res))

    if ds is not None and dr is not None:
        if ds < dr:
            lines.append(
                f"Closer to support (~{sup}); resistance ~{res}. "
                "Watch for bounces or breaks near these levels."
            )
        elif dr < ds:
            lines.append(
                f"Closer to resistance (~{res}); support ~{sup}. "
                "Watch for rejections or breaks near these levels."
            )
        else:
            lines.append(f"Roughly between support ~{sup} and resistance ~{res}.")
    elif ds is not None:
        lines.append(f"Nearest support ~{sup}.")
    elif dr is not None:
        lines.append(f"Nearest resistance ~{res}.")
    return "\n".join(lines)


async def _job_morning_brief(bot: Bot, trader_chat_id: str) -> None:
    logger.info("Scheduler job running: morning_brief")
    data = await _load_market_pipeline()
    if data is None:
        await _broadcast_alerts(bot, FRIENDLY_DATA_ERROR, trader_chat_id)
        return
    try:
        results = await analyze_all_pairs(
            data["indicators_map"],
            data["levels_map"],
            data["events"],
        )
    except Exception:
        logger.exception("Morning brief: analyze_all_pairs failed")
        await _broadcast_alerts(bot, FRIENDLY_DATA_ERROR, trader_chat_id)
        return

    for key in ("EURUSD", "XAUUSD"):
        brief = results.get(key)
        if brief is None:
            await _broadcast_alerts(
                bot,
                f"{_display_pair(key)} — brief unavailable right now. "
                "Please try again later 🙏",
                trader_chat_id,
            )
            continue
        await _broadcast_alerts(bot, _format_brief_message(key, brief), trader_chat_id)


async def _job_ny_open(bot: Bot, trader_chat_id: str) -> None:
    logger.info("Scheduler job running: ny_open")
    try:
        events = await asyncio.to_thread(fetch_todays_economic_events)
        high = get_high_impact_events(events)
    except Exception:
        logger.exception("NY open: calendar fetch failed")
        await _broadcast_alerts(
            bot,
            "⚡ NY Open — couldn’t load the calendar. Stay cautious around news.",
            trader_chat_id,
        )
        return

    if high:
        titles = ", ".join(e.get("title", "") for e in high if e.get("title"))
        msg = (
            "⚡ NY Open — heads up, high-impact news today: "
            f"{titles}. Consider waiting for the dust to settle."
        )
    else:
        msg = "✅ NY Open — no major news. Clean conditions for setups."
    await _broadcast_alerts(bot, msg, trader_chat_id)


async def _job_midday(bot: Bot, trader_chat_id: str) -> None:
    logger.info("Scheduler job running: midday")
    data = await _load_market_pipeline()
    if data is None:
        await _broadcast_alerts(bot, FRIENDLY_DATA_ERROR, trader_chat_id)
        return
    ind = data["indicators_map"]
    lev = data["levels_map"]
    for key in ("EURUSD", "XAUUSD"):
        i = ind.get(key)
        l = lev.get(key)
        if i is None or l is None:
            await _broadcast_alerts(
                bot,
                f"{_display_pair(key)} — midday levels unavailable right now 🙏",
                trader_chat_id,
            )
            continue
        await _broadcast_alerts(bot, _midday_snapshot(key, i, l), trader_chat_id)


async def _job_session_wrap(bot: Bot, trader_chat_id: str) -> None:
    logger.info("Scheduler job running: session_wrap")
    msg = (
        "📅 NY session closing. Review your open trades, move stops to breakeven if in profit. "
        "See you tomorrow 👋"
    )
    await _broadcast_alerts(bot, msg, trader_chat_id)


async def _job_news_warning(bot: Bot, trader_chat_id: str) -> None:
    logger.info("Scheduler job running: news_warning")
    try:
        events = await asyncio.to_thread(fetch_todays_economic_events)
        high = get_high_impact_events(events)
    except Exception:
        logger.exception("News warning job: calendar fetch failed")
        return

    now_est = datetime.now(NY_TZ)
    soonest: Optional[tuple[dict[str, Any], int]] = None
    for e in high:
        dt = _parse_time_to_est_today(str(e.get("time") or ""), now_est=now_est)
        if dt is None:
            continue
        mins = _minutes_until_event(dt, now_est=now_est)
        if 0 <= mins <= 60:
            if soonest is None or mins < soonest[1]:
                soonest = (e, mins)

    if not soonest:
        logger.debug("news_warning: no high-impact event in the next 60 minutes")
        return

    event, mins = soonest
    title = str(event.get("title") or "").strip()
    if not title:
        return
    msg = (
        f"⚠️ heads up — {title} in {mins} mins. "
        "Consider protecting open trades or staying out until after the release."
    )
    await _broadcast_alerts(bot, msg, trader_chat_id)


def setup_scheduler(
    bot: Bot,
    trader_chat_id: str,
    *,
    event_loop: asyncio.AbstractEventLoop,
) -> AsyncIOScheduler:
    """
    Schedule NY-time jobs.

    Primary delivery: ``TRADER_CHAT_ID`` (from config) always receives alerts when set.
    Additional recipients: SQLite ``user_settings`` rows with ``alerts_on = 1``.

    Pass ``asyncio.get_running_loop()`` from your aiogram ``main()`` so APScheduler uses
    the same event loop as the bot.
    """

    loop = event_loop
    tid = (trader_chat_id or "").strip()

    now_ny = datetime.now(NY_TZ)
    logger.info(
        "Scheduler setup | timezone=America/New_York | now=%s | TRADER_CHAT_ID configured=%s",
        now_ny.isoformat(),
        bool(tid),
    )

    scheduler = AsyncIOScheduler(timezone=NY_TZ, event_loop=loop)

    scheduler.add_job(
        _job_morning_brief,
        CronTrigger(hour=7, minute=0, timezone=NY_TZ),
        args=[bot, tid],
        id="morning_brief",
        replace_existing=True,
    )
    scheduler.add_job(
        _job_ny_open,
        CronTrigger(hour=8, minute=30, timezone=NY_TZ),
        args=[bot, tid],
        id="ny_open",
        replace_existing=True,
    )
    scheduler.add_job(
        _job_midday,
        CronTrigger(hour=12, minute=0, timezone=NY_TZ),
        args=[bot, tid],
        id="midday",
        replace_existing=True,
    )
    scheduler.add_job(
        _job_session_wrap,
        CronTrigger(hour=16, minute=0, timezone=NY_TZ),
        args=[bot, tid],
        id="session_wrap",
        replace_existing=True,
    )

    scheduler.add_job(
        _job_news_warning,
        IntervalTrigger(minutes=30),
        args=[bot, tid],
        id="news_warning",
        replace_existing=True,
    )

    logger.info(
        "Scheduler jobs registered: morning_brief, ny_open, midday(12:00 NY), "
        "session_wrap, news_warning(30m)"
    )
    registered_ids = [job.id for job in scheduler.get_jobs()]
    print("REGISTERED JOBS:", registered_ids)
    logger.info("REGISTERED JOBS: %s", registered_ids)
    return scheduler
