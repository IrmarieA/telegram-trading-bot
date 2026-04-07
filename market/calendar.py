"""Economic calendar integration (ForexFactory)."""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

FOREXFACTORY_CALENDAR_URL = "https://www.forexfactory.com/calendar"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

REQUEST_TIMEOUT_SEC = 10

ALLOWED_CURRENCIES = frozenset({"USD", "EUR"})
ALLOWED_IMPACTS = frozenset({"high", "medium"})


def _parse_impact_from_cell(impact_cell) -> Optional[str]:
    """Map ForexFactory impact icons/classes to 'high', 'medium', or 'low'."""

    if impact_cell is None:
        return None

    blob = str(impact_cell)

    if "ff-impact-red" in blob or "icon--ff-impact-red" in blob:
        return "high"
    if "ff-impact-ora" in blob or "icon--ff-impact-ora" in blob or "ff-impact-orange" in blob:
        return "medium"
    if "ff-impact-yel" in blob or "icon--ff-impact-yel" in blob:
        return "low"

    for el in impact_cell.find_all(["span", "div", "img"]):
        title = (el.get("title") or "").strip().lower()
        if "high impact" in title:
            return "high"
        if "med impact" in title or "medium impact" in title:
            return "medium"
        if "low impact" in title:
            return "low"

    return None


def _parse_calendar_date(text: str) -> Optional[date]:
    """Parse date cells like 'Wed Mar 25' (year inferred from *today*)."""

    text = re.sub(r"\s+", " ", text.strip())
    if not text:
        return None

    y = date.today().year
    for fmt in ("%a %b %d %Y", "%a %b %d"):
        try:
            if fmt.endswith("%Y"):
                return datetime.strptime(text, fmt).date()
            return datetime.strptime(f"{text} {y}", "%a %b %d %Y").date()
        except ValueError:
            continue
    return None


def _get_cell(tr, field: str):
    cell = tr.select_one(f"td.calendar__cell.calendar__{field}.{field}")
    if cell is None:
        cell = tr.select_one(f"td.calendar__{field}")
    return cell


def _cell_text(tr, field: str) -> str:
    cell = _get_cell(tr, field)
    if cell is None:
        return ""
    return cell.get_text(separator=" ", strip=True)


def _empty_to_none(s: str) -> Optional[str]:
    s = (s or "").strip()
    return s if s else None


def get_high_impact_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return only events with ``impact == 'high'`` (major news warnings)."""

    return [e for e in events if e.get("impact") == "high"]


def _extract_events(html: str, *, filter_session_today: bool) -> List[Dict[str, Any]]:
    """Parse calendar rows; if ``filter_session_today``, only rows for ``date.today()``."""

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_=re.compile(r"calendar__table"))
    if table is None:
        return []

    rows = table.find_all("tr", class_=re.compile(r"calendar__row"))
    if not rows:
        return []

    today = date.today()
    current_day: Optional[date] = None
    out: List[Dict[str, Any]] = []

    for tr in rows:
        try:
            date_cell = _get_cell(tr, "date")
            if date_cell is not None:
                dtext = date_cell.get_text(separator=" ", strip=True)
                parsed = _parse_calendar_date(dtext)
                if parsed is not None:
                    current_day = parsed

            time_cell = _get_cell(tr, "time")
            time_text = (time_cell.get_text(separator=" ", strip=True) if time_cell else "") or ""
            if not time_text:
                continue

            if filter_session_today:
                if current_day is None or current_day != today:
                    continue

            currency = _cell_text(tr, "currency").strip().upper()
            if currency not in ALLOWED_CURRENCIES:
                continue

            impact_cell = _get_cell(tr, "impact")
            impact = _parse_impact_from_cell(impact_cell)
            if impact is None or impact not in ALLOWED_IMPACTS:
                continue

            title = _cell_text(tr, "event").strip()
            if not title:
                continue

            forecast = _empty_to_none(_cell_text(tr, "forecast"))
            previous = _empty_to_none(_cell_text(tr, "previous"))

            out.append(
                {
                    "time": time_text,
                    "currency": currency,
                    "impact": impact,
                    "title": title,
                    "forecast": forecast,
                    "previous": previous,
                }
            )
        except Exception:
            continue

    return out


def fetch_todays_economic_events() -> List[Dict[str, Any]]:
    """
    Scrape ForexFactory for **today's** calendar rows (EUR/USD & Gold-relevant: USD/EUR).

    First requests the canonical ``https://www.forexfactory.com/calendar`` (no query) and
    keeps rows whose session date matches **local** ``today``. If nothing matches (e.g.
    sparse date cells), retries once with ``?day=today`` and treats all rows as today.

    Returns a list of dicts with keys: time, currency, impact, title, forecast, previous.
    On any failure or HTML change, returns an empty list.
    """

    try:
        headers = {"User-Agent": USER_AGENT}
        resp = requests.get(
            FOREXFACTORY_CALENDAR_URL,
            headers=headers,
            timeout=REQUEST_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        out = _extract_events(resp.text, filter_session_today=True)
        if not out:
            resp2 = requests.get(
                FOREXFACTORY_CALENDAR_URL,
                params={"day": "today"},
                headers=headers,
                timeout=REQUEST_TIMEOUT_SEC,
            )
            resp2.raise_for_status()
            out = _extract_events(resp2.text, filter_session_today=False)
        return out
    except Exception:
        return []
