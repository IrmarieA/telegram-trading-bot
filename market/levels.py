"""Support/resistance and key level detection."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

REQUIRED_COLS = ("Open", "High", "Low", "Close", "Volume")
MIN_COMPLETE_DAYS = 3
DECIMALS = 5


def _round_price(x: float) -> float:
    return round(float(x), DECIMALS)


def compute_levels(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    Derive session extremes, swing levels, and nearest support/resistance from OHLCV.

    Expects a **DatetimeIndex** (e.g. from yfinance) and columns Open, High, Low,
    Close, Volume.

    Sessions are **calendar days** in the index’s timezone (``index.date``).

    Returns ``None`` if there are fewer than three distinct calendar days with data.

    Returned mapping:

    - **session_high** / **session_low**: Highest High and lowest Low on the latest
      calendar day present in the frame (treated as the most recent session in the data).

    - **prev_session_high** / **prev_session_low**: Same aggregates for the prior
      calendar day.

    - **swing_highs**: Up to three most recent swing highs (oldest of the three first),
      where a swing high at bar *i* has ``High[i]`` strictly greater than the High of
      the two bars before and the two bars after *i*.

    - **swing_lows**: Same for swing lows (strictly lower than two bars on each side).

    - **nearest_resistance**: The swing high strictly above the latest Close that is
      closest to price from below (smallest such high). ``None`` if none exists.

    - **nearest_support**: The swing low strictly below the latest Close that is
      closest to price from above (largest such low). ``None`` if none exists.

    All scalar prices and list elements are rounded to five decimal places.
    """

    if df is None or df.empty:
        return None

    if not isinstance(df.index, pd.DatetimeIndex):
        return None

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        return None

    unique_dates = sorted(pd.unique(df.index.date))
    if len(unique_dates) < MIN_COMPLETE_DAYS:
        return None

    daily = (
        df.groupby(df.index.date)
        .agg(session_high=("High", "max"), session_low=("Low", "min"))
        .sort_index()
    )

    session_high = _round_price(daily["session_high"].iloc[-1])
    session_low = _round_price(daily["session_low"].iloc[-1])
    prev_session_high = _round_price(daily["session_high"].iloc[-2])
    prev_session_low = _round_price(daily["session_low"].iloc[-2])

    high_s = df["High"].to_numpy()
    low_s = df["Low"].to_numpy()
    n = len(df)

    swing_high_prices: List[float] = []
    swing_low_prices: List[float] = []

    for i in range(2, n - 2):
        h = high_s[i]
        if h > high_s[i - 1] and h > high_s[i - 2] and h > high_s[i + 1] and h > high_s[i + 2]:
            swing_high_prices.append(_round_price(h))

        lo = low_s[i]
        if lo < low_s[i - 1] and lo < low_s[i - 2] and lo < low_s[i + 1] and lo < low_s[i + 2]:
            swing_low_prices.append(_round_price(lo))

    swing_highs = swing_high_prices[-3:] if len(swing_high_prices) > 3 else swing_high_prices
    swing_lows = swing_low_prices[-3:] if len(swing_low_prices) > 3 else swing_low_prices

    current_price = float(df["Close"].iloc[-1])

    nearest_resistance: Optional[float] = None
    above = [p for p in swing_high_prices if p > current_price]
    if above:
        nearest_resistance = _round_price(min(above))

    nearest_support: Optional[float] = None
    below = [p for p in swing_low_prices if p < current_price]
    if below:
        nearest_support = _round_price(max(below))

    return {
        "session_high": session_high,
        "session_low": session_low,
        "prev_session_high": prev_session_high,
        "prev_session_low": prev_session_low,
        "swing_highs": swing_highs,
        "swing_lows": swing_lows,
        "nearest_resistance": nearest_resistance,
        "nearest_support": nearest_support,
    }
