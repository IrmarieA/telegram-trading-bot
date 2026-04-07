"""Technical indicators built on OHLCV DataFrames."""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd
from ta.trend import EMAIndicator
from ta.volatility import AverageTrueRange


REQUIRED_COLS = ("Open", "High", "Low", "Close", "Volume")
MIN_ROWS = 50


def compute_indicators(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    Compute trend, EMAs, ATR, volatility flag, and last price from OHLCV bars.

    Expects columns: Open, High, Low, Close, Volume (as produced by ``data_fetcher``).

    Returns ``None`` if the frame has fewer than 50 rows (not enough history for a
    meaningful 50-period EMA and related logic).

    When successful, returns a dictionary with:

    - **trend** (``str``): Market bias from price vs moving averages.
        - ``'bullish'`` — latest Close is above both the 20- and 50-period EMAs.
        - ``'bearish'`` — latest Close is below both EMAs.
        - ``'ranging'`` — price is not clearly on one side of both (e.g. between
          the two EMAs, or mixed), so neither strong bull nor bear from this rule.

    - **ema_20** (``float``): Latest value of the 20-period exponential moving
      average of Close.

    - **ema_50** (``float``): Latest value of the 50-period exponential moving
      average of Close.

    - **atr** (``float``): Latest 14-period Average True Range (volatility in
      price units).

    - **is_volatile** (``bool``): ``True`` if the current ATR is greater than the
      simple average of the last 10 ATR values (including the current bar’s ATR),
      i.e. short-term volatility is elevated vs its own recent baseline.

    - **current_price** (``float``): Latest Close (last row).
    """

    if df is None or len(df) < MIN_ROWS:
        return None

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        return None

    close = df["Close"]
    high = df["High"]
    low = df["Low"]

    ema_20_series = EMAIndicator(close=close, window=20).ema_indicator()
    ema_50_series = EMAIndicator(close=close, window=50).ema_indicator()
    atr_series = AverageTrueRange(
        high=high,
        low=low,
        close=close,
        window=14,
    ).average_true_range()

    ema_20_val = float(ema_20_series.iloc[-1])
    ema_50_val = float(ema_50_series.iloc[-1])
    atr_last = float(atr_series.iloc[-1])
    current_price = float(close.iloc[-1])

    if pd.isna(ema_20_val) or pd.isna(ema_50_val) or pd.isna(atr_last):
        return None

    if current_price > ema_20_val and current_price > ema_50_val:
        trend = "bullish"
    elif current_price < ema_20_val and current_price < ema_50_val:
        trend = "bearish"
    else:
        trend = "ranging"

    atr_mean_10 = atr_series.rolling(window=10, min_periods=10).mean().iloc[-1]
    if pd.isna(atr_mean_10):
        is_volatile = False
    else:
        is_volatile = atr_last > float(atr_mean_10)

    return {
        "trend": trend,
        "ema_20": ema_20_val,
        "ema_50": ema_50_val,
        "atr": atr_last,
        "is_volatile": is_volatile,
        "current_price": current_price,
    }
