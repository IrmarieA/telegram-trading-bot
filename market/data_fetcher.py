from __future__ import annotations

import logging
from typing import Tuple

import pandas as pd
import yfinance as yf

# Loads `.env` and exposes YFINANCE_* (see `config.py`).
import config

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL = config.YFINANCE_INTERVAL
DEFAULT_PERIOD = config.YFINANCE_PERIOD

EXPECTED_OHLCV_COLS = ["Open", "High", "Low", "Close", "Volume"]


def _normalize_ohlcv(df: pd.DataFrame, *, symbol: str) -> pd.DataFrame:
    """
    Ensure we return a DataFrame with columns:
    Open, High, Low, Close, Volume (in that order).
    Returns an empty DataFrame if normalization fails.
    """

    if df is None or df.empty:
        return pd.DataFrame()

    data = df.copy()

    # yfinance can sometimes return MultiIndex columns; collapse to the last level.
    if isinstance(data.columns, pd.MultiIndex):
        try:
            data.columns = data.columns.get_level_values(0)
        except Exception:
            logger.warning("Failed to normalize MultiIndex columns for %s", symbol)

    # Common fallback when Close is missing.
    if "Close" not in data.columns and "Adj Close" in data.columns:
        data = data.rename(columns={"Adj Close": "Close"})

    missing = [c for c in EXPECTED_OHLCV_COLS if c not in data.columns]
    if missing:
        logger.warning(
            "Missing expected OHLCV columns for %s: %s",
            symbol,
            ", ".join(missing),
        )
        return pd.DataFrame()

    data = data[EXPECTED_OHLCV_COLS].dropna(how="all")
    data.sort_index(inplace=True)
    return data


def fetch_ohlcv(symbol: str, *, interval: str | None = None, period: str | None = None) -> pd.DataFrame:
    """
    Fetch OHLCV candles for a single symbol from yfinance.

    Returns an empty DataFrame if data is unavailable.
    """

    interval = interval or DEFAULT_INTERVAL
    period = period or DEFAULT_PERIOD

    try:
        df = yf.download(
            symbol,
            interval=interval,
            period=period,
            progress=False,
            threads=False,
        )
    except Exception:
        logger.exception("yfinance download failed for %s (interval=%s, period=%s)", symbol, interval, period)
        return pd.DataFrame()

    if df is None or df.empty:
        logger.warning("No data returned for %s (interval=%s, period=%s)", symbol, interval, period)
        return pd.DataFrame()

    normalized = _normalize_ohlcv(df, symbol=symbol)
    if normalized.empty:
        logger.warning("No usable OHLCV data after normalization for %s", symbol)
    return normalized


def fetch_eurusd_x_and_gc_ohlcv(
    *, interval: str | None = None, period: str | None = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetch 1-hour OHLCV for:
      - EURUSD=X
      - GC=F

    Returns:
      (eurusd_df, gc_df)
    """

    eurusd_df = fetch_ohlcv("EURUSD=X", interval=interval, period=period)
    gc_df = fetch_ohlcv("GC=F", interval=interval, period=period)
    return eurusd_df, gc_df
