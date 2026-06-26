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

YFINANCE_SYMBOL_BY_PAIR = {
    "EURUSD": "EURUSD=X",
    "XAUUSD": "GC=F",
    "GOLD": "GC=F",
    "NAS100": "NQ=F",
    "NASDAQ100": "NQ=F",
    "US100": "NQ=F",
    "US30": "YM=F",
    "DOW30": "YM=F",
    "SPX500": "ES=F",
    "US500": "ES=F",
    "BTCUSD": "BTC-USD",
    "ETHUSD": "ETH-USD",
}


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


def normalize_pair_key(pair: str) -> str:
    """Normalize common pair formats such as EUR/USD or xauusd."""

    return str(pair or "").strip().upper().replace("/", "").replace(" ", "")


def yfinance_symbol_for_pair(pair: str) -> str:
    """Map journal pair names to yfinance symbols."""

    key = normalize_pair_key(pair)
    if key in YFINANCE_SYMBOL_BY_PAIR:
        return YFINANCE_SYMBOL_BY_PAIR[key]
    if key.endswith("USD") and len(key) == 6:
        return f"{key}=X"
    return key


def fetch_latest_price(pair: str) -> float | None:
    """Fetch the latest close for a journal pair using yfinance."""

    symbol = yfinance_symbol_for_pair(pair)
    df = fetch_ohlcv(symbol, interval="1m", period="1d")
    if df.empty:
        df = fetch_ohlcv(symbol, interval=DEFAULT_INTERVAL, period="5d")
    if df.empty or "Close" not in df.columns:
        return None
    close = df["Close"].dropna()
    if close.empty:
        return None
    try:
        return float(close.iloc[-1])
    except (TypeError, ValueError):
        logger.warning("Latest close for %s/%s could not be converted to float", pair, symbol)
        return None


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
