"""Application configuration from environment (see `.env`)."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load `.env` from the forex_bot package root (same directory as this file).
_BASE = Path(__file__).resolve().parent
load_dotenv(_BASE / ".env")

# YFinance
YFINANCE_INTERVAL = os.getenv("YFINANCE_INTERVAL", "1h")
YFINANCE_PERIOD = os.getenv("YFINANCE_PERIOD", "60d")

# Telegram — BOT_TOKEN or TELEGRAM_BOT_TOKEN (empty string if unset)
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()

# Primary chat for alerts — CHAT_ID or TRADER_CHAT_ID
TRADER_CHAT_ID = (os.getenv("TRADER_CHAT_ID") or os.getenv("CHAT_ID") or "").strip()

# OpenAI — OPENAI_API_KEY or API_KEY
OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or os.getenv("API_KEY") or "").strip()

# Optional third-party keys
ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY", "").strip()
