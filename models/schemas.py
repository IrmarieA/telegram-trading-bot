"""Pydantic or dataclass schemas for API and AI outputs."""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class Scenario(BaseModel):
    label: str  # e.g. 'Bull case'
    trigger: str  # what price action confirms entry
    entry_zone: str  # price range to enter
    stop_loss: str  # where to place stop
    target: str  # profit target
    risk_reward: str  # e.g. '1:2.5'


class MarketBrief(BaseModel):
    pair: str  # 'EURUSD' or 'XAUUSD'
    bias: Literal["bullish", "bearish", "ranging", "neutral"]
    confidence: int = Field(ge=1, le=5)
    summary: str  # 2-3 sentence plain English overview
    avoid_reason: Optional[str] = None  # populated if confidence < 3
    scenarios: list[Scenario] = Field(min_length=1, max_length=3)
    risk_warning: Optional[str] = None  # news or conditions increasing risk
    one_line_advice: str  # single sentence for the trader
