"""Reply and inline keyboards for Telegram."""

from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup

# Callback data for trade reasons matches button text exactly (Telegram limit: 64 bytes).
TRADE_REASON_LABELS: tuple[str, ...] = (
    "🏗 Structure break",
    "💧 Liquidity sweep",
    "📊 EMA bounce",
    "🎯 S/R level",
    "📰 News play",
    "📐 Trend continuation",
)


def main_reply_keyboard() -> ReplyKeyboardMarkup:
    # Returns a ReplyKeyboardMarkup that sits permanently at the bottom of Telegram chat.
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Today's Brief"), KeyboardButton(text="📍 Key Levels")],
            [KeyboardButton(text="🎯 Should I Trade?"), KeyboardButton(text="📰 News Today")],
            [KeyboardButton(text="📓 Log Trade"), KeyboardButton(text="📈 My Stats")],
            [KeyboardButton(text="🔒 Close Trade"), KeyboardButton(text="❓ Help")],
        ],
        resize_keyboard=True,
        is_persistent=True,
        one_time_keyboard=False,
    )


def reason_keyboard() -> InlineKeyboardMarkup:
    """Inline keyboard: trade reason taxonomy, 2 buttons per row; callback_data == button text."""
    a, b, c, d, e, f = TRADE_REASON_LABELS
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=a, callback_data=a),
                InlineKeyboardButton(text=b, callback_data=b),
            ],
            [
                InlineKeyboardButton(text=c, callback_data=c),
                InlineKeyboardButton(text=d, callback_data=d),
            ],
            [
                InlineKeyboardButton(text=e, callback_data=e),
                InlineKeyboardButton(text=f, callback_data=f),
            ],
        ]
    )
