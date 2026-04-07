"""Entry point for the Forex Telegram bot."""

from __future__ import annotations

import asyncio
import logging

from aiogram import Bot, Dispatcher

from handlers import router
from bot.scheduler import setup_scheduler
from config import TELEGRAM_BOT_TOKEN, TRADER_CHAT_ID
from storage.db import init_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN is not set; cannot start bot.")
        return

    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    await init_db()

    chat_id = TRADER_CHAT_ID.strip() if TRADER_CHAT_ID else ""
    loop = asyncio.get_running_loop()
    scheduler = setup_scheduler(bot, chat_id, event_loop=loop)
    scheduler.start()
    logger.info("AsyncIOScheduler started on the same event loop as aiogram polling")

    if chat_id:
        try:
            await bot.send_message(
                int(chat_id),
                "🤖 Bot is online and watching the markets for you.",
            )
        except Exception:
            logger.exception("Startup message to TRADER_CHAT_ID failed")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
