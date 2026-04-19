from __future__ import annotations

import asyncio
import logging

from aiogram import Bot
from aiogram.types import BotCommand

from summit_partner_bot.bot import create_dispatcher
from summit_partner_bot.broadcasts import BroadcastScheduler
from summit_partner_bot.config import load_settings
from summit_partner_bot.content import ContentLoader
from summit_partner_bot.db import Database


async def main() -> None:
    settings = load_settings()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    db = Database(settings.database_url)
    await db.connect()
    await db.init_schema()

    bot = Bot(token=settings.bot_token)
    scheduler = BroadcastScheduler(bot=bot, db=db)
    content_loader = ContentLoader(db=db, path=settings.content_file)
    await content_loader.bootstrap_defaults()
    dp = await create_dispatcher(
        bot=bot,
        db=db,
        settings=settings,
        content_loader=content_loader,
        scheduler=scheduler,
    )

    await scheduler.start()

    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Запуск бота"),
            BotCommand(command="code", description="Ввести код приглашения"),
            BotCommand(command="menu", description="Открыть меню"),
            BotCommand(command="cancel", description="Отменить текущее действие"),
        ]
    )

    try:
        await dp.start_polling(bot)
    finally:
        await scheduler.shutdown()
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
