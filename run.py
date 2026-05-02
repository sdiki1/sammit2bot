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

    content_loader = ContentLoader(db=db, path=settings.content_file)
    await content_loader.bootstrap_defaults()

    if len(settings.bot_profiles) < 4:
        logging.warning(
            "Configured %s bot(s). Expected 4: summit, partner, expert, influencer.",
            len(settings.bot_profiles),
        )

    bots: list[Bot] = []
    schedulers: list[BroadcastScheduler] = []
    polling_tasks: list[asyncio.Task[None]] = []

    for profile in settings.bot_profiles:
        bot = Bot(token=profile.token)
        scheduler = BroadcastScheduler(bot=bot, db=db)
        dp = await create_dispatcher(
            bot=bot,
            db=db,
            settings=settings,
            content_loader=content_loader,
            scheduler=scheduler,
            profile=profile,
        )
        bots.append(bot)
        schedulers.append(scheduler)

        await bot.set_my_commands(
            [
                BotCommand(command="start", description="Запуск бота"),
                BotCommand(command="code", description="Ввести код приглашения"),
                BotCommand(command="menu", description="Открыть меню"),
                BotCommand(command="cancel", description="Отменить текущее действие"),
            ]
        )
        polling_tasks.append(asyncio.create_task(dp.start_polling(bot)))

        logging.info("Started polling for %s bot", profile.key)

    if schedulers:
        await schedulers[0].start()

    try:
        await asyncio.gather(*polling_tasks)
    finally:
        for task in polling_tasks:
            task.cancel()
        if polling_tasks:
            await asyncio.gather(*polling_tasks, return_exceptions=True)
        for scheduler in schedulers:
            await scheduler.shutdown()
        await db.close()
        for bot in bots:
            await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
