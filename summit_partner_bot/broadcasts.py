from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from aiogram import Bot
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter
from aiogram.types import FSInputFile

from summit_partner_bot.db import Database, normalize_target_role

logger = logging.getLogger(__name__)


def parse_iso_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def send_broadcast(bot: Bot, db: Database, broadcast_id: int) -> tuple[int, int]:
    broadcast = await db.get_broadcast(broadcast_id)
    if broadcast is None:
        return (0, 0)

    target_role = normalize_target_role(str(broadcast["target_role"]))
    target_subcategory = str(broadcast["target_subcategory"] or "").strip()
    user_ids = await db.list_authorized_user_ids(target_role, target_subcategory)
    if not user_ids:
        await db.set_broadcast_sent(broadcast_id, status="sent")
        return (0, 0)

    message_text = broadcast["message_text"]
    image_path = broadcast["image_path"]
    source_chat_id = broadcast["source_chat_id"]
    source_message_id = broadcast["source_message_id"]

    delivered = 0
    failed = 0

    for user_id in user_ids:
        try:
            delivered_message_id: int | None = None
            if source_chat_id and source_message_id:
                result = await bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=source_chat_id,
                    message_id=source_message_id,
                )
                delivered_message_id = int(result.message_id)
            elif image_path:
                result = await bot.send_photo(
                    user_id,
                    photo=FSInputFile(str(image_path)),
                    caption=message_text or None,
                )
                delivered_message_id = int(result.message_id)
            elif message_text:
                result = await bot.send_message(user_id, message_text)
                delivered_message_id = int(result.message_id)
            else:
                raise RuntimeError("Broadcast message has no payload")

            await db.add_delivery(
                broadcast_id=broadcast_id,
                telegram_id=user_id,
                status="delivered",
                delivered_message_id=delivered_message_id,
            )
            delivered += 1
        except TelegramRetryAfter as exc:
            await asyncio.sleep(exc.retry_after)
            try:
                delivered_message_id = None
                if source_chat_id and source_message_id:
                    result = await bot.copy_message(
                        chat_id=user_id,
                        from_chat_id=source_chat_id,
                        message_id=source_message_id,
                    )
                    delivered_message_id = int(result.message_id)
                elif image_path:
                    result = await bot.send_photo(
                        user_id,
                        photo=FSInputFile(str(image_path)),
                        caption=message_text or None,
                    )
                    delivered_message_id = int(result.message_id)
                elif message_text:
                    result = await bot.send_message(user_id, message_text)
                    delivered_message_id = int(result.message_id)
                await db.add_delivery(
                    broadcast_id=broadcast_id,
                    telegram_id=user_id,
                    status="delivered",
                    delivered_message_id=delivered_message_id,
                )
                delivered += 1
            except Exception as err:  # noqa: BLE001
                failed += 1
                await db.add_delivery(
                    broadcast_id=broadcast_id,
                    telegram_id=user_id,
                    status="failed",
                    error_text=str(err),
                )
        except TelegramForbiddenError as exc:
            failed += 1
            await db.add_delivery(
                broadcast_id=broadcast_id,
                telegram_id=user_id,
                status="failed",
                error_text=str(exc),
            )
        except Exception as exc:  # noqa: BLE001
            failed += 1
            await db.add_delivery(
                broadcast_id=broadcast_id,
                telegram_id=user_id,
                status="failed",
                error_text=str(exc),
            )

        await asyncio.sleep(0.04)

    status = "sent" if failed == 0 else "sent_with_errors"
    await db.set_broadcast_sent(broadcast_id, status=status)
    logger.info(
        "Broadcast %s completed. delivered=%s failed=%s",
        broadcast_id,
        delivered,
        failed,
    )
    return (delivered, failed)


class BroadcastScheduler:
    def __init__(self, bot: Bot, db: Database, poll_interval_seconds: int = 10) -> None:
        self.bot = bot
        self.db = db
        self.poll_interval_seconds = max(poll_interval_seconds, 3)
        self.tasks: dict[int, asyncio.Task[None]] = {}
        self._watcher_task: asyncio.Task[None] | None = None

    def schedule(self, broadcast_id: int, run_at: datetime) -> None:
        now = datetime.now(timezone.utc)
        delay = max((run_at - now).total_seconds(), 0)

        old_task = self.tasks.pop(broadcast_id, None)
        if old_task and not old_task.done():
            old_task.cancel()

        self.tasks[broadcast_id] = asyncio.create_task(self._runner(broadcast_id, delay))

    async def start(self) -> None:
        await self.restore()
        if self._watcher_task is None or self._watcher_task.done():
            self._watcher_task = asyncio.create_task(self._watcher())

    async def restore(self) -> None:
        pending = await self.db.get_pending_broadcasts()
        for row in pending:
            broadcast_id = int(row["id"])
            if broadcast_id in self.tasks:
                continue
            scheduled_at = row["scheduled_at"]
            if scheduled_at:
                run_at = parse_iso_datetime(scheduled_at)
            else:
                run_at = datetime.now(timezone.utc)
            self.schedule(broadcast_id, run_at)

    async def shutdown(self) -> None:
        if self._watcher_task:
            self._watcher_task.cancel()
            await asyncio.gather(self._watcher_task, return_exceptions=True)
            self._watcher_task = None
        for task in self.tasks.values():
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        self.tasks.clear()

    async def _runner(self, broadcast_id: int, delay: float) -> None:
        try:
            if delay > 0:
                await asyncio.sleep(delay)
            await send_broadcast(self.bot, self.db, broadcast_id)
        except asyncio.CancelledError:
            return
        except Exception:  # noqa: BLE001
            logger.exception("Broadcast task failed: %s", broadcast_id)
        finally:
            self.tasks.pop(broadcast_id, None)

    async def _watcher(self) -> None:
        while True:
            try:
                await self.restore()
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                logger.exception("Broadcast watcher loop failed")
            await asyncio.sleep(self.poll_interval_seconds)
