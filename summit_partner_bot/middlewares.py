from __future__ import annotations

from time import monotonic
from typing import Any, Awaitable, Callable

from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery, Message, TelegramObject


class RateLimitMiddleware(BaseMiddleware):
    def __init__(self, limit_seconds: float) -> None:
        self.limit_seconds = limit_seconds
        self._last_event_at: dict[int, float] = {}

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        if user is None:
            return await handler(event, data)

        now = monotonic()
        last_seen = self._last_event_at.get(user.id, 0.0)
        if now - last_seen < self.limit_seconds:
            if isinstance(event, Message):
                await event.answer("Слишком много запросов. Повторите через пару секунд.")
            elif isinstance(event, CallbackQuery):
                await event.answer("Слишком часто", show_alert=False)
            return None

        self._last_event_at[user.id] = now
        return await handler(event, data)

