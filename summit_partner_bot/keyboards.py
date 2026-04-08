from __future__ import annotations

from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

BTN_NEWS = "📢 Новости и объявления"
BTN_PROGRAM = "📅 Программа саммита"
BTN_LINKS = "🔗 Полезные ссылки"
BTN_MANAGER = "🧑‍💼 Связаться с менеджером"
BTN_MATERIALS = "📎 Материалы для партнёров"
BTN_CANCEL = "Отмена"


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_NEWS), KeyboardButton(text=BTN_PROGRAM)],
            [KeyboardButton(text=BTN_LINKS), KeyboardButton(text=BTN_MATERIALS)],
            [KeyboardButton(text=BTN_MANAGER)],
        ],
        resize_keyboard=True,
    )


def cancel_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_CANCEL)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def url_keyboard(items: list[dict[str, str]]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for item in items:
        title = item.get("title", "").strip()
        url = item.get("url", "").strip()
        if not title or not url:
            continue
        rows.append([InlineKeyboardButton(text=title, url=url)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

