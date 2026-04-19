from __future__ import annotations

from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

from summit_partner_bot.db import ROLE_EXPERT, ROLE_INFLUENCER, ROLE_PARTNER

BTN_ABOUT = "О СТАММИТ26"
BTN_BUY_TICKET = "Купить билет"
BTN_FOR_PARTNERS = "Для партнеров"
BTN_FOR_INFLUENCERS = "Для инфлюенсеров"
BTN_FOR_EXPERTS = "Для экспертов"
BTN_PROGRAM_PUBLIC = "Программа"
BTN_SPEAKERS = "Спикеры"
BTN_ROUTE = "Как добраться"
BTN_FAQ = "Ответы на вопросы"
BTN_CLINIC_BOOST = "ПРОКАЧКА клиник"
BTN_MATCH_APP = "Приложение МЭТЧ для участников"
BTN_CHANNEL = "Канал СТАММИТ26"
BTN_SITE = "Сайт СТАММИТ26"
BTN_FEEDBACK = "Оставить отзыв"
BTN_REFERRAL = "Пригласи коллег и выиграй айфон"

PUBLIC_MENU_BUTTONS = [
    BTN_ABOUT,
    BTN_BUY_TICKET,
    BTN_FOR_PARTNERS,
    BTN_FOR_INFLUENCERS,
    BTN_FOR_EXPERTS,
    BTN_PROGRAM_PUBLIC,
    BTN_SPEAKERS,
    BTN_ROUTE,
    BTN_FAQ,
    BTN_CLINIC_BOOST,
    BTN_MATCH_APP,
    BTN_CHANNEL,
    BTN_SITE,
    BTN_FEEDBACK,
    BTN_REFERRAL,
]

BTN_NEWS = "📢 Новости и объявления"
BTN_PROGRAM = "📅 Программа саммита"
BTN_LINKS = "🔗 Полезные ссылки"
BTN_MANAGER = "🧑‍💼 Связаться с менеджером"
BTN_MATERIALS = "📎 Материалы"
BTN_INFLUENCER_CONDITIONS = "Условия для инфлюенсеров"
BTN_INFLUENCER_APPLICATION = "Заявка"

BTN_BACK = "⬅️ Назад"
BTN_CANCEL = "Отмена"
BTN_TO_PUBLIC_MENU = "🏠 Общее меню"


def public_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_ABOUT), KeyboardButton(text=BTN_BUY_TICKET)],
            [KeyboardButton(text=BTN_FOR_PARTNERS), KeyboardButton(text=BTN_FOR_INFLUENCERS)],
            [KeyboardButton(text=BTN_FOR_EXPERTS), KeyboardButton(text=BTN_PROGRAM_PUBLIC)],
            [KeyboardButton(text=BTN_SPEAKERS), KeyboardButton(text=BTN_ROUTE)],
            [KeyboardButton(text=BTN_FAQ), KeyboardButton(text=BTN_CLINIC_BOOST)],
            [KeyboardButton(text=BTN_MATCH_APP), KeyboardButton(text=BTN_CHANNEL)],
            [KeyboardButton(text=BTN_SITE), KeyboardButton(text=BTN_FEEDBACK)],
            [KeyboardButton(text=BTN_REFERRAL)],
        ],
        resize_keyboard=True,
    )


def private_menu_keyboard(role: str) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text=BTN_NEWS), KeyboardButton(text=BTN_PROGRAM)],
        [KeyboardButton(text=BTN_LINKS), KeyboardButton(text=BTN_MATERIALS)],
    ]

    if role == ROLE_INFLUENCER:
        rows.append(
            [
                KeyboardButton(text=BTN_INFLUENCER_CONDITIONS),
                KeyboardButton(text=BTN_INFLUENCER_APPLICATION),
            ]
        )

    rows.append([KeyboardButton(text=BTN_MANAGER), KeyboardButton(text=BTN_TO_PUBLIC_MENU)])

    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
    )


def cancel_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_CANCEL)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def back_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_BACK)]],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def section_keyboard(titles: list[str], include_back: bool = True) -> ReplyKeyboardMarkup:
    rows: list[list[KeyboardButton]] = []
    cleaned = [title.strip() for title in titles if title.strip()]

    for idx in range(0, len(cleaned), 2):
        chunk = cleaned[idx : idx + 2]
        rows.append([KeyboardButton(text=item) for item in chunk])

    if include_back:
        rows.append([KeyboardButton(text=BTN_BACK)])

    return ReplyKeyboardMarkup(
        keyboard=rows or [[KeyboardButton(text=BTN_BACK)]],
        resize_keyboard=True,
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
