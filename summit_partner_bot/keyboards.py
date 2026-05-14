from __future__ import annotations

from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

from summit_partner_bot.db import ROLE_EXPERT, ROLE_INFLUENCER, ROLE_PARTNER

BTN_ABOUT = "ℹ️ О СТАММИТ26"
BTN_BUY_TICKET = "🎟 Купить билет"
BTN_FOR_PARTNERS = "🤝 Для партнеров"
BTN_FOR_INFLUENCERS = "📣 Для инфлюенсеров"
BTN_FOR_EXPERTS = "🎓 Для экспертов"
BTN_PROGRAM_PUBLIC = "🗓 Программа"
BTN_SPEAKERS = "🎤 Спикеры"
BTN_ROUTE = "🧭 Как добраться"
BTN_FAQ = "❓ Ответы на вопросы"
BTN_CLINIC_BOOST = "🚀 ПРОКАЧКА клиник"
BTN_MATCH_APP = "📱 Приложение МЭТЧ для участников"
BTN_CHANNEL = "📢 Канал СТАММИТ26"
BTN_SITE = "🌐 Сайт СТАММИТ26"
BTN_FEEDBACK = "💬 Оставить отзыв"
BTN_REFERRAL = "🎁 Пригласи коллег и выиграй айфон"
BTN_BOOTH_BOOKING = "🏗 Забронировать стенд"

PUBLIC_MENU_BUTTONS = [
    BTN_ABOUT,
    BTN_BUY_TICKET,
    BTN_FOR_PARTNERS,
    BTN_FOR_INFLUENCERS,
    BTN_FOR_EXPERTS,
    BTN_BOOTH_BOOKING,
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
BTN_INFLUENCER_CONDITIONS = "📋 Условия для инфлюенсеров"
BTN_INFLUENCER_APPLICATION = "📝 Заявка"

BTN_BACK = "⬅️ Назад"
BTN_CANCEL = "❌ Отмена"
BTN_TO_PUBLIC_MENU = "🏠 Общее меню"
BTN_REGISTER_NO_CODE = "📝 Подать заявку без кода"
BTN_SHARE_CONTACT = "📱 Поделиться контактом"
BTN_CONSENT_ACCEPT = "✅ Согласен"
BTN_CLOSE_CHAT = "❌ Завершить чат"
BTN_START_APPLICATION = "🚀 Старт"
BTN_INFLUENCER_ALREADY = "Я уже инфлюенсер проекта"
BTN_INFLUENCER_APPLY = "Хочу стать инфлюенсером СТАММИТ’26"
BTN_INFL_SKIP = "⏭ Пропустить"

BTN_EXPERT_ALREADY = "Я уже спикер"
BTN_EXPERT_APPLY = "Хочу оставить заявку на доклад"
BTN_EXPERT_OTHER = "Другое"

EXPERT_FORMATS = [
    "Онлайн-конференция",
    "Трек «Прокачка клиник»",
    "Экспертное участие в менторской зоне",
    "Выступление в открытом лектории",
    "Сцена СТАММИТ, 2 зал",
    "Круглые столы на СТАММИТ",
    "Другое",
]
EXPERT_AUDIENCES = [
    "Владельцы клиник",
    "Главные врачи",
    "Врачи-стоматологи",
    "Управляющие / администраторы",
    "Маркетологи клиник",
    "Студенты / ординаторы",
    "Партнёры и представители бизнеса",
    "Другое",
]
EXPERT_EXPERIENCES = [
    "Да",
    "Нет",
    "Выступал(а) онлайн",
    "Выступал(а) на крупных мероприятиях",
]

INFL_PLATFORMS = ["Instagram", "Telegram", "VK", "YouTube", "TikTok", "Другое"]
INFL_TOPICS = ["Стоматология", "Медицина", "Бизнес", "Маркетинг", "Личный бренд", "Образование", "Lifestyle", "Другое"]
INFL_COLLAB = [
    "Информационное партнёрство",
    "Бартер",
    "Промокод / партнёрская ссылка",
    "Съёмка контента на мероприятии",
    "Участие в квестах / активностях",
    "Другое",
]
INFL_FORMATS = [
    "Stories",
    "Reels / Shorts",
    "Пост",
    "Telegram-публикация",
    "YouTube-интеграция",
    "Прямой эфир",
    "Обзор мероприятия",
    "Другое",
]


def _chunk_buttons(items: list[str], width: int = 2) -> list[list[KeyboardButton]]:
    rows: list[list[KeyboardButton]] = []
    for idx in range(0, len(items), width):
        rows.append([KeyboardButton(text=item) for item in items[idx : idx + width]])
    return rows


def public_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=_chunk_buttons(PUBLIC_MENU_BUTTONS),
        resize_keyboard=True,
    )


def private_menu_keyboard(role: str, include_public_menu: bool = True) -> ReplyKeyboardMarkup:
    rows = _chunk_buttons([BTN_NEWS, BTN_PROGRAM, BTN_LINKS, BTN_MATERIALS])

    if role == ROLE_INFLUENCER:
        rows.extend(_chunk_buttons([BTN_INFLUENCER_CONDITIONS, BTN_INFLUENCER_APPLICATION]))

    bottom_row = [KeyboardButton(text=BTN_MANAGER)]
    if include_public_menu:
        bottom_row.append(KeyboardButton(text=BTN_TO_PUBLIC_MENU))
    rows.append(bottom_row)

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


def code_or_register_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_REGISTER_NO_CODE)],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def contact_request_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_SHARE_CONTACT, request_contact=True)],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def expert_start_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_EXPERT_ALREADY)],
            [KeyboardButton(text=BTN_EXPERT_APPLY)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def influencer_start_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_INFLUENCER_ALREADY)],
            [KeyboardButton(text=BTN_INFLUENCER_APPLY)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def options_keyboard(options: list[str], add_skip: bool = False) -> ReplyKeyboardMarkup:
    rows: list[list[KeyboardButton]] = []
    for i in range(0, len(options), 2):
        rows.append([KeyboardButton(text=opt) for opt in options[i:i + 2]])
    if add_skip:
        rows.append([KeyboardButton(text=BTN_INFL_SKIP)])
    rows.append([KeyboardButton(text=BTN_CANCEL)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=False)


def start_application_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_START_APPLICATION)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def support_chat_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_CLOSE_CHAT)]],
        resize_keyboard=True,
    )


def consent_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_CONSENT_ACCEPT)],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
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
