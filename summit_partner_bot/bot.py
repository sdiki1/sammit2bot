from __future__ import annotations

import csv
import io
import logging
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramForbiddenError
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BufferedInputFile, InlineKeyboardMarkup, Message, ReplyKeyboardMarkup

from summit_partner_bot.broadcasts import BroadcastScheduler, parse_iso_datetime, send_broadcast
from summit_partner_bot.config import BotProfile, Settings
from summit_partner_bot.content import ContentLoader
from summit_partner_bot.db import (
    ROLE_ALL,
    ROLE_EXPERT,
    ROLE_INFLUENCER,
    ROLE_PARTNER,
    APPLICATION_STATUS_IN_PROGRESS,
    SECTION_EXPERT_MATERIALS,
    SECTION_EXPERT_USEFUL_LINKS,
    SECTION_INFLUENCER_MATERIALS,
    SECTION_INFLUENCER_USEFUL_LINKS,
    SECTION_PARTNER_MATERIALS,
    SECTION_PARTNER_USEFUL_LINKS,
    SECTION_PUBLIC_MENU_LINKS,
    STATUS_APPROVED,
    STATUS_PENDING,
    STATUS_REJECTED,
    Database,
    normalize_code,
    normalize_role,
    normalize_subcategory,
    normalize_target_role,
    parse_target_role_subcategory,
    role_title,
)
from summit_partner_bot.keyboards import (
    BTN_ABOUT,
    BTN_BACK,
    BTN_BOOTH_BOOKING,
    BTN_BUY_TICKET,
    BTN_CANCEL,
    BTN_CHANNEL,
    BTN_CLINIC_BOOST,
    BTN_FAQ,
    BTN_FEEDBACK,
    BTN_FOR_EXPERTS,
    BTN_FOR_INFLUENCERS,
    BTN_FOR_PARTNERS,
    BTN_INFLUENCER_APPLICATION,
    BTN_INFLUENCER_CONDITIONS,
    BTN_LINKS,
    BTN_MANAGER,
    BTN_MATERIALS,
    BTN_MATCH_APP,
    BTN_NEWS,
    BTN_PROGRAM,
    BTN_PROGRAM_PUBLIC,
    BTN_REFERRAL,
    BTN_ROUTE,
    BTN_SITE,
    BTN_SPEAKERS,
    BTN_TO_PUBLIC_MENU,
    PUBLIC_MENU_BUTTONS,
    cancel_keyboard,
    private_menu_keyboard,
    public_menu_keyboard,
    section_keyboard,
    url_keyboard,
)
from summit_partner_bot.middlewares import RateLimitMiddleware
from summit_partner_bot.states import AccessRequestFlow, BoothBookingFlow, FeedbackFlow, NavigationFlow, SupportFlow

logger = logging.getLogger(__name__)
USER_MARKER_RE = re.compile(r"#USER_(\d+)")
REF_START_RE = re.compile(r"^ref_(.+)$", re.IGNORECASE)
APP_START_RE = re.compile(r"^app_([0-9a-fA-F]{8,64})$", re.IGNORECASE)
PHONE_RE = re.compile(r"^[+\d][\d\s\-()]{6,}$")
CAPTION_PREFIX_RE = re.compile(r"^[^0-9A-Za-zА-Яа-яЁё]+")

PUBLIC_LINKABLE_BUTTONS = {
    BTN_ABOUT,
    BTN_BUY_TICKET,
    BTN_PROGRAM_PUBLIC,
    BTN_SPEAKERS,
    BTN_ROUTE,
    BTN_FAQ,
    BTN_CLINIC_BOOST,
    BTN_MATCH_APP,
    BTN_CHANNEL,
    BTN_SITE,
}

ROLE_ENTRY_BUTTONS = {
    BTN_FOR_PARTNERS: ROLE_PARTNER,
    BTN_FOR_EXPERTS: ROLE_EXPERT,
    BTN_FOR_INFLUENCERS: ROLE_INFLUENCER,
}

ALL_MAIN_BUTTONS = set(PUBLIC_MENU_BUTTONS) | {
    BTN_NEWS,
    BTN_PROGRAM,
    BTN_LINKS,
    BTN_MATERIALS,
    BTN_BOOTH_BOOKING,
    BTN_MANAGER,
    BTN_INFLUENCER_CONDITIONS,
    BTN_INFLUENCER_APPLICATION,
    BTN_BACK,
    BTN_CANCEL,
    BTN_TO_PUBLIC_MENU,
}


def _extract_command_payload(text: str) -> str:
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return ""
    return parts[1].strip()


def _extract_possible_code(raw_text: str) -> str:
    text = raw_text.strip()
    if not text:
        return ""

    lowered = text.lower()
    if "start=" in lowered:
        _, _, chunk = lowered.partition("start=")
        if chunk:
            original_tail = text[-len(chunk):]
            text = original_tail.strip()

    if text.startswith("http://") or text.startswith("https://") or "t.me/" in text:
        url_value = text if text.startswith(("http://", "https://")) else f"https://{text.lstrip('/')}"
        try:
            parsed = urlparse(url_value)
            query = parse_qs(parsed.query)
            start_value = (
                query.get("start", [None])[0]
                or query.get("startapp", [None])[0]
                or query.get("code", [None])[0]
            )
            if start_value:
                text = str(start_value).strip()
        except Exception:  # noqa: BLE001
            pass

    if text.startswith("/"):
        parts = text.split(maxsplit=1)
        if len(parts) == 1:
            return ""
        text = parts[1].strip()

    text = text.split("&", maxsplit=1)[0]
    text = text.split("#", maxsplit=1)[0]

    text = text.strip(" \t\n\r'\"`.,;:!?()[]{}<>")
    if not text:
        return ""
    text = text.split()[0].strip()
    if not text:
        return ""
    return normalize_code(text)


def _looks_like_access_code_candidate(raw_text: str) -> bool:
    code = _extract_possible_code(raw_text)
    if not code:
        return False
    if code.startswith("REF_"):
        return False
    if len(code) < 4 or len(code) > 64:
        return False
    return all(ch.isalnum() or ch in {"_", "-", "."} for ch in code)


def _normalize_caption_for_match(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    text = CAPTION_PREFIX_RE.sub("", text).strip()
    text = re.sub(r"\s+", " ", text)
    return text.casefold()


def _is_admin(message: Message, settings: Settings) -> bool:
    return bool(message.from_user and message.from_user.id in settings.admin_ids)


def _format_welcome(first_name: str, summit_name: str, role: str, content: dict) -> str:
    default_template = (
        "👋 Добро пожаловать, {first_name}!\n"
        "Вы авторизованы как {role_title} саммита {summit_name}.\n\n"
        "Используйте меню ниже для быстрого доступа к информации."
    )
    template = str(content.get("welcome_template", default_template))
    values = {
        "first_name": first_name,
        "summit_name": summit_name,
        "role_title": role_title(role),
    }
    try:
        return template.format(**values)
    except KeyError:
        return default_template.format(**values)


def _program_keyboard(content: dict) -> InlineKeyboardMarkup | None:
    program = content.get("program", {})
    if not isinstance(program, dict):
        return None
    url = str(program.get("url", "")).strip()
    title = str(program.get("title", "Открыть программу")).strip() or "Открыть программу"
    if not url:
        return None
    return url_keyboard([{"title": title, "url": url}])


def _role_sections(role: str) -> tuple[str, str]:
    if role == ROLE_EXPERT:
        return (SECTION_EXPERT_USEFUL_LINKS, SECTION_EXPERT_MATERIALS)
    if role == ROLE_INFLUENCER:
        return (SECTION_INFLUENCER_USEFUL_LINKS, SECTION_INFLUENCER_MATERIALS)
    return (SECTION_PARTNER_USEFUL_LINKS, SECTION_PARTNER_MATERIALS)


def _group_links_by_title(items: list[dict[str, str]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in items:
        title = str(item.get("title", "")).strip()
        url = str(item.get("url", "")).strip()
        if title and url:
            result[title] = url
    return result


def _get_manager_contact(content: dict[str, object], role: str) -> dict[str, str]:
    contacts = content.get("manager_contacts", {})
    if isinstance(contacts, dict):
        candidate = contacts.get(role, {})
        if isinstance(candidate, dict):
            title = str(candidate.get("title", "")).strip()
            url = str(candidate.get("url", "")).strip()
            if title and url:
                return {"title": title, "url": url}

    fallback = content.get("manager_contact", {})
    if isinstance(fallback, dict):
        title = str(fallback.get("title", "")).strip()
        url = str(fallback.get("url", "")).strip()
        if title and url:
            return {"title": title, "url": url}

    return {}


def _role_bot_username(settings: Settings, role: str) -> str:
    normalized = normalize_role(role)
    if normalized == ROLE_EXPERT:
        return settings.expert_bot_username
    if normalized == ROLE_INFLUENCER:
        return settings.influencer_bot_username
    return settings.partner_bot_username


def _role_bot_link(settings: Settings, role: str) -> str:
    username = _role_bot_username(settings, role).strip().lstrip("@")
    if not username:
        return ""
    return f"https://t.me/{username}"


async def _send_role_bot_transition(message: Message, settings: Settings, role: str) -> bool:
    link = _role_bot_link(settings, role)
    if not link:
        return False

    title = role_title(role)
    await message.answer(
        f"Перейдите в отдельный бот для роли «{title}».",
        reply_markup=url_keyboard([{"title": f"Открыть бот: {title}", "url": link}]),
    )
    return True


def _parse_broadcast_target(payload: str) -> tuple[str, str, str]:
    text = payload.strip()
    if not text:
        return (ROLE_ALL, "", "")
    parts = text.split(maxsplit=1)
    candidate, subcategory = parse_target_role_subcategory(parts[0])
    if candidate != ROLE_ALL or parts[0].lower() == ROLE_ALL or parts[0].lower().startswith(f"{ROLE_ALL}:"):
        body = parts[1].strip() if len(parts) > 1 else ""
        return (candidate, subcategory, body)
    return (ROLE_ALL, "", text)


async def _send_link_or_file(message: Message, title: str, value: str) -> None:
    if value.startswith("file_id:"):
        file_id = value.split(":", maxsplit=1)[1].strip()
        if file_id:
            await message.answer_document(document=file_id, caption=title)
            return
    if value.startswith("photo_id:"):
        file_id = value.split(":", maxsplit=1)[1].strip()
        if file_id:
            await message.answer_photo(photo=file_id, caption=title)
            return
    if value.startswith("video_id:"):
        file_id = value.split(":", maxsplit=1)[1].strip()
        if file_id:
            await message.answer_video(video=file_id, caption=title)
            return

    await message.answer(
        f"🔗 {title}",
        reply_markup=url_keyboard([{"title": "Открыть", "url": value}]),
    )


async def _show_public_menu(message: Message, content_loader: ContentLoader) -> None:
    content = await content_loader.load()
    text = str(content.get("public_welcome_text", "Добро пожаловать!"))
    hint = "\n\n🔐 Для приватных разделов нажмите нужную роль и отправьте код приглашения."
    if "код" not in text.lower():
        text += hint
    await message.answer(text, reply_markup=public_menu_keyboard())


async def _show_private_menu(
    message: Message,
    db: Database,
    settings: Settings,
    content_loader: ContentLoader,
    include_public_menu: bool = True,
) -> None:
    if not message.from_user:
        return

    user_row = await db.get_user(message.from_user.id)
    if user_row is None or str(user_row["access_status"]) != STATUS_APPROVED:
        await _show_public_menu(message, content_loader)
        return

    await db.update_user_profile(
        telegram_id=message.from_user.id,
        username=message.from_user.username,
        first_name=message.from_user.first_name,
    )

    content = await content_loader.load()
    role = normalize_role(str(user_row["role"]))
    welcome = _format_welcome(
        first_name=message.from_user.first_name or str(user_row["first_name"] or "участник"),
        summit_name=settings.summit_name,
        role=role,
        content=content,
    )
    await message.answer(welcome, reply_markup=private_menu_keyboard(role, include_public_menu=include_public_menu))


async def _deny_access(message: Message, content_loader: ContentLoader) -> None:
    content = await content_loader.load()
    restricted_text = str(content.get("restricted_text", "Доступ ограничен."))
    await message.answer(
        f"🚫 {restricted_text}\n\n"
        "Для входа нужен персональный deep-link от организатора:\n"
        "`https://t.me/<bot>?start=ВАШ_КОД`",
    )


async def _notify_access_request(
    bot: Bot,
    settings: Settings,
    user_id: int,
    role: str,
    subcategory: str | None,
    full_name: str,
    phone: str,
    company: str | None,
    inn: str | None,
    code: str,
) -> None:
    lines = [
        "🆕 Новая заявка на доступ",
        f"Роль: {role_title(role)} ({role})",
        f"Подкатегория: {subcategory or '—'}",
        f"Telegram ID: {user_id}",
        f"ФИО: {full_name or '—'}",
        f"Телефон: {phone or '—'}",
        f"Компания: {company or '—'}",
        f"ИНН: {inn or '—'}",
        f"Код приглашения: {code}",
        "",
        f"Подтвердить: /approve_user {user_id}",
        f"Отклонить: /reject_user {user_id} причина",
    ]
    text = "\n".join(lines)

    targets = set(settings.admin_ids) | set(settings.support_chat_ids)
    for chat_id in targets:
        try:
            await bot.send_message(chat_id=chat_id, text=text)
        except Exception:  # noqa: BLE001
            logger.exception("Failed to notify access request to %s", chat_id)


async def _notify_application(
    bot: Bot,
    settings: Settings,
    application_id: int,
    user_id: int | None,
    source: str,
    role: str,
    request_text: str | None,
    booth_number: str | None,
    full_name: str | None,
    phone: str | None,
    company: str | None,
    inn: str | None,
) -> None:
    lines = [
        "🆕 Новая заявка / бронь стенда",
        f"ID: {application_id}",
        f"Источник: {source}",
        f"Роль: {role_title(role)} ({role})",
        f"Telegram ID: {user_id or '—'}",
        f"Стенд: {booth_number or '—'}",
        f"Компания: {company or '—'}",
        f"ИНН: {inn or '—'}",
        f"Контакт: {full_name or '—'}",
        f"Телефон: {phone or '—'}",
        "",
        request_text or "Сообщение не указано.",
    ]
    text = "\n".join(lines)

    targets = set(settings.admin_ids) | set(settings.support_chat_ids)
    for chat_id in targets:
        try:
            await bot.send_message(chat_id=chat_id, text=text)
        except Exception:  # noqa: BLE001
            logger.exception("Failed to notify application to %s", chat_id)


async def _start_access_flow(message: Message, state: FSMContext, code_row: dict | object) -> None:
    role = normalize_role(str(code_row["role"]))
    subcategory = normalize_subcategory(str(code_row["subcategory"] or ""))
    code = str(code_row["code"])
    description = str(code_row["description"] or "").strip()

    await state.set_data({"requested_role": role, "requested_subcategory": subcategory, "access_code": code})

    if role == ROLE_PARTNER:
        await state.set_state(AccessRequestFlow.waiting_partner_inn)
        intro = "🤝 Заявка партнёра."
        if description:
            intro += f"\nКомпания в приглашении: {description}"
        await message.answer(
            f"{intro}\n\n"
            "Введите ИНН компании (10 или 12 цифр):",
            reply_markup=cancel_keyboard(),
        )
        return

    await state.set_state(AccessRequestFlow.waiting_name)
    role_caption = "эксперта" if role == ROLE_EXPERT else "инфлюенсера"
    await message.answer(
        f"✅ Код подтверждён для роли {role_caption}.\n"
        "Введите ваше имя и фамилию:",
        reply_markup=cancel_keyboard(),
    )


async def _process_access_code_input(
    message: Message,
    state: FSMContext,
    db: Database,
    raw_input: str,
    expected_role: str | None = None,
) -> bool:
    code = _extract_possible_code(raw_input)
    if not code:
        await message.answer(
            "⚠️ Не удалось распознать код.\n"
            "Отправьте код одним сообщением или ссылку `...start=КОД`.",
            reply_markup=cancel_keyboard(),
        )
        return False

    code_row = await db.get_access_code(code)
    if code_row is None:
        await message.answer(
            "🚫 Код не найден или неактивен.\n"
            "Проверьте код и отправьте ещё раз. Если не получается, запросите новый у организатора.",
            reply_markup=cancel_keyboard(),
        )
        return False

    code_role = normalize_role(str(code_row["role"]))
    if expected_role is not None and code_role != normalize_role(expected_role):
        await message.answer(
            f"⚠️ Этот код относится к роли «{role_title(code_role)}».\n"
            f"Для раздела «{role_title(expected_role)}» нужен другой код.",
            reply_markup=cancel_keyboard(),
        )
        return False

    await _start_access_flow(message, state, code_row)
    return True


async def _complete_request(
    message: Message,
    state: FSMContext,
    db: Database,
    settings: Settings,
    content_loader: ContentLoader,
    full_name: str,
    phone: str,
    company: str | None = None,
    inn: str | None = None,
) -> None:
    if not message.from_user:
        return

    data = await state.get_data()
    role = normalize_role(str(data.get("requested_role", ROLE_PARTNER)))
    subcategory = normalize_subcategory(str(data.get("requested_subcategory", "")))
    code = normalize_code(str(data.get("access_code", "")))

    if not code:
        await state.clear()
        await message.answer("⌛ Сессия истекла. Запустите /start по ссылке-приглашению заново.")
        return

    referred_by = await db.get_referrer_for_guest(message.from_user.id)

    await db.upsert_access_request(
        telegram_id=message.from_user.id,
        username=message.from_user.username,
        first_name=message.from_user.first_name,
        role=role,
        subcategory=subcategory,
        access_code=code,
        full_name=full_name,
        phone=phone,
        company=company,
        inn=inn,
        referred_by=referred_by,
    )

    await _notify_access_request(
        bot=message.bot,
        settings=settings,
        user_id=message.from_user.id,
        role=role,
        subcategory=subcategory,
        full_name=full_name,
        phone=phone,
        company=company,
        inn=inn,
        code=code,
    )

    content = await content_loader.load()
    await state.clear()

    await message.answer(
        "✅ Заявка отправлена организатору."
        "\nПосле подтверждения вы получите уведомление и доступ к меню.",
        reply_markup=public_menu_keyboard(),
    )

    if role == ROLE_PARTNER:
        presentation_url = str(content.get("partner_presentation_url", "")).strip()
        if presentation_url:
            await message.answer(
                "📎 Презентация для партнёров доступна уже сейчас:",
                reply_markup=url_keyboard(
                    [{"title": "Открыть презентацию", "url": presentation_url}]
                ),
            )

    if role in (ROLE_EXPERT, ROLE_INFLUENCER):
        key = "expert_form_url" if role == ROLE_EXPERT else "influencer_form_url"
        form_url = str(content.get(key, "")).strip()
        if form_url:
            await message.answer(
                "📝 Заполните форму для ускорения согласования:",
                reply_markup=url_keyboard([{"title": "Открыть форму", "url": form_url}]),
            )


async def _continue_application_access_flow(
    message: Message,
    state: FSMContext,
    db: Database,
    settings: Settings,
    content_loader: ContentLoader,
    application: dict,
) -> None:
    if not message.from_user:
        return

    role = normalize_role(str(application.get("role", ROLE_PARTNER)))
    access_code = f"APP{application.get('id')}"
    full_name = str(application.get("full_name") or "").strip()
    phone = str(application.get("phone") or "").strip()
    company = str(application.get("company") or "").strip()
    inn = str(application.get("inn") or "").strip()

    await state.set_data(
        {
            "requested_role": role,
            "requested_subcategory": "",
            "access_code": access_code,
            "partner_inn": inn,
            "partner_company": company,
            "partner_contact_name": full_name,
            "full_name": full_name,
        }
    )

    if role == ROLE_PARTNER:
        if not inn or not inn.isdigit() or len(inn) not in (10, 12):
            await state.set_state(AccessRequestFlow.waiting_partner_inn)
            await message.answer("Введите ИНН компании (10 или 12 цифр):", reply_markup=cancel_keyboard())
            return
        if not company:
            await state.set_state(AccessRequestFlow.waiting_partner_company)
            await message.answer("🏢 Введите наименование компании:", reply_markup=cancel_keyboard())
            return
        if not full_name:
            await state.set_state(AccessRequestFlow.waiting_partner_contact_name)
            await message.answer("👤 Введите имя контактного лица:", reply_markup=cancel_keyboard())
            return
        if not phone or not PHONE_RE.match(phone):
            await state.set_state(AccessRequestFlow.waiting_partner_phone)
            await message.answer("📞 Введите телефон контактного лица (пример: +79991234567):", reply_markup=cancel_keyboard())
            return

        await _complete_request(
            message=message,
            state=state,
            db=db,
            settings=settings,
            content_loader=content_loader,
            full_name=full_name,
            phone=phone,
            company=company,
            inn=inn,
        )
        return

    if not full_name:
        await state.set_state(AccessRequestFlow.waiting_name)
        await message.answer("Введите ваше имя и фамилию:", reply_markup=cancel_keyboard())
        return
    if not phone or not PHONE_RE.match(phone):
        await state.set_state(AccessRequestFlow.waiting_phone)
        await message.answer("📞 Введите телефон (пример: +79991234567):", reply_markup=cancel_keyboard())
        return

    await _complete_request(
        message=message,
        state=state,
        db=db,
        settings=settings,
        content_loader=content_loader,
        full_name=full_name,
        phone=phone,
    )


async def _handle_application_start(
    message: Message,
    state: FSMContext,
    db: Database,
    settings: Settings,
    content_loader: ContentLoader,
    token: str,
    include_public_menu: bool = True,
) -> bool:
    if not message.from_user:
        return False

    row = await db.get_application_by_token(token)
    if row is None:
        await message.answer(
            "⚠️ Заявка по этой ссылке не найдена. Проверьте ссылку или заполните форму заново.",
            reply_markup=public_menu_keyboard(),
        )
        return True

    updated = await db.attach_application_telegram(int(row["id"]), message.from_user.id)
    application = dict(updated or row)
    request_text = str(application.get("request_text") or "").strip()
    booth_number = str(application.get("booth_number") or "").strip()

    await message.answer(
        "👋 Добро пожаловать в бот партнёров СТАММИТ26.\n"
        "Мы нашли вашу заявку с сайта и привязали её к этому Telegram-чату."
    )
    if request_text or booth_number:
        lines = ["Ваше сообщение с сайта:"]
        if request_text:
            lines.append(request_text)
        if booth_number:
            lines.append(f"Стенд: {booth_number}")
        await message.answer("\n".join(lines))

    user_row = await db.get_user(message.from_user.id)
    if user_row is not None and str(user_row["access_status"]) == STATUS_APPROVED:
        await _show_private_menu(
            message,
            db,
            settings,
            content_loader,
            include_public_menu=include_public_menu,
        )
        return True

    await _continue_application_access_flow(
        message=message,
        state=state,
        db=db,
        settings=settings,
        content_loader=content_loader,
        application=application,
    )
    return True


async def _ensure_private_user(
    message: Message,
    db: Database,
    content_loader: ContentLoader,
    required_role: str | None = None,
    include_public_menu: bool = True,
) -> dict | None:
    if not message.from_user:
        return None
    user_row = await db.get_user(message.from_user.id)
    if user_row is None:
        await _deny_access(message, content_loader)
        return None

    status = str(user_row["access_status"])
    if status == STATUS_PENDING:
        await message.answer(
            "⏳ Ваша заявка ещё на согласовании у организатора.",
            reply_markup=public_menu_keyboard() if include_public_menu else None,
        )
        return None
    if status == STATUS_REJECTED:
        reason = str(user_row["rejection_reason"] or "").strip()
        text = "🚫 В доступе отказано."
        if reason:
            text += f"\nПричина: {reason}"
        await message.answer(text, reply_markup=public_menu_keyboard() if include_public_menu else None)
        return None
    if status != STATUS_APPROVED:
        await _deny_access(message, content_loader)
        return None

    role = normalize_role(str(user_row["role"]))
    if required_role and role != normalize_role(required_role):
        await message.answer(
            f"🔒 Этот раздел доступен только для роли: {role_title(required_role)}.",
            reply_markup=private_menu_keyboard(role, include_public_menu=include_public_menu),
        )
        return None
    return dict(user_row)


def _find_public_link(content: dict, title: str) -> dict[str, str] | None:
    links = content.get(SECTION_PUBLIC_MENU_LINKS, [])
    if not isinstance(links, list):
        return None
    target_title = _normalize_caption_for_match(title)
    for item in links:
        if not isinstance(item, dict):
            continue
        item_title = str(item.get("title", "")).strip()
        if _normalize_caption_for_match(item_title) == target_title:
            url = str(item.get("url", "")).strip()
            if url:
                return {"title": item_title, "url": url}
    return None


def _get_role_links(content: dict, role: str, is_materials: bool) -> list[dict[str, str]]:
    section_links, section_materials = _role_sections(role)
    target = section_materials if is_materials else section_links
    raw = content.get(target, [])
    if not isinstance(raw, list):
        return []
    result: list[dict[str, str]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        url = str(item.get("url", "")).strip()
        category = str(item.get("category", "")).strip()
        subcategory = str(item.get("subcategory", "")).strip()
        if title and url:
            result.append({"title": title, "url": url, "category": category, "subcategory": subcategory})
    return result


def _label_or_default(value: str, default: str) -> str:
    text = value.strip()
    return text or default


def _items_have_categories(items: list[dict[str, str]]) -> bool:
    return any(item.get("category") or item.get("subcategory") for item in items)


def _category_titles(items: list[dict[str, str]]) -> list[str]:
    titles: list[str] = []
    seen: set[str] = set()
    for item in items:
        title = _label_or_default(str(item.get("category", "")), "Без категории")
        key = title.casefold()
        if key in seen:
            continue
        seen.add(key)
        titles.append(title)
    return titles


def _subcategory_titles(items: list[dict[str, str]], category: str) -> list[str]:
    titles: list[str] = []
    seen: set[str] = set()
    for item in items:
        item_category = _label_or_default(str(item.get("category", "")), "Без категории")
        if item_category != category:
            continue
        title = _label_or_default(str(item.get("subcategory", "")), "Без подкатегории")
        key = title.casefold()
        if key in seen:
            continue
        seen.add(key)
        titles.append(title)
    return titles


def _links_map_from_items(items: list[dict[str, str]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in items:
        title = str(item.get("title", "")).strip()
        url = str(item.get("url", "")).strip()
        if title and url:
            result[title] = url
    return result


async def _start_nested_navigation(
    message: Message,
    state: FSMContext,
    role: str,
    items: list[dict[str, str]],
    is_materials: bool,
) -> None:
    if not _items_have_categories(items):
        links_map = _group_links_by_title(items)
        await state.set_state(NavigationFlow.waiting_material_choice if is_materials else NavigationFlow.waiting_link_choice)
        await state.update_data(nav_role=role, nav_links=links_map, nav_is_materials=is_materials)
        await message.answer(
            "📎 Выберите материал:" if is_materials else "🔗 Выберите нужную ссылку:",
            reply_markup=section_keyboard(list(links_map.keys())),
        )
        return

    categories = _category_titles(items)
    await state.set_state(NavigationFlow.waiting_category_choice)
    await state.update_data(nav_role=role, nav_items=items, nav_is_materials=is_materials)
    await message.answer(
        "📂 Выберите категорию:",
        reply_markup=section_keyboard(categories),
    )


async def _send_public_button_link(message: Message, content_loader: ContentLoader, button_title: str) -> bool:
    content = await content_loader.load()
    item = _find_public_link(content, button_title)
    if item is None:
        await message.answer(
            "⚠️ Ссылка в этом разделе ещё не опубликована. Уточните у организатора.",
            reply_markup=public_menu_keyboard(),
        )
        return False
    await message.answer(
        f"🔗 {item['title']}",
        reply_markup=url_keyboard([{"title": "Открыть", "url": item["url"]}]),
    )
    return True


def _extract_media_file_id(message: Message) -> tuple[str, str] | None:
    if message.document is not None:
        file_name = message.document.file_name or "Документ"
        return (file_name, f"file_id:{message.document.file_id}")
    if message.photo:
        return ("Фото", f"photo_id:{message.photo[-1].file_id}")
    if message.video is not None:
        return (message.video.file_name or "Видео", f"video_id:{message.video.file_id}")
    return None


async def create_dispatcher(
    bot: Bot,
    db: Database,
    settings: Settings,
    content_loader: ContentLoader,
    scheduler: BroadcastScheduler,
    profile: BotProfile | None = None,
) -> Dispatcher:
    dp = Dispatcher(storage=MemoryStorage())
    router = Router()
    profile = profile or BotProfile(
        key="summit",
        token=settings.bot_token,
        username=settings.bot_username,
        role=None,
        is_public=True,
    )
    profile_role = normalize_role(profile.role) if profile.role else None
    include_public_menu = profile_role is None

    def private_keyboard(role: str) -> ReplyKeyboardMarkup:
        return private_menu_keyboard(role, include_public_menu=include_public_menu)

    dp.message.middleware(RateLimitMiddleware(settings.rate_limit_seconds))
    dp.callback_query.middleware(RateLimitMiddleware(settings.rate_limit_seconds))

    @router.message(CommandStart())
    async def cmd_start(message: Message, state: FSMContext) -> None:
        if not message.from_user:
            return

        user_id = message.from_user.id
        text = message.text or ""
        raw_payload = _extract_command_payload(text)
        payload = _extract_possible_code(raw_payload)

        user_row = await db.get_user(user_id)
        if user_row is not None:
            await db.update_user_profile(
                telegram_id=user_id,
                username=message.from_user.username,
                first_name=message.from_user.first_name,
            )

        ref_match = REF_START_RE.match(raw_payload)
        if ref_match:
            ref_code = ref_match.group(1).strip().upper()
            owner = await db.get_user_by_referral_code(ref_code)
            if owner and int(owner["telegram_id"]) != user_id:
                await db.record_referral_click(
                    owner_telegram_id=int(owner["telegram_id"]),
                    guest_telegram_id=user_id,
                    ref_code=ref_code,
                )
                await message.answer(
                    "🎁 Реферальная метка зафиксирована."
                    "\nЕсли вы зарегистрируетесь через приглашение организатора,"
                    " участие будет зачтено пригласившему.",
                )
            payload = ""

        app_match = APP_START_RE.match(raw_payload)
        if app_match:
            handled = await _handle_application_start(
                message=message,
                state=state,
                db=db,
                settings=settings,
                content_loader=content_loader,
                token=app_match.group(1),
                include_public_menu=include_public_menu,
            )
            if handled:
                return

        if user_row is not None and str(user_row["access_status"]) == STATUS_APPROVED:
            if profile_role and normalize_role(str(user_row["role"])) != profile_role:
                await state.set_state(AccessRequestFlow.waiting_access_code)
                await state.set_data({"entry_role": profile_role})
                await message.answer(
                    f"Этот бот предназначен для роли «{role_title(profile_role)}».\n"
                    "Введите код приглашения для этой роли.",
                    reply_markup=cancel_keyboard(),
                )
                return
            await state.clear()
            await _show_private_menu(
                message,
                db,
                settings,
                content_loader,
                include_public_menu=include_public_menu,
            )
            return

        if payload:
            ok = await _process_access_code_input(
                message=message,
                state=state,
                db=db,
                raw_input=payload,
                expected_role=profile_role,
            )
            if ok:
                return
            await state.set_state(AccessRequestFlow.waiting_access_code)
            await state.set_data({})
            return

        if user_row is not None and str(user_row["access_status"]) == STATUS_PENDING:
            await message.answer(
                "⏳ Ваша заявка уже отправлена и ожидает подтверждения организатором.",
                reply_markup=public_menu_keyboard() if include_public_menu else None,
            )
            return

        if user_row is not None and str(user_row["access_status"]) == STATUS_REJECTED:
            reason = str(user_row["rejection_reason"] or "").strip()
            text_out = "🚫 Ранее заявка была отклонена."
            if reason:
                text_out += f"\nПричина: {reason}"
            text_out += "\nДля нового доступа обратитесь к организатору."
            await message.answer(text_out, reply_markup=public_menu_keyboard() if include_public_menu else None)
            return

        if profile_role:
            await state.set_state(AccessRequestFlow.waiting_access_code)
            await state.set_data({"entry_role": profile_role})
            await message.answer(
                f"Добро пожаловать в бот для роли «{role_title(profile_role)}».\n"
                "Введите код приглашения или откройте персональную ссылку от организатора.",
                reply_markup=cancel_keyboard(),
            )
            return

        await _show_public_menu(message, content_loader)

    @router.message(Command("code"))
    async def cmd_code(message: Message, state: FSMContext) -> None:
        if not message.from_user:
            return

        user_row = await db.get_user(message.from_user.id)
        if user_row is not None and str(user_row["access_status"]) == STATUS_APPROVED:
            await _show_private_menu(
                message,
                db,
                settings,
                content_loader,
                include_public_menu=include_public_menu,
            )
            return

        payload = _extract_command_payload(message.text or "")
        if not payload:
            await state.set_state(AccessRequestFlow.waiting_access_code)
            await state.set_data({})
            await message.answer(
                "🔐 Отправьте код приглашения одним сообщением.\n"
                "Можно отправить сам код или ссылку вида `...start=КОД`.",
                reply_markup=cancel_keyboard(),
            )
            return

        await _process_access_code_input(
            message=message,
            state=state,
            db=db,
            raw_input=payload,
            expected_role=profile_role,
        )

    @router.message(Command("menu"))
    async def cmd_menu(message: Message, state: FSMContext) -> None:
        if not message.from_user:
            return
        user_row = await db.get_user(message.from_user.id)
        if user_row is not None and str(user_row["access_status"]) == STATUS_APPROVED:
            if profile_role and normalize_role(str(user_row["role"])) != profile_role:
                await message.answer(
                    f"Этот бот предназначен для роли «{role_title(profile_role)}».",
                    reply_markup=cancel_keyboard(),
                )
                return
            await _show_private_menu(
                message,
                db,
                settings,
                content_loader,
                include_public_menu=include_public_menu,
            )
            return
        if profile_role:
            await state.set_state(AccessRequestFlow.waiting_access_code)
            await state.set_data({"entry_role": profile_role})
            await message.answer(
                f"Введите код приглашения для роли «{role_title(profile_role)}».",
                reply_markup=cancel_keyboard(),
            )
            return
        await _show_public_menu(message, content_loader)

    @router.message(Command("cancel"))
    async def cancel_any_flow(message: Message, state: FSMContext) -> None:
        await state.clear()
        if not message.from_user:
            if include_public_menu:
                await _show_public_menu(message, content_loader)
            return
        user_row = await db.get_user(message.from_user.id)
        if user_row is not None and str(user_row["access_status"]) == STATUS_APPROVED:
            await message.answer("❌ Действие отменено.", reply_markup=private_keyboard(str(user_row["role"])))
        elif profile_role:
            await message.answer(
                "❌ Действие отменено. Для входа отправьте код приглашения.",
                reply_markup=cancel_keyboard(),
            )
        else:
            await message.answer("❌ Действие отменено.", reply_markup=public_menu_keyboard())

    @router.message(F.text == BTN_CANCEL)
    async def cancel_any_flow_btn(message: Message, state: FSMContext) -> None:
        await cancel_any_flow(message, state)

    @router.message(AccessRequestFlow.waiting_access_code)
    async def request_access_code(message: Message, state: FSMContext) -> None:
        raw_text = (message.text or "").strip()
        if raw_text in PUBLIC_MENU_BUTTONS:
            if include_public_menu:
                await state.clear()
                await _show_public_menu(message, content_loader)
            else:
                await message.answer(
                    f"Введите код приглашения для роли «{role_title(profile_role or ROLE_PARTNER)}».",
                    reply_markup=cancel_keyboard(),
                )
            return

        data = await state.get_data()
        expected_role: str | None = None
        if data.get("entry_role"):
            expected_role = normalize_role(str(data["entry_role"]))

        await _process_access_code_input(
            message=message,
            state=state,
            db=db,
            raw_input=raw_text,
            expected_role=expected_role,
        )

    @router.message(AccessRequestFlow.waiting_partner_inn)
    async def request_partner_inn(message: Message, state: FSMContext) -> None:
        inn = (message.text or "").strip()
        if not inn.isdigit() or len(inn) not in (10, 12):
            await message.answer("⚠️ ИНН должен содержать 10 или 12 цифр. Попробуйте снова.")
            return
        await state.update_data(partner_inn=inn)
        await state.set_state(AccessRequestFlow.waiting_partner_company)
        await message.answer("🏢 Введите наименование компании:")

    @router.message(AccessRequestFlow.waiting_partner_company)
    async def request_partner_company(message: Message, state: FSMContext) -> None:
        company = (message.text or "").strip()
        if len(company) < 2:
            await message.answer("⚠️ Укажите корректное наименование компании.")
            return
        await state.update_data(partner_company=company)
        await state.set_state(AccessRequestFlow.waiting_partner_contact_name)
        await message.answer("👤 Введите имя контактного лица:")

    @router.message(AccessRequestFlow.waiting_partner_contact_name)
    async def request_partner_contact(message: Message, state: FSMContext) -> None:
        full_name = (message.text or "").strip()
        if len(full_name) < 2:
            await message.answer("⚠️ Введите имя и фамилию контактного лица.")
            return
        await state.update_data(partner_contact_name=full_name)
        await state.set_state(AccessRequestFlow.waiting_partner_phone)
        await message.answer("📞 Введите телефон контактного лица (пример: +79991234567):")

    @router.message(AccessRequestFlow.waiting_partner_phone)
    async def request_partner_phone(message: Message, state: FSMContext) -> None:
        phone = (message.text or "").strip()
        if not PHONE_RE.match(phone):
            await message.answer("⚠️ Неверный формат телефона. Попробуйте снова.")
            return
        data = await state.get_data()
        await _complete_request(
            message=message,
            state=state,
            db=db,
            settings=settings,
            content_loader=content_loader,
            full_name=str(data.get("partner_contact_name", "")),
            phone=phone,
            company=str(data.get("partner_company", "")),
            inn=str(data.get("partner_inn", "")),
        )

    @router.message(AccessRequestFlow.waiting_name)
    async def request_name(message: Message, state: FSMContext) -> None:
        full_name = (message.text or "").strip()
        if len(full_name) < 2:
            await message.answer("⚠️ Введите корректные имя и фамилию.")
            return
        await state.update_data(full_name=full_name)
        await state.set_state(AccessRequestFlow.waiting_phone)
        await message.answer("📞 Введите телефон (пример: +79991234567):")

    @router.message(AccessRequestFlow.waiting_phone)
    async def request_phone(message: Message, state: FSMContext) -> None:
        phone = (message.text or "").strip()
        if not PHONE_RE.match(phone):
            await message.answer("⚠️ Неверный формат телефона. Попробуйте снова.")
            return
        data = await state.get_data()
        await _complete_request(
            message=message,
            state=state,
            db=db,
            settings=settings,
            content_loader=content_loader,
            full_name=str(data.get("full_name", "")),
            phone=phone,
        )

    @router.message(F.text.in_(PUBLIC_MENU_BUTTONS))
    async def public_menu_actions(message: Message, state: FSMContext) -> None:
        if not message.from_user:
            return

        text = (message.text or "").strip()
        user_row = await db.get_user(message.from_user.id)
        is_approved = user_row is not None and str(user_row["access_status"]) == STATUS_APPROVED

        if profile_role:
            if is_approved and normalize_role(str(user_row["role"])) == profile_role:
                await message.answer(
                    "ℹ️ Используйте профильное меню этого бота.",
                    reply_markup=private_keyboard(str(user_row["role"])),
                )
                return
            await state.set_state(AccessRequestFlow.waiting_access_code)
            await state.set_data({"entry_role": profile_role})
            await message.answer(
                f"Введите код приглашения для роли «{role_title(profile_role)}».",
                reply_markup=cancel_keyboard(),
            )
            return

        if text == BTN_FEEDBACK:
            await state.set_state(FeedbackFlow.waiting_for_feedback)
            await message.answer(
                "✍️ Напишите ваш отзыв или пожелание одним сообщением.",
                reply_markup=cancel_keyboard(),
            )
            return

        if text == BTN_REFERRAL:
            if not is_approved:
                await message.answer(
                    "🔐 Реферальная ссылка станет доступна после подтверждения доступа в одном из приватных разделов.",
                    reply_markup=public_menu_keyboard(),
                )
                return

            me = await message.bot.get_me()
            seed = message.from_user.username or message.from_user.first_name or str(message.from_user.id)
            code = await db.get_or_create_referral_code(message.from_user.id, seed)
            link = f"https://t.me/{me.username}?start=ref_{code}" if me.username else f"ref_{code}"
            stats = await db.get_referral_stats(message.from_user.id)
            content = await content_loader.load()
            prize = str(content.get("referral_prize_text", "Пригласите коллег и выиграйте iPhone"))

            await message.answer(
                f"🎁 {prize}\n\n"
                f"Ваша ссылка:\n{link}\n\n"
                f"Переходов: {stats['clicks']}\n"
                f"Заявок: {stats['pending']}\n"
                f"Подтверждено: {stats['approved']}",
                reply_markup=private_keyboard(str(user_row["role"])),
            )
            return

        if text in ROLE_ENTRY_BUTTONS:
            role = ROLE_ENTRY_BUTTONS[text]
            if not profile_role:
                if await _send_role_bot_transition(message, settings, role):
                    return

            if is_approved and normalize_role(str(user_row["role"])) == role:
                await _show_private_menu(
                    message,
                    db,
                    settings,
                    content_loader,
                    include_public_menu=include_public_menu,
                )
                return

            await state.set_state(AccessRequestFlow.waiting_access_code)
            await state.set_data({"entry_role": role})
            await message.answer(
                f"🔐 Введите код приглашения для роли «{role_title(role)}».\n"
                "Можно отправить код одним сообщением без команды.",
                reply_markup=cancel_keyboard(),
            )
            return

        if text in PUBLIC_LINKABLE_BUTTONS:
            await _send_public_button_link(message, content_loader, text)
            return

        await _show_public_menu(message, content_loader)

    @router.message(FeedbackFlow.waiting_for_feedback)
    async def save_feedback(message: Message, state: FSMContext) -> None:
        if not message.from_user:
            return

        text = (message.text or "").strip()
        if not text:
            await message.answer("✍️ Отправьте текст сообщения одним сообщением.")
            return

        user_row = await db.get_user(message.from_user.id)
        role = str(user_row["role"]) if user_row else None
        feedback_id = await db.add_feedback(
            telegram_id=message.from_user.id,
            message_text=text,
            user_role=role,
            source="feedback",
        )

        username = f"@{message.from_user.username}" if message.from_user.username else "—"
        notify_text = (
            "💬 Новый отзыв\n"
            f"ID: {feedback_id}\n"
            f"User: {message.from_user.id} ({username})\n"
            f"Роль: {role_title(normalize_role(role)) if role else '—'}\n\n"
            f"{text}"
        )

        targets = set(settings.admin_ids) | set(settings.support_chat_ids)
        for chat_id in targets:
            try:
                await message.bot.send_message(chat_id=chat_id, text=notify_text)
            except Exception:  # noqa: BLE001
                logger.exception("Failed to notify feedback to chat %s", chat_id)

        await state.clear()
        if user_row is not None and str(user_row["access_status"]) == STATUS_APPROVED:
            await message.answer("✅ Спасибо! Отзыв отправлен.", reply_markup=private_keyboard(str(user_row["role"])))
        else:
            await message.answer(
                "✅ Спасибо! Отзыв отправлен.",
                reply_markup=public_menu_keyboard() if include_public_menu else None,
            )

    @router.message(F.text == BTN_TO_PUBLIC_MENU)
    async def go_to_public_menu(message: Message) -> None:
        if not include_public_menu:
            if not message.from_user:
                return
            user_row = await db.get_user(message.from_user.id)
            if user_row is not None and str(user_row["access_status"]) == STATUS_APPROVED:
                await message.answer("🏠 Главное меню.", reply_markup=private_keyboard(str(user_row["role"])))
                return
            await message.answer(
                f"Введите код приглашения для роли «{role_title(profile_role or ROLE_PARTNER)}».",
                reply_markup=cancel_keyboard(),
            )
            return
        await _show_public_menu(message, content_loader)

    @router.message(F.text == BTN_NEWS)
    async def show_news(message: Message) -> None:
        user_row = await _ensure_private_user(message, db, content_loader, include_public_menu=include_public_menu)
        if user_row is None:
            return

        role = normalize_role(str(user_row["role"]))
        subcategory = normalize_subcategory(str(user_row.get("subcategory") or ""))
        rows = await db.get_recent_sent_broadcasts(limit=7, role=role, subcategory=subcategory)
        if not rows:
            await message.answer("📢 Пока нет объявлений от организаторов.")
            return

        lines = ["📢 Последние объявления:"]
        for row in rows:
            sent_at = row["sent_at"]
            try:
                sent_dt = parse_iso_datetime(sent_at).astimezone()
                stamp = sent_dt.strftime("%d.%m.%Y %H:%M")
            except Exception:  # noqa: BLE001
                stamp = str(sent_at or "время неизвестно")

            text = (row["message_text"] or "").strip()
            if not text:
                text = "Медиа-объявление"
            if len(text) > 180:
                text = text[:177] + "..."
            target_role = normalize_target_role(str(row["target_role"]))
            target_suffix = "(всем)" if target_role == ROLE_ALL else f"({target_role})"
            target_subcategory = normalize_subcategory(str(row["target_subcategory"] or ""))
            if target_subcategory:
                target_suffix = f"{target_suffix}[{target_subcategory}]"
            lines.append(f"• {stamp} {target_suffix}: {text}")

        await message.answer("\n".join(lines), reply_markup=private_keyboard(role))

    @router.message(F.text == BTN_PROGRAM)
    async def show_program(message: Message) -> None:
        user_row = await _ensure_private_user(message, db, content_loader, include_public_menu=include_public_menu)
        if user_row is None:
            return

        role = normalize_role(str(user_row["role"]))
        content = await content_loader.load()
        markup = _program_keyboard(content)
        if markup is None:
            await message.answer("📅 Программа пока не опубликована.", reply_markup=private_keyboard(role))
            return

        await message.answer("📅 Актуальная программа саммита:", reply_markup=markup)

    @router.message(F.text == BTN_LINKS)
    async def show_links(message: Message, state: FSMContext) -> None:
        user_row = await _ensure_private_user(message, db, content_loader, include_public_menu=include_public_menu)
        if user_row is None:
            return

        role = normalize_role(str(user_row["role"]))
        content = await content_loader.load()
        items = _get_role_links(content, role, is_materials=False)
        if not items:
            await message.answer("🔗 Полезные ссылки пока не добавлены.", reply_markup=private_keyboard(role))
            return

        await _start_nested_navigation(message, state, role, items, is_materials=False)

    @router.message(F.text == BTN_MATERIALS)
    async def show_materials(message: Message, state: FSMContext) -> None:
        user_row = await _ensure_private_user(message, db, content_loader, include_public_menu=include_public_menu)
        if user_row is None:
            return

        role = normalize_role(str(user_row["role"]))
        content = await content_loader.load()
        items = _get_role_links(content, role, is_materials=True)
        if not items:
            await message.answer("📎 Материалы пока не добавлены.", reply_markup=private_keyboard(role))
            return

        await _start_nested_navigation(message, state, role, items, is_materials=True)

    @router.message(F.text == BTN_BACK)
    async def go_back(message: Message, state: FSMContext) -> None:
        await state.clear()
        if not message.from_user:
            if include_public_menu:
                await _show_public_menu(message, content_loader)
            return

        user_row = await db.get_user(message.from_user.id)
        if user_row is not None and str(user_row["access_status"]) == STATUS_APPROVED:
            await message.answer("🏠 Главное меню.", reply_markup=private_keyboard(str(user_row["role"])))
        elif profile_role:
            await message.answer(
                f"Введите код приглашения для роли «{role_title(profile_role)}».",
                reply_markup=cancel_keyboard(),
            )
        else:
            await message.answer("🏠 Главное меню.", reply_markup=public_menu_keyboard())

    @router.message(NavigationFlow.waiting_link_choice)
    async def handle_link_choice(message: Message, state: FSMContext) -> None:
        text = (message.text or "").strip()
        if text == BTN_BACK:
            await go_back(message, state)
            return

        data = await state.get_data()
        role = normalize_role(str(data.get("nav_role", ROLE_PARTNER)))
        links_map = data.get("nav_links", {})
        if not isinstance(links_map, dict):
            links_map = {}

        target = str(links_map.get(text, "")).strip()
        if not target:
            await message.answer("⚠️ Выберите пункт из списка или нажмите «⬅️ Назад».")
            return

        await _send_link_or_file(message, text, target)
        await message.answer("👉 Выберите следующую ссылку или вернитесь назад.", reply_markup=section_keyboard(list(links_map.keys())))

    @router.message(NavigationFlow.waiting_category_choice)
    async def handle_category_choice(message: Message, state: FSMContext) -> None:
        text = (message.text or "").strip()
        if text == BTN_BACK:
            await go_back(message, state)
            return

        data = await state.get_data()
        items = data.get("nav_items", [])
        if not isinstance(items, list):
            items = []
        categories = _category_titles(items)
        if text not in categories:
            await message.answer("⚠️ Выберите категорию из списка или нажмите «⬅️ Назад».")
            return

        subcategories = _subcategory_titles(items, text)
        await state.update_data(nav_category=text)
        if len(subcategories) > 1 or (subcategories and subcategories[0] != "Без подкатегории"):
            await state.set_state(NavigationFlow.waiting_subcategory_choice)
            await message.answer("📁 Выберите подкатегорию:", reply_markup=section_keyboard(subcategories))
            return

        selected = [
            item
            for item in items
            if _label_or_default(str(item.get("category", "")), "Без категории") == text
        ]
        links_map = _links_map_from_items(selected)
        is_materials = bool(data.get("nav_is_materials"))
        await state.set_state(NavigationFlow.waiting_material_choice if is_materials else NavigationFlow.waiting_link_choice)
        await state.update_data(nav_links=links_map)
        await message.answer(
            "📎 Выберите материал:" if is_materials else "🔗 Выберите нужную ссылку:",
            reply_markup=section_keyboard(list(links_map.keys())),
        )

    @router.message(NavigationFlow.waiting_subcategory_choice)
    async def handle_subcategory_choice(message: Message, state: FSMContext) -> None:
        text = (message.text or "").strip()
        if text == BTN_BACK:
            await go_back(message, state)
            return

        data = await state.get_data()
        items = data.get("nav_items", [])
        category = str(data.get("nav_category", ""))
        if not isinstance(items, list):
            items = []
        subcategories = _subcategory_titles(items, category)
        if text not in subcategories:
            await message.answer("⚠️ Выберите подкатегорию из списка или нажмите «⬅️ Назад».")
            return

        selected = [
            item
            for item in items
            if _label_or_default(str(item.get("category", "")), "Без категории") == category
            and _label_or_default(str(item.get("subcategory", "")), "Без подкатегории") == text
        ]
        links_map = _links_map_from_items(selected)
        is_materials = bool(data.get("nav_is_materials"))
        await state.set_state(NavigationFlow.waiting_material_choice if is_materials else NavigationFlow.waiting_link_choice)
        await state.update_data(nav_links=links_map)
        await message.answer(
            "📎 Выберите материал:" if is_materials else "🔗 Выберите нужную ссылку:",
            reply_markup=section_keyboard(list(links_map.keys())),
        )

    @router.message(NavigationFlow.waiting_material_choice)
    async def handle_material_choice(message: Message, state: FSMContext) -> None:
        text = (message.text or "").strip()
        if text == BTN_BACK:
            await go_back(message, state)
            return

        data = await state.get_data()
        links_map = data.get("nav_links", {})
        if not isinstance(links_map, dict):
            links_map = {}

        target = str(links_map.get(text, "")).strip()
        if not target:
            await message.answer("⚠️ Выберите пункт из списка или нажмите «⬅️ Назад».")
            return

        await _send_link_or_file(message, text, target)
        await message.answer("👉 Выберите следующий материал или вернитесь назад.", reply_markup=section_keyboard(list(links_map.keys())))

    @router.message(F.text == BTN_INFLUENCER_CONDITIONS)
    async def influencer_conditions(message: Message) -> None:
        user_row = await _ensure_private_user(
            message,
            db,
            content_loader,
            required_role=ROLE_INFLUENCER,
            include_public_menu=include_public_menu,
        )
        if user_row is None:
            return

        content = await content_loader.load()
        item = _find_public_link(content, BTN_INFLUENCER_CONDITIONS)
        if item is None:
            items = _get_role_links(content, ROLE_INFLUENCER, is_materials=True)
            for candidate in items:
                if _normalize_caption_for_match(candidate["title"]) == _normalize_caption_for_match(
                    BTN_INFLUENCER_CONDITIONS
                ):
                    item = candidate
                    break

        if item is None:
            await message.answer("⚠️ Раздел с условиями пока не опубликован.")
            return

        await _send_link_or_file(message, item["title"], item["url"])

    @router.message(F.text == BTN_INFLUENCER_APPLICATION)
    async def influencer_application(message: Message) -> None:
        user_row = await _ensure_private_user(
            message,
            db,
            content_loader,
            required_role=ROLE_INFLUENCER,
            include_public_menu=include_public_menu,
        )
        if user_row is None:
            return

        content = await content_loader.load()
        item = _find_public_link(content, BTN_INFLUENCER_APPLICATION)
        if item is None:
            items = _get_role_links(content, ROLE_INFLUENCER, is_materials=True)
            for candidate in items:
                if _normalize_caption_for_match(candidate["title"]) == _normalize_caption_for_match(
                    BTN_INFLUENCER_APPLICATION
                ):
                    item = candidate
                    break

        if item is None:
            form_url = str(content.get("influencer_form_url", "")).strip()
            if form_url:
                item = {"title": "Открыть заявку", "url": form_url}

        if item is None:
            await message.answer("⚠️ Раздел с заявкой пока не опубликован.")
            return

        await _send_link_or_file(message, item["title"], item["url"])

    @router.message(F.text == BTN_BOOTH_BOOKING)
    async def start_booth_booking(message: Message, state: FSMContext) -> None:
        user_row = await _ensure_private_user(
            message,
            db,
            content_loader,
            required_role=ROLE_PARTNER,
            include_public_menu=include_public_menu,
        )
        if user_row is None:
            return

        await state.set_state(BoothBookingFlow.waiting_booth)
        await state.set_data(
            {
                "booking_company": str(user_row.get("company") or ""),
                "booking_contact_name": str(user_row.get("full_name") or user_row.get("first_name") or ""),
                "booking_phone": str(user_row.get("phone") or ""),
                "booking_inn": str(user_row.get("inn") or ""),
            }
        )
        await message.answer(
            "🏗 Укажите номер стенда, который хотите забронировать (например: А1.16):",
            reply_markup=cancel_keyboard(),
        )

    @router.message(BoothBookingFlow.waiting_booth)
    async def booking_booth(message: Message, state: FSMContext) -> None:
        booth = (message.text or "").strip()
        if len(booth) < 2:
            await message.answer("⚠️ Укажите номер стенда.")
            return
        await state.update_data(booking_booth=booth)
        data = await state.get_data()
        if str(data.get("booking_company", "")).strip():
            await state.set_state(BoothBookingFlow.waiting_contact_name)
            await message.answer("👤 Введите имя контактного лица:")
            return
        await state.set_state(BoothBookingFlow.waiting_company)
        await message.answer("🏢 Введите наименование компании:")

    @router.message(BoothBookingFlow.waiting_company)
    async def booking_company(message: Message, state: FSMContext) -> None:
        company = (message.text or "").strip()
        if len(company) < 2:
            await message.answer("⚠️ Укажите корректное наименование компании.")
            return
        await state.update_data(booking_company=company)
        await state.set_state(BoothBookingFlow.waiting_contact_name)
        await message.answer("👤 Введите имя контактного лица:")

    @router.message(BoothBookingFlow.waiting_contact_name)
    async def booking_contact(message: Message, state: FSMContext) -> None:
        full_name = (message.text or "").strip()
        if len(full_name) < 2:
            await message.answer("⚠️ Введите имя контактного лица.")
            return
        await state.update_data(booking_contact_name=full_name)
        await state.set_state(BoothBookingFlow.waiting_phone)
        await message.answer("📞 Введите телефон контактного лица (пример: +79991234567):")

    @router.message(BoothBookingFlow.waiting_phone)
    async def booking_phone(message: Message, state: FSMContext) -> None:
        phone = (message.text or "").strip()
        if not PHONE_RE.match(phone):
            await message.answer("⚠️ Неверный формат телефона. Попробуйте снова.")
            return
        await state.update_data(booking_phone=phone)
        await state.set_state(BoothBookingFlow.waiting_comment)
        await message.answer("✍️ Добавьте комментарий к заявке или отправьте «-», если комментария нет.")

    @router.message(BoothBookingFlow.waiting_comment)
    async def booking_comment(message: Message, state: FSMContext) -> None:
        if not message.from_user:
            return
        comment = (message.text or "").strip()
        if comment == "-":
            comment = ""
        data = await state.get_data()
        token = f"bot{message.from_user.id}{int(datetime.now(timezone.utc).timestamp())}"
        request_text = comment or f"Хочу забронировать стенд {data.get('booking_booth')}"
        application = await db.create_application(
            token=token,
            role=ROLE_PARTNER,
            source="bot_booth",
            request_text=request_text,
            booth_number=str(data.get("booking_booth", "")),
            full_name=str(data.get("booking_contact_name", "")),
            phone=str(data.get("booking_phone", "")),
            company=str(data.get("booking_company", "")),
            inn=str(data.get("booking_inn", "")),
            telegram_id=message.from_user.id,
            status=APPLICATION_STATUS_IN_PROGRESS,
        )
        await _notify_application(
            bot=message.bot,
            settings=settings,
            application_id=int(application["id"]),
            user_id=message.from_user.id,
            source="bot_booth",
            role=ROLE_PARTNER,
            request_text=request_text,
            booth_number=str(data.get("booking_booth", "")),
            full_name=str(data.get("booking_contact_name", "")),
            phone=str(data.get("booking_phone", "")),
            company=str(data.get("booking_company", "")),
            inn=str(data.get("booking_inn", "")),
        )
        await state.clear()
        await message.answer(
            "✅ Заявка на бронирование стенда отправлена. Менеджер увидит её в админ-панели.",
            reply_markup=private_keyboard(ROLE_PARTNER),
        )

    @router.message(F.text == BTN_MANAGER)
    async def manager_contact(message: Message, state: FSMContext) -> None:
        user_row = await _ensure_private_user(message, db, content_loader, include_public_menu=include_public_menu)
        if user_row is None:
            return

        role = normalize_role(str(user_row["role"]))

        if settings.support_chat_ids:
            await state.set_state(SupportFlow.waiting_for_question)
            await message.answer(
                "🧑‍💼 Напишите ваш вопрос менеджеру.\n"
                "Можно отправить текст, фото или документ.\n"
                "Для отмены нажмите «Отмена».",
                reply_markup=cancel_keyboard(),
            )
            return

        content = await content_loader.load()
        manager = _get_manager_contact(content, role)
        if manager.get("url"):
            await message.answer(
                "🧑‍💼 Поддержка работает через прямой контакт:",
                reply_markup=url_keyboard([manager]),
            )
        else:
            await message.answer(
                "🧑‍💼 Контакт менеджера временно недоступен.",
                reply_markup=private_keyboard(role),
            )

    @router.message(SupportFlow.waiting_for_question)
    async def process_support_question(message: Message, state: FSMContext) -> None:
        if not message.from_user:
            return

        user_row = await _ensure_private_user(message, db, content_loader, include_public_menu=include_public_menu)
        if user_row is None:
            await state.clear()
            return

        role = normalize_role(str(user_row["role"]))

        if not settings.support_chat_ids:
            await state.clear()
            await message.answer(
                "⚠️ Поддержка не настроена. Обратитесь к организатору.",
                reply_markup=private_keyboard(role),
            )
            return

        username = f"@{user_row['username']}" if user_row["username"] else "—"
        question_header = (
            "🧑‍💼 Новый вопрос участника\n"
            f"#USER_{message.from_user.id}\n"
            f"Роль: {role_title(role)}\n"
            f"Имя: {user_row['full_name'] or user_row['first_name'] or message.from_user.first_name or '—'}\n"
            f"Username: {username}\n"
            f"Телефон: {user_row['phone'] or '—'}\n"
            f"Компания: {user_row['company'] or '—'}\n\n"
            f"Подключиться к диалогу: /connect_user {message.from_user.id}\n"
            f"Закрыть диалог: /disconnect_user {message.from_user.id}"
        )

        delivered_to_support = 0
        for chat_id in settings.support_chat_ids:
            try:
                meta_message = await message.bot.send_message(chat_id=chat_id, text=question_header)
                await message.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=message.chat.id,
                    message_id=message.message_id,
                    reply_to_message_id=meta_message.message_id,
                )
                delivered_to_support += 1
            except Exception:  # noqa: BLE001
                logger.exception("Failed to deliver support question to chat %s", chat_id)

        await state.clear()
        if delivered_to_support:
            await message.answer(
                "✅ Сообщение отправлено менеджеру. Ответ поступит в этот чат.",
                reply_markup=private_keyboard(role),
            )
        else:
            await message.answer(
                "⚠️ Не удалось доставить вопрос в поддержку. Попробуйте позже.",
                reply_markup=private_keyboard(role),
            )

    @router.message(Command("connect_user"))
    async def manager_connect_user(message: Message) -> None:
        if not message.from_user:
            return
        if message.chat.id not in settings.support_chat_ids and not _is_admin(message, settings):
            return

        payload = _extract_command_payload(message.text or "")
        if not payload:
            await message.answer("Формат: /connect_user TELEGRAM_ID")
            return
        try:
            user_id = int(payload.split()[0])
        except ValueError:
            await message.answer("TELEGRAM_ID должен быть числом.")
            return

        support_chat_id = message.chat.id
        if support_chat_id not in settings.support_chat_ids and settings.support_chat_ids:
            support_chat_id = next(iter(settings.support_chat_ids))

        await db.connect_support_session(
            telegram_id=user_id,
            support_chat_id=support_chat_id,
            manager_telegram_id=message.from_user.id,
        )
        await message.answer(
            f"✅ Менеджер подключён к пользователю {user_id}.\n"
            f"Чтобы закрыть диалог: /disconnect_user {user_id}"
        )
        try:
            await message.bot.send_message(chat_id=user_id, text="🧑‍💼 Менеджер подключился к диалогу.")
        except Exception:  # noqa: BLE001
            logger.exception("Failed to notify user about manager connect %s", user_id)

    @router.message(Command("disconnect_user"))
    async def manager_disconnect_user(message: Message) -> None:
        if not message.from_user:
            return
        if message.chat.id not in settings.support_chat_ids and not _is_admin(message, settings):
            return

        payload = _extract_command_payload(message.text or "")
        if not payload:
            await message.answer("Формат: /disconnect_user TELEGRAM_ID")
            return
        try:
            user_id = int(payload.split()[0])
        except ValueError:
            await message.answer("TELEGRAM_ID должен быть числом.")
            return

        row = await db.close_support_session(user_id)
        if row is None:
            await message.answer("Активный диалог не найден.")
            return

        await message.answer(f"✅ Диалог с пользователем {user_id} закрыт.")
        try:
            await message.bot.send_message(chat_id=user_id, text="🧑‍💼 Менеджер покинул чат.")
        except Exception:  # noqa: BLE001
            logger.exception("Failed to notify user about manager disconnect %s", user_id)

    @router.message(Command("pending_requests"))
    async def admin_pending_requests(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        rows = await db.list_pending_users(limit=80)
        if not rows:
            await message.answer("Нет заявок в статусе pending.")
            return

        lines = ["⏳ Заявки на согласовании:"]
        for row in rows:
            subcategory = f" / {row['subcategory']}" if row["subcategory"] else ""
            lines.append(
                f"• {row['telegram_id']} | {role_title(str(row['role']))}{subcategory} | "
                f"{row['full_name'] or row['first_name'] or '—'} | {row['phone'] or '—'}"
            )
        text = "\n".join(lines)
        if len(text) > 3900:
            text = text[:3890] + "\n..."
        await message.answer(text)

    @router.message(Command("approve_user"))
    async def admin_approve_user(message: Message) -> None:
        if not _is_admin(message, settings) or not message.from_user:
            return
        payload = _extract_command_payload(message.text or "")
        if not payload:
            await message.answer("Формат: /approve_user TELEGRAM_ID")
            return
        try:
            telegram_id = int(payload.split()[0])
        except ValueError:
            await message.answer("TELEGRAM_ID должен быть числом.")
            return

        row = await db.approve_user(telegram_id=telegram_id, approved_by=message.from_user.id)
        if row is None:
            await message.answer("Пользователь не найден.")
            return

        role = normalize_role(str(row["role"]))
        await message.answer(
            f"✅ Доступ подтверждён для {telegram_id} ({role_title(role)})."
        )
        try:
            await message.bot.send_message(
                chat_id=telegram_id,
                text="✅ Ваша заявка подтверждена. Доступ к меню открыт.",
                reply_markup=private_keyboard(role),
            )
        except Exception:  # noqa: BLE001
            logger.exception("Failed to notify approved user %s", telegram_id)

    @router.message(Command("reject_user"))
    async def admin_reject_user(message: Message) -> None:
        if not _is_admin(message, settings) or not message.from_user:
            return
        payload = _extract_command_payload(message.text or "")
        if not payload:
            await message.answer("Формат: /reject_user TELEGRAM_ID причина")
            return

        parts = payload.split(maxsplit=1)
        try:
            telegram_id = int(parts[0])
        except ValueError:
            await message.answer("TELEGRAM_ID должен быть числом.")
            return

        reason = parts[1].strip() if len(parts) > 1 else ""
        row = await db.reject_user(
            telegram_id=telegram_id,
            approved_by=message.from_user.id,
            reason=reason,
        )
        if row is None:
            await message.answer("Пользователь не найден.")
            return

        await message.answer("🚫 Доступ отклонён.")
        notify = "🚫 Ваша заявка отклонена организатором."
        if reason:
            notify += f"\nПричина: {reason}"
        try:
            await message.bot.send_message(
                chat_id=telegram_id,
                text=notify,
                reply_markup=public_menu_keyboard(),
            )
        except Exception:  # noqa: BLE001
            logger.exception("Failed to notify rejected user %s", telegram_id)

    @router.message(Command("add_code"))
    async def admin_add_code(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        payload = _extract_command_payload(message.text or "")
        if not payload:
            await message.answer("Формат: /add_code CODE ROLE [ПОДКАТЕГОРИЯ] | описание")
            return

        left, separator, right = payload.partition("|")
        parts = left.strip().split(maxsplit=2)
        if len(parts) < 2:
            await message.answer("Формат: /add_code CODE ROLE [ПОДКАТЕГОРИЯ] | описание")
            return

        code = normalize_code(parts[0])
        role = normalize_role(parts[1])
        subcategory = normalize_subcategory(parts[2]) if len(parts) > 2 else ""
        description = right.strip() if separator else ""
        await db.add_or_update_access_code(
            code=code,
            role=role,
            subcategory=subcategory,
            description=description,
            is_active=True,
        )
        suffix = f", подкатегория: {subcategory}" if subcategory else ""
        await message.answer(f"Код {code} ({role}{suffix}) добавлен/обновлён.")

    @router.message(Command("disable_code"))
    async def admin_disable_code(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        payload = _extract_command_payload(message.text or "")
        if not payload:
            await message.answer("Формат: /disable_code CODE")
            return
        count = await db.set_access_code_status(payload.split()[0], is_active=False)
        await message.answer("Код деактивирован." if count else "Код не найден.")

    @router.message(Command("enable_code"))
    async def admin_enable_code(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        payload = _extract_command_payload(message.text or "")
        if not payload:
            await message.answer("Формат: /enable_code CODE")
            return
        count = await db.set_access_code_status(payload.split()[0], is_active=True)
        await message.answer("Код активирован." if count else "Код не найден.")

    @router.message(Command("delete_code"))
    async def admin_delete_code(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        payload = _extract_command_payload(message.text or "")
        if not payload:
            await message.answer("Формат: /delete_code CODE")
            return
        count = await db.delete_access_code(payload.split()[0])
        await message.answer("Код удалён." if count else "Код не найден.")

    @router.message(Command("list_codes"))
    async def admin_list_codes(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        rows = await db.list_access_codes(limit=300)
        if not rows:
            await message.answer("Список кодов пуст.")
            return

        lines = ["Коды доступа:"]
        for row in rows:
            status = "🟢" if row["is_active"] else "🔴"
            description = row["description"] or "Без описания"
            subcategory = f":{row['subcategory']}" if row["subcategory"] else ""
            lines.append(f"{status} {row['code']} [{row['role']}{subcategory}] — {description}")

        text = "\n".join(lines)
        if len(text) > 3900:
            text = text[:3890] + "\n..."
        await message.answer(text)

    @router.message(Command("check_code"))
    async def admin_check_code(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        payload = _extract_command_payload(message.text or "")
        if not payload:
            await message.answer("Формат: /check_code CODE")
            return

        code = payload.split()[0]
        row = await db.find_access_code(code)
        if row is None:
            await message.answer("Код не найден в базе.")
            return

        await message.answer(
            "Проверка кода:\n"
            f"Код: {row['code']}\n"
            f"Роль: {row['role']}\n"
            f"Подкатегория: {row['subcategory'] or '—'}\n"
            f"Активен: {'да' if row['is_active'] else 'нет'}\n"
            f"Описание: {row['description'] or '—'}\n"
            f"Создан: {row['created_at']}"
        )

    @router.message(Command("save_link"))
    async def admin_save_link(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        payload = _extract_command_payload(message.text or "")
        parts = [chunk.strip() for chunk in payload.split("|")]
        if len(parts) < 3:
            await message.answer(
                "Формат: /save_link SECTION | Название | URL\n"
                "Пример: /save_link partner_materials | Презентация | https://..."
            )
            return

        section, title, url = parts[0], parts[1], parts[2]
        try:
            link_id = await db.add_content_link(section=section, title=title, url=url, position=100, is_active=True)
        except ValueError:
            await message.answer("Некорректная секция.")
            return

        await message.answer(f"Ссылка сохранена. ID: {link_id}")

    @router.message(Command("save_material"))
    async def admin_save_material(message: Message) -> None:
        if not _is_admin(message, settings):
            return

        payload = _extract_command_payload(message.text or "")
        parts = [chunk.strip() for chunk in payload.split("|", maxsplit=1)]
        if len(parts) < 2:
            await message.answer(
                "Формат: /save_material SECTION | Название\n"
                "Команду отправляйте в reply на документ/фото/видео."
            )
            return

        if message.reply_to_message is None:
            await message.answer("Нужно ответить на сообщение с файлом/фото/видео.")
            return

        media_info = _extract_media_file_id(message.reply_to_message)
        if media_info is None:
            await message.answer("В reply должен быть документ, фото или видео.")
            return

        _, media_ref = media_info
        section = parts[0]
        title = parts[1]

        try:
            link_id = await db.add_content_link(section=section, title=title, url=media_ref, position=100, is_active=True)
        except ValueError:
            await message.answer("Некорректная секция.")
            return

        await message.answer(f"Материал сохранён. ID: {link_id}")

    @router.message(Command("export_users"))
    async def admin_export_users(message: Message) -> None:
        if not _is_admin(message, settings):
            return

        rows = await db.list_users(limit=5000)
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "id",
                "telegram_id",
                "username",
                "first_name",
                "role",
                "subcategory",
                "access_status",
                "access_code",
                "full_name",
                "phone",
                "company",
                "inn",
                "requested_at",
                "approved_at",
                "approved_by",
                "rejection_reason",
                "referred_by",
                "referral_code",
                "registered_at",
            ]
        )

        for row in rows:
            writer.writerow(
                [
                    row["id"],
                    row["telegram_id"],
                    row["username"] or "",
                    row["first_name"] or "",
                    row["role"] or "",
                    row["subcategory"] or "",
                    row["access_status"] or "",
                    row["access_code"] or "",
                    row["full_name"] or "",
                    row["phone"] or "",
                    row["company"] or "",
                    row["inn"] or "",
                    row["requested_at"] or "",
                    row["approved_at"] or "",
                    row["approved_by"] or "",
                    row["rejection_reason"] or "",
                    row["referred_by"] or "",
                    row["referral_code"] or "",
                    row["registered_at"] or "",
                ]
            )

        data = output.getvalue().encode("utf-8")
        file_name = f"summit_users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        document = BufferedInputFile(data, filename=file_name)
        await message.answer_document(document=document, caption="Выгрузка базы пользователей")

    @router.message(Command("broadcast"))
    async def admin_broadcast(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        if not message.from_user:
            return

        payload = _extract_command_payload(message.text or "")
        target_role, target_subcategory, text_payload = _parse_broadcast_target(payload)

        source_chat_id = None
        source_message_id = None
        if message.reply_to_message is not None:
            source_chat_id = message.reply_to_message.chat.id
            source_message_id = message.reply_to_message.message_id

        if not text_payload and not source_message_id:
            await message.answer(
                "Формат:\n"
                "1) /broadcast [all|partner|expert|influencer|role:subcategory] Текст\n"
                "2) Ответьте на медиа и отправьте /broadcast [role:subcategory]"
            )
            return

        broadcast_id = await db.create_broadcast(
            created_by=message.from_user.id,
            target_role=target_role,
            target_subcategory=target_subcategory,
            message_text=text_payload or None,
            image_path=None,
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
            scheduled_at=None,
            status="scheduled",
        )
        delivered, failed = await send_broadcast(bot, db, broadcast_id=broadcast_id)
        await message.answer(
            "✅ Рассылка завершена.\n"
            f"ID: {broadcast_id}\n"
            f"Роль: {target_role}\n"
            f"Подкатегория: {target_subcategory or '—'}\n"
            f"Доставлено: {delivered}\n"
            f"Ошибок: {failed}"
        )

    @router.message(Command("broadcast_in"))
    async def admin_broadcast_in(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        if not message.from_user:
            return

        text = message.text or ""
        parts = text.split(maxsplit=3)
        if len(parts) < 2:
            await message.answer(
                "Формат:\n"
                "1) /broadcast_in МИНУТЫ [all|partner|expert|influencer|role:subcategory] Текст\n"
                "2) Ответьте на медиа и отправьте /broadcast_in МИНУТЫ [role:subcategory]"
            )
            return

        try:
            minutes = int(parts[1])
            if minutes <= 0:
                raise ValueError
        except ValueError:
            await message.answer("Количество минут должно быть целым числом больше нуля.")
            return

        role = ROLE_ALL
        subcategory = ""
        payload = ""
        if len(parts) >= 3:
            candidate, candidate_subcategory = parse_target_role_subcategory(parts[2])
            if candidate != ROLE_ALL or parts[2].lower() == ROLE_ALL or parts[2].lower().startswith(f"{ROLE_ALL}:"):
                role = candidate
                subcategory = candidate_subcategory
                payload = parts[3].strip() if len(parts) > 3 else ""
            else:
                payload = text.split(maxsplit=2)[2].strip() if len(text.split(maxsplit=2)) > 2 else ""

        source_chat_id = None
        source_message_id = None
        if message.reply_to_message is not None:
            source_chat_id = message.reply_to_message.chat.id
            source_message_id = message.reply_to_message.message_id

        if not payload and not source_message_id:
            await message.answer("Передайте текст или ответьте на сообщение/медиа для рассылки.")
            return

        run_at = datetime.now(timezone.utc) + timedelta(minutes=minutes)
        broadcast_id = await db.create_broadcast(
            created_by=message.from_user.id,
            target_role=role,
            target_subcategory=subcategory,
            message_text=payload or None,
            image_path=None,
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
            scheduled_at=run_at.isoformat(),
            status="scheduled",
        )
        scheduler.schedule(broadcast_id=broadcast_id, run_at=run_at)

        await message.answer(
            "⏳ Рассылка запланирована.\n"
            f"ID: {broadcast_id}\n"
            f"Роль: {role}\n"
            f"Подкатегория: {subcategory or '—'}\n"
            f"Время (UTC): {run_at.strftime('%Y-%m-%d %H:%M')}"
        )

    @router.message(Command("broadcast_stats"))
    async def admin_broadcast_stats(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        payload = _extract_command_payload(message.text or "")
        if not payload:
            await message.answer("Формат: /broadcast_stats ID")
            return
        try:
            broadcast_id = int(payload.split()[0])
        except ValueError:
            await message.answer("ID рассылки должен быть числом.")
            return

        row = await db.get_broadcast(broadcast_id)
        if row is None:
            await message.answer("Рассылка не найдена.")
            return

        stats = await db.get_delivery_stats(broadcast_id)
        await message.answer(
            "Статистика рассылки:\n"
            f"ID: {broadcast_id}\n"
            f"Роль: {row['target_role']}\n"
            f"Подкатегория: {row['target_subcategory'] or '—'}\n"
            f"Статус: {row['status']}\n"
            f"Отправлено: {row['sent_at'] or 'ещё не отправлена'}\n"
            f"Доставлено: {stats['delivered']}\n"
            f"Ошибок: {stats['failed']}"
        )

    @router.message(
        lambda message: (
            message.reply_to_message is not None
            and (
                message.chat.id in settings.support_chat_ids
                or (message.from_user is not None and message.from_user.id in settings.admin_ids)
            )
        )
    )
    async def bridge_manager_reply(message: Message) -> None:
        if not message.from_user:
            return
        if message.from_user.id == bot.id:
            return
        reply_to = message.reply_to_message
        if reply_to is None or reply_to.from_user is None:
            return
        if reply_to.from_user.id != bot.id:
            return

        marker_source = (reply_to.text or "") + "\n" + (reply_to.caption or "")
        if not USER_MARKER_RE.search(marker_source) and reply_to.reply_to_message is not None:
            nested_reply = reply_to.reply_to_message
            marker_source = (nested_reply.text or "") + "\n" + (nested_reply.caption or "")
        match = USER_MARKER_RE.search(marker_source)
        if not match:
            return

        user_id = int(match.group(1))
        try:
            await message.bot.copy_message(
                chat_id=user_id,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
            )
            await message.reply("✅ Ответ отправлен пользователю.")
        except TelegramForbiddenError:
            await message.reply("Пользователь заблокировал бота. Ответ не доставлен.")
        except Exception:  # noqa: BLE001
            logger.exception("Failed to forward manager response to user %s", user_id)
            await message.reply("Не удалось отправить ответ пользователю.")

    @router.message()
    async def fallback(message: Message, state: FSMContext) -> None:
        if message.chat.id in settings.support_chat_ids:
            return

        if not message.from_user:
            if include_public_menu:
                await _show_public_menu(message, content_loader)
            return

        user_row = await db.get_user(message.from_user.id)
        raw_text = (message.text or "").strip()

        active_session = await db.get_active_support_session(message.from_user.id)
        if active_session is not None and raw_text not in ALL_MAIN_BUTTONS:
            username = f"@{message.from_user.username}" if message.from_user.username else "—"
            header = (
                "💬 Сообщение в активном диалоге\n"
                f"#USER_{message.from_user.id}\n"
                f"Username: {username}"
            )
            try:
                meta_message = await message.bot.send_message(
                    chat_id=int(active_session["support_chat_id"]),
                    text=header,
                )
                await message.bot.copy_message(
                    chat_id=int(active_session["support_chat_id"]),
                    from_chat_id=message.chat.id,
                    message_id=message.message_id,
                    reply_to_message_id=meta_message.message_id,
                )
                await message.answer("✅ Сообщение отправлено менеджеру.")
            except Exception:  # noqa: BLE001
                logger.exception("Failed to forward active support message from %s", message.from_user.id)
                await message.answer("⚠️ Не удалось отправить сообщение менеджеру. Попробуйте позже.")
            return

        if user_row is not None and str(user_row["access_status"]) == STATUS_APPROVED:
            await message.answer(
                "ℹ️ Используйте кнопки меню для навигации.",
                reply_markup=private_keyboard(str(user_row["role"])),
            )
            return

        current_state = await state.get_state()
        if current_state is None and raw_text and raw_text not in ALL_MAIN_BUTTONS:
            if _looks_like_access_code_candidate(raw_text):
                ok = await _process_access_code_input(
                    message=message,
                    state=state,
                    db=db,
                    raw_input=raw_text,
                    expected_role=profile_role,
                )
                if ok:
                    return

        if profile_role:
            await state.set_state(AccessRequestFlow.waiting_access_code)
            await state.set_data({"entry_role": profile_role})
            await message.answer(
                f"Введите код приглашения для роли «{role_title(profile_role)}».",
                reply_markup=cancel_keyboard(),
            )
            return

        await message.answer(
            "ℹ️ Используйте кнопки меню для навигации.\n"
            "Если у вас есть код приглашения, просто отправьте его одним сообщением.",
            reply_markup=public_menu_keyboard(),
        )

    dp.include_router(router)
    return dp
