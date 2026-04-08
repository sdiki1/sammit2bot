from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramForbiddenError
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, Message

from summit_partner_bot.broadcasts import BroadcastScheduler, parse_iso_datetime, send_broadcast
from summit_partner_bot.config import Settings
from summit_partner_bot.content import ContentLoader
from summit_partner_bot.db import Database, normalize_code
from summit_partner_bot.keyboards import (
    BTN_CANCEL,
    BTN_LINKS,
    BTN_MANAGER,
    BTN_MATERIALS,
    BTN_NEWS,
    BTN_PROGRAM,
    cancel_keyboard,
    main_menu_keyboard,
    url_keyboard,
)
from summit_partner_bot.middlewares import RateLimitMiddleware
from summit_partner_bot.states import SupportFlow

logger = logging.getLogger(__name__)
USER_MARKER_RE = re.compile(r"#USER_(\d+)")


def _extract_command_payload(text: str) -> str:
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return ""
    return parts[1].strip()


def _is_admin(message: Message, settings: Settings) -> bool:
    return bool(message.from_user and message.from_user.id in settings.admin_ids)


def _format_welcome(first_name: str, summit_name: str, content: dict) -> str:
    default_template = (
        "Добро пожаловать, {first_name}!\n"
        "Вы авторизованы как партнёр {summit_name}.\n\n"
        "Используйте меню ниже для быстрого доступа к информации:"
    )
    template = str(content.get("welcome_template", default_template))
    try:
        return template.format(first_name=first_name, summit_name=summit_name)
    except KeyError:
        return default_template.format(first_name=first_name, summit_name=summit_name)


def _program_keyboard(content: dict) -> InlineKeyboardMarkup | None:
    program = content.get("program", {})
    if not isinstance(program, dict):
        return None
    url = str(program.get("url", "")).strip()
    title = str(program.get("title", "Открыть программу")).strip() or "Открыть программу"
    if not url:
        return None
    return url_keyboard([{"title": title, "url": url}])


async def _show_menu(message: Message, db: Database, settings: Settings, content_loader: ContentLoader) -> None:
    if not message.from_user:
        return
    await db.update_user_profile(
        telegram_id=message.from_user.id,
        username=message.from_user.username,
        first_name=message.from_user.first_name,
    )
    content = await content_loader.load()
    welcome = _format_welcome(
        first_name=message.from_user.first_name or "партнёр",
        summit_name=settings.summit_name,
        content=content,
    )
    await message.answer(welcome, reply_markup=main_menu_keyboard())


async def _deny_access(message: Message, content_loader: ContentLoader) -> None:
    content = await content_loader.load()
    restricted_text = str(
        content.get(
            "restricted_text",
            "Доступ к этому боту ограничен. Получите персональный код у организатора.",
        )
    )
    await message.answer(
        f"🚫 {restricted_text}\n\n"
        "Для входа используйте команду:\n"
        "/code ВАШ_КОД"
    )


async def _ensure_authorized(message: Message, db: Database, content_loader: ContentLoader) -> bool:
    if not message.from_user:
        return False
    if await db.is_authorized(message.from_user.id):
        return True
    await _deny_access(message, content_loader)
    return False


async def create_dispatcher(
    bot: Bot,
    db: Database,
    settings: Settings,
    content_loader: ContentLoader,
    scheduler: BroadcastScheduler,
) -> Dispatcher:
    dp = Dispatcher(storage=MemoryStorage())
    router = Router()

    dp.message.middleware(RateLimitMiddleware(settings.rate_limit_seconds))
    dp.callback_query.middleware(RateLimitMiddleware(settings.rate_limit_seconds))

    @router.message(CommandStart())
    async def cmd_start(message: Message) -> None:
        if not message.from_user:
            return

        text = message.text or ""
        deep_link_code = _extract_command_payload(text)

        if await db.is_authorized(message.from_user.id):
            await _show_menu(message, db, settings, content_loader)
            return

        if deep_link_code:
            code_row = await db.get_access_code(deep_link_code)
            if code_row is not None:
                await db.upsert_authorized_user(
                    telegram_id=message.from_user.id,
                    username=message.from_user.username,
                    first_name=message.from_user.first_name,
                    access_code=code_row["code"],
                    company=code_row["description"],
                )
                await _show_menu(message, db, settings, content_loader)
                return

        await _deny_access(message, content_loader)

    @router.message(Command("code"))
    async def cmd_code(message: Message) -> None:
        if not message.from_user:
            return

        payload = _extract_command_payload(message.text or "")
        if not payload:
            await message.answer("Формат команды: /code ВАШ_КОД")
            return

        code = normalize_code(payload.split()[0])
        code_row = await db.get_access_code(code)
        if code_row is None:
            await message.answer("Код не найден или неактивен. Проверьте код и попробуйте снова.")
            return

        await db.upsert_authorized_user(
            telegram_id=message.from_user.id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            access_code=code_row["code"],
            company=code_row["description"],
        )
        await _show_menu(message, db, settings, content_loader)

    @router.message(Command("menu"))
    async def cmd_menu(message: Message) -> None:
        if not await _ensure_authorized(message, db, content_loader):
            return
        await _show_menu(message, db, settings, content_loader)

    @router.message(F.text == BTN_NEWS)
    async def show_news(message: Message) -> None:
        if not await _ensure_authorized(message, db, content_loader):
            return

        rows = await db.get_recent_sent_broadcasts(limit=5)
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
                stamp = sent_at or "время неизвестно"

            text = (row["message_text"] or "").strip()
            if not text:
                text = "Медиа-объявление"
            if len(text) > 180:
                text = text[:177] + "..."
            lines.append(f"• {stamp}: {text}")

        await message.answer("\n".join(lines))

    @router.message(F.text == BTN_PROGRAM)
    async def show_program(message: Message) -> None:
        if not await _ensure_authorized(message, db, content_loader):
            return

        content = await content_loader.load()
        markup = _program_keyboard(content)
        if markup is None:
            await message.answer("📅 Программа пока не опубликована. Уточните у организатора.")
            return

        await message.answer("📅 Актуальная программа саммита:", reply_markup=markup)

    @router.message(F.text == BTN_LINKS)
    async def show_useful_links(message: Message) -> None:
        if not await _ensure_authorized(message, db, content_loader):
            return

        content = await content_loader.load()
        links = content.get("useful_links", [])
        if not isinstance(links, list) or not links:
            await message.answer("🔗 Полезные ссылки пока не добавлены.")
            return

        await message.answer(
            "🔗 Полезные ссылки:",
            reply_markup=url_keyboard(links),
        )

    @router.message(F.text == BTN_MATERIALS)
    async def show_materials(message: Message) -> None:
        if not await _ensure_authorized(message, db, content_loader):
            return

        content = await content_loader.load()
        items = content.get("partner_materials", [])
        if not isinstance(items, list) or not items:
            await message.answer("📎 Материалы для партнёров пока не добавлены.")
            return

        await message.answer(
            "📎 Материалы для партнёров:",
            reply_markup=url_keyboard(items),
        )

    @router.message(F.text == BTN_MANAGER)
    async def manager_contact(message: Message, state: FSMContext) -> None:
        if not await _ensure_authorized(message, db, content_loader):
            return

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
        manager = content.get("manager_contact", {})
        if isinstance(manager, dict) and manager.get("url"):
            await message.answer(
                "🧑‍💼 Поддержка работает через прямой контакт:",
                reply_markup=url_keyboard([manager]),
            )
        else:
            await message.answer("🧑‍💼 Контакт менеджера временно недоступен.")

    @router.message(SupportFlow.waiting_for_question, F.text == BTN_CANCEL)
    async def cancel_support_flow(message: Message, state: FSMContext) -> None:
        await state.clear()
        await message.answer("Действие отменено.", reply_markup=main_menu_keyboard())

    @router.message(SupportFlow.waiting_for_question, Command("cancel"))
    async def cancel_support_flow_cmd(message: Message, state: FSMContext) -> None:
        await state.clear()
        await message.answer("Действие отменено.", reply_markup=main_menu_keyboard())

    @router.message(SupportFlow.waiting_for_question)
    async def process_support_question(message: Message, state: FSMContext) -> None:
        if not message.from_user:
            return

        if not await _ensure_authorized(message, db, content_loader):
            await state.clear()
            return

        user_row = await db.get_user(message.from_user.id)
        if user_row is None:
            await state.clear()
            await _deny_access(message, content_loader)
            return

        if not settings.support_chat_ids:
            await state.clear()
            await message.answer(
                "Поддержка не настроена. Обратитесь к организатору.",
                reply_markup=main_menu_keyboard(),
            )
            return

        username = f"@{user_row['username']}" if user_row["username"] else "—"
        question_header = (
            "🧑‍💼 Новый вопрос от партнёра\n"
            f"#USER_{message.from_user.id}\n"
            f"Имя: {user_row['first_name'] or message.from_user.first_name or '—'}\n"
            f"Username: {username}\n"
            f"Код доступа: {user_row['access_code']}\n"
            f"Компания: {user_row['company'] or '—'}"
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
                reply_markup=main_menu_keyboard(),
            )
        else:
            await message.answer(
                "Не удалось доставить вопрос в поддержку. Попробуйте позже.",
                reply_markup=main_menu_keyboard(),
            )

    @router.message(Command("add_code"))
    async def admin_add_code(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        payload = _extract_command_payload(message.text or "")
        if not payload:
            await message.answer("Формат: /add_code CODE Описание")
            return

        chunks = payload.split(maxsplit=1)
        code = normalize_code(chunks[0])
        description = chunks[1].strip() if len(chunks) > 1 else ""
        await db.add_or_update_access_code(code, description, is_active=True)
        await message.answer(f"Код {code} добавлен/обновлён.")

    @router.message(Command("disable_code"))
    async def admin_disable_code(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        payload = _extract_command_payload(message.text or "")
        if not payload:
            await message.answer("Формат: /disable_code CODE")
            return
        count = await db.set_access_code_status(payload.split()[0], is_active=False)
        if count:
            await message.answer("Код деактивирован.")
        else:
            await message.answer("Код не найден.")

    @router.message(Command("enable_code"))
    async def admin_enable_code(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        payload = _extract_command_payload(message.text or "")
        if not payload:
            await message.answer("Формат: /enable_code CODE")
            return
        count = await db.set_access_code_status(payload.split()[0], is_active=True)
        if count:
            await message.answer("Код активирован.")
        else:
            await message.answer("Код не найден.")

    @router.message(Command("list_codes"))
    async def admin_list_codes(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        rows = await db.list_access_codes(limit=200)
        if not rows:
            await message.answer("Список кодов пуст.")
            return

        lines = ["Коды доступа:"]
        for row in rows:
            status = "🟢" if row["is_active"] else "🔴"
            description = row["description"] or "Без описания"
            lines.append(f"{status} {row['code']} — {description}")

        text = "\n".join(lines)
        if len(text) > 3800:
            text = text[:3790] + "\n..."
        await message.answer(text)

    @router.message(Command("broadcast"))
    async def admin_broadcast(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        if not message.from_user:
            return

        payload = _extract_command_payload(message.text or "")
        source_chat_id = None
        source_message_id = None
        if message.reply_to_message is not None:
            source_chat_id = message.reply_to_message.chat.id
            source_message_id = message.reply_to_message.message_id

        if not payload and not source_message_id:
            await message.answer(
                "Формат:\n"
                "1) /broadcast Текст рассылки\n"
                "2) Ответьте на медиа/сообщение командой /broadcast"
            )
            return

        broadcast_id = await db.create_broadcast(
            created_by=message.from_user.id,
            message_text=payload or None,
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
            scheduled_at=None,
            status="scheduled",
        )
        delivered, failed = await send_broadcast(bot, db, broadcast_id=broadcast_id)
        await message.answer(
            "✅ Рассылка завершена.\n"
            f"ID: {broadcast_id}\n"
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
        parts = text.split(maxsplit=2)
        if len(parts) < 2:
            await message.answer(
                "Формат:\n"
                "1) /broadcast_in МИНУТЫ Текст рассылки\n"
                "2) Ответьте на медиа и отправьте /broadcast_in МИНУТЫ"
            )
            return

        try:
            minutes = int(parts[1])
            if minutes <= 0:
                raise ValueError
        except ValueError:
            await message.answer("Количество минут должно быть целым числом больше нуля.")
            return

        payload = parts[2].strip() if len(parts) > 2 else ""
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
            message_text=payload or None,
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
            scheduled_at=run_at.isoformat(),
            status="scheduled",
        )
        scheduler.schedule(broadcast_id=broadcast_id, run_at=run_at)

        await message.answer(
            "⏳ Рассылка запланирована.\n"
            f"ID: {broadcast_id}\n"
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
            f"Статус: {row['status']}\n"
            f"Отправлено: {row['sent_at'] or 'ещё не отправлена'}\n"
            f"Доставлено: {stats['delivered']}\n"
            f"Ошибок: {stats['failed']}"
        )

    @router.message(lambda message: message.chat.id in settings.support_chat_ids and message.reply_to_message is not None)
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
            await message.reply("✅ Ответ отправлен партнёру.")
        except TelegramForbiddenError:
            await message.reply("Пользователь заблокировал бота. Ответ не доставлен.")
        except Exception:  # noqa: BLE001
            logger.exception("Failed to forward manager response to user %s", user_id)
            await message.reply("Не удалось отправить ответ пользователю.")

    @router.message()
    async def fallback(message: Message) -> None:
        if message.chat.id in settings.support_chat_ids:
            return
        if not await _ensure_authorized(message, db, content_loader):
            return
        await message.answer(
            "Используйте кнопки меню для навигации.",
            reply_markup=main_menu_keyboard(),
        )

    dp.include_router(router)
    return dp
