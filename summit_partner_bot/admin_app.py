from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from fastapi import FastAPI, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from starlette.middleware.sessions import SessionMiddleware


class SiteApplicationRequest(BaseModel):
    company: str = ""
    inn: str = ""
    contact_name: str = ""
    phone: str = ""
    email: str = ""
    booth: str = ""
    message: str = ""
from starlette.status import HTTP_303_SEE_OTHER

from summit_partner_bot.config import load_settings
from summit_partner_bot.content import ContentLoader
from summit_partner_bot.messages import MESSAGE_REGISTRY
from summit_partner_bot.db import (
    APPLICATION_STATUS_DONE,
    APPLICATION_STATUS_IN_PROGRESS,
    APPLICATION_STATUS_NEW,
    APPLICATION_STATUS_REJECTED,
    ROLE_ALL,
    ROLE_EXPERT,
    ROLE_INFLUENCER,
    ROLE_PARTNER,
    SECTION_EXPERT_MATERIALS,
    SECTION_EXPERT_USEFUL_LINKS,
    SECTION_INFLUENCER_MATERIALS,
    SECTION_INFLUENCER_USEFUL_LINKS,
    SECTION_PARTNER_MATERIALS,
    SECTION_PARTNER_USEFUL_LINKS,
    SECTION_PUBLIC_MENU_LINKS,
    Database,
    normalize_subcategory,
    normalize_role,
    normalize_target_role,
)


PRESET_SECTIONS = [
    SECTION_PUBLIC_MENU_LINKS,
    SECTION_PARTNER_USEFUL_LINKS,
    SECTION_EXPERT_USEFUL_LINKS,
    SECTION_INFLUENCER_USEFUL_LINKS,
    SECTION_PARTNER_MATERIALS,
    SECTION_EXPERT_MATERIALS,
    SECTION_INFLUENCER_MATERIALS,
]

TAB_PUBLIC = "public"
TAB_PARTNERS = "partners"
TAB_EXPERTS = "experts"
TAB_INFLUENCERS = "influencers"
TAB_CHATS = "chats"
TAB_APPLICATIONS = "applications"
TAB_BROADCASTS = "broadcasts"
TAB_USERS = "users"
TAB_FEEDBACK = "feedback"
TAB_TEXTS = "texts"
TAB_SYSTEM = "system"

TAB_BOT_KEY = {
    TAB_PUBLIC: "summit",
    TAB_PARTNERS: "partner",
    TAB_EXPERTS: "expert",
    TAB_INFLUENCERS: "influencer",
}

TAB_ORDER = [
    TAB_CHATS,
    TAB_APPLICATIONS,
    TAB_BROADCASTS,
    TAB_USERS,
    TAB_FEEDBACK,
    TAB_TEXTS,
    TAB_PUBLIC,
    TAB_PARTNERS,
    TAB_EXPERTS,
    TAB_INFLUENCERS,
]
TAB_TITLES = {
    TAB_PUBLIC: "Публичное",
    TAB_PARTNERS: "Партнёры",
    TAB_EXPERTS: "Эксперты",
    TAB_INFLUENCERS: "Инфлюенсеры",
    TAB_CHATS: "Переписка",
    TAB_APPLICATIONS: "Заявки/брони",
    TAB_BROADCASTS: "Рассылки",
    TAB_USERS: "Все пользователи",
    TAB_FEEDBACK: "Отзывы",
    TAB_TEXTS: "Скрипты",
}

APPLICATION_STATUSES = [
    APPLICATION_STATUS_NEW,
    APPLICATION_STATUS_IN_PROGRESS,
    APPLICATION_STATUS_DONE,
    APPLICATION_STATUS_REJECTED,
]

TAB_ROLE = {
    TAB_PARTNERS: ROLE_PARTNER,
    TAB_EXPERTS: ROLE_EXPERT,
    TAB_INFLUENCERS: ROLE_INFLUENCER,
}

TAB_SECTIONS = {
    TAB_PUBLIC: [SECTION_PUBLIC_MENU_LINKS],
    TAB_PARTNERS: [SECTION_PARTNER_USEFUL_LINKS, SECTION_PARTNER_MATERIALS],
    TAB_EXPERTS: [SECTION_EXPERT_USEFUL_LINKS, SECTION_EXPERT_MATERIALS],
    TAB_INFLUENCERS: [SECTION_INFLUENCER_USEFUL_LINKS, SECTION_INFLUENCER_MATERIALS],
    TAB_CHATS: [],
    TAB_APPLICATIONS: [],
    TAB_BROADCASTS: [],
    TAB_USERS: [],
    TAB_FEEDBACK: [],
    TAB_TEXTS: [],
    TAB_SYSTEM: [],
}


def _is_authenticated(request: Request) -> bool:
    return bool(request.session.get("admin_ok"))


def _set_flash(request: Request, text: str) -> None:
    request.session["flash"] = text


def _pop_flash(request: Request) -> str:
    return str(request.session.pop("flash", ""))


def _redirect(to: str) -> RedirectResponse:
    return RedirectResponse(url=to, status_code=HTTP_303_SEE_OTHER)


def _require_auth(request: Request) -> RedirectResponse | None:
    if _is_authenticated(request):
        return None
    return _redirect("/login")


def _sanitize_int(value: str | None, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _sanitize_tab(raw_value: str | None) -> str:
    value = (raw_value or "").strip().lower()
    if value in TAB_ORDER:
        return value
    return TAB_PUBLIC


def _dashboard_url(tab: str) -> str:
    return f"/dashboard?tab={tab}"


def _broadcast_uploads_dir(content_file: Path) -> Path:
    return content_file.resolve().parent / "uploads" / "broadcasts"


def _bot_link_base(settings: Any) -> str:
    username = settings.partner_bot_username or settings.bot_username
    if username:
        return f"https://t.me/{username}"
    return ""


def _guess_upload_suffix(upload: UploadFile) -> str:
    suffix = Path(upload.filename or "").suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png", ".webp"}:
        return suffix

    content_type = (upload.content_type or "").lower()
    if content_type == "image/jpeg":
        return ".jpg"
    if content_type == "image/png":
        return ".png"
    if content_type == "image/webp":
        return ".webp"
    return ""


def _bot_token_for_user(settings: Any, role: str | None, bot_key: str | None) -> tuple[str, str]:
    table = {
        "summit": settings.summit_bot_token,
        "partner": settings.partner_bot_token,
        "expert": settings.expert_bot_token,
        "influencer": settings.influencer_bot_token,
    }
    candidates: list[str] = []
    if bot_key:
        candidates.append(bot_key)
    if role and role not in candidates:
        candidates.append(role)
    if "summit" not in candidates:
        candidates.append("summit")
    for key in candidates:
        token = table.get(key, "")
        if token:
            return token, key
    return settings.summit_bot_token, "summit"


async def _effective_support_chat_ids(db: Any, settings: Any) -> set[int]:
    smap = await db.get_content_settings_map()
    raw = (smap.get("support_chat_ids_override") or "").strip()
    if raw:
        ids: set[int] = set()
        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                ids.add(int(part))
            except ValueError:
                continue
        if ids:
            return ids
    return set(settings.support_chat_ids)


def _parse_admin_datetime(value: str | None) -> datetime | None:
    text = (value or "").strip()
    if not text:
        return None
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("Europe/Moscow"))
    return parsed.astimezone(timezone.utc)


def create_app() -> FastAPI:
    settings = load_settings()
    db = Database(settings.database_url)
    content_loader = ContentLoader(db=db, path=settings.content_file)
    templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

    app = FastAPI(title="Summit Admin")
    app.add_middleware(
        SessionMiddleware,
        secret_key=settings.admin_panel_secret,
        same_site="lax",
        https_only=False,
    )

    @app.on_event("startup")
    async def on_startup() -> None:
        await db.connect()
        await db.init_schema()
        await content_loader.bootstrap_defaults()

    @app.on_event("shutdown")
    async def on_shutdown() -> None:
        await db.close()

    @app.get("/")
    async def root(request: Request) -> RedirectResponse:
        if _is_authenticated(request):
            return _redirect("/dashboard")
        return _redirect("/login")

    @app.get("/login")
    async def login_page(request: Request) -> Any:
        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context={"error": _pop_flash(request)},
        )

    @app.post("/login")
    async def login_submit(
        request: Request,
        username: str = Form(...),
        password: str = Form(...),
    ) -> RedirectResponse:
        if username.strip() != settings.admin_panel_username or password != settings.admin_panel_password:
            _set_flash(request, "Неверный логин или пароль.")
            return _redirect("/login")
        request.session["admin_ok"] = True
        return _redirect("/dashboard")

    @app.get("/logout")
    async def logout(request: Request) -> RedirectResponse:
        request.session.clear()
        return _redirect("/login")

    @app.get("/partner-application")
    async def partner_application_page(request: Request) -> Any:
        return templates.TemplateResponse(
            request=request,
            name="partner_application.html",
            context={
                "application": None,
                "bot_link": "",
                "bot_link_base": _bot_link_base(settings),
            },
        )

    @app.post("/partner-application")
    async def partner_application_submit(
        request: Request,
        request_text: str = Form(""),
        booth_number: str = Form(""),
        company: str = Form(""),
        inn: str = Form(""),
        full_name: str = Form(""),
        phone: str = Form(""),
        email: str = Form(""),
    ) -> Any:
        token = uuid4().hex[:24]
        application = await db.create_application(
            token=token,
            role=ROLE_PARTNER,
            source="site",
            request_text=request_text,
            booth_number=booth_number,
            full_name=full_name,
            phone=phone,
            email=email,
            company=company,
            inn=inn,
        )
        base = _bot_link_base(settings)
        bot_link = f"{base}?start=app_{token}" if base else ""
        return templates.TemplateResponse(
            request=request,
            name="partner_application.html",
            context={
                "application": application,
                "bot_link": bot_link,
                "bot_link_base": base,
            },
        )

    @app.post("/api/v1/partner-application")
    async def api_create_partner_application(
        body: SiteApplicationRequest,
        x_api_key: str | None = Header(default=None, alias="X-Api-Key"),
    ) -> JSONResponse:
        if not settings.site_api_key:
            raise HTTPException(status_code=503, detail="API endpoint is disabled")
        if x_api_key != settings.site_api_key:
            raise HTTPException(status_code=401, detail="Invalid API key")

        token = uuid4().hex[:24]
        request_text = body.message.strip() or "Заявка с сайта"
        await db.create_application(
            token=token,
            role=ROLE_PARTNER,
            source="site",
            request_text=request_text,
            booth_number=body.booth,
            full_name=body.contact_name,
            phone=body.phone,
            email=body.email,
            company=body.company,
            inn=body.inn,
        )

        partner_username = (settings.partner_bot_username or settings.bot_username).strip()
        deep_link = f"https://t.me/{partner_username}?start=app_{token}" if partner_username else ""

        return JSONResponse({"token": token, "deep_link": deep_link})

    @app.get("/dashboard")
    async def dashboard(request: Request) -> Any:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect

        active_tab = _sanitize_tab(request.query_params.get("tab"))

        stats = await db.get_stats()
        users = await db.list_users(limit=300)
        pending_users = await db.list_pending_users(limit=200)
        subcategories = await db.list_subcategories()
        broadcasts = await db.list_broadcasts(limit=120)
        feedback_items = await db.list_feedback(limit=120)
        applications = await db.list_applications(limit=200)
        support_sessions = await db.list_active_support_sessions(limit=100)
        admin_users = await db.list_admin_users()
        settings_map = await db.get_content_settings_map()

        all_consent_docs = await db.get_all_consent_documents()
        consent_docs_by_bot: dict[str, list[Any]] = {}
        for doc in all_consent_docs:
            consent_docs_by_bot.setdefault(str(doc["bot_key"]), []).append(doc)

        chat_users = await db.list_chat_users(limit=300) if active_tab == TAB_CHATS else []

        links_all = await db.list_all_content_links(include_inactive=True)
        links_by_section: dict[str, list[Any]] = {}
        for row in links_all:
            section = str(row["section"])
            links_by_section.setdefault(section, []).append(row)

        dynamic_sections = await db.list_content_sections()
        sections = sorted(set(PRESET_SECTIONS + dynamic_sections))

        visible_sections = TAB_SECTIONS.get(active_tab, [])
        if active_tab == TAB_SYSTEM:
            visible_sections = sections

        subcategories_by_role: dict[str, list[Any]] = {role: [] for role in [ROLE_PARTNER, ROLE_EXPERT, ROLE_INFLUENCER]}
        for row in subcategories:
            subcategories_by_role.setdefault(str(row["role"]), []).append(row)

        return templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "flash": _pop_flash(request),
                "stats": stats,
                "users": users,
                "pending_users": pending_users,
                "subcategories": subcategories,
                "subcategories_by_role": subcategories_by_role,
                "broadcasts": broadcasts,
                "feedback_items": feedback_items,
                "applications": applications,
                "application_statuses": APPLICATION_STATUSES,
                "application_status_labels": [
                    (APPLICATION_STATUS_NEW, "Новая"),
                    (APPLICATION_STATUS_IN_PROGRESS, "В работе"),
                    (APPLICATION_STATUS_DONE, "Завершена"),
                    (APPLICATION_STATUS_REJECTED, "Отклонена"),
                ],
                "support_sessions": support_sessions,
                "admin_users": admin_users,
                "env_admin_ids": sorted(settings.admin_ids),
                "links_by_section": links_by_section,
                "sections": sections,
                "visible_sections": visible_sections,
                "settings_map": settings_map,
                "settings": settings,
                "roles": [ROLE_PARTNER, ROLE_EXPERT, ROLE_INFLUENCER],
                "targets": [ROLE_ALL, ROLE_PARTNER, ROLE_EXPERT, ROLE_INFLUENCER],
                "active_tab": active_tab,
                "tabs": [(tab, TAB_TITLES[tab]) for tab in TAB_ORDER],
                "bot_link_base": _bot_link_base(settings),
                "consent_docs_by_bot": consent_docs_by_bot,
                "tab_bot_key": TAB_BOT_KEY,
                "chat_users": chat_users,
                "message_registry": MESSAGE_REGISTRY,
            },
        )

    @app.post("/texts/save")
    async def save_texts(request: Request) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        form = await request.form()
        updates: dict[str, str] = {}
        for key, _, _ in MESSAGE_REGISTRY:
            if key in form:
                updates[key] = str(form.get(key) or "").strip()
        if updates:
            await db.upsert_content_settings(updates)
        _set_flash(request, "Тексты сохранены.")
        return _redirect(_dashboard_url(TAB_TEXTS))

    @app.post("/texts/reset")
    async def reset_text(request: Request, key: str = Form(...)) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        await db.upsert_content_settings({key: ""})
        _set_flash(request, f"Текст «{key}» сброшен до значения по умолчанию.")
        return _redirect(_dashboard_url(TAB_TEXTS))

    @app.get("/chats/{telegram_id}")
    async def chat_view(request: Request, telegram_id: int) -> Any:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect

        messages = await db.list_support_messages(telegram_id=telegram_id, limit=2000)
        user_row = await db.get_user(telegram_id)
        last_bot_key = await db.get_last_chat_bot_key(telegram_id)
        role = str(user_row["role"]) if user_row else None
        _, chosen_bot_key = _bot_token_for_user(settings, role, last_bot_key)
        active_session = await db.get_active_support_session(telegram_id)
        return templates.TemplateResponse(
            request=request,
            name="chat.html",
            context={
                "flash": _pop_flash(request),
                "telegram_id": telegram_id,
                "messages": messages,
                "user": user_row,
                "settings": settings,
                "active_bot_key": chosen_bot_key,
                "active_session": active_session,
            },
        )

    @app.post("/chats/{telegram_id}/close")
    async def chat_close(request: Request, telegram_id: int) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect

        from aiogram import Bot
        from aiogram.exceptions import TelegramAPIError

        closed = await db.close_support_session(telegram_id)
        if closed is None:
            _set_flash(request, "Активного диалога нет.")
            return _redirect(f"/chats/{telegram_id}")

        user_row = await db.get_user(telegram_id)
        last_bot_key = await db.get_last_chat_bot_key(telegram_id)
        role = str(user_row["role"]) if user_row else None
        token, chosen_key = _bot_token_for_user(settings, role, last_bot_key)
        if chosen_key == "partner":
            farewell = (
                "Спасибо за диалог!\n\n"
                "Дальше этот бот будет работать как информационный канал для партнёров СТАММИТ’26.\n\n"
                "Здесь будут появляться важные обновления: сроки подготовки, технические требования, "
                "новости саммита, информация по экспо-зоне, партнёрские материалы и организационные напоминания."
            )
        else:
            farewell = "🧑‍💼 Менеджер завершил диалог. Если будет нужно — обращайтесь снова."
        if token:
            bot = Bot(token=token)
            try:
                await bot.send_message(chat_id=telegram_id, text=farewell)
            except TelegramAPIError:
                pass
            finally:
                await bot.session.close()

        _set_flash(request, "Диалог завершён.")
        return _redirect(f"/chats/{telegram_id}")

    @app.post("/chats/{telegram_id}/send")
    async def chat_send(
        request: Request,
        telegram_id: int,
        message_text: str = Form(""),
        attachment: UploadFile | None = File(default=None),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect

        from aiogram import Bot
        from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError
        from aiogram.types import BufferedInputFile

        text = (message_text or "").strip()
        user_row = await db.get_user(telegram_id)
        last_bot_key = await db.get_last_chat_bot_key(telegram_id)
        role = str(user_row["role"]) if user_row else None
        token, chosen_key = _bot_token_for_user(settings, role, last_bot_key)

        if not token:
            _set_flash(request, "Не настроен токен бота для отправки.")
            return _redirect(f"/chats/{telegram_id}")

        has_attachment = attachment is not None and (attachment.filename or "").strip()
        if not text and not has_attachment:
            _set_flash(request, "Введите текст или приложите файл.")
            return _redirect(f"/chats/{telegram_id}")

        bot = Bot(token=token)
        sent_media_type: str | None = None
        sent_file_id: str | None = None
        try:
            if has_attachment:
                data = await attachment.read()
                content_type = (attachment.content_type or "").lower()
                filename = (attachment.filename or "file").strip() or "file"
                input_file = BufferedInputFile(data, filename=filename)
                if content_type.startswith("image/"):
                    sent = await bot.send_photo(chat_id=telegram_id, photo=input_file, caption=text or None)
                    sent_media_type = "photo"
                    if sent.photo:
                        sent_file_id = sent.photo[-1].file_id
                elif content_type.startswith("video/"):
                    sent = await bot.send_video(chat_id=telegram_id, video=input_file, caption=text or None)
                    sent_media_type = "video"
                    if sent.video:
                        sent_file_id = sent.video.file_id
                else:
                    sent = await bot.send_document(chat_id=telegram_id, document=input_file, caption=text or None)
                    sent_media_type = "document"
                    if sent.document:
                        sent_file_id = sent.document.file_id
            else:
                await bot.send_message(chat_id=telegram_id, text=text)

            await db.log_support_message(
                telegram_id=telegram_id,
                direction="manager",
                text=text or None,
                media_type=sent_media_type,
                file_id=sent_file_id,
                manager_telegram_id=None,
                bot_key=chosen_key,
            )
            existing_session = await db.get_active_support_session(telegram_id)
            if existing_session is None:
                effective_support = await _effective_support_chat_ids(db, settings)
                if effective_support:
                    support_chat_id = next(iter(effective_support))
                elif settings.admin_ids:
                    support_chat_id = next(iter(settings.admin_ids))
                else:
                    support_chat_id = 0
                await db.connect_support_session(
                    telegram_id=telegram_id,
                    support_chat_id=support_chat_id,
                    manager_telegram_id=None,
                )
            _set_flash(request, "✅ Сообщение отправлено пользователю.")
        except TelegramForbiddenError:
            _set_flash(request, "⚠️ Пользователь заблокировал бота. Сообщение не доставлено.")
        except TelegramAPIError as exc:
            _set_flash(request, f"⚠️ Ошибка Telegram: {exc}")
        finally:
            await bot.session.close()

        return _redirect(f"/chats/{telegram_id}")

    @app.post("/subcategories/add")
    async def add_subcategory(
        request: Request,
        role: str = Form(ROLE_PARTNER),
        name: str = Form(...),
        tab: str = Form(TAB_PUBLIC),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        subcategory_id = await db.add_subcategory(role=normalize_role(role), name=name)
        _set_flash(request, "Подкатегория создана." if subcategory_id else "Укажите название подкатегории.")
        return _redirect(_dashboard_url(_sanitize_tab(tab)))

    @app.post("/subcategories/delete")
    async def delete_subcategory(
        request: Request,
        role: str = Form(ROLE_PARTNER),
        name: str = Form(...),
        tab: str = Form(TAB_PUBLIC),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        deleted = await db.delete_subcategory(role=normalize_role(role), name=name)
        _set_flash(request, "Подкатегория удалена." if deleted else "Подкатегория не найдена.")
        return _redirect(_dashboard_url(_sanitize_tab(tab)))

    @app.post("/users/approve")
    async def approve_user(
        request: Request,
        telegram_id: int = Form(...),
        tab: str = Form(TAB_PUBLIC),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        await db.approve_user(telegram_id=telegram_id, approved_by=0)
        _set_flash(request, f"Пользователь {telegram_id} подтверждён.")
        return _redirect(_dashboard_url(_sanitize_tab(tab)))

    @app.post("/users/reject")
    async def reject_user(
        request: Request,
        telegram_id: int = Form(...),
        reason: str = Form(""),
        tab: str = Form(TAB_PUBLIC),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        await db.reject_user(telegram_id=telegram_id, approved_by=0, reason=reason)
        _set_flash(request, f"Пользователь {telegram_id} отклонён.")
        return _redirect(_dashboard_url(_sanitize_tab(tab)))

    @app.post("/users/subcategory")
    async def set_user_subcategory(
        request: Request,
        telegram_id: int = Form(...),
        subcategory: str = Form(""),
        tab: str = Form(TAB_USERS),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        row = await db.update_user_subcategory(telegram_id=telegram_id, subcategory=subcategory)
        _set_flash(request, f"Подкатегория обновлена." if row else "Пользователь не найден.")
        return _redirect(_dashboard_url(_sanitize_tab(tab)))

    @app.post("/links/add")
    async def add_link(
        request: Request,
        section: str = Form(...),
        title: str = Form(...),
        url: str = Form(""),
        position: int = Form(100),
        is_active: str | None = Form(default=None),
        attachment: UploadFile | None = File(default=None),
        tab: str = Form(TAB_PUBLIC),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        file_bytes: bytes | None = None
        file_filename: str | None = None
        file_mime: str | None = None
        if attachment is not None and (attachment.filename or "").strip():
            file_bytes = await attachment.read()
            file_filename = (attachment.filename or "file").strip()
            file_mime = (attachment.content_type or "").strip() or None
        if not (url or "").strip() and not file_bytes:
            _set_flash(request, "Укажите ссылку или прикрепите файл.")
            return _redirect(_dashboard_url(_sanitize_tab(tab)))
        try:
            await db.add_content_link(
                section=section,
                category="",
                subcategory="",
                title=title,
                url=url,
                position=position,
                is_active=bool(is_active),
                file_bytes=file_bytes,
                file_filename=file_filename,
                file_mime=file_mime,
            )
            _set_flash(request, "Ссылка добавлена.")
        except ValueError:
            _set_flash(request, "Некорректная секция ссылки.")
        return _redirect(_dashboard_url(_sanitize_tab(tab)))

    @app.post("/links/update")
    async def update_link(
        request: Request,
        link_id: int = Form(...),
        section: str = Form(...),
        title: str = Form(...),
        url: str = Form(""),
        position: int = Form(100),
        is_active: str | None = Form(default=None),
        attachment: UploadFile | None = File(default=None),
        clear_file: str | None = Form(default=None),
        tab: str = Form(TAB_PUBLIC),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect

        existing = await db.get_content_link(link_id)
        had_file = existing is not None and (existing["file_bytes"] is not None or (existing["cached_file_id"] or ""))
        has_new_attachment = attachment is not None and (attachment.filename or "").strip()

        # Если файл уже привязан и новый не загружают, не сбрасываем — оставим существующий db_file:N
        if had_file and not has_new_attachment and not clear_file:
            url_to_save = (existing["url"] or "").strip()
        else:
            url_to_save = (url or "").strip()

        await db.update_content_link(
            link_id=link_id,
            section=section,
            category="",
            subcategory="",
            title=title,
            url=url_to_save,
            position=position,
            is_active=bool(is_active),
        )

        if has_new_attachment:
            data = await attachment.read()
            await db.replace_content_link_file(
                link_id=link_id,
                file_bytes=data,
                file_filename=(attachment.filename or "file").strip(),
                file_mime=(attachment.content_type or "").strip() or None,
            )
            _set_flash(request, "Ссылка обновлена, файл прикреплён.")
        elif clear_file:
            await db.clear_content_link_file(link_id=link_id, new_url=(url or "").strip())
            _set_flash(request, "Файл удалён, оставлена ссылка.")
        else:
            _set_flash(request, "Ссылка обновлена.")
        return _redirect(_dashboard_url(_sanitize_tab(tab)))

    @app.post("/links/delete")
    async def delete_link(
        request: Request,
        link_id: int = Form(...),
        tab: str = Form(TAB_PUBLIC),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        await db.delete_content_link(link_id)
        _set_flash(request, "Ссылка удалена.")
        return _redirect(_dashboard_url(_sanitize_tab(tab)))

    @app.post("/applications/status")
    async def update_application_status(
        request: Request,
        application_id: int = Form(...),
        status: str = Form(APPLICATION_STATUS_NEW),
        manager_note: str = Form(""),
        tab: str = Form(TAB_USERS),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        normalized_status = status if status in APPLICATION_STATUSES else APPLICATION_STATUS_NEW
        row = await db.set_application_status(
            application_id=application_id,
            status=normalized_status,
            manager_note=manager_note,
        )
        _set_flash(request, "Статус заявки обновлён." if row else "Заявка не найдена.")
        return _redirect(_dashboard_url(_sanitize_tab(tab)))

    @app.post("/settings/save")
    async def save_settings(
        request: Request,
        program_title: str = Form(""),
        program_url: str = Form(""),
        manager_title: str = Form(""),
        manager_url: str = Form(""),
        manager_title_partner: str = Form(""),
        manager_url_partner: str = Form(""),
        manager_title_expert: str = Form(""),
        manager_url_expert: str = Form(""),
        manager_title_influencer: str = Form(""),
        manager_url_influencer: str = Form(""),
        restricted_text: str = Form(""),
        welcome_template: str = Form(""),
        public_welcome_text: str = Form(""),
        partner_presentation_url: str = Form(""),
        expert_form_url: str = Form(""),
        influencer_form_url: str = Form(""),
        referral_prize_text: str = Form(""),
        tab: str = Form(TAB_PUBLIC),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        await db.upsert_content_settings(
            {
                "program_title": program_title.strip(),
                "program_url": program_url.strip(),
                "manager_title": manager_title.strip(),
                "manager_url": manager_url.strip(),
                "manager_title_partner": manager_title_partner.strip(),
                "manager_url_partner": manager_url_partner.strip(),
                "manager_title_expert": manager_title_expert.strip(),
                "manager_url_expert": manager_url_expert.strip(),
                "manager_title_influencer": manager_title_influencer.strip(),
                "manager_url_influencer": manager_url_influencer.strip(),
                "restricted_text": restricted_text.strip(),
                "welcome_template": welcome_template.strip(),
                "public_welcome_text": public_welcome_text.strip(),
                "partner_presentation_url": partner_presentation_url.strip(),
                "expert_form_url": expert_form_url.strip(),
                "influencer_form_url": influencer_form_url.strip(),
                "referral_prize_text": referral_prize_text.strip(),
            }
        )
        _set_flash(request, "Настройки сохранены.")
        return _redirect(_dashboard_url(_sanitize_tab(tab)))

    @app.post("/broadcasts/create")
    async def create_broadcast(
        request: Request,
        message_text: str = Form(""),
        target_role: str = Form(ROLE_ALL),
        target_subcategory: str = Form(""),
        scheduled_at: str = Form(""),
        delay_minutes: str | None = Form(default="0"),
        broadcast_image: UploadFile | None = File(default=None),
        tab: str = Form(TAB_PUBLIC),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect

        text = message_text.strip()
        image_path: str | None = None
        image_bytes: bytes | None = None
        image_filename: str | None = None
        if broadcast_image is not None and (broadcast_image.filename or "").strip():
            content_type = (broadcast_image.content_type or "").lower()
            if not content_type.startswith("image/"):
                _set_flash(request, "Можно загружать только изображения.")
                return _redirect(_dashboard_url(_sanitize_tab(tab)))

            suffix = _guess_upload_suffix(broadcast_image)
            if not suffix:
                _set_flash(request, "Поддерживаются JPG, PNG и WEBP.")
                return _redirect(_dashboard_url(_sanitize_tab(tab)))

            image_bytes = await broadcast_image.read()
            image_filename = Path(broadcast_image.filename or f"broadcast{suffix}").name

        if not text and not image_path and not image_bytes:
            _set_flash(request, "Укажите текст рассылки или добавьте изображение.")
            return _redirect(_dashboard_url(_sanitize_tab(tab)))

        try:
            scheduled_dt = _parse_admin_datetime(scheduled_at)
        except ValueError:
            _set_flash(request, "Некорректная дата рассылки.")
            return _redirect(_dashboard_url(_sanitize_tab(tab)))
        minutes = max(_sanitize_int(delay_minutes, 0), 0)
        if scheduled_dt is None:
            scheduled_dt = datetime.now(timezone.utc) + timedelta(minutes=minutes)

        normalized_role = normalize_target_role(target_role)
        normalized_subcategory = normalize_subcategory(target_subcategory)
        broadcast_targets = [normalized_role]
        if normalized_role == ROLE_ALL:
            broadcast_targets = [ROLE_PARTNER, ROLE_EXPERT, ROLE_INFLUENCER]

        recipients_count = 0
        broadcast_ids: list[int] = []
        for role_target in broadcast_targets:
            recipients_count += len(await db.list_authorized_user_ids(role_target, normalized_subcategory))
            broadcast_ids.append(
                await db.create_broadcast(
                    created_by=0,
                    target_role=role_target,
                    target_subcategory=normalized_subcategory,
                    sender_role=role_target,
                    message_text=text or None,
                    image_path=image_path,
                    image_bytes=image_bytes,
                    image_filename=image_filename,
                    source_chat_id=None,
                    source_message_id=None,
                    scheduled_at=scheduled_dt,
                    status="scheduled",
                )
            )
        if recipients_count == 0:
            _set_flash(request, "Рассылка создана, но получателей сейчас 0. Проверьте активные доступы и подкатегорию.")
        elif scheduled_at.strip():
            _set_flash(request, f"Рассылка запланирована на выбранную дату. Получателей: {recipients_count}. ID: {', '.join(map(str, broadcast_ids))}.")
        elif minutes == 0:
            _set_flash(request, f"Рассылка создана и будет отправлена в ближайший цикл планировщика. Получателей: {recipients_count}. ID: {', '.join(map(str, broadcast_ids))}.")
        else:
            _set_flash(request, f"Рассылка запланирована через {minutes} мин. Получателей: {recipients_count}. ID: {', '.join(map(str, broadcast_ids))}.")
        return _redirect(_dashboard_url(_sanitize_tab(tab)))

    def _parse_support_override(raw: str) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for chunk in (raw or "").replace(";", ",").split(","):
            chunk = chunk.strip()
            if not chunk:
                continue
            try:
                normalized = str(int(chunk))
            except ValueError:
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            result.append(normalized)
        return result

    @app.post("/chats/support-ids/add")
    async def add_support_chat_id(request: Request, support_id: str = Form(...)) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        raw_value = (support_id or "").strip()
        try:
            new_id = str(int(raw_value))
        except ValueError:
            _set_flash(request, "ID должен быть числом.")
            return _redirect(_dashboard_url(TAB_CHATS))
        smap = await db.get_content_settings_map()
        ids = _parse_support_override(smap.get("support_chat_ids_override", "") or "")
        if new_id in ids:
            _set_flash(request, f"ID {new_id} уже в списке.")
        else:
            ids.append(new_id)
            await db.upsert_content_settings({"support_chat_ids_override": ",".join(ids)})
            _set_flash(request, f"Менеджер {new_id} добавлен.")
        return _redirect(_dashboard_url(TAB_CHATS))

    @app.post("/chats/support-ids/remove")
    async def remove_support_chat_id(request: Request, support_id: str = Form(...)) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        target = (support_id or "").strip()
        smap = await db.get_content_settings_map()
        ids = _parse_support_override(smap.get("support_chat_ids_override", "") or "")
        if target in ids:
            ids.remove(target)
            await db.upsert_content_settings({"support_chat_ids_override": ",".join(ids)})
            _set_flash(request, f"Менеджер {target} удалён.")
        else:
            _set_flash(request, f"ID {target} не найден.")
        return _redirect(_dashboard_url(TAB_CHATS))

    @app.post("/consents/upload")
    async def upload_consent_document(
        request: Request,
        consent_file: UploadFile = File(...),
        tab: str = Form(TAB_PUBLIC),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        bot_key = TAB_BOT_KEY.get(_sanitize_tab(tab), "summit")
        filename = (consent_file.filename or "document.pdf").strip()
        data = await consent_file.read()
        if not data:
            _set_flash(request, "Файл пустой.")
            return _redirect(_dashboard_url(_sanitize_tab(tab)))
        await db.save_consent_document(bot_key=bot_key, filename=filename, file_bytes=data)
        _set_flash(request, f"Документ «{filename}» загружен.")
        return _redirect(_dashboard_url(_sanitize_tab(tab)))

    @app.post("/consents/delete")
    async def delete_consent_document(
        request: Request,
        doc_id: int = Form(...),
        tab: str = Form(TAB_PUBLIC),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        deleted = await db.delete_consent_document(doc_id)
        _set_flash(request, "Документ удалён." if deleted else "Документ не найден.")
        return _redirect(_dashboard_url(_sanitize_tab(tab)))

    @app.post("/broadcasts/delete")
    async def delete_broadcast(
        request: Request,
        broadcast_id: str = Form(...),
        tab: str = Form(TAB_PUBLIC),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect

        try:
            target_id = int(broadcast_id)
        except ValueError:
            _set_flash(request, "ID рассылки должен быть числом.")
            return _redirect(_dashboard_url(_sanitize_tab(tab)))

        deleted = await db.delete_broadcast(target_id, only_unsent=False)
        if deleted:
            _set_flash(request, f"Рассылка #{target_id} удалена.")
        else:
            _set_flash(request, f"Рассылка #{target_id} не найдена.")
        return _redirect(_dashboard_url(_sanitize_tab(tab)))

    return app
