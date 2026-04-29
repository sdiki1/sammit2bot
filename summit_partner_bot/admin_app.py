from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.status import HTTP_303_SEE_OTHER

from summit_partner_bot.config import load_settings
from summit_partner_bot.content import ContentLoader
from summit_partner_bot.db import (
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
TAB_SYSTEM = "system"

TAB_ORDER = [TAB_PUBLIC, TAB_PARTNERS, TAB_EXPERTS, TAB_INFLUENCERS, TAB_SYSTEM]
TAB_TITLES = {
    TAB_PUBLIC: "Публичное",
    TAB_PARTNERS: "Партнёры",
    TAB_EXPERTS: "Эксперты",
    TAB_INFLUENCERS: "Инфлюенсеры",
    TAB_SYSTEM: "Сервис",
}

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

    @app.get("/dashboard")
    async def dashboard(request: Request) -> Any:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect

        active_tab = _sanitize_tab(request.query_params.get("tab"))

        stats = await db.get_stats()
        users = await db.list_users(limit=300)
        pending_users = await db.list_pending_users(limit=200)
        all_codes = await db.list_access_codes(limit=300)
        broadcasts = await db.list_broadcasts(limit=120)
        feedback_items = await db.list_feedback(limit=120)
        settings_map = await db.get_content_settings_map()

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

        role_for_tab = TAB_ROLE.get(active_tab)
        if role_for_tab:
            codes = [row for row in all_codes if str(row["role"]) == role_for_tab]
        elif active_tab == TAB_PUBLIC:
            codes = []
        else:
            codes = all_codes

        return templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "flash": _pop_flash(request),
                "stats": stats,
                "users": users,
                "pending_users": pending_users,
                "codes": codes,
                "all_codes": all_codes,
                "broadcasts": broadcasts,
                "feedback_items": feedback_items,
                "links_by_section": links_by_section,
                "sections": sections,
                "visible_sections": visible_sections,
                "settings_map": settings_map,
                "settings": settings,
                "roles": [ROLE_PARTNER, ROLE_EXPERT, ROLE_INFLUENCER],
                "targets": [ROLE_ALL, ROLE_PARTNER, ROLE_EXPERT, ROLE_INFLUENCER],
                "active_tab": active_tab,
                "tabs": [(tab, TAB_TITLES[tab]) for tab in TAB_ORDER],
            },
        )

    @app.post("/codes/add")
    async def add_code(
        request: Request,
        code: str = Form(...),
        role: str = Form(ROLE_PARTNER),
        description: str = Form(""),
        tab: str = Form(TAB_PUBLIC),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        await db.add_or_update_access_code(code=code, role=normalize_role(role), description=description, is_active=True)
        _set_flash(request, "Код сохранён.")
        return _redirect(_dashboard_url(_sanitize_tab(tab)))

    @app.post("/codes/status")
    async def set_code_status(
        request: Request,
        code: str = Form(...),
        is_active: str = Form(...),
        tab: str = Form(TAB_PUBLIC),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        await db.set_access_code_status(code=code, is_active=(is_active == "1"))
        _set_flash(request, "Статус кода обновлён.")
        return _redirect(_dashboard_url(_sanitize_tab(tab)))

    @app.post("/codes/delete")
    async def delete_code(
        request: Request,
        code: str = Form(...),
        tab: str = Form(TAB_PUBLIC),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        deleted = await db.delete_access_code(code=code)
        if deleted:
            _set_flash(request, "Код удалён.")
        else:
            _set_flash(request, "Код не найден.")
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

    @app.post("/links/add")
    async def add_link(
        request: Request,
        section: str = Form(...),
        title: str = Form(...),
        url: str = Form(...),
        position: int = Form(100),
        is_active: str | None = Form(default=None),
        tab: str = Form(TAB_PUBLIC),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        try:
            await db.add_content_link(
                section=section,
                title=title,
                url=url,
                position=position,
                is_active=bool(is_active),
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
        url: str = Form(...),
        position: int = Form(100),
        is_active: str | None = Form(default=None),
        tab: str = Form(TAB_PUBLIC),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        await db.update_content_link(
            link_id=link_id,
            section=section,
            title=title,
            url=url,
            position=position,
            is_active=bool(is_active),
        )
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
        delay_minutes: str | None = Form(default="0"),
        broadcast_image: UploadFile | None = File(default=None),
        tab: str = Form(TAB_PUBLIC),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect

        text = message_text.strip()
        image_path: str | None = None
        if broadcast_image is not None and (broadcast_image.filename or "").strip():
            content_type = (broadcast_image.content_type or "").lower()
            if not content_type.startswith("image/"):
                _set_flash(request, "Можно загружать только изображения.")
                return _redirect(_dashboard_url(_sanitize_tab(tab)))

            suffix = _guess_upload_suffix(broadcast_image)
            if not suffix:
                _set_flash(request, "Поддерживаются JPG, PNG и WEBP.")
                return _redirect(_dashboard_url(_sanitize_tab(tab)))

            uploads_dir = _broadcast_uploads_dir(settings.content_file)
            uploads_dir.mkdir(parents=True, exist_ok=True)
            file_path = uploads_dir / f"{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{uuid4().hex}{suffix}"
            file_path.write_bytes(await broadcast_image.read())
            image_path = str(file_path)

        if not text and not image_path:
            _set_flash(request, "Укажите текст рассылки или добавьте изображение.")
            return _redirect(_dashboard_url(_sanitize_tab(tab)))

        minutes = max(_sanitize_int(delay_minutes, 0), 0)
        scheduled_at = datetime.now(timezone.utc) + timedelta(minutes=minutes)

        await db.create_broadcast(
            created_by=0,
            target_role=normalize_target_role(target_role),
            message_text=text or None,
            image_path=image_path,
            source_chat_id=None,
            source_message_id=None,
            scheduled_at=scheduled_at,
            status="scheduled",
        )
        if minutes == 0:
            _set_flash(request, "Рассылка создана и будет отправлена в ближайший цикл планировщика.")
        else:
            _set_flash(request, f"Рассылка запланирована через {minutes} мин.")
        return _redirect(_dashboard_url(_sanitize_tab(tab)))

    return app
