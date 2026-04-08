from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Form, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.status import HTTP_303_SEE_OTHER

from summit_partner_bot.config import load_settings
from summit_partner_bot.content import ContentLoader
from summit_partner_bot.db import Database


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


def create_app() -> FastAPI:
    settings = load_settings()
    db = Database(settings.database_url)
    content_loader = ContentLoader(db=db, path=settings.content_file)
    templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

    app = FastAPI(title="Summit Partner Admin")
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

        stats = await db.get_stats()
        users = await db.list_users(limit=150)
        codes = await db.list_access_codes(limit=300)
        broadcasts = await db.list_broadcasts(limit=100)
        settings_map = await db.get_content_settings_map()
        links_all = await db.list_all_content_links()
        useful_links = [row for row in links_all if row["section"] == "useful_links"]
        materials = [row for row in links_all if row["section"] == "partner_materials"]

        return templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "flash": _pop_flash(request),
                "stats": stats,
                "users": users,
                "codes": codes,
                "broadcasts": broadcasts,
                "useful_links": useful_links,
                "materials": materials,
                "settings_map": settings_map,
                "settings": settings,
            },
        )

    @app.post("/codes/add")
    async def add_code(
        request: Request,
        code: str = Form(...),
        description: str = Form(""),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        await db.add_or_update_access_code(code=code, description=description, is_active=True)
        _set_flash(request, "Код сохранён.")
        return _redirect("/dashboard")

    @app.post("/codes/status")
    async def set_code_status(
        request: Request,
        code: str = Form(...),
        is_active: str = Form(...),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        await db.set_access_code_status(code=code, is_active=(is_active == "1"))
        _set_flash(request, "Статус кода обновлён.")
        return _redirect("/dashboard")

    @app.post("/links/add")
    async def add_link(
        request: Request,
        section: str = Form(...),
        title: str = Form(...),
        url: str = Form(...),
        position: int = Form(100),
        is_active: str | None = Form(default=None),
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
        return _redirect("/dashboard")

    @app.post("/links/update")
    async def update_link(
        request: Request,
        link_id: int = Form(...),
        title: str = Form(...),
        url: str = Form(...),
        position: int = Form(100),
        is_active: str | None = Form(default=None),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        await db.update_content_link(
            link_id=link_id,
            title=title,
            url=url,
            position=position,
            is_active=bool(is_active),
        )
        _set_flash(request, "Ссылка обновлена.")
        return _redirect("/dashboard")

    @app.post("/links/delete")
    async def delete_link(
        request: Request,
        link_id: int = Form(...),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect
        await db.delete_content_link(link_id)
        _set_flash(request, "Ссылка удалена.")
        return _redirect("/dashboard")

    @app.post("/settings/save")
    async def save_settings(
        request: Request,
        program_title: str = Form(""),
        program_url: str = Form(""),
        manager_title: str = Form(""),
        manager_url: str = Form(""),
        restricted_text: str = Form(""),
        welcome_template: str = Form(""),
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
                "restricted_text": restricted_text.strip(),
                "welcome_template": welcome_template.strip(),
            }
        )
        _set_flash(request, "Настройки сохранены.")
        return _redirect("/dashboard")

    @app.post("/broadcasts/create")
    async def create_broadcast(
        request: Request,
        message_text: str = Form(...),
        delay_minutes: str | None = Form(default="0"),
    ) -> RedirectResponse:
        maybe_redirect = _require_auth(request)
        if maybe_redirect is not None:
            return maybe_redirect

        text = message_text.strip()
        if not text:
            _set_flash(request, "Текст рассылки не может быть пустым.")
            return _redirect("/dashboard")

        minutes = max(_sanitize_int(delay_minutes, 0), 0)
        scheduled_at = datetime.now(timezone.utc) + timedelta(minutes=minutes)

        await db.create_broadcast(
            created_by=0,
            message_text=text,
            source_chat_id=None,
            source_message_id=None,
            scheduled_at=scheduled_at,
            status="scheduled",
        )
        if minutes == 0:
            _set_flash(request, "Рассылка создана и будет отправлена в ближайший цикл планировщика.")
        else:
            _set_flash(request, f"Рассылка запланирована через {minutes} мин.")
        return _redirect("/dashboard")

    return app
