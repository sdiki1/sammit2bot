from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import asyncpg


DEFAULT_RESTRICTED_TEXT = (
    "Доступ к этому боту ограничен. Напишите организатору и получите персональный код доступа."
)
DEFAULT_WELCOME_TEMPLATE = (
    "Добро пожаловать, {first_name}!\n"
    "Вы авторизованы как партнёр {summit_name}.\n\n"
    "Используйте меню ниже для быстрого доступа к информации:"
)

SECTION_USEFUL_LINKS = "useful_links"
SECTION_PARTNER_MATERIALS = "partner_materials"


def normalize_code(code: str) -> str:
    return code.strip().upper()


def _normalize_dt(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _extract_rowcount(status: str) -> int:
    # asyncpg returns "UPDATE <count>", "DELETE <count>"...
    parts = status.split()
    if not parts:
        return 0
    try:
        return int(parts[-1])
    except ValueError:
        return 0


class Database:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            dsn=self.database_url,
            min_size=1,
            max_size=10,
            command_timeout=30,
        )

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("Database is not connected")
        return self._pool

    async def init_schema(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id BIGSERIAL PRIMARY KEY,
                    telegram_id BIGINT NOT NULL UNIQUE,
                    username TEXT,
                    first_name TEXT,
                    access_code TEXT NOT NULL,
                    company TEXT,
                    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS access_codes (
                    code TEXT PRIMARY KEY,
                    description TEXT,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS broadcasts (
                    id BIGSERIAL PRIMARY KEY,
                    created_by BIGINT NOT NULL,
                    message_text TEXT,
                    source_chat_id BIGINT,
                    source_message_id BIGINT,
                    scheduled_at TIMESTAMPTZ,
                    sent_at TIMESTAMPTZ,
                    status TEXT NOT NULL DEFAULT 'scheduled'
                );

                CREATE TABLE IF NOT EXISTS broadcast_deliveries (
                    id BIGSERIAL PRIMARY KEY,
                    broadcast_id BIGINT NOT NULL REFERENCES broadcasts(id) ON DELETE CASCADE,
                    telegram_id BIGINT NOT NULL,
                    status TEXT NOT NULL,
                    error_text TEXT,
                    delivered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS content_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL DEFAULT '',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS content_links (
                    id BIGSERIAL PRIMARY KEY,
                    section TEXT NOT NULL CHECK (section IN ('useful_links', 'partner_materials')),
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    position INTEGER NOT NULL DEFAULT 100,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);
                CREATE INDEX IF NOT EXISTS idx_broadcasts_scheduled_at ON broadcasts(scheduled_at);
                CREATE INDEX IF NOT EXISTS idx_deliveries_broadcast_id ON broadcast_deliveries(broadcast_id);
                CREATE INDEX IF NOT EXISTS idx_content_links_section_position ON content_links(section, position, id);
                """
            )

    async def get_user(self, telegram_id: int) -> asyncpg.Record | None:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT * FROM users WHERE telegram_id = $1",
                telegram_id,
            )

    async def is_authorized(self, telegram_id: int) -> bool:
        return (await self.get_user(telegram_id)) is not None

    async def get_access_code(self, code: str) -> asyncpg.Record | None:
        normalized = normalize_code(code)
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT * FROM access_codes WHERE code = $1 AND is_active = TRUE",
                normalized,
            )

    async def add_or_update_access_code(
        self,
        code: str,
        description: str,
        is_active: bool = True,
    ) -> None:
        normalized = normalize_code(code)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO access_codes(code, description, is_active, created_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT(code) DO UPDATE SET
                    description = EXCLUDED.description,
                    is_active = EXCLUDED.is_active
                """,
                normalized,
                description.strip(),
                is_active,
            )

    async def set_access_code_status(self, code: str, is_active: bool) -> int:
        normalized = normalize_code(code)
        async with self.pool.acquire() as conn:
            status = await conn.execute(
                "UPDATE access_codes SET is_active = $1 WHERE code = $2",
                is_active,
                normalized,
            )
        return _extract_rowcount(status)

    async def list_access_codes(self, limit: int = 200) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT code, description, is_active, created_at
                FROM access_codes
                ORDER BY created_at DESC
                LIMIT $1
                """,
                limit,
            )

    async def upsert_authorized_user(
        self,
        telegram_id: int,
        username: str | None,
        first_name: str | None,
        access_code: str,
        company: str | None,
    ) -> None:
        normalized = normalize_code(access_code)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO users(telegram_id, username, first_name, access_code, company, registered_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT(telegram_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    access_code = EXCLUDED.access_code,
                    company = EXCLUDED.company
                """,
                telegram_id,
                username,
                first_name,
                normalized,
                (company or "").strip() or None,
            )

    async def list_authorized_user_ids(self) -> list[int]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT telegram_id FROM users")
        return [int(row["telegram_id"]) for row in rows]

    async def list_users(self, limit: int = 300) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT id, telegram_id, username, first_name, access_code, company, registered_at
                FROM users
                ORDER BY registered_at DESC
                LIMIT $1
                """,
                limit,
            )

    async def create_broadcast(
        self,
        created_by: int,
        message_text: str | None,
        source_chat_id: int | None,
        source_message_id: int | None,
        scheduled_at: datetime | str | None,
        status: str = "scheduled",
    ) -> int:
        scheduled_dt = _normalize_dt(scheduled_at)
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO broadcasts(created_by, message_text, source_chat_id, source_message_id, scheduled_at, status)
                VALUES($1, $2, $3, $4, $5, $6)
                RETURNING id
                """,
                created_by,
                message_text,
                source_chat_id,
                source_message_id,
                scheduled_dt,
                status,
            )
        return int(row["id"])

    async def get_broadcast(self, broadcast_id: int) -> asyncpg.Record | None:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT * FROM broadcasts WHERE id = $1",
                broadcast_id,
            )

    async def set_broadcast_sent(self, broadcast_id: int, status: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE broadcasts SET sent_at = NOW(), status = $1 WHERE id = $2",
                status,
                broadcast_id,
            )

    async def get_pending_broadcasts(self) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT *
                FROM broadcasts
                WHERE sent_at IS NULL AND status = 'scheduled'
                ORDER BY COALESCE(scheduled_at, NOW()) ASC
                """
            )

    async def get_recent_sent_broadcasts(self, limit: int = 5) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT id, message_text, source_chat_id, source_message_id, sent_at
                FROM broadcasts
                WHERE sent_at IS NOT NULL
                ORDER BY sent_at DESC
                LIMIT $1
                """,
                limit,
            )

    async def list_broadcasts(self, limit: int = 100) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT id, created_by, message_text, scheduled_at, sent_at, status
                FROM broadcasts
                ORDER BY id DESC
                LIMIT $1
                """,
                limit,
            )

    async def add_delivery(
        self,
        broadcast_id: int,
        telegram_id: int,
        status: str,
        error_text: str | None = None,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO broadcast_deliveries(broadcast_id, telegram_id, status, error_text, delivered_at)
                VALUES($1, $2, $3, $4, NOW())
                """,
                broadcast_id,
                telegram_id,
                status,
                (error_text or "").strip()[:1000] or None,
            )

    async def get_delivery_stats(self, broadcast_id: int) -> dict[str, int]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT status, COUNT(*) AS total
                FROM broadcast_deliveries
                WHERE broadcast_id = $1
                GROUP BY status
                """,
                broadcast_id,
            )

        result = {"delivered": 0, "failed": 0}
        for row in rows:
            key = str(row["status"])
            if key in result:
                result[key] = int(row["total"])
        return result

    async def update_user_profile(
        self,
        telegram_id: int,
        username: str | None,
        first_name: str | None,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE users
                SET username = $1, first_name = $2
                WHERE telegram_id = $3
                """,
                username,
                first_name,
                telegram_id,
            )

    async def upsert_content_settings(self, values: dict[str, str]) -> None:
        if not values:
            return
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for key, value in values.items():
                    await conn.execute(
                        """
                        INSERT INTO content_settings(key, value, updated_at)
                        VALUES($1, $2, NOW())
                        ON CONFLICT(key) DO UPDATE SET
                            value = EXCLUDED.value,
                            updated_at = NOW()
                        """,
                        key,
                        value,
                    )

    async def get_content_settings_map(self) -> dict[str, str]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT key, value FROM content_settings")
        return {str(row["key"]): str(row["value"]) for row in rows}

    async def add_content_link(
        self,
        section: str,
        title: str,
        url: str,
        position: int = 100,
        is_active: bool = True,
    ) -> int:
        section = section.strip()
        if section not in (SECTION_USEFUL_LINKS, SECTION_PARTNER_MATERIALS):
            raise ValueError("Invalid section")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO content_links(section, title, url, position, is_active)
                VALUES($1, $2, $3, $4, $5)
                RETURNING id
                """,
                section,
                title.strip(),
                url.strip(),
                position,
                is_active,
            )
        return int(row["id"])

    async def update_content_link(
        self,
        link_id: int,
        title: str,
        url: str,
        position: int,
        is_active: bool,
    ) -> int:
        async with self.pool.acquire() as conn:
            status = await conn.execute(
                """
                UPDATE content_links
                SET title = $1, url = $2, position = $3, is_active = $4
                WHERE id = $5
                """,
                title.strip(),
                url.strip(),
                position,
                is_active,
                link_id,
            )
        return _extract_rowcount(status)

    async def delete_content_link(self, link_id: int) -> int:
        async with self.pool.acquire() as conn:
            status = await conn.execute("DELETE FROM content_links WHERE id = $1", link_id)
        return _extract_rowcount(status)

    async def list_content_links(
        self,
        section: str,
        include_inactive: bool = False,
    ) -> list[asyncpg.Record]:
        section = section.strip()
        if section not in (SECTION_USEFUL_LINKS, SECTION_PARTNER_MATERIALS):
            return []
        async with self.pool.acquire() as conn:
            if include_inactive:
                return await conn.fetch(
                    """
                    SELECT id, section, title, url, position, is_active, created_at
                    FROM content_links
                    WHERE section = $1
                    ORDER BY position ASC, id ASC
                    """,
                    section,
                )
            return await conn.fetch(
                """
                SELECT id, section, title, url, position, is_active, created_at
                FROM content_links
                WHERE section = $1 AND is_active = TRUE
                ORDER BY position ASC, id ASC
                """,
                section,
            )

    async def list_all_content_links(self) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT id, section, title, url, position, is_active, created_at
                FROM content_links
                ORDER BY section ASC, position ASC, id ASC
                """
            )

    async def seed_content_if_empty(self, content: dict[str, Any]) -> None:
        async with self.pool.acquire() as conn:
            settings_count = int(await conn.fetchval("SELECT COUNT(*) FROM content_settings"))
            links_count = int(await conn.fetchval("SELECT COUNT(*) FROM content_links"))
        if settings_count > 0 or links_count > 0:
            return

        program = content.get("program", {}) if isinstance(content, dict) else {}
        manager = content.get("manager_contact", {}) if isinstance(content, dict) else {}

        await self.upsert_content_settings(
            {
                "program_title": str(program.get("title", "Актуальная программа саммита")),
                "program_url": str(program.get("url", "")),
                "manager_title": str(manager.get("title", "Написать менеджеру напрямую")),
                "manager_url": str(manager.get("url", "")),
                "restricted_text": str(content.get("restricted_text", DEFAULT_RESTRICTED_TEXT)),
                "welcome_template": str(content.get("welcome_template", DEFAULT_WELCOME_TEMPLATE)),
            }
        )

        useful_links = content.get(SECTION_USEFUL_LINKS, []) if isinstance(content, dict) else []
        if isinstance(useful_links, list):
            for idx, item in enumerate(useful_links):
                if not isinstance(item, dict):
                    continue
                title = str(item.get("title", "")).strip()
                url = str(item.get("url", "")).strip()
                if title and url:
                    await self.add_content_link(
                        section=SECTION_USEFUL_LINKS,
                        title=title,
                        url=url,
                        position=(idx + 1) * 10,
                        is_active=True,
                    )

        materials = content.get(SECTION_PARTNER_MATERIALS, []) if isinstance(content, dict) else []
        if isinstance(materials, list):
            for idx, item in enumerate(materials):
                if not isinstance(item, dict):
                    continue
                title = str(item.get("title", "")).strip()
                url = str(item.get("url", "")).strip()
                if title and url:
                    await self.add_content_link(
                        section=SECTION_PARTNER_MATERIALS,
                        title=title,
                        url=url,
                        position=(idx + 1) * 10,
                        is_active=True,
                    )

    async def get_content_bundle(self) -> dict[str, Any]:
        settings_map = await self.get_content_settings_map()
        useful_links = await self.list_content_links(SECTION_USEFUL_LINKS, include_inactive=False)
        partner_materials = await self.list_content_links(SECTION_PARTNER_MATERIALS, include_inactive=False)

        return {
            "program": {
                "title": settings_map.get("program_title", "Актуальная программа саммита"),
                "url": settings_map.get("program_url", ""),
            },
            "useful_links": [
                {"title": str(item["title"]), "url": str(item["url"])}
                for item in useful_links
            ],
            "partner_materials": [
                {"title": str(item["title"]), "url": str(item["url"])}
                for item in partner_materials
            ],
            "manager_contact": {
                "title": settings_map.get("manager_title", "Написать менеджеру напрямую"),
                "url": settings_map.get("manager_url", ""),
            },
            "restricted_text": settings_map.get("restricted_text", DEFAULT_RESTRICTED_TEXT),
            "welcome_template": settings_map.get("welcome_template", DEFAULT_WELCOME_TEMPLATE),
        }

    async def get_stats(self) -> dict[str, int]:
        async with self.pool.acquire() as conn:
            users_total = int(await conn.fetchval("SELECT COUNT(*) FROM users"))
            codes_total = int(await conn.fetchval("SELECT COUNT(*) FROM access_codes"))
            active_codes = int(await conn.fetchval("SELECT COUNT(*) FROM access_codes WHERE is_active = TRUE"))
            broadcasts_total = int(await conn.fetchval("SELECT COUNT(*) FROM broadcasts"))
            pending_broadcasts = int(
                await conn.fetchval("SELECT COUNT(*) FROM broadcasts WHERE sent_at IS NULL AND status = 'scheduled'")
            )

        return {
            "users_total": users_total,
            "codes_total": codes_total,
            "active_codes": active_codes,
            "broadcasts_total": broadcasts_total,
            "pending_broadcasts": pending_broadcasts,
        }

