from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import asyncpg

DEFAULT_RESTRICTED_TEXT = (
    "Доступ к приватным разделам ограничен. Напишите организатору и получите персональный код приглашения."
)
DEFAULT_WELCOME_TEMPLATE = (
    "Добро пожаловать, {first_name}!\n"
    "Ваша заявка на роль {role_title} по саммиту {summit_name} принята.\n\n"
    "После подтверждения организатором откроется полное меню."
)
DEFAULT_PUBLIC_WELCOME = (
    "Добро пожаловать в бот СТАММИТ26.\n"
    "Выберите нужный раздел в меню ниже."
)

ROLE_PARTNER = "partner"
ROLE_EXPERT = "expert"
ROLE_INFLUENCER = "influencer"
ROLE_ALL = "all"
ROLES = {ROLE_PARTNER, ROLE_EXPERT, ROLE_INFLUENCER}

STATUS_PENDING = "pending"
STATUS_APPROVED = "approved"
STATUS_REJECTED = "rejected"

SECTION_PUBLIC_MENU_LINKS = "public_menu_links"
SECTION_PARTNER_USEFUL_LINKS = "partner_useful_links"
SECTION_EXPERT_USEFUL_LINKS = "expert_useful_links"
SECTION_INFLUENCER_USEFUL_LINKS = "influencer_useful_links"
SECTION_PARTNER_MATERIALS = "partner_materials"
SECTION_EXPERT_MATERIALS = "expert_materials"
SECTION_INFLUENCER_MATERIALS = "influencer_materials"

LEGACY_SECTION_USEFUL_LINKS = "useful_links"

APPLICATION_STATUS_NEW = "new"
APPLICATION_STATUS_IN_PROGRESS = "in_progress"
APPLICATION_STATUS_DONE = "done"
APPLICATION_STATUS_REJECTED = "rejected"

SUPPORT_STATUS_ACTIVE = "active"
SUPPORT_STATUS_CLOSED = "closed"


def normalize_code(code: str) -> str:
    return code.strip().upper()


def normalize_role(value: str | None, default: str = ROLE_PARTNER) -> str:
    role = (value or "").strip().lower()
    if role in ROLES:
        return role
    return default


def normalize_target_role(value: str | None) -> str:
    role = (value or "").strip().lower()
    if role in ROLES:
        return role
    return ROLE_ALL


def normalize_subcategory(value: str | None) -> str:
    return (value or "").strip()


def parse_target_role_subcategory(value: str | None) -> tuple[str, str]:
    text = (value or "").strip()
    if not text:
        return (ROLE_ALL, "")
    for separator in (":", "/"):
        if separator in text:
            raw_role, raw_subcategory = text.split(separator, maxsplit=1)
            return (normalize_target_role(raw_role), normalize_subcategory(raw_subcategory))
    return (normalize_target_role(text), "")


def is_internal_access_code(code: str | None) -> bool:
    value = normalize_code(code or "")
    return value.startswith("APP") or value.startswith("NO_CODE_")


def role_title(role: str) -> str:
    if role == ROLE_EXPERT:
        return "эксперт"
    if role == ROLE_INFLUENCER:
        return "инфлюенсер"
    return "партнёр"


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
                    role TEXT NOT NULL DEFAULT 'partner',
                    subcategory TEXT,
                    access_status TEXT NOT NULL DEFAULT 'approved',
                    access_code TEXT,
                    full_name TEXT,
                    phone TEXT,
                    email TEXT,
                    company TEXT,
                    inn TEXT,
                    consent_accepted BOOLEAN NOT NULL DEFAULT FALSE,
                    consent_accepted_at TIMESTAMPTZ,
                    requested_at TIMESTAMPTZ,
                    approved_at TIMESTAMPTZ,
                    approved_by BIGINT,
                    rejection_reason TEXT,
                    referred_by BIGINT,
                    referral_code TEXT UNIQUE,
                    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS access_codes (
                    code TEXT PRIMARY KEY,
                    role TEXT NOT NULL DEFAULT 'partner',
                    subcategory TEXT,
                    description TEXT,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS role_subcategories (
                    id BIGSERIAL PRIMARY KEY,
                    role TEXT NOT NULL,
                    name TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(role, name)
                );

                CREATE TABLE IF NOT EXISTS broadcasts (
                    id BIGSERIAL PRIMARY KEY,
                    created_by BIGINT NOT NULL,
                    target_role TEXT NOT NULL DEFAULT 'all',
                    target_subcategory TEXT,
                    sender_role TEXT,
                    message_text TEXT,
                    image_path TEXT,
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
                    delivered_message_id BIGINT,
                    delivered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS content_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL DEFAULT '',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS content_links (
                    id BIGSERIAL PRIMARY KEY,
                    section TEXT NOT NULL,
                    category TEXT,
                    subcategory TEXT,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    position INTEGER NOT NULL DEFAULT 100,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS applications (
                    id BIGSERIAL PRIMARY KEY,
                    token TEXT NOT NULL UNIQUE,
                    telegram_id BIGINT,
                    role TEXT NOT NULL DEFAULT 'partner',
                    source TEXT NOT NULL DEFAULT 'site',
                    status TEXT NOT NULL DEFAULT 'new',
                    request_text TEXT,
                    booth_number TEXT,
                    full_name TEXT,
                    phone TEXT,
                    email TEXT,
                    company TEXT,
                    inn TEXT,
                    manager_note TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS support_sessions (
                    id BIGSERIAL PRIMARY KEY,
                    telegram_id BIGINT NOT NULL,
                    support_chat_id BIGINT NOT NULL,
                    manager_telegram_id BIGINT,
                    status TEXT NOT NULL DEFAULT 'active',
                    opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    closed_at TIMESTAMPTZ
                );

                CREATE TABLE IF NOT EXISTS feedback_messages (
                    id BIGSERIAL PRIMARY KEY,
                    telegram_id BIGINT NOT NULL,
                    user_role TEXT,
                    message_text TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'feedback',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS referral_clicks (
                    id BIGSERIAL PRIMARY KEY,
                    owner_telegram_id BIGINT NOT NULL,
                    guest_telegram_id BIGINT NOT NULL UNIQUE,
                    ref_code TEXT NOT NULL,
                    clicked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);
                CREATE INDEX IF NOT EXISTS idx_users_status_role ON users(access_status, role);
                CREATE INDEX IF NOT EXISTS idx_broadcasts_scheduled_at ON broadcasts(scheduled_at);
                CREATE INDEX IF NOT EXISTS idx_deliveries_broadcast_id ON broadcast_deliveries(broadcast_id);
                CREATE INDEX IF NOT EXISTS idx_content_links_section_position ON content_links(section, position, id);
                CREATE INDEX IF NOT EXISTS idx_applications_status_created ON applications(status, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_applications_token ON applications(token);
                CREATE INDEX IF NOT EXISTS idx_support_sessions_chat ON support_sessions(support_chat_id, status);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_support_sessions_one_active
                    ON support_sessions(telegram_id)
                    WHERE status = 'active';
                CREATE INDEX IF NOT EXISTS idx_referral_clicks_owner ON referral_clicks(owner_telegram_id);
                CREATE INDEX IF NOT EXISTS idx_role_subcategories_role_name ON role_subcategories(role, name);
                """
            )

            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS role TEXT")
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS subcategory TEXT")
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS access_status TEXT")
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS full_name TEXT")
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS phone TEXT")
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT")
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS inn TEXT")
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS consent_accepted BOOLEAN")
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS consent_accepted_at TIMESTAMPTZ")
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS requested_at TIMESTAMPTZ")
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ")
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS approved_by BIGINT")
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS rejection_reason TEXT")
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by BIGINT")
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_code TEXT")

            await conn.execute("ALTER TABLE access_codes ADD COLUMN IF NOT EXISTS role TEXT")
            await conn.execute("ALTER TABLE access_codes ADD COLUMN IF NOT EXISTS subcategory TEXT")
            await conn.execute("ALTER TABLE role_subcategories ADD COLUMN IF NOT EXISTS role TEXT")
            await conn.execute("ALTER TABLE role_subcategories ADD COLUMN IF NOT EXISTS name TEXT")
            await conn.execute("ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS target_role TEXT")
            await conn.execute("ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS target_subcategory TEXT")
            await conn.execute("ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS sender_role TEXT")
            await conn.execute("ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS image_path TEXT")
            await conn.execute("ALTER TABLE broadcast_deliveries ADD COLUMN IF NOT EXISTS delivered_message_id BIGINT")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_users_status_role_subcategory ON users(access_status, role, subcategory)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_broadcasts_target ON broadcasts(target_role, target_subcategory)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_broadcasts_sender ON broadcasts(sender_role, status, sent_at)"
            )
            await conn.execute("ALTER TABLE content_links ADD COLUMN IF NOT EXISTS category TEXT")
            await conn.execute("ALTER TABLE content_links ADD COLUMN IF NOT EXISTS subcategory TEXT")

            await conn.execute("ALTER TABLE applications ADD COLUMN IF NOT EXISTS telegram_id BIGINT")
            await conn.execute("ALTER TABLE applications ADD COLUMN IF NOT EXISTS role TEXT")
            await conn.execute("ALTER TABLE applications ADD COLUMN IF NOT EXISTS source TEXT")
            await conn.execute("ALTER TABLE applications ADD COLUMN IF NOT EXISTS status TEXT")
            await conn.execute("ALTER TABLE applications ADD COLUMN IF NOT EXISTS request_text TEXT")
            await conn.execute("ALTER TABLE applications ADD COLUMN IF NOT EXISTS booth_number TEXT")
            await conn.execute("ALTER TABLE applications ADD COLUMN IF NOT EXISTS full_name TEXT")
            await conn.execute("ALTER TABLE applications ADD COLUMN IF NOT EXISTS phone TEXT")
            await conn.execute("ALTER TABLE applications ADD COLUMN IF NOT EXISTS email TEXT")
            await conn.execute("ALTER TABLE applications ADD COLUMN IF NOT EXISTS company TEXT")
            await conn.execute("ALTER TABLE applications ADD COLUMN IF NOT EXISTS inn TEXT")
            await conn.execute("ALTER TABLE applications ADD COLUMN IF NOT EXISTS manager_note TEXT")
            await conn.execute("ALTER TABLE applications ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ")

            await conn.execute("UPDATE users SET role = COALESCE(NULLIF(role, ''), 'partner')")
            await conn.execute("UPDATE users SET access_status = COALESCE(NULLIF(access_status, ''), 'approved')")
            await conn.execute("UPDATE users SET consent_accepted = COALESCE(consent_accepted, FALSE)")
            await conn.execute("UPDATE access_codes SET role = COALESCE(NULLIF(role, ''), 'partner')")
            await conn.execute("UPDATE role_subcategories SET role = COALESCE(NULLIF(role, ''), 'partner')")
            await conn.execute("UPDATE broadcasts SET target_role = COALESCE(NULLIF(target_role, ''), 'all')")
            await conn.execute("UPDATE applications SET role = COALESCE(NULLIF(role, ''), 'partner')")
            await conn.execute("UPDATE applications SET source = COALESCE(NULLIF(source, ''), 'site')")
            await conn.execute("UPDATE applications SET status = COALESCE(NULLIF(status, ''), 'new')")
            await conn.execute("UPDATE applications SET updated_at = COALESCE(updated_at, created_at, NOW())")
            await conn.execute(
                """
                UPDATE users
                SET access_status = 'rejected',
                    rejection_reason = COALESCE(rejection_reason, 'Код доступа удалён'),
                    approved_at = NULL
                WHERE access_status = 'approved'
                  AND access_code IS NOT NULL
                  AND access_code NOT LIKE 'APP%'
                  AND access_code NOT LIKE 'NO_CODE_%'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM access_codes
                      WHERE access_codes.code = users.access_code
                  )
                """
            )
            await conn.execute(
                """
                INSERT INTO role_subcategories(role, name)
                SELECT DISTINCT normalize_role, subcategory
                FROM (
                    SELECT role AS normalize_role, NULLIF(TRIM(subcategory), '') AS subcategory
                    FROM access_codes
                    UNION
                    SELECT role AS normalize_role, NULLIF(TRIM(subcategory), '') AS subcategory
                    FROM users
                ) source
                WHERE subcategory IS NOT NULL
                  AND normalize_role IN ('partner', 'expert', 'influencer')
                ON CONFLICT(role, name) DO NOTHING
                """
            )

            await conn.execute("ALTER TABLE users ALTER COLUMN role SET DEFAULT 'partner'")
            await conn.execute("ALTER TABLE users ALTER COLUMN access_status SET DEFAULT 'approved'")
            await conn.execute("ALTER TABLE users ALTER COLUMN consent_accepted SET DEFAULT FALSE")
            await conn.execute("ALTER TABLE access_codes ALTER COLUMN role SET DEFAULT 'partner'")
            await conn.execute("ALTER TABLE role_subcategories ALTER COLUMN role SET DEFAULT 'partner'")
            await conn.execute("ALTER TABLE broadcasts ALTER COLUMN target_role SET DEFAULT 'all'")
            await conn.execute("ALTER TABLE applications ALTER COLUMN role SET DEFAULT 'partner'")
            await conn.execute("ALTER TABLE applications ALTER COLUMN source SET DEFAULT 'site'")
            await conn.execute("ALTER TABLE applications ALTER COLUMN status SET DEFAULT 'new'")
            await conn.execute("ALTER TABLE applications ALTER COLUMN updated_at SET DEFAULT NOW()")

            await conn.execute("ALTER TABLE users ALTER COLUMN role SET NOT NULL")
            await conn.execute("ALTER TABLE users ALTER COLUMN access_status SET NOT NULL")
            await conn.execute("ALTER TABLE users ALTER COLUMN consent_accepted SET NOT NULL")
            await conn.execute("ALTER TABLE access_codes ALTER COLUMN role SET NOT NULL")
            await conn.execute("ALTER TABLE role_subcategories ALTER COLUMN role SET NOT NULL")
            await conn.execute("ALTER TABLE role_subcategories ALTER COLUMN name SET NOT NULL")
            await conn.execute("ALTER TABLE broadcasts ALTER COLUMN target_role SET NOT NULL")
            await conn.execute("ALTER TABLE applications ALTER COLUMN role SET NOT NULL")
            await conn.execute("ALTER TABLE applications ALTER COLUMN source SET NOT NULL")
            await conn.execute("ALTER TABLE applications ALTER COLUMN status SET NOT NULL")
            await conn.execute("ALTER TABLE applications ALTER COLUMN updated_at SET NOT NULL")

            constraints = await conn.fetch(
                """
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = 'content_links'::regclass
                  AND contype = 'c'
                """
            )
            for row in constraints:
                name = str(row["conname"])
                if "content_links_section_check" in name:
                    await conn.execute(f'ALTER TABLE content_links DROP CONSTRAINT "{name}"')

            await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code)")

    async def get_user(self, telegram_id: int) -> asyncpg.Record | None:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                """
                SELECT
                    users.*,
                    EXISTS(
                        SELECT 1
                        FROM access_codes
                        WHERE access_codes.code = users.access_code
                          AND access_codes.is_active = TRUE
                    ) AS access_code_is_active
                FROM users
                WHERE telegram_id = $1
                """,
                telegram_id,
            )

    async def is_authorized(self, telegram_id: int) -> bool:
        user = await self.get_user(telegram_id)
        if user is None:
            return False
        if str(user["access_status"]) != STATUS_APPROVED:
            return False
        return bool(user["access_code_is_active"]) or is_internal_access_code(str(user["access_code"] or ""))

    async def is_authorized_role(self, telegram_id: int, role: str) -> bool:
        user = await self.get_user(telegram_id)
        if user is None:
            return False
        if not await self.is_authorized(telegram_id):
            return False
        return normalize_role(user["role"]) == normalize_role(role)

    async def get_access_code(self, code: str) -> asyncpg.Record | None:
        normalized = normalize_code(code)
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT * FROM access_codes WHERE code = $1 AND is_active = TRUE",
                normalized,
            )

    async def find_access_code(self, code: str) -> asyncpg.Record | None:
        normalized = normalize_code(code)
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT * FROM access_codes WHERE code = $1",
                normalized,
            )

    async def add_or_update_access_code(
        self,
        code: str,
        description: str,
        role: str = ROLE_PARTNER,
        subcategory: str | None = None,
        is_active: bool = True,
    ) -> None:
        normalized = normalize_code(code)
        role = normalize_role(role)
        subcategory_value = normalize_subcategory(subcategory) or None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                if subcategory_value:
                    await conn.execute(
                        """
                        INSERT INTO role_subcategories(role, name, created_at)
                        VALUES($1, $2, NOW())
                        ON CONFLICT(role, name) DO NOTHING
                        """,
                        role,
                        subcategory_value,
                    )
                await conn.execute(
                    """
                    INSERT INTO access_codes(code, role, subcategory, description, is_active, created_at)
                    VALUES ($1, $2, $3, $4, $5, NOW())
                    ON CONFLICT(code) DO UPDATE SET
                        role = EXCLUDED.role,
                        subcategory = EXCLUDED.subcategory,
                        description = EXCLUDED.description,
                        is_active = EXCLUDED.is_active
                    """,
                    normalized,
                    role,
                    subcategory_value,
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

    async def delete_access_code(self, code: str) -> int:
        normalized = normalize_code(code)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                status = await conn.execute(
                    "DELETE FROM access_codes WHERE code = $1",
                    normalized,
                )
                if _extract_rowcount(status):
                    await conn.execute(
                        """
                        UPDATE users
                        SET access_status = 'rejected',
                            rejection_reason = 'Код доступа удалён',
                            approved_at = NULL
                        WHERE access_code = $1
                          AND access_status = 'approved'
                        """,
                        normalized,
                    )
        return _extract_rowcount(status)

    async def list_access_codes(self, limit: int = 200) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT code, role, subcategory, description, is_active, created_at
                FROM access_codes
                ORDER BY created_at DESC
                LIMIT $1
                """,
                limit,
            )

    async def add_subcategory(self, role: str, name: str) -> int:
        role_value = normalize_role(role)
        name_value = normalize_subcategory(name)
        if not name_value:
            return 0
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO role_subcategories(role, name, created_at)
                VALUES($1, $2, NOW())
                ON CONFLICT(role, name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
                """,
                role_value,
                name_value,
            )
        return int(row["id"])

    async def delete_subcategory(self, role: str, name: str) -> int:
        role_value = normalize_role(role)
        name_value = normalize_subcategory(name)
        if not name_value:
            return 0
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "UPDATE access_codes SET subcategory = NULL WHERE role = $1 AND subcategory = $2",
                    role_value,
                    name_value,
                )
                await conn.execute(
                    "UPDATE users SET subcategory = NULL WHERE role = $1 AND subcategory = $2",
                    role_value,
                    name_value,
                )
                status = await conn.execute(
                    "DELETE FROM role_subcategories WHERE role = $1 AND name = $2",
                    role_value,
                    name_value,
                )
        return _extract_rowcount(status)

    async def list_subcategories(self, role: str | None = None) -> list[asyncpg.Record]:
        role_value = normalize_role(role) if role else None
        async with self.pool.acquire() as conn:
            if role_value:
                return await conn.fetch(
                    """
                    SELECT id, role, name, created_at
                    FROM role_subcategories
                    WHERE role = $1
                    ORDER BY name ASC
                    """,
                    role_value,
                )
            return await conn.fetch(
                """
                SELECT id, role, name, created_at
                FROM role_subcategories
                ORDER BY role ASC, name ASC
                """
            )

    async def upsert_access_request(
        self,
        telegram_id: int,
        username: str | None,
        first_name: str | None,
        role: str,
        subcategory: str | None,
        access_code: str,
        full_name: str | None,
        phone: str | None,
        email: str | None = None,
        company: str | None = None,
        inn: str | None = None,
        consent_accepted: bool = False,
        referred_by: int | None = None,
    ) -> None:
        role = normalize_role(role)
        subcategory_value = normalize_subcategory(subcategory) or None
        normalized = normalize_code(access_code)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO users(
                    telegram_id,
                    username,
                    first_name,
                    role,
                    subcategory,
                    access_status,
                    access_code,
                    full_name,
                    phone,
                    email,
                    company,
                    inn,
                    consent_accepted,
                    consent_accepted_at,
                    requested_at,
                    approved_at,
                    approved_by,
                    rejection_reason,
                    referred_by,
                    registered_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, 'pending', $6, $7, $8, $9, $10, $11,
                    $12, CASE WHEN $12 THEN NOW() ELSE NULL END,
                    NOW(), NULL, NULL, NULL, $13, NOW()
                )
                ON CONFLICT(telegram_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    role = EXCLUDED.role,
                    subcategory = EXCLUDED.subcategory,
                    access_status = 'pending',
                    access_code = EXCLUDED.access_code,
                    full_name = EXCLUDED.full_name,
                    phone = EXCLUDED.phone,
                    email = EXCLUDED.email,
                    company = EXCLUDED.company,
                    inn = EXCLUDED.inn,
                    consent_accepted = EXCLUDED.consent_accepted,
                    consent_accepted_at = EXCLUDED.consent_accepted_at,
                    requested_at = NOW(),
                    approved_at = NULL,
                    approved_by = NULL,
                    rejection_reason = NULL,
                    referred_by = COALESCE(EXCLUDED.referred_by, users.referred_by)
                """,
                telegram_id,
                username,
                first_name,
                role,
                subcategory_value,
                normalized,
                (full_name or "").strip() or None,
                (phone or "").strip() or None,
                (email or "").strip() or None,
                (company or "").strip() or None,
                (inn or "").strip() or None,
                consent_accepted,
                referred_by,
            )

    async def approve_user(self, telegram_id: int, approved_by: int) -> asyncpg.Record | None:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                """
                UPDATE users
                SET access_status = 'approved',
                    approved_at = NOW(),
                    approved_by = $2,
                    rejection_reason = NULL
                WHERE telegram_id = $1
                RETURNING *
                """,
                telegram_id,
                approved_by,
            )

    async def reject_user(self, telegram_id: int, approved_by: int, reason: str | None = None) -> asyncpg.Record | None:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                """
                UPDATE users
                SET access_status = 'rejected',
                    approved_by = $2,
                    rejection_reason = $3
                WHERE telegram_id = $1
                RETURNING *
                """,
                telegram_id,
                approved_by,
                (reason or "").strip() or None,
            )

    async def list_pending_users(self, limit: int = 200) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT *
                FROM users
                WHERE access_status = 'pending'
                ORDER BY requested_at DESC NULLS LAST, registered_at DESC
                LIMIT $1
                """,
                limit,
            )

    async def list_authorized_user_ids(self, role: str = ROLE_ALL, subcategory: str | None = None) -> list[int]:
        target = normalize_target_role(role)
        subcategory_value = normalize_subcategory(subcategory)
        active_access_condition = """
            access_status = 'approved'
            AND (
                access_code LIKE 'APP%'
                OR access_code LIKE 'NO_CODE_%'
                OR EXISTS (
                    SELECT 1
                    FROM access_codes
                    WHERE access_codes.code = users.access_code
                      AND access_codes.is_active = TRUE
                )
            )
        """
        async with self.pool.acquire() as conn:
            if target == ROLE_ALL and not subcategory_value:
                rows = await conn.fetch(
                    f"SELECT telegram_id FROM users WHERE {active_access_condition}"
                )
            elif target == ROLE_ALL:
                rows = await conn.fetch(
                    f"SELECT telegram_id FROM users WHERE {active_access_condition} AND subcategory = $1",
                    subcategory_value,
                )
            elif subcategory_value:
                rows = await conn.fetch(
                    f"SELECT telegram_id FROM users WHERE {active_access_condition} AND role = $1 AND subcategory = $2",
                    target,
                    subcategory_value,
                )
            else:
                rows = await conn.fetch(
                    f"SELECT telegram_id FROM users WHERE {active_access_condition} AND role = $1",
                    target,
                )
        return [int(row["telegram_id"]) for row in rows]

    async def list_users(self, limit: int = 500) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT
                    id,
                    telegram_id,
                    username,
                    first_name,
                    role,
                    subcategory,
                    access_status,
                    access_code,
                    full_name,
                    phone,
                    email,
                    company,
                    inn,
                    consent_accepted,
                    consent_accepted_at,
                    requested_at,
                    approved_at,
                    approved_by,
                    rejection_reason,
                    referred_by,
                    referral_code,
                    registered_at
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
        image_path: str | None,
        source_chat_id: int | None,
        source_message_id: int | None,
        scheduled_at: datetime | str | None,
        target_role: str = ROLE_ALL,
        target_subcategory: str | None = None,
        sender_role: str | None = None,
        status: str = "scheduled",
    ) -> int:
        scheduled_dt = _normalize_dt(scheduled_at)
        role = normalize_target_role(target_role)
        subcategory_value = normalize_subcategory(target_subcategory) or None
        sender_role_value = normalize_role(sender_role) if sender_role else None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO broadcasts(
                    created_by,
                    target_role,
                    target_subcategory,
                    sender_role,
                    message_text,
                    image_path,
                    source_chat_id,
                    source_message_id,
                    scheduled_at,
                    status
                )
                VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                RETURNING id
                """,
                created_by,
                role,
                subcategory_value,
                sender_role_value,
                message_text,
                image_path,
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

    async def get_pending_broadcasts(self, sender_role: str | None = None) -> list[asyncpg.Record]:
        sender_role_value = normalize_role(sender_role) if sender_role else None
        async with self.pool.acquire() as conn:
            if sender_role_value is None:
                return await conn.fetch(
                    """
                    SELECT *
                    FROM broadcasts
                    WHERE sent_at IS NULL
                      AND status = 'scheduled'
                      AND sender_role IS NULL
                    ORDER BY COALESCE(scheduled_at, NOW()) ASC
                    """
                )
            return await conn.fetch(
                """
                SELECT *
                FROM broadcasts
                WHERE sent_at IS NULL
                  AND status = 'scheduled'
                  AND sender_role = $1
                ORDER BY COALESCE(scheduled_at, NOW()) ASC
                """,
                sender_role_value,
            )

    async def get_recent_sent_broadcasts(
        self,
        limit: int = 5,
        role: str = ROLE_ALL,
        subcategory: str | None = None,
    ) -> list[asyncpg.Record]:
        target = normalize_target_role(role)
        subcategory_value = normalize_subcategory(subcategory)
        async with self.pool.acquire() as conn:
            if target == ROLE_ALL:
                return await conn.fetch(
                    """
                    SELECT id, target_role, target_subcategory, message_text, source_chat_id, source_message_id, sent_at
                    FROM broadcasts
                    WHERE sent_at IS NOT NULL
                      AND (target_subcategory IS NULL OR target_subcategory = $2)
                    ORDER BY sent_at DESC
                    LIMIT $1
                    """,
                    limit,
                    subcategory_value,
                )
            return await conn.fetch(
                """
                SELECT id, target_role, target_subcategory, message_text, source_chat_id, source_message_id, sent_at
                FROM broadcasts
                WHERE sent_at IS NOT NULL
                  AND (target_role = 'all' OR target_role = $2)
                  AND (target_subcategory IS NULL OR target_subcategory = $3)
                ORDER BY sent_at DESC
                LIMIT $1
                """,
                limit,
                target,
                subcategory_value,
            )

    async def list_broadcasts(self, limit: int = 120) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT id, created_by, target_role, target_subcategory, sender_role, message_text, image_path, scheduled_at, sent_at, status
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
        delivered_message_id: int | None = None,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO broadcast_deliveries(
                    broadcast_id,
                    telegram_id,
                    status,
                    error_text,
                    delivered_message_id,
                    delivered_at
                )
                VALUES($1, $2, $3, $4, $5, NOW())
                """,
                broadcast_id,
                telegram_id,
                status,
                (error_text or "").strip()[:1000] or None,
                delivered_message_id,
            )

    async def list_broadcast_deliveries(self, broadcast_id: int) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT telegram_id, delivered_message_id, status
                FROM broadcast_deliveries
                WHERE broadcast_id = $1
                  AND status = 'delivered'
                  AND delivered_message_id IS NOT NULL
                ORDER BY id ASC
                """,
                broadcast_id,
            )

    async def delete_broadcast(self, broadcast_id: int, only_unsent: bool = True) -> int:
        async with self.pool.acquire() as conn:
            if only_unsent:
                status = await conn.execute(
                    """
                    DELETE FROM broadcasts
                    WHERE id = $1
                      AND sent_at IS NULL
                      AND status = 'scheduled'
                    """,
                    broadcast_id,
                )
            else:
                status = await conn.execute("DELETE FROM broadcasts WHERE id = $1", broadcast_id)
        return _extract_rowcount(status)

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
        category: str | None = None,
        subcategory: str | None = None,
    ) -> int:
        section_value = section.strip()
        if not section_value:
            raise ValueError("Invalid section")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO content_links(section, category, subcategory, title, url, position, is_active)
                VALUES($1, $2, $3, $4, $5, $6, $7)
                RETURNING id
                """,
                section_value,
                (category or "").strip() or None,
                (subcategory or "").strip() or None,
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
        section: str | None = None,
        category: str | None = None,
        subcategory: str | None = None,
    ) -> int:
        async with self.pool.acquire() as conn:
            if section is None:
                status = await conn.execute(
                    """
                    UPDATE content_links
                    SET category = $1, subcategory = $2, title = $3, url = $4, position = $5, is_active = $6
                    WHERE id = $7
                    """,
                    (category or "").strip() or None,
                    (subcategory or "").strip() or None,
                    title.strip(),
                    url.strip(),
                    position,
                    is_active,
                    link_id,
                )
            else:
                status = await conn.execute(
                    """
                    UPDATE content_links
                    SET section = $1, category = $2, subcategory = $3, title = $4, url = $5, position = $6, is_active = $7
                    WHERE id = $8
                    """,
                    section.strip(),
                    (category or "").strip() or None,
                    (subcategory or "").strip() or None,
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
        section_value = section.strip()
        if not section_value:
            return []
        async with self.pool.acquire() as conn:
            if include_inactive:
                return await conn.fetch(
                    """
                    SELECT id, section, category, subcategory, title, url, position, is_active, created_at
                    FROM content_links
                    WHERE section = $1
                    ORDER BY COALESCE(category, '') ASC, COALESCE(subcategory, '') ASC, position ASC, id ASC
                    """,
                    section_value,
                )
            return await conn.fetch(
                """
                SELECT id, section, category, subcategory, title, url, position, is_active, created_at
                FROM content_links
                WHERE section = $1 AND is_active = TRUE
                ORDER BY COALESCE(category, '') ASC, COALESCE(subcategory, '') ASC, position ASC, id ASC
                """,
                section_value,
            )

    async def list_all_content_links(self, include_inactive: bool = True) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            if include_inactive:
                return await conn.fetch(
                    """
                    SELECT id, section, category, subcategory, title, url, position, is_active, created_at
                    FROM content_links
                    ORDER BY section ASC, COALESCE(category, '') ASC, COALESCE(subcategory, '') ASC, position ASC, id ASC
                    """
                )
            return await conn.fetch(
                """
                SELECT id, section, category, subcategory, title, url, position, is_active, created_at
                FROM content_links
                WHERE is_active = TRUE
                ORDER BY section ASC, COALESCE(category, '') ASC, COALESCE(subcategory, '') ASC, position ASC, id ASC
                """
            )

    async def list_content_sections(self) -> list[str]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT DISTINCT section FROM content_links ORDER BY section")
        return [str(row["section"]) for row in rows]

    async def add_feedback(
        self,
        telegram_id: int,
        message_text: str,
        user_role: str | None,
        source: str = "feedback",
    ) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO feedback_messages(telegram_id, user_role, message_text, source, created_at)
                VALUES($1, $2, $3, $4, NOW())
                RETURNING id
                """,
                telegram_id,
                normalize_role(user_role) if user_role else None,
                message_text.strip(),
                source.strip() or "feedback",
            )
        return int(row["id"])

    async def list_feedback(self, limit: int = 200) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT id, telegram_id, user_role, message_text, source, created_at
                FROM feedback_messages
                ORDER BY id DESC
                LIMIT $1
                """,
                limit,
            )

    async def create_application(
        self,
        token: str,
        role: str,
        source: str,
        request_text: str | None,
        booth_number: str | None,
        full_name: str | None,
        phone: str | None,
        email: str | None = None,
        company: str | None = None,
        inn: str | None = None,
        telegram_id: int | None = None,
        status: str = APPLICATION_STATUS_NEW,
    ) -> asyncpg.Record:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                """
                INSERT INTO applications(
                    token,
                    telegram_id,
                    role,
                    source,
                    status,
                    request_text,
                    booth_number,
                    full_name,
                    phone,
                    email,
                    company,
                    inn,
                    created_at,
                    updated_at
                )
                VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW(), NOW())
                RETURNING *
                """,
                token.strip(),
                telegram_id,
                normalize_role(role),
                source.strip() or "site",
                status.strip() or APPLICATION_STATUS_NEW,
                (request_text or "").strip() or None,
                (booth_number or "").strip() or None,
                (full_name or "").strip() or None,
                (phone or "").strip() or None,
                (email or "").strip() or None,
                (company or "").strip() or None,
                (inn or "").strip() or None,
            )

    async def get_application_by_token(self, token: str) -> asyncpg.Record | None:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT * FROM applications WHERE token = $1",
                token.strip(),
            )

    async def attach_application_telegram(self, application_id: int, telegram_id: int) -> asyncpg.Record | None:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                """
                UPDATE applications
                SET telegram_id = $2,
                    status = CASE WHEN status = 'new' THEN 'in_progress' ELSE status END,
                    updated_at = NOW()
                WHERE id = $1
                RETURNING *
                """,
                application_id,
                telegram_id,
            )

    async def set_application_status(
        self,
        application_id: int,
        status: str,
        manager_note: str | None = None,
    ) -> asyncpg.Record | None:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                """
                UPDATE applications
                SET status = $2,
                    manager_note = NULLIF($3, ''),
                    updated_at = NOW()
                WHERE id = $1
                RETURNING *
                """,
                application_id,
                status.strip() or APPLICATION_STATUS_NEW,
                (manager_note or "").strip(),
            )

    async def list_applications(self, limit: int = 200) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT *
                FROM applications
                ORDER BY created_at DESC, id DESC
                LIMIT $1
                """,
                limit,
            )

    async def connect_support_session(
        self,
        telegram_id: int,
        support_chat_id: int,
        manager_telegram_id: int | None,
    ) -> asyncpg.Record:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                """
                INSERT INTO support_sessions(
                    telegram_id,
                    support_chat_id,
                    manager_telegram_id,
                    status,
                    opened_at,
                    closed_at
                )
                VALUES($1, $2, $3, 'active', NOW(), NULL)
                ON CONFLICT(telegram_id) WHERE status = 'active'
                DO UPDATE SET
                    support_chat_id = EXCLUDED.support_chat_id,
                    manager_telegram_id = EXCLUDED.manager_telegram_id,
                    opened_at = NOW(),
                    closed_at = NULL
                RETURNING *
                """,
                telegram_id,
                support_chat_id,
                manager_telegram_id,
            )

    async def close_support_session(self, telegram_id: int) -> asyncpg.Record | None:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                """
                UPDATE support_sessions
                SET status = 'closed',
                    closed_at = NOW()
                WHERE telegram_id = $1 AND status = 'active'
                RETURNING *
                """,
                telegram_id,
            )

    async def get_active_support_session(self, telegram_id: int) -> asyncpg.Record | None:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                """
                SELECT *
                FROM support_sessions
                WHERE telegram_id = $1 AND status = 'active'
                ORDER BY opened_at DESC
                LIMIT 1
                """,
                telegram_id,
            )

    async def list_active_support_sessions(self, limit: int = 100) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT *
                FROM support_sessions
                WHERE status = 'active'
                ORDER BY opened_at DESC
                LIMIT $1
                """,
                limit,
            )

    async def get_or_create_referral_code(self, telegram_id: int, seed: str) -> str:
        user = await self.get_user(telegram_id)
        if user is not None and user["referral_code"]:
            return str(user["referral_code"])

        base = "".join(ch for ch in seed.upper() if ch.isalnum()) or "USER"
        candidate = f"R{base[:8]}{str(telegram_id)[-4:]}"
        candidate = candidate[:24]

        async with self.pool.acquire() as conn:
            suffix = 0
            while True:
                code = candidate if suffix == 0 else f"{candidate[:20]}{suffix:04d}"[-24:]
                try:
                    await conn.execute(
                        "UPDATE users SET referral_code = $1 WHERE telegram_id = $2",
                        code,
                        telegram_id,
                    )
                    row = await conn.fetchrow(
                        "SELECT referral_code FROM users WHERE telegram_id = $1",
                        telegram_id,
                    )
                    if row and row["referral_code"]:
                        return str(row["referral_code"])
                except asyncpg.UniqueViolationError:
                    suffix += 1
                    continue
                suffix += 1
                if suffix > 9999:
                    raise RuntimeError("Unable to generate unique referral code")

    async def get_user_by_referral_code(self, ref_code: str) -> asyncpg.Record | None:
        code = ref_code.strip().upper()
        if not code:
            return None
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT * FROM users WHERE referral_code = $1",
                code,
            )

    async def record_referral_click(self, owner_telegram_id: int, guest_telegram_id: int, ref_code: str) -> None:
        if owner_telegram_id == guest_telegram_id:
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO referral_clicks(owner_telegram_id, guest_telegram_id, ref_code, clicked_at)
                VALUES($1, $2, $3, NOW())
                ON CONFLICT(guest_telegram_id) DO UPDATE SET
                    owner_telegram_id = EXCLUDED.owner_telegram_id,
                    ref_code = EXCLUDED.ref_code,
                    clicked_at = NOW()
                """,
                owner_telegram_id,
                guest_telegram_id,
                ref_code.strip().upper(),
            )

    async def get_referrer_for_guest(self, guest_telegram_id: int) -> int | None:
        async with self.pool.acquire() as conn:
            owner = await conn.fetchval(
                "SELECT owner_telegram_id FROM referral_clicks WHERE guest_telegram_id = $1",
                guest_telegram_id,
            )
        if owner is None:
            return None
        return int(owner)

    async def get_referral_stats(self, owner_telegram_id: int) -> dict[str, int]:
        async with self.pool.acquire() as conn:
            clicks = int(
                await conn.fetchval(
                    "SELECT COUNT(*) FROM referral_clicks WHERE owner_telegram_id = $1",
                    owner_telegram_id,
                )
            )
            pending = int(
                await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM users
                    WHERE referred_by = $1 AND access_status = 'pending'
                    """,
                    owner_telegram_id,
                )
            )
            approved = int(
                await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM users
                    WHERE referred_by = $1 AND access_status = 'approved'
                    """,
                    owner_telegram_id,
                )
            )
        return {"clicks": clicks, "pending": pending, "approved": approved}

    async def seed_content_if_empty(self, content: dict[str, Any]) -> None:
        async with self.pool.acquire() as conn:
            settings_count = int(await conn.fetchval("SELECT COUNT(*) FROM content_settings"))
            links_count = int(await conn.fetchval("SELECT COUNT(*) FROM content_links"))
        if settings_count > 0 or links_count > 0:
            return

        payload = content if isinstance(content, dict) else {}
        program = payload.get("program", {}) if isinstance(payload.get("program", {}), dict) else {}
        manager = payload.get("manager_contact", {}) if isinstance(payload.get("manager_contact", {}), dict) else {}
        manager_contacts = payload.get("manager_contacts", {}) if isinstance(payload.get("manager_contacts", {}), dict) else {}

        def manager_for(role: str) -> dict[str, Any]:
            raw = manager_contacts.get(role, {})
            if isinstance(raw, dict):
                return raw
            return {}

        await self.upsert_content_settings(
            {
                "program_title": str(program.get("title", "Актуальная программа саммита")),
                "program_url": str(program.get("url", "")),
                "manager_title": str(manager.get("title", "Написать менеджеру")),
                "manager_url": str(manager.get("url", "")),
                "manager_title_partner": str(manager_for(ROLE_PARTNER).get("title", manager.get("title", "Написать менеджеру"))),
                "manager_url_partner": str(manager_for(ROLE_PARTNER).get("url", manager.get("url", ""))),
                "manager_title_expert": str(manager_for(ROLE_EXPERT).get("title", manager.get("title", "Написать менеджеру"))),
                "manager_url_expert": str(manager_for(ROLE_EXPERT).get("url", manager.get("url", ""))),
                "manager_title_influencer": str(manager_for(ROLE_INFLUENCER).get("title", manager.get("title", "Написать менеджеру"))),
                "manager_url_influencer": str(manager_for(ROLE_INFLUENCER).get("url", manager.get("url", ""))),
                "restricted_text": str(payload.get("restricted_text", DEFAULT_RESTRICTED_TEXT)),
                "welcome_template": str(payload.get("welcome_template", DEFAULT_WELCOME_TEMPLATE)),
                "public_welcome_text": str(payload.get("public_welcome_text", DEFAULT_PUBLIC_WELCOME)),
                "partner_presentation_url": str(payload.get("partner_presentation_url", "")),
                "expert_form_url": str(payload.get("expert_form_url", "")),
                "influencer_form_url": str(payload.get("influencer_form_url", "")),
                "referral_prize_text": str(payload.get("referral_prize_text", "Пригласите коллег и выиграйте iPhone")),
            }
        )

        section_aliases: dict[str, str] = {
            LEGACY_SECTION_USEFUL_LINKS: SECTION_PARTNER_USEFUL_LINKS,
            SECTION_PUBLIC_MENU_LINKS: SECTION_PUBLIC_MENU_LINKS,
            SECTION_PARTNER_USEFUL_LINKS: SECTION_PARTNER_USEFUL_LINKS,
            SECTION_EXPERT_USEFUL_LINKS: SECTION_EXPERT_USEFUL_LINKS,
            SECTION_INFLUENCER_USEFUL_LINKS: SECTION_INFLUENCER_USEFUL_LINKS,
            SECTION_PARTNER_MATERIALS: SECTION_PARTNER_MATERIALS,
            SECTION_EXPERT_MATERIALS: SECTION_EXPERT_MATERIALS,
            SECTION_INFLUENCER_MATERIALS: SECTION_INFLUENCER_MATERIALS,
        }

        for raw_key, section in section_aliases.items():
            items = payload.get(raw_key, [])
            if not isinstance(items, list):
                continue
            for idx, item in enumerate(items):
                if not isinstance(item, dict):
                    continue
                title = str(item.get("title", "")).strip()
                url = str(item.get("url", "")).strip()
                if not title or not url:
                    continue
                await self.add_content_link(
                    section=section,
                    title=title,
                    url=url,
                    position=(idx + 1) * 10,
                    is_active=True,
                )

    async def get_content_bundle(self) -> dict[str, Any]:
        settings_map = await self.get_content_settings_map()
        links = await self.list_all_content_links(include_inactive=False)
        grouped: dict[str, list[dict[str, str]]] = {}
        for item in links:
            section = str(item["section"])
            grouped.setdefault(section, []).append(
                {
                    "title": str(item["title"]),
                    "url": str(item["url"]),
                    "category": str(item["category"] or ""),
                    "subcategory": str(item["subcategory"] or ""),
                }
            )

        manager_title = settings_map.get("manager_title", "Написать менеджеру")
        manager_url = settings_map.get("manager_url", "")

        return {
            "program": {
                "title": settings_map.get("program_title", "Актуальная программа саммита"),
                "url": settings_map.get("program_url", ""),
            },
            "manager_contact": {
                "title": manager_title,
                "url": manager_url,
            },
            "manager_contacts": {
                ROLE_PARTNER: {
                    "title": settings_map.get("manager_title_partner", manager_title),
                    "url": settings_map.get("manager_url_partner", manager_url),
                },
                ROLE_EXPERT: {
                    "title": settings_map.get("manager_title_expert", manager_title),
                    "url": settings_map.get("manager_url_expert", manager_url),
                },
                ROLE_INFLUENCER: {
                    "title": settings_map.get("manager_title_influencer", manager_title),
                    "url": settings_map.get("manager_url_influencer", manager_url),
                },
            },
            "restricted_text": settings_map.get("restricted_text", DEFAULT_RESTRICTED_TEXT),
            "welcome_template": settings_map.get("welcome_template", DEFAULT_WELCOME_TEMPLATE),
            "public_welcome_text": settings_map.get("public_welcome_text", DEFAULT_PUBLIC_WELCOME),
            "partner_presentation_url": settings_map.get("partner_presentation_url", ""),
            "expert_form_url": settings_map.get("expert_form_url", ""),
            "influencer_form_url": settings_map.get("influencer_form_url", ""),
            "referral_prize_text": settings_map.get("referral_prize_text", "Пригласите коллег и выиграйте iPhone"),
            "sections": grouped,
            SECTION_PUBLIC_MENU_LINKS: grouped.get(SECTION_PUBLIC_MENU_LINKS, []),
            SECTION_PARTNER_USEFUL_LINKS: grouped.get(SECTION_PARTNER_USEFUL_LINKS, []),
            SECTION_EXPERT_USEFUL_LINKS: grouped.get(SECTION_EXPERT_USEFUL_LINKS, []),
            SECTION_INFLUENCER_USEFUL_LINKS: grouped.get(SECTION_INFLUENCER_USEFUL_LINKS, []),
            SECTION_PARTNER_MATERIALS: grouped.get(SECTION_PARTNER_MATERIALS, []),
            SECTION_EXPERT_MATERIALS: grouped.get(SECTION_EXPERT_MATERIALS, []),
            SECTION_INFLUENCER_MATERIALS: grouped.get(SECTION_INFLUENCER_MATERIALS, []),
        }

    async def get_stats(self) -> dict[str, int]:
        async with self.pool.acquire() as conn:
            users_total = int(await conn.fetchval("SELECT COUNT(*) FROM users"))
            pending_users = int(await conn.fetchval("SELECT COUNT(*) FROM users WHERE access_status = 'pending'"))
            approved_users = int(await conn.fetchval("SELECT COUNT(*) FROM users WHERE access_status = 'approved'"))
            rejected_users = int(await conn.fetchval("SELECT COUNT(*) FROM users WHERE access_status = 'rejected'"))
            codes_total = int(await conn.fetchval("SELECT COUNT(*) FROM access_codes"))
            active_codes = int(await conn.fetchval("SELECT COUNT(*) FROM access_codes WHERE is_active = TRUE"))
            broadcasts_total = int(await conn.fetchval("SELECT COUNT(*) FROM broadcasts"))
            pending_broadcasts = int(
                await conn.fetchval("SELECT COUNT(*) FROM broadcasts WHERE sent_at IS NULL AND status = 'scheduled'")
            )
            feedback_total = int(await conn.fetchval("SELECT COUNT(*) FROM feedback_messages"))
            applications_total = int(await conn.fetchval("SELECT COUNT(*) FROM applications"))
            new_applications = int(await conn.fetchval("SELECT COUNT(*) FROM applications WHERE status = 'new'"))
            active_support_sessions = int(
                await conn.fetchval("SELECT COUNT(*) FROM support_sessions WHERE status = 'active'")
            )

        return {
            "users_total": users_total,
            "pending_users": pending_users,
            "approved_users": approved_users,
            "rejected_users": rejected_users,
            "codes_total": codes_total,
            "active_codes": active_codes,
            "broadcasts_total": broadcasts_total,
            "pending_broadcasts": pending_broadcasts,
            "feedback_total": feedback_total,
            "applications_total": applications_total,
            "new_applications": new_applications,
            "active_support_sessions": active_support_sessions,
        }
