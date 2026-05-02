from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _parse_int_set(raw_value: str) -> set[int]:
    result: set[int] = set()
    for part in raw_value.split(","):
        chunk = part.strip()
        if not chunk:
            continue
        result.add(int(chunk))
    return result


@dataclass(frozen=True, slots=True)
class BotProfile:
    key: str
    token: str
    username: str
    role: str | None = None
    is_public: bool = False


@dataclass(slots=True)
class Settings:
    bot_token: str
    bot_username: str
    summit_bot_token: str
    summit_bot_username: str
    partner_bot_token: str
    partner_bot_username: str
    expert_bot_token: str
    expert_bot_username: str
    influencer_bot_token: str
    influencer_bot_username: str
    bot_profiles: tuple[BotProfile, ...]
    admin_ids: set[int]
    support_chat_ids: set[int]
    database_url: str
    content_file: Path
    rate_limit_seconds: float
    summit_name: str
    admin_panel_username: str
    admin_panel_password: str
    admin_panel_secret: str
    admin_panel_port: int


def load_settings() -> Settings:
    load_dotenv()

    legacy_bot_token = os.getenv("BOT_TOKEN", "").strip()
    legacy_bot_username = os.getenv("BOT_USERNAME", "").strip().lstrip("@")

    summit_bot_token = os.getenv("SUMMIT_BOT_TOKEN", "").strip() or legacy_bot_token
    summit_bot_username = os.getenv("SUMMIT_BOT_USERNAME", "").strip().lstrip("@") or legacy_bot_username
    partner_bot_token = os.getenv("PARTNER_BOT_TOKEN", "").strip()
    partner_bot_username = os.getenv("PARTNER_BOT_USERNAME", "").strip().lstrip("@")
    expert_bot_token = os.getenv("EXPERT_BOT_TOKEN", "").strip()
    expert_bot_username = os.getenv("EXPERT_BOT_USERNAME", "").strip().lstrip("@")
    influencer_bot_token = os.getenv("INFLUENCER_BOT_TOKEN", "").strip()
    influencer_bot_username = os.getenv("INFLUENCER_BOT_USERNAME", "").strip().lstrip("@")

    if not summit_bot_token:
        raise RuntimeError("Environment variable SUMMIT_BOT_TOKEN is required")

    bot_profiles = tuple(
        profile
        for profile in (
            BotProfile(
                key="summit",
                token=summit_bot_token,
                username=summit_bot_username,
                role=None,
                is_public=True,
            ),
            BotProfile(
                key="partner",
                token=partner_bot_token,
                username=partner_bot_username,
                role="partner",
            ),
            BotProfile(
                key="expert",
                token=expert_bot_token,
                username=expert_bot_username,
                role="expert",
            ),
            BotProfile(
                key="influencer",
                token=influencer_bot_token,
                username=influencer_bot_username,
                role="influencer",
            ),
        )
        if profile.token
    )
    tokens_by_key: dict[str, str] = {}
    duplicate_keys: list[str] = []
    for profile in bot_profiles:
        existing_key = tokens_by_key.get(profile.token)
        if existing_key:
            duplicate_keys.append(f"{existing_key}/{profile.key}")
        else:
            tokens_by_key[profile.token] = profile.key
    if duplicate_keys:
        raise RuntimeError(
            "Each Telegram bot must have a unique token. Duplicate bot profiles: "
            + ", ".join(duplicate_keys)
        )

    bot_token = summit_bot_token
    bot_username = summit_bot_username

    admin_ids_raw = os.getenv("ADMIN_IDS", "")
    if not admin_ids_raw.strip():
        raise RuntimeError("Environment variable ADMIN_IDS is required")

    support_chat_ids = _parse_int_set(os.getenv("SUPPORT_CHAT_IDS", ""))
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("Environment variable DATABASE_URL is required")
    content_file = Path(os.getenv("CONTENT_FILE", "data/content.json"))
    rate_limit_seconds = float(os.getenv("RATE_LIMIT_SECONDS", "0.7"))
    summit_name = os.getenv("SUMMIT_NAME", "Саммит 2026")
    admin_panel_username = os.getenv("ADMIN_PANEL_USERNAME", "admin")
    admin_panel_password = os.getenv("ADMIN_PANEL_PASSWORD", "")
    if not admin_panel_password:
        raise RuntimeError("Environment variable ADMIN_PANEL_PASSWORD is required")
    admin_panel_secret = os.getenv("ADMIN_PANEL_SECRET", "")
    if not admin_panel_secret:
        raise RuntimeError("Environment variable ADMIN_PANEL_SECRET is required")
    admin_panel_port = int(os.getenv("ADMIN_PANEL_PORT", "8030"))

    return Settings(
        bot_token=bot_token,
        bot_username=bot_username,
        summit_bot_token=summit_bot_token,
        summit_bot_username=summit_bot_username,
        partner_bot_token=partner_bot_token,
        partner_bot_username=partner_bot_username,
        expert_bot_token=expert_bot_token,
        expert_bot_username=expert_bot_username,
        influencer_bot_token=influencer_bot_token,
        influencer_bot_username=influencer_bot_username,
        bot_profiles=bot_profiles,
        admin_ids=_parse_int_set(admin_ids_raw),
        support_chat_ids=support_chat_ids,
        database_url=database_url,
        content_file=content_file,
        rate_limit_seconds=rate_limit_seconds,
        summit_name=summit_name,
        admin_panel_username=admin_panel_username,
        admin_panel_password=admin_panel_password,
        admin_panel_secret=admin_panel_secret,
        admin_panel_port=admin_panel_port,
    )
