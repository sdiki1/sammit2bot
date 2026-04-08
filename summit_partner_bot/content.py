from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from summit_partner_bot.db import Database

logger = logging.getLogger(__name__)


class ContentLoader:
    def __init__(self, db: Database, path: Path) -> None:
        self.db = db
        self.path = path

    def _load_fallback(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        raw = self.path.read_text(encoding="utf-8")
        if not raw.strip():
            return {}
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.exception("Invalid JSON in content file: %s", self.path)
            return {}
        if not isinstance(data, dict):
            raise RuntimeError(f"Invalid content format in {self.path}")
        return data

    async def bootstrap_defaults(self) -> None:
        fallback = self._load_fallback()
        await self.db.seed_content_if_empty(fallback)

    async def load(self) -> dict[str, Any]:
        return await self.db.get_content_bundle()
