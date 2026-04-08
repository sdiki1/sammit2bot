from __future__ import annotations

import uvicorn

from summit_partner_bot.config import load_settings


def main() -> None:
    settings = load_settings()
    uvicorn.run(
        "summit_partner_bot.admin_app:create_app",
        factory=True,
        host="0.0.0.0",
        port=settings.admin_panel_port,
    )


if __name__ == "__main__":
    main()

