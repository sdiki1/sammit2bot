# Summit 2026 Bot

Telegram-бот для СТАММИТ26 с общим меню и приватными сценариями для партнёров, экспертов и инфлюенсеров.

## Что реализовано

- Публичный бот с меню для участников (ссылки, программа, FAQ, канал, сайт, отзывы, реферальная кнопка).
- Приватный доступ по deep-link приглашениям с ролью:
  - `partner`;
  - `expert`;
  - `influencer`.
- Процесс согласования доступа: заявка `pending` -> подтверждение/отклонение админом.
- Раздельные меню и материалы по ролям.
- Рассылки:
  - мгновенные и отложенные;
  - с таргетом `all|partner|expert|influencer`;
  - лог доставки и статистика.
- Связь с менеджером через support-чат + reply-мост обратно пользователю.
- Сбор отзывов и их лог в БД.
- Реферальные ссылки для подтверждённых пользователей + учёт переходов/конверсий.
- Выгрузка базы пользователей в CSV.
- Админ-панель FastAPI для управления кодами, контентом, заявками и рассылками.

## Стек

- Python 3.11
- aiogram 3.x
- FastAPI + Jinja2
- PostgreSQL
- Docker Compose

## Переменные окружения

Скопируйте пример:

```bash
cp .env.example .env
```

Ключевые переменные:

- `BOT_TOKEN` — токен Telegram-бота.
- `ADMIN_IDS` — Telegram ID администраторов (через запятую).
- `SUPPORT_CHAT_IDS` — Telegram ID чатов поддержки (через запятую).
- `DATABASE_URL` — строка подключения PostgreSQL.
- `CONTENT_FILE` — JSON начального контента.
- `RATE_LIMIT_SECONDS` — ограничение частоты сообщений.
- `SUMMIT_NAME` — имя события.
- `ADMIN_PANEL_USERNAME` / `ADMIN_PANEL_PASSWORD` — доступ в веб-админку.
- `ADMIN_PANEL_SECRET` — секрет сессии админки.
- `ADMIN_PANEL_PORT` — порт веб-админки.

## Локальный запуск

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run.py
```

Админка:

```bash
python run_admin.py
```

## Запуск через Docker Compose

```bash
docker compose up -d --build
```

## Админ-команды в Telegram

Доступны только для `ADMIN_IDS`.

- `/add_code CODE ROLE описание`
- `/disable_code CODE`
- `/enable_code CODE`
- `/list_codes`
- `/pending_requests`
- `/approve_user TELEGRAM_ID`
- `/reject_user TELEGRAM_ID причина`
- `/broadcast [all|partner|expert|influencer] Текст`
- `reply + /broadcast [роль]` для медиа-рассылок
- `/broadcast_in МИНУТЫ [роль] Текст`
- `/broadcast_stats ID`
- `/save_link SECTION | Название | URL`
- `/save_material SECTION | Название` (команда в reply на документ/фото/видео)
- `/export_users`

## Секции ссылок/материалов

Редактируются через админку или командами `/save_link` / `/save_material`.

- `public_menu_links`
- `partner_useful_links`
- `expert_useful_links`
- `influencer_useful_links`
- `partner_materials`
- `expert_materials`
- `influencer_materials`

## Форматы медиа-материалов

В секциях материалов `url` может быть:

- обычный URL (`https://...`)
- `file_id:...`
- `photo_id:...`
- `video_id:...`

Это позволяет администратору загружать файлы прямо через Telegram-бота.
