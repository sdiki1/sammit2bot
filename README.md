# Summit Partner Bot

Telegram-бот для коммуникации организаторов саммита с партнёрами + веб-админка.

## Что реализовано

- Приватный доступ партнёров через deep-link `/start CODE` и `/code CODE`.
- Главное меню в Telegram: новости, программа, полезные ссылки, материалы, связь с менеджером.
- Рассылки: мгновенные и отложенные, лог доставки, статистика.
- Вопросы в поддержку с reply-механизмом ответа менеджера.
- Веб-админка (FastAPI):
  - управление кодами доступа;
  - управление ссылками и материалами;
  - редактирование текстов/контента бота;
  - создание текстовых рассылок.
- PostgreSQL как основная БД.
- Запуск всех сервисов через `docker-compose`.

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
- `ADMIN_IDS` — id админов бота (через запятую).
- `SUPPORT_CHAT_IDS` — id support-чатов (через запятую).
- `DATABASE_URL` — строка подключения PostgreSQL.
- `CONTENT_FILE` — JSON с начальными данными для первичного заполнения БД.
- `ADMIN_PANEL_USERNAME` / `ADMIN_PANEL_PASSWORD` — логин/пароль веб-админки.
- `ADMIN_PANEL_SECRET` — секрет сессий админки.
- `ADMIN_PANEL_PORT` — порт админки в контейнере (по умолчанию `8080`).

## Локальный запуск без Docker

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Запуск бота:

```bash
python run.py
```

Запуск админки:

```bash
python run_admin.py
```

## Запуск через Docker Compose

```bash
docker compose up -d --build
```

Сервисы:

- `db` — PostgreSQL, внешний порт `5544`.
- `bot` — Telegram-бот.
- `admin` — веб-админка на `http://localhost:8080`.

Проверка:

```bash
docker compose ps
docker compose logs -f bot
docker compose logs -f admin
```

## PostgreSQL (порт 5544)

В `docker-compose.yml` используется:

- порт хоста: `5544`
- порт контейнера: `5432`
- БД: `summit_bot`
- пользователь: `summit`
- пароль: `summit`

Строка подключения внутри контейнеров:

`postgresql://summit:summit@db:5432/summit_bot`

Строка подключения с хоста:

`postgresql://summit:summit@localhost:5544/summit_bot`

## Админ-команды в Telegram (дополнительно)

- `/add_code CODE Описание`
- `/disable_code CODE`
- `/enable_code CODE`
- `/list_codes`
- `/broadcast Текст`
- `reply + /broadcast` для медиа
- `/broadcast_in МИНУТЫ Текст`
- `/broadcast_stats ID`

