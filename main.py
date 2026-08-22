import os
import re
import asyncio
from collections import Counter
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from urllib.parse import quote

import asyncpg
from unidecode import unidecode
from rapidfuzz import fuzz

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand,
)

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    AuthKeyUnregisteredError,
)
from telethon.tl.types import User


# ============================================================
# CONFIG
# ============================================================

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]

# Необязательно:
# если добавишь ADMIN_ID в Railway Variables,
# бот будет доступен только этому Telegram user ID.
ADMIN_ID = int(os.environ["ADMIN_ID"]) if os.environ.get("ADMIN_ID") else None

CHATS_PER_PAGE = 8
QUERIES_PER_PAGE = 8
MATCH_THRESHOLD = 76

# Если один и тот же пользователь раскидывает один и тот же запрос
# по нескольким чатам подряд, показываем только первое сообщение.
# Через 120 секунд такой же запрос снова разрешён.
DEDUP_WINDOW_SECONDS = 120

# Небольшая задержка перед отправкой найденного сообщения
# в управляющий бот. Мониторинг при этом продолжает работать.
NOTIFICATION_DELAY_SECONDS = 15

# Часовой пояс аналитики.
# В Railway можно добавить, например:
# ANALYTICS_TZ=Europe/Moscow
# ANALYTICS_TZ=Europe/Amsterdam
ANALYTICS_TZ_NAME = os.environ.get("ANALYTICS_TZ", "UTC")

try:
    ANALYTICS_TZ = ZoneInfo(ANALYTICS_TZ_NAME)
except ZoneInfoNotFoundError:
    ANALYTICS_TZ_NAME = "UTC"
    ANALYTICS_TZ = timezone.utc

bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

db_pool: asyncpg.Pool | None = None

# Одновременно поддерживаем максимум два Telegram user-аккаунта.
MAX_ACCOUNTS = 2

# account_id из PostgreSQL -> Telethon client/task
clients: dict[int, TelegramClient] = {}
telethon_tasks: dict[int, asyncio.Task] = {}

# Временный клиент только на время /login.
# Ключ — Telegram user ID владельца управляющего бота.
login_clients: dict[int, TelegramClient] = {}


# ============================================================
# FSM
# ============================================================

class LoginStates(StatesGroup):
    waiting_phone = State()
    waiting_code = State()
    waiting_password = State()


class QueryStates(StatesGroup):
    waiting_query = State()


class ChatSearchStates(StatesGroup):
    waiting_search = State()


# ============================================================
# DATABASE
# ============================================================

async def init_db():
    global db_pool

    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=5,
    )

    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS queries (
                id BIGSERIAL PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                query TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        # ЧИСТЫЙ список поиска v2.
        # Старую таблицу queries намеренно НЕ мигрируем и НЕ читаем:
        # в ней мог остаться повреждённый/зависший список.
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS monitor_queries_v2 (
                id BIGSERIAL PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                query TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_monitor_queries_v2_owner
            ON monitor_queries_v2 (owner_id, id DESC)
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS selected_chats (
                owner_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                title TEXT NOT NULL,
                PRIMARY KEY (owner_id, chat_id)
            )
        """)

        # Для совместимости со старой БД просто добавляем account_id.
        # Старый PRIMARY KEY (owner_id, chat_id) оставляем:
        # один и тот же чат будет мониториться только одним аккаунтом,
        # что заодно убирает дубли.
        await conn.execute("""
            ALTER TABLE selected_chats
            ADD COLUMN IF NOT EXISTS account_id BIGINT
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS telegram_accounts (
                id BIGSERIAL PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                label TEXT NOT NULL,
                session TEXT NOT NULL,
                tg_user_id BIGINT,
                username TEXT,
                first_name TEXT,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_telegram_accounts_owner_tg
            ON telegram_accounts (owner_id, tg_user_id)
            WHERE tg_user_id IS NOT NULL
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_messages (
                owner_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (owner_id, chat_id, message_id)
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS analytics_events (
                owner_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                chat_title TEXT,
                monitor_query TEXT NOT NULL,
                found_request TEXT NOT NULL,
                brand TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (owner_id, chat_id, message_id)
            )
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_analytics_owner_time
            ON analytics_events (owner_id, created_at DESC)
        """)

        # Аналитика ВСЕХ реальных запросов в выбранных чатах.
        # Она не зависит от списка monitor_queries_v2.
        #
        # Старую analytics_events оставляем для совместимости,
        # но новые отчёты читают только эту чистую таблицу.
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS all_request_analytics_events (
                owner_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                chat_title TEXT,
                found_request TEXT NOT NULL,
                brand TEXT,
                seller_key TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (owner_id, chat_id, message_id)
            )
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_all_request_analytics_owner_time
            ON all_request_analytics_events (owner_id, created_at DESC)
        """)

        # Отдельный антидубль для аналитики.
        # Не используем notification-dedup, чтобы аналитика никак
        # не могла подавить обычное уведомление.
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS recent_all_request_analytics_dedup (
                owner_id BIGINT NOT NULL,
                seller_key TEXT NOT NULL,
                request_key TEXT NOT NULL,
                first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (owner_id, seller_key, request_key)
            )
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_recent_all_request_analytics_time
            ON recent_all_request_analytics_dedup (first_seen_at)
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS recent_request_dedup (
                owner_id BIGINT NOT NULL,
                seller_key TEXT NOT NULL,
                request_key TEXT NOT NULL,
                last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (owner_id, seller_key, request_key)
            )
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_recent_request_dedup_time
            ON recent_request_dedup (last_seen)
        """)


async def get_setting(key: str) -> str | None:
    async with db_pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT value FROM app_settings WHERE key=$1",
            key,
        )


async def set_setting(key: str, value: str | None):
    async with db_pool.acquire() as conn:
        if value is None:
            await conn.execute(
                "DELETE FROM app_settings WHERE key=$1",
                key,
            )
        else:
            await conn.execute("""
                INSERT INTO app_settings (key, value)
                VALUES ($1, $2)
                ON CONFLICT (key)
                DO UPDATE SET value=EXCLUDED.value
            """, key, value)


async def get_owner_id() -> int | None:
    if ADMIN_ID:
        return ADMIN_ID

    value = await get_setting("owner_id")
    return int(value) if value else None


async def claim_or_check_owner(user_id: int) -> bool:
    if ADMIN_ID:
        return user_id == ADMIN_ID

    owner = await get_owner_id()

    if owner is None:
        await set_setting("owner_id", str(user_id))
        return True

    return owner == user_id


async def owner_allowed(user_id: int) -> bool:
    owner = await get_owner_id()
    return owner is not None and owner == user_id


async def guard_message(message: Message) -> bool:
    if await owner_allowed(message.from_user.id):
        return True

    await message.answer("⛔ Этот бот закрыт.")
    return False


async def guard_callback(callback: CallbackQuery) -> bool:
    if await owner_allowed(callback.from_user.id):
        return True

    await callback.answer("Этот бот закрыт.", show_alert=True)
    return False


def canonical_query_key(query: str) -> str:
    """
    Один и тот же запрос из старой базы приводим к одному ключу:
    лишние пробелы / регистр / буллет в начале не имеют значения.
    """
    item = (
        query
        or ""
    ).strip()

    item = re.sub(
        r"^\s*(?:[•▪▫◦·*]|[-–—])\s*",
        "",
        item,
    )

    item = re.sub(
        r"\s+",
        " ",
        item,
    ).strip()

    return item.casefold()


async def get_queries(owner_id: int):
    """
    Возвращает УЖЕ ОЧИЩЕННЫЙ список без дублей,
    даже если в старой PostgreSQL ещё лежат повторные строки.
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, query
            FROM monitor_queries_v2
            WHERE owner_id=$1
            ORDER BY id DESC
            """,
            owner_id,
        )

    result = []
    seen = set()

    for row in rows:
        key = canonical_query_key(
            row["query"]
        )

        if not key or key in seen:
            continue

        seen.add(key)
        result.append(row)

    return result


async def query_still_active(
    owner_id: int,
    query_id: int,
) -> bool:
    """
    Прямая проверка PostgreSQL без кэша.

    Нужна из-за 15-секундной задержки:
    если за эти 15 секунд список очистили или запрос удалили,
    старое уведомление НЕ должно прилететь.
    """
    async with db_pool.acquire() as conn:
        return bool(
            await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM monitor_queries_v2
                    WHERE owner_id=$1
                      AND id=$2
                )
                """,
                owner_id,
                query_id,
            )
        )


def parse_query_entries(raw_text: str) -> list[str]:
    """
    Позволяет добавлять как один запрос, так и сразу список строками.

    Пример:
      S25 256 Navy
      iPhone 17 256 Lavender
      iPhone 17 Pro Max 512 Blue eSim

    Каждая строка станет ОТДЕЛЬНОЙ записью в PostgreSQL.

    Также убираем визуальные маркеры:
      • query
      - query
      * query
      1. query
    """
    raw_text = (
        raw_text
        or ""
    ).strip()

    if not raw_text:
        return []

    lines = re.split(
        r"[\r\n]+",
        raw_text,
    )

    result = []
    seen = set()

    for line in lines:
        item = line.strip()

        if not item:
            continue

        # Убираем буллеты/нумерацию только в начале строки.
        item = re.sub(
            r"^\s*(?:[•▪▫◦·*]|[-–—])\s*",
            "",
            item,
        )

        item = re.sub(
            r"^\s*\d{1,3}[\.\)]\s*",
            "",
            item,
        )

        item = re.sub(
            r"\s+",
            " ",
            item,
        ).strip()

        if len(item) < 2:
            continue

        key = item.casefold()

        if key in seen:
            continue

        seen.add(key)
        result.append(item)

    return result


async def add_query(owner_id: int, query: str) -> bool:
    """
    Добавляет ровно один запрос.
    Старые/новые дубли не создаются.
    """
    query = re.sub(
        r"\s+",
        " ",
        (query or "").strip(),
    )

    if len(query) < 2:
        return False

    new_key = canonical_query_key(
        query
    )

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, query
            FROM monitor_queries_v2
            WHERE owner_id=$1
            """,
            owner_id,
        )

        for row in rows:
            if canonical_query_key(
                row["query"]
            ) == new_key:
                return False

        await conn.execute(
            """
            INSERT INTO monitor_queries_v2 (
                owner_id,
                query
            )
            VALUES ($1, $2)
            """,
            owner_id,
            query,
        )

    return True


async def add_queries(
    owner_id: int,
    raw_text: str,
) -> tuple[int, list[str]]:
    """
    Добавляет список запросов из одного сообщения.
    Возвращает:
      сколько реально добавлено,
      список добавленных запросов.
    """
    entries = parse_query_entries(
        raw_text
    )

    added = []

    for query in entries:
        if await add_query(
            owner_id,
            query,
        ):
            added.append(query)

    return len(added), added


async def delete_query(
    owner_id: int,
    query_id: int,
) -> str | None:
    """
    Удаляет выбранный запрос И все его старые дубли.

    Это специально для старой базы, где один и тот же S25
    мог сохраниться много раз под разными id.
    """
    async with db_pool.acquire() as conn:
        selected = await conn.fetchrow(
            """
            SELECT id, query
            FROM monitor_queries_v2
            WHERE owner_id=$1
              AND id=$2
            """,
            owner_id,
            query_id,
        )

        if selected is None:
            return None

        selected_query = selected["query"]
        selected_key = canonical_query_key(
            selected_query
        )

        rows = await conn.fetch(
            """
            SELECT id, query
            FROM monitor_queries_v2
            WHERE owner_id=$1
            """,
            owner_id,
        )

        ids_to_delete = [
            int(row["id"])
            for row in rows
            if canonical_query_key(
                row["query"]
            ) == selected_key
        ]

        if ids_to_delete:
            await conn.execute(
                """
                DELETE FROM monitor_queries_v2
                WHERE owner_id=$1
                  AND id = ANY($2::bigint[])
                """,
                owner_id,
                ids_to_delete,
            )

        return selected_query


async def cleanup_duplicate_queries():
    """
    Одноразовая/безопасная чистка старой базы.

    Если там накопилось:
      S25 256 Navy
      S25 256 Navy
      S25 256 Navy
      ...

    оставляем только одну запись.

    Также удаляем пустые/мусорные записи.
    """
    async with db_pool.acquire() as conn:
        owners = await conn.fetch(
            """
            SELECT DISTINCT owner_id
            FROM monitor_queries_v2
            """
        )

    removed = 0

    for owner_row in owners:
        owner_id = int(
            owner_row["owner_id"]
        )

        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, query
                FROM monitor_queries_v2
                WHERE owner_id=$1
                ORDER BY id DESC
                """,
                owner_id,
            )

        keep = set()
        ids_to_delete = []

        for row in rows:
            key = canonical_query_key(
                row["query"]
            )

            if not key:
                ids_to_delete.append(
                    int(row["id"])
                )
                continue

            if key in keep:
                ids_to_delete.append(
                    int(row["id"])
                )
                continue

            keep.add(key)

        if ids_to_delete:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """
                    DELETE FROM monitor_queries_v2
                    WHERE owner_id=$1
                      AND id = ANY($2::bigint[])
                    """,
                    owner_id,
                    ids_to_delete,
                )

            removed += len(
                ids_to_delete
            )

    if removed:
        print(
            "QUERY CLEANUP | "
            f"removed duplicate/stale rows={removed}"
        )


async def reset_all_queries(owner_id: int) -> int:
    """
    Полностью удаляет старый список отслеживания ТОЛЬКО у владельца.
    Чаты, аккаунты, аналитика и остальные настройки не затрагиваются.
    """
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            """
            DELETE FROM monitor_queries_v2
            WHERE owner_id=$1
            """,
            owner_id,
        )

    try:
        return int(result.split()[-1])
    except Exception:
        return 0


async def migrate_multiline_queries():
    """
    Исправляет старые записи, которые были сохранены одним большим
    многострочным запросом.

    Например одна строка БД:
      S25 256 Navy
      iPhone 17 256 Lavender
      ray-ban meta

    автоматически превращается в три независимых запроса.

    Миграция безопасна и выполняется при каждом старте;
    обычные однострочные запросы она не трогает.
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, owner_id, query
            FROM monitor_queries_v2
            WHERE query LIKE '%' || CHR(10) || '%'
               OR query LIKE '%' || CHR(13) || '%'
            ORDER BY id ASC
            """
        )

    migrated = 0

    for row in rows:
        entries = parse_query_entries(
            row["query"]
        )

        # Если по факту это одна нормальная строка — не трогаем.
        if len(entries) <= 1:
            continue

        owner_id = int(
            row["owner_id"]
        )
        old_id = int(
            row["id"]
        )

        # Сначала добавляем отдельные строки.
        for entry in entries:
            await add_query(
                owner_id,
                entry,
            )

        # Потом удаляем старый "пакет".
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM monitor_queries_v2
                WHERE id=$1
                  AND owner_id=$2
                """,
                old_id,
                owner_id,
            )

        migrated += 1

    if migrated:
        print(
            "QUERY MIGRATION | "
            f"split multiline rows={migrated}"
        )


async def get_accounts(owner_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT
                id,
                owner_id,
                label,
                session,
                tg_user_id,
                username,
                first_name,
                active,
                created_at
            FROM telegram_accounts
            WHERE owner_id=$1
              AND active=TRUE
            ORDER BY id ASC
            """,
            owner_id,
        )


async def get_account(account_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT
                id,
                owner_id,
                label,
                session,
                tg_user_id,
                username,
                first_name,
                active
            FROM telegram_accounts
            WHERE id=$1
            """,
            account_id,
        )


async def account_count(owner_id: int) -> int:
    async with db_pool.acquire() as conn:
        return int(
            await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM telegram_accounts
                WHERE owner_id=$1
                  AND active=TRUE
                """,
                owner_id,
            )
            or 0
        )


async def save_account(
    owner_id: int,
    session_string: str,
    tg_user_id: int,
    username: str | None,
    first_name: str | None,
) -> int:
    """
    Сохраняет отдельную StringSession для Telegram-аккаунта.
    API_ID/API_HASH остаются общими.
    """
    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow(
            """
            SELECT id
            FROM telegram_accounts
            WHERE owner_id=$1
              AND tg_user_id=$2
            """,
            owner_id,
            tg_user_id,
        )

        if existing:
            account_id = int(existing["id"])

            await conn.execute(
                """
                UPDATE telegram_accounts
                SET session=$1,
                    username=$2,
                    first_name=$3,
                    active=TRUE
                WHERE id=$4
                """,
                session_string,
                username,
                first_name,
                account_id,
            )

            return account_id

        count = int(
            await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM telegram_accounts
                WHERE owner_id=$1
                  AND active=TRUE
                """,
                owner_id,
            )
            or 0
        )

        if count >= MAX_ACCOUNTS:
            raise RuntimeError(
                f"Уже подключено максимум аккаунтов: {MAX_ACCOUNTS}"
            )

        label = f"Аккаунт {count + 1}"

        account_id = await conn.fetchval(
            """
            INSERT INTO telegram_accounts (
                owner_id,
                label,
                session,
                tg_user_id,
                username,
                first_name,
                active
            )
            VALUES ($1, $2, $3, $4, $5, $6, TRUE)
            RETURNING id
            """,
            owner_id,
            label,
            session_string,
            tg_user_id,
            username,
            first_name,
        )

        return int(account_id)


async def migrate_legacy_session():
    """
    Если бот уже работал в старой версии с app_settings.telethon_session,
    переносим первую сессию в telegram_accounts автоматически.
    Повторно логинить первый аккаунт не нужно.
    """
    legacy_session = await get_setting("telethon_session")

    if not legacy_session:
        return

    owner_id = await get_owner_id()

    if owner_id is None:
        return

    async with db_pool.acquire() as conn:
        exists = await conn.fetchval(
            """
            SELECT 1
            FROM telegram_accounts
            WHERE owner_id=$1
            LIMIT 1
            """,
            owner_id,
        )

        if not exists:
            await conn.execute(
                """
                INSERT INTO telegram_accounts (
                    owner_id,
                    label,
                    session,
                    active
                )
                VALUES ($1, 'Аккаунт 1', $2, TRUE)
                """,
                owner_id,
                legacy_session,
            )

        # Старый ключ больше не нужен.
        await conn.execute(
            "DELETE FROM app_settings WHERE key='telethon_session'"
        )


async def migrate_legacy_selected_chats(owner_id: int):
    """
    Старые выбранные чаты привязываем к первому аккаунту.
    """
    accounts = await get_accounts(owner_id)

    if not accounts:
        return

    first_account_id = int(accounts[0]["id"])

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE selected_chats
            SET account_id=$1
            WHERE owner_id=$2
              AND account_id IS NULL
            """,
            first_account_id,
            owner_id,
        )


async def get_selected_chat_ids(
    owner_id: int,
    account_id: int,
) -> set[int]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT chat_id
            FROM selected_chats
            WHERE owner_id=$1
              AND account_id=$2
            """,
            owner_id,
            account_id,
        )

    return {
        int(row["chat_id"])
        for row in rows
    }


async def get_selected_chat_keys(owner_id: int) -> set[tuple[int, int]]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT account_id, chat_id
            FROM selected_chats
            WHERE owner_id=$1
              AND account_id IS NOT NULL
            """,
            owner_id,
        )

    return {
        (
            int(row["account_id"]),
            int(row["chat_id"]),
        )
        for row in rows
    }


async def toggle_selected_chat(
    owner_id: int,
    account_id: int,
    chat_id: int,
    title: str,
) -> bool:
    """
    В старой таблице PK = (owner_id, chat_id).
    Поэтому один конкретный Telegram-чат назначается одному аккаунту.
    Если такой же чат есть на обоих аккаунтах — это хорошо:
    не будет двух одинаковых уведомлений.
    """
    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow(
            """
            SELECT account_id
            FROM selected_chats
            WHERE owner_id=$1
              AND chat_id=$2
            """,
            owner_id,
            chat_id,
        )

        if existing:
            existing_account_id = existing["account_id"]

            if (
                existing_account_id is not None
                and int(existing_account_id) == account_id
            ):
                await conn.execute(
                    """
                    DELETE FROM selected_chats
                    WHERE owner_id=$1
                      AND chat_id=$2
                    """,
                    owner_id,
                    chat_id,
                )
                return False

            # Чат был выбран на другом аккаунте.
            # Просто переносим мониторинг на текущий.
            await conn.execute(
                """
                UPDATE selected_chats
                SET account_id=$1,
                    title=$2
                WHERE owner_id=$3
                  AND chat_id=$4
                """,
                account_id,
                title,
                owner_id,
                chat_id,
            )
            return True

        await conn.execute(
            """
            INSERT INTO selected_chats (
                owner_id,
                chat_id,
                title,
                account_id
            )
            VALUES ($1, $2, $3, $4)
            """,
            owner_id,
            chat_id,
            title,
            account_id,
        )

        return True


async def get_selected_chats_details(owner_id: int):
    """
    Возвращает выбранные чаты вместе с номером Telegram-аккаунта.
    """
    account_rows = await get_accounts(owner_id)

    account_index = {
        int(row["id"]): index
        for index, row in enumerate(account_rows, start=1)
    }

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                account_id,
                chat_id,
                title
            FROM selected_chats
            WHERE owner_id=$1
              AND account_id IS NOT NULL
            ORDER BY LOWER(title), chat_id
            """,
            owner_id,
        )

    result = []

    for row in rows:
        account_id = int(row["account_id"])

        result.append({
            "account_id": account_id,
            "account_index": account_index.get(account_id, "?"),
            "id": int(row["chat_id"]),
            "name": row["title"] or "Без названия",
        })

    return result


async def mark_seen(
    owner_id: int,
    chat_id: int,
    message_id: int,
) -> bool:
    """
    account_id намеренно НЕ входит в dedupe:
    если оба аккаунта состоят в одном чате,
    одно сообщение не придёт два раза.
    """
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            """
            INSERT INTO seen_messages (
                owner_id,
                chat_id,
                message_id
            )
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
            """,
            owner_id,
            chat_id,
            message_id,
        )

    return result.endswith("1")


# ============================================================
# TELETHON — TWO ACCOUNTS
# ============================================================

def make_monitor_handler(account_id: int):
    async def _handler(event):
        await monitor_handler(
            event,
            account_id,
        )

    return _handler


def start_telethon_monitor(
    account_id: int,
    tg_client: TelegramClient,
):
    old_task = telethon_tasks.get(
        account_id
    )

    if old_task and not old_task.done():
        return

    telethon_tasks[account_id] = asyncio.create_task(
        tg_client.run_until_disconnected()
    )


async def connect_saved_account(row) -> bool:
    account_id = int(row["id"])
    session_string = row["session"]

    tg_client = TelegramClient(
        StringSession(session_string),
        API_ID,
        API_HASH,
        receive_updates=True,
    )

    tg_client.add_event_handler(
        make_monitor_handler(account_id),
        events.NewMessage(),
    )

    try:
        await tg_client.connect()
        authorized = await tg_client.is_user_authorized()

        if not authorized:
            await tg_client.disconnect()

            async with db_pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE telegram_accounts
                    SET active=FALSE
                    WHERE id=$1
                    """,
                    account_id,
                )

            return False

        me = await tg_client.get_me()

        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE telegram_accounts
                SET tg_user_id=$1,
                    username=$2,
                    first_name=$3,
                    session=$4,
                    active=TRUE
                WHERE id=$5
                """,
                int(me.id),
                getattr(me, "username", None),
                getattr(me, "first_name", None),
                tg_client.session.save(),
                account_id,
            )

        await tg_client.set_receive_updates(
            True
        )

        clients[account_id] = tg_client

        start_telethon_monitor(
            account_id,
            tg_client,
        )

        print(
            "Telegram account connected | "
            f"account_id={account_id} | "
            f"tg_id={me.id} | "
            f"username={getattr(me, 'username', None)!r}"
        )

        return True

    except AuthKeyUnregisteredError:
        try:
            await tg_client.disconnect()
        except Exception:
            pass

        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE telegram_accounts
                SET active=FALSE
                WHERE id=$1
                """,
                account_id,
            )

        print(
            "Telegram session invalid | "
            f"account_id={account_id}"
        )
        return False

    except Exception as error:
        try:
            await tg_client.disconnect()
        except Exception:
            pass

        print(
            "ACCOUNT CONNECT ERROR | "
            f"account_id={account_id} | "
            f"{error!r}"
        )
        return False


async def load_saved_clients(owner_id: int):
    accounts = await get_accounts(
        owner_id
    )

    for row in accounts:
        await connect_saved_account(
            row
        )


async def register_logged_in_client(
    owner_id: int,
    tg_client: TelegramClient,
) -> tuple[int, object]:
    me = await tg_client.get_me()
    session_string = tg_client.session.save()

    if not session_string:
        raise RuntimeError(
            "Не удалось сохранить Telegram session."
        )

    account_id = await save_account(
        owner_id=owner_id,
        session_string=session_string,
        tg_user_id=int(me.id),
        username=getattr(me, "username", None),
        first_name=getattr(me, "first_name", None),
    )

    # На случай повторного логина этого же аккаунта.
    old_client = clients.get(
        account_id
    )

    if old_client and old_client is not tg_client:
        try:
            await old_client.disconnect()
        except Exception:
            pass

    old_task = telethon_tasks.pop(
        account_id,
        None,
    )

    if old_task and not old_task.done():
        old_task.cancel()

    tg_client.add_event_handler(
        make_monitor_handler(account_id),
        events.NewMessage(),
    )

    await tg_client.set_receive_updates(
        True
    )

    clients[account_id] = tg_client

    start_telethon_monitor(
        account_id,
        tg_client,
    )

    await migrate_legacy_selected_chats(
        owner_id
    )

    return account_id, me


async def close_login_client(user_id: int):
    tg_client = login_clients.pop(
        user_id,
        None,
    )

    if tg_client:
        try:
            await tg_client.disconnect()
        except Exception:
            pass


# ============================================================
# UNIVERSAL PRODUCT SEARCH
# ============================================================
# ============================================================
# UNIVERSAL PRODUCT SEARCH
# ============================================================

# Большой словарь нормализации для техники.
# Все варианты приводятся к одной канонической форме ДО fuzzy-поиска.
#
# Важно:
# - это не только цвета;
# - учитываются рус/англ варианты, транслит, разговорные названия;
# - цвета/состояние из запроса дополнительно проверяются как ограничения,
#   чтобы "iPhone 14 blue" не ловил "iPhone 14 red".

PHONETIC_ALIASES = {
    # Apple / iPhone
    "айфон": "iphone",
    "айфоны": "iphone",
    "айфона": "iphone",
    "айфоне": "iphone",
    "айфоном": "iphone",
    "ифон": "iphone",
    "эпл": "apple",
    "аппл": "apple",
    "макбук": "macbook",
    "макбуки": "macbook",
    "макбука": "macbook",
    "мак": "mac",
    "айпад": "ipad",
    "айпэд": "ipad",
    "эйрподс": "airpods",
    "аирподс": "airpods",
    "эпл вотч": "apple watch",
    "апл вотч": "apple watch",

    # Google
    "гугл": "google",
    "пиксель": "pixel",
    "пиксел": "pixel",
    "пиксельфон": "pixel",

    # Samsung
    "самсунг": "samsung",
    "самс": "samsung",
    "галакси": "galaxy",
    "гэлэкси": "galaxy",

    # Xiaomi / Redmi / Poco
    "сяоми": "xiaomi",
    "ксиаоми": "xiaomi",
    "редми": "redmi",
    "поко": "poco",

    # Huawei / Honor
    "хуавей": "huawei",
    "хуавэй": "huawei",
    "хонор": "honor",

    # Sony / consoles
    "сони": "sony",
    "плейстейшен": "playstation",
    "плейстейшн": "playstation",
    "плейстейция": "playstation",
    "плойка": "playstation",
    "пс5": "playstation 5",
    "пс 5": "playstation 5",
    "пс4": "playstation 4",
    "пс 4": "playstation 4",
    "ps5 pro": "playstation 5 pro",
    "ps 5 pro": "playstation 5 pro",
    "ps5": "playstation 5",
    "ps 5": "playstation 5",
    "ps4": "playstation 4",
    "ps 4": "playstation 4",

    # Microsoft / Xbox
    "майкрософт": "microsoft",
    "иксбокс": "xbox",
    "хбокс": "xbox",

    # Fitbit
    # Все частые варианты приводим к одному токену "fitbit".
    "fit bit": "fitbit",
    "fit-bit": "fitbit",
    "fitbit": "fitbit",
    "фитбит": "fitbit",
    "фит бит": "fitbit",
    "фит-бит": "fitbit",

    # Ray-Ban / Meta glasses
    # ВАЖНО: приводим к ОДНОМУ токену "rayban".
    # Иначе "ray ban meta" мог случайно совпасть с Apple Watch "... sport band ...",
    # потому что fuzzy видел ban ~ band.
    "ray-ban": "rayban",
    "ray ban": "rayban",
    "rayban": "rayban",
    "рейбан": "rayban",
    "рей бан": "rayban",
    "рэйбан": "rayban",
    "рэй бан": "rayban",

    # Other common brands
    "леново": "lenovo",
    "асус": "asus",
    "эйсус": "asus",
    "асер": "acer",
    "делл": "dell",
    "рейзер": "razer",
    "гармин": "garmin",
    "гопро": "gopro",
    "нинтендо": "nintendo",
}


# Синонимы категорий техники.
TECH_ALIASES = {
    # Phones
    "смартфон": "phone",
    "телефон": "phone",
    "мобильник": "phone",
    "mobile phone": "phone",
    "smartphone": "phone",

    # Laptops
    "ноут": "laptop",
    "ноутбук": "laptop",
    "ноутбуки": "laptop",
    "notebook": "laptop",

    # Tablets
    "планшет": "tablet",
    "таблет": "tablet",

    # Headphones
    "наушники": "headphones",
    "наушник": "headphones",
    "earphones": "headphones",
    "earbuds": "headphones",
    "buds": "headphones",

    # Watches
    "смарт часы": "smartwatch",
    "смарт-часы": "smartwatch",
    "умные часы": "smartwatch",
    "smart watch": "smartwatch",

    # Consoles
    "игровая приставка": "console",
    "приставка": "console",
    "консоль": "console",

    # PC parts
    "видеокарта": "gpu",
    "видюха": "gpu",
    "graphics card": "gpu",
    "процессор": "cpu",
    "проц": "cpu",
    "материнка": "motherboard",
    "материнская плата": "motherboard",
    "оперативка": "ram",
    "оперативная память": "ram",

    # Displays
    "монитор": "display",
    "экран": "display",
}


# Цвета. Каноническое значение справа.
# Маркетинговые оттенки сопоставлены с ближайшим базовым цветом.
COLOR_ALIASES = {
    # blue
    "синий": "blue",
    "синяя": "blue",
    "синее": "blue",
    "синие": "blue",
    "голубой": "blue",
    "голубая": "blue",
    "голубое": "blue",
    "голубые": "blue",
    "navy": "blue",
    "navy blue": "blue",
    "dark blue": "blue",
    "light blue": "blue",
    "sky blue": "blue",
    "azure": "blue",
    "ultramarine": "blue",

    # black
    "черный": "black",
    "чёрный": "black",
    "черная": "black",
    "чёрная": "black",
    "черное": "black",
    "чёрное": "black",
    "черные": "black",
    "чёрные": "black",
    "jet black": "black",
    "midnight": "black",
    "midnight black": "black",

    # white
    "белый": "white",
    "белая": "white",
    "белое": "white",
    "белые": "white",
    "snow": "white",
    "pearl white": "white",
    "ceramic white": "white",

    # gray / graphite
    "серый": "gray",
    "серая": "gray",
    "серое": "gray",
    "серые": "gray",
    "grey": "gray",
    "graphite": "gray",
    "графит": "gray",
    "графитовый": "gray",
    "space gray": "gray",
    "space grey": "gray",
    "spacegray": "gray",

    # silver
    "серебристый": "silver",
    "серебряный": "silver",
    "серебро": "silver",

    # gold
    "золотой": "gold",
    "золотистый": "gold",
    "золото": "gold",
    "champagne": "gold",
    "champagne gold": "gold",

    # rose / pink
    "розовый": "pink",
    "розовая": "pink",
    "розовое": "pink",
    "розовые": "pink",
    "rose": "pink",
    "rose gold": "pink",
    "розовое золото": "pink",

    # red
    "красный": "red",
    "красная": "red",
    "красное": "red",
    "красные": "red",
    "product red": "red",
    "(product) red": "red",
    "бордовый": "red",
    "burgundy": "red",

    # green
    "зеленый": "green",
    "зелёный": "green",
    "зеленая": "green",
    "зелёная": "green",
    "зеленое": "green",
    "зелёное": "green",
    "mint": "green",
    "mint green": "green",
    "мятный": "green",
    "olive": "green",
    "оливковый": "green",

    # purple
    "фиолетовый": "purple",
    "фиолетовая": "purple",
    "лиловый": "purple",
    "violet": "purple",
    "lavender": "purple",
    "лавандовый": "purple",
    "deep purple": "purple",

    # yellow
    "желтый": "yellow",
    "жёлтый": "yellow",
    "желтая": "yellow",
    "жёлтая": "yellow",
    "лимонный": "yellow",
    "lemon": "yellow",

    # orange
    "оранжевый": "orange",
    "оранжевая": "orange",
    "coral": "orange",
    "коралловый": "orange",

    # brown
    "коричневый": "brown",
    "коричневая": "brown",
    "chocolate": "brown",

    # beige / cream
    "бежевый": "beige",
    "беж": "beige",
    "кремовый": "beige",
    "cream": "beige",
    "starlight": "beige",

    # titanium shades
    "natural titanium": "titanium",
    "натуральный титан": "titanium",
    "титан": "titanium",
    "titanium": "titanium",
}


# Состояние товара.
CONDITION_ALIASES = {
    # new / sealed
    "новый": "condnew",
    "новая": "condnew",
    "новое": "condnew",
    "новые": "condnew",
    "new": "condnew",
    "brand new": "condnew",
    "абсолютно новый": "condnew",
    "запечатан": "condnew",
    "запечатанный": "condnew",
    "запечатанная": "condnew",
    "sealed": "condnew",
    "factory sealed": "condnew",
    "не вскрывался": "condnew",
    "не вскрыт": "condnew",
    "unopened": "condnew",

    # used
    "бу": "condused",
    "б/у": "condused",
    "б у": "condused",
    "used": "condused",
    "пользованный": "condused",
    "пользовался": "condused",

    # like new
    "как новый": "condlikenew",
    "как новая": "condlikenew",
    "like new": "condlikenew",
    "идеал": "condlikenew",
    "идеальное состояние": "condlikenew",
    "mint condition": "condlikenew",

    # refurbished
    "восстановленный": "condrefurb",
    "восстановлен": "condrefurb",
    "реф": "condrefurb",
    "refurb": "condrefurb",
    "refurbished": "condrefurb",
}


# Память/накопитель: унифицируем написание единиц.
MEMORY_ALIASES = {
    "гб": "gb",
    "гбайт": "gb",
    "гигабайт": "gb",
    "гигабайта": "gb",
    "гигабайтов": "gb",
    "гиг": "gb",
    "гига": "gb",
    "gbyte": "gb",
    "gigabyte": "gb",
    "gigabytes": "gb",

    "тб": "tb",
    "терабайт": "tb",
    "терабайта": "tb",
    "терабайтов": "tb",
    "тера": "tb",
    "terabyte": "tb",
    "terabytes": "tb",

    "мб": "mb",
    "мегабайт": "mb",
    "megabyte": "mb",
}


# SIM / connectivity.
CONNECTIVITY_ALIASES = {
    "е-сим": "esim",
    "е сим": "esim",
    "e-sim": "esim",
    "электронная сим": "esim",

    "дуал сим": "dualsim",
    "dual sim": "dualsim",
    "dual-sim": "dualsim",
    "2 sim": "dualsim",
    "2 сим": "dualsim",
    "две сим": "dualsim",

    "вайфай": "wifi",
    "wi-fi": "wifi",
    "wifi only": "wifionly",
    "только wifi": "wifionly",

    "cellular": "cellular",
    "lte": "cellular",
    "4g": "cellular",
}


BRAND_WORDS = {
    "apple",
    "google",
    "samsung",
    "sony",
    "microsoft",
    "xiaomi",
    "huawei",
    "honor",
    "lenovo",
    "asus",
    "acer",
    "dell",
    "hp",
    "lg",
    "meta",
    "rayban",
    "fitbit",
    "garmin",
    "razer",
    "nintendo",
    "oneplus",
    "nothing",
    "dji",
    "gopro",
    "valve",
    "nvidia",
    "amd",
    "intel",
}

# ============================================================
# STRICT PRODUCT IDENTITY
# ============================================================
#
# Fuzzy-поиск хорош для опечаток, но он не должен решать,
# КАКОЙ ЭТО БРЕНД/ЛИНЕЙКА.
#
# Поэтому перед fuzzy-score ставим "identity gate":
# если в запросе явно указан бренд, найденное сообщение обязано
# содержать этот бренд ИЛИ продукт, однозначно его подразумевающий.
#
# Примеры:
#   ray-ban meta -> Meta Quest                      ❌
#   ray-ban meta -> Apple Watch ... sport band    ❌
#   google pixel -> Pixel 9                       ✅
#   apple iphone -> iPhone 17                     ✅
#   sony playstation -> PlayStation 5             ✅

EXPLICIT_BRAND_TOKENS = {
    "apple": {"apple"},
    "google": {"google"},
    "samsung": {"samsung"},
    "sony": {"sony"},
    "microsoft": {"microsoft"},
    "xiaomi": {"xiaomi"},
    "huawei": {"huawei"},
    "honor": {"honor"},
    "lenovo": {"lenovo"},
    "asus": {"asus"},
    "acer": {"acer"},
    "dell": {"dell"},
    "hp": {"hp"},
    "lg": {"lg"},
    "meta": {"meta"},
    "rayban": {"rayban"},
    "fitbit": {"fitbit"},
    "garmin": {"garmin"},
    "razer": {"razer"},
    "nintendo": {"nintendo"},
    "oneplus": {"oneplus"},
    "nothing": {"nothing"},
    "dji": {"dji"},
    "gopro": {"gopro"},
    "valve": {"valve"},
    "nvidia": {"nvidia"},
    "amd": {"amd"},
    "intel": {"intel"},
}

# Токены, которые достаточно однозначно показывают бренд даже если
# само имя бренда продавец не написал.
BRAND_PRESENCE_TOKENS = {
    "apple": {
        "apple", "iphone", "ipad", "macbook", "airpods", "imac",
    },
    "google": {
        "google", "pixel", "fitbit", "nest",
    },
    "samsung": {
        "samsung", "galaxy",
    },
    "sony": {
        "sony", "playstation", "xperia",
    },
    "microsoft": {
        "microsoft", "xbox", "surface",
    },
    "xiaomi": {
        "xiaomi", "redmi", "poco",
    },
    "huawei": {
        "huawei", "matebook",
    },
    "honor": {
        "honor",
    },
    "lenovo": {
        "lenovo", "thinkpad", "legion",
    },
    "asus": {
        "asus", "rog", "zenbook", "vivobook",
    },
    "acer": {
        "acer", "predator",
    },
    "dell": {
        "dell", "alienware", "xps",
    },
    "hp": {
        "hp", "omen", "spectre", "elitebook",
    },
    "lg": {
        "lg",
    },
    "meta": {
        "meta", "quest", "oculus",
    },
    "rayban": {
        "rayban",
    },
    "fitbit": {
        "fitbit",
    },
    "garmin": {
        "garmin",
    },
    "razer": {
        "razer",
    },
    "nintendo": {
        "nintendo", "switch",
    },
    "oneplus": {
        "oneplus",
    },
    "nothing": {
        "nothing",
    },
    "dji": {
        "dji",
    },
    "gopro": {
        "gopro",
    },
    "valve": {
        "valve", "steamdeck",
    },
    "nvidia": {
        "nvidia", "geforce", "rtx", "gtx",
    },
    "amd": {
        "amd", "radeon", "ryzen",
    },
    "intel": {
        "intel",
    },
}

# Слова, которые сами по себе слишком общие и не должны становиться
# "якорем идентичности" товара.
GENERIC_PRODUCT_WORDS = {
    "phone",
    "laptop",
    "tablet",
    "headphones",
    "smartwatch",
    "console",
    "display",
    "gpu",
    "cpu",
    "ram",
    "motherboard",
    "watch",
    "glasses",
    "smart",
    "device",
    "model",
}

# Продуктовые слова, для которых мы хотим требовать совпадение,
# если они есть в запросе.
#
# Это не полный список моделей в мире: все НЕ-общие слова запроса
# автоматически тоже считаются якорями ниже.
KNOWN_PRODUCT_FAMILIES = {
    "iphone",
    "ipad",
    "macbook",
    "airpods",
    "pixel",
    "fitbit",
    "galaxy",
    "playstation",
    "xbox",
    "surface",
    "quest",
    "xperia",
    "redmi",
    "poco",
    "thinkpad",
    "legion",
    "zenbook",
    "vivobook",
    "predator",
    "alienware",
    "steamdeck",
}

# Название семейства можно опустить только там, где продавцы
# действительно регулярно пишут модель без самого семейства.
#
# ВАЖНО:
# PlayStation сюда НЕ входит. "5 Pro" слишком общее сочетание и
# раньше из-за него запрос PlayStation 5 Pro мог цеплять MacBook Pro M5.
OMITTABLE_IDENTITY_FAMILIES = {
    "iphone",
}

# Если в сообщении явно написано ДРУГОЕ семейство товара,
# даже iPhone-сокращение не разрешаем.
IDENTITY_FAMILY_CONFLICTS = (
    KNOWN_PRODUCT_FAMILIES
    | {
        "mac",
        "watch",
        "applewatch",
    }
)

MODEL_TIER_ALIASES = {
    "promax": "pro max",
    "pro-max": "pro max",
    "про макс": "pro max",
    "про-макс": "pro max",
    "макс": "max",
    "про": "pro",
    "ультра": "ultra",
    "плюс": "plus",
    "мини": "mini",
    "лайт": "lite",
    "эйр": "air",
    "аир": "air",
    "фолд": "fold",
    "флип": "flip",
}

MODEL_MODIFIERS = {
    "pro",
    "max",
    "mini",
    "air",
    "ultra",
    "plus",
    "lite",
    "classic",
    "standard",
    "edition",
    "series",
    "gen",
    "generation",
    "se",
    "fold",
    "flip",
    "edge",
}

# Если такая модификация явно указана в запросе, она обязана
# присутствовать в найденном сообщении.
#
# Например:
# iPhone 17 Pro Max -> обязательно и Pro, и Max.
# Samsung S25 Ultra -> обязательно Ultra.
#
# Air тоже строгий для большинства техники, но для Fitbit оставляем
# старое поведение проекта: "Google Fitbit Air" может поймать просто Fitbit.
STRICT_MODEL_MODIFIERS = {
    "pro",
    "max",
    "mini",
    "air",
    "ultra",
    "plus",
    "lite",
    "classic",
    "se",
    "fold",
    "flip",
    "edge",
}

OPTIONAL_MODIFIERS_BY_PRODUCT = {
    "fitbit": {"air"},
}

CANONICAL_CONNECTIVITY = {
    "esim",
    "dualsim",
    "wifi",
    "wifionly",
    "cellular",
}

CANONICAL_COLORS = {
    "blue",
    "black",
    "white",
    "gray",
    "silver",
    "gold",
    "pink",
    "red",
    "green",
    "purple",
    "yellow",
    "orange",
    "brown",
    "beige",
    "titanium",
}

CANONICAL_CONDITIONS = {
    "condnew",
    "condused",
    "condlikenew",
    "condrefurb",
}

ATTRIBUTE_WORDS = (
    CANONICAL_COLORS
    | CANONICAL_CONDITIONS
    | {
        "gb",
        "tb",
        "mb",
        "esim",
        "dualsim",
        "wifi",
        "wifionly",
        "cellular",
    }
)


def _replace_aliases(text: str, mapping: dict[str, str]) -> str:
    """
    Заменяем сначала длинные фразы, потом короткие.
    Границы построены через буквенно-цифровые символы,
    поэтому 'синий' не заменится внутри другого слова.
    """
    for source in sorted(
        mapping,
        key=len,
        reverse=True,
    ):
        target = mapping[source]

        pattern = (
            rf"(?<![\w])"
            rf"{re.escape(source)}"
            rf"(?![\w])"
        )

        text = re.sub(
            pattern,
            target,
            text,
            flags=re.IGNORECASE,
        )

    return text


def normalize_text(text: str) -> str:
    if not text:
        return ""

    text = text.casefold().replace(
        "ё",
        "е",
    )

    # Сначала фразы и смысловые синонимы.
    text = _replace_aliases(
        text,
        CONDITION_ALIASES,
    )
    text = _replace_aliases(
        text,
        COLOR_ALIASES,
    )
    text = _replace_aliases(
        text,
        MEMORY_ALIASES,
    )
    text = _replace_aliases(
        text,
        CONNECTIVITY_ALIASES,
    )
    text = _replace_aliases(
        text,
        TECH_ALIASES,
    )
    text = _replace_aliases(
        text,
        MODEL_TIER_ALIASES,
    )
    text = _replace_aliases(
        text,
        PHONETIC_ALIASES,
    )

    # Остаток кириллицы -> латиница.
    text = unidecode(
        text
    )

    # Samsung часто пишут S26+ вместо S26 Plus.
    # Сохраняем "+" как модификацию модели, а не выбрасываем.
    text = re.sub(
        r"(?<=\d)\s*\+",
        " plus ",
        text,
    )

    # iphone17 -> iphone 17
    # 256gb -> 256 gb
    text = re.sub(
        r"([a-z])(\d)",
        r"\1 \2",
        text,
    )
    text = re.sub(
        r"(\d)([a-z])",
        r"\1 \2",
        text,
    )

    text = re.sub(
        r"[^a-z0-9]+",
        " ",
        text,
    )

    return " ".join(
        text.split()
    )


def numeric_tokens(text: str) -> list[str]:
    return re.findall(
        r"\d+",
        normalize_text(text),
    )


def token_match_score(
    needle: str,
    hay_tokens: list[str],
) -> float:
    if not needle or not hay_tokens:
        return 0.0

    return max(
        fuzz.ratio(
            needle,
            token,
        )
        for token in hay_tokens
    )


def extract_color_constraints(
    normalized_text: str,
) -> set[str]:
    tokens = set(
        normalized_text.split()
    )

    return (
        tokens
        & CANONICAL_COLORS
    )


def extract_condition_constraints(
    normalized_text: str,
) -> set[str]:
    tokens = set(
        normalized_text.split()
    )

    return (
        tokens
        & CANONICAL_CONDITIONS
    )


def _fuzzy_token_present(
    expected: str,
    message_tokens: list[str],
) -> bool:
    """
    Для коротких названий брендов требуем точное совпадение.
    Для длинных разрешаем небольшую опечатку.
    """
    if not expected:
        return False

    if expected in message_tokens:
        return True

    if len(expected) <= 4:
        return False

    return any(
        fuzz.ratio(expected, token) >= 90
        for token in message_tokens
        if len(token) >= 4
    )


def extract_explicit_brand_constraints(
    normalized_query: str,
) -> set[str]:
    query_tokens = set(
        normalized_query.split()
    )

    required = set()

    for brand, explicit_tokens in EXPLICIT_BRAND_TOKENS.items():
        if query_tokens & explicit_tokens:
            required.add(brand)

    return required


def brand_is_present(
    brand: str,
    normalized_message: str,
) -> bool:
    message_tokens = normalized_message.split()

    allowed = BRAND_PRESENCE_TOKENS.get(
        brand,
        {brand},
    )

    for token in allowed:
        if _fuzzy_token_present(
            token,
            message_tokens,
        ):
            return True

    return False


def extract_identity_anchors(
    normalized_query: str,
) -> list[str]:
    """
    Возвращает смысловые якоря товара после удаления:
    - явных брендов
    - цветов/состояния/памяти/SIM
    - модификаторов Pro/Max/Ultra и т.д.
    - цифр модели (они проверяются отдельно)

    Для многословного товара каждый такой якорь должен реально
    присутствовать в сообщении (с небольшой терпимостью к опечаткам).
    """
    query_tokens = normalized_query.split()

    explicit_brand_words = set()

    for tokens in EXPLICIT_BRAND_TOKENS.values():
        explicit_brand_words |= tokens

    anchors = []

    for token in query_tokens:
        if token.isdigit():
            continue

        if token in explicit_brand_words:
            continue

        if token in ATTRIBUTE_WORDS:
            continue

        if token in MODEL_MODIFIERS:
            continue

        if token in GENERIC_PRODUCT_WORDS:
            continue

        if len(token) < 3:
            continue

        anchors.append(token)

    return anchors


def identity_anchor_present(
    anchor: str,
    normalized_message: str,
) -> bool:
    message_tokens = normalized_message.split()

    if anchor in message_tokens:
        return True

    # Для коротких слов fuzzy опасен: ban/band и подобные случаи.
    if len(anchor) <= 4:
        return False

    # Для длинных названий допускаем обычную опечатку.
    return any(
        fuzz.ratio(anchor, token) >= 86
        for token in message_tokens
        if len(token) >= 4
    )


def has_strong_structured_signature(
    normalized_query: str,
    normalized_message: str,
) -> bool:
    """
    Разрешает опустить название семейства товара только когда
    остальная сигнатура очень специфична.

    Пример:
      запрос:  iPhone 17 Pro Max eSim 512
      сообщение: 17 Pro Max 512 eSim
    Это всё ещё достаточно однозначно.

    Одного числа или одного слова для такого исключения недостаточно.
    """
    score = 0

    q_storage = extract_storage_constraints(
        normalized_query
    )
    m_storage = extract_storage_constraints(
        normalized_message
    )

    if q_storage and storage_equivalent(
        q_storage,
        m_storage,
    ):
        score += 1

    q_modifiers = extract_model_modifier_constraints(
        normalized_query
    )
    m_modifiers = extract_model_modifier_constraints(
        normalized_message
    )

    if q_modifiers and q_modifiers.issubset(
        m_modifiers
    ):
        score += 1

    q_connectivity = extract_connectivity_constraints(
        normalized_query
    )
    m_connectivity = extract_connectivity_constraints(
        normalized_message
    )

    if q_connectivity and q_connectivity.issubset(
        m_connectivity
    ):
        score += 1

    q_colors = extract_color_constraints(
        normalized_query
    )
    m_colors = extract_color_constraints(
        normalized_message
    )

    if q_colors and (q_colors & m_colors):
        score += 1

    storage_numbers = {
        str(value)
        for value, _ in q_storage
    }

    q_model_numbers = [
        n
        for n in numeric_tokens(
            normalized_query
        )
        if n not in storage_numbers
    ]

    m_numbers = set(
        numeric_tokens(
            normalized_message
        )
    )

    if (
        q_model_numbers
        and all(
            n in m_numbers
            for n in q_model_numbers
        )
    ):
        score += 1

    return score >= 2


def can_omit_identity_anchor(
    anchor: str,
    normalized_query: str,
    normalized_message: str,
) -> bool:
    """
    Безопасное исключение для сокращённых названий.

    Пример разрешён:
      iPhone 17 Pro Max 512 eSIM
      -> "17 Pro Max 512 eSIM"

    Пример ЗАПРЕЩЁН:
      PlayStation 5 Pro
      -> "MacBook Pro M5"

    Раньше здесь было слишком общее правило:
    число + Pro считались "сильной сигнатурой" для ЛЮБОГО товара.
    Именно из-за этого PlayStation мог превращаться в MacBook.
    """
    if anchor not in OMITTABLE_IDENTITY_FAMILIES:
        return False

    message_tokens = set(
        normalized_message.split()
    )

    conflicting = (
        IDENTITY_FAMILY_CONFLICTS
        - {anchor}
    )

    if message_tokens & conflicting:
        return False

    return has_strong_structured_signature(
        normalized_query,
        normalized_message,
    )


def passes_product_identity_gate(
    normalized_query: str,
    normalized_message: str,
) -> bool:
    """
    Строгий фильтр идентичности ДО fuzzy-score.
    """
    # 1. Все явно указанные бренды обязательны.
    required_brands = extract_explicit_brand_constraints(
        normalized_query
    )

    for brand in required_brands:
        if not brand_is_present(
            brand,
            normalized_message,
        ):
            return False

    # 2. Смысловые якоря товара тоже обязательны.
    #
    # Например:
    # Meta Quest -> Quest обязателен.
    # Поэтому Ray-Ban Meta не станет Meta Quest и наоборот.
    anchors = extract_identity_anchors(
        normalized_query
    )

    for anchor in anchors:
        if identity_anchor_present(
            anchor,
            normalized_message,
        ):
            continue

        # Иногда продавец пишет только:
        # "17 Pro Max 512 eSIM" без слова iPhone.
        # Разрешаем такое ТОЛЬКО для известного семейства товара
        # и только при сильной структурной сигнатуре.
        if can_omit_identity_anchor(
            anchor,
            normalized_query,
            normalized_message,
        ):
            continue

        return False

    return True


def extract_compact_model_codes(
    normalized_text: str,
) -> set[str]:
    """
    Распознаёт короткие буквенно-цифровые модели после normalize_text.

    Примеры:
      S26   -> "s 26" -> {"s26"}
      A56   -> "a 56" -> {"a56"}
      M4    -> "m 4"  -> {"m4"}

    Это не даёт S26 случайно совпасть с A26 или iPhone 26.
    """
    tokens = normalized_text.split()
    result = set()

    for index in range(
        len(tokens) - 1
    ):
        prefix = tokens[index]
        number = tokens[index + 1]

        if (
            len(prefix) == 1
            and prefix.isalpha()
            and number.isdigit()
            and 1 <= len(number) <= 4
        ):
            result.add(
                f"{prefix}{number}"
            )

    return result


def samsung_s_series_codes(
    normalized_text: str,
) -> set[str]:
    """
    Samsung Galaxy S-series: S24, S25, S26 и т.д.
    """
    return {
        code
        for code in extract_compact_model_codes(
            normalized_text
        )
        if re.fullmatch(
            r"s\d{1,3}",
            code,
        )
    }


STRICT_BASE_VARIANT_FAMILIES = {
    "iphone",
    "ipad",
    "macbook",
    "pixel",
    "galaxy",
    "playstation",
    "xbox",
    "surface",
    "quest",
    "xperia",
}


def enforce_numbered_family_variant(
    normalized_query: str,
    normalized_message: str,
) -> bool:
    """
    Если пользователь запросил базовую НУМЕРОВАННУЮ модель,
    версия Pro/Plus/Ultra/Max/etc считается другим товаром.

    Примеры:
      PlayStation 5       -> PlayStation 5 Pro   ❌
      iPhone 17           -> iPhone 17 Pro       ❌
      Pixel 9             -> Pixel 9 Pro         ❌
      PlayStation 5 Pro   -> PlayStation 5 Pro   ✅

    Для широкого запроса без номера ("macbook", "pixel") правило
    не включается.
    """
    q_tokens = set(
        normalized_query.split()
    )
    m_tokens = set(
        normalized_message.split()
    )

    families = (
        q_tokens
        & STRICT_BASE_VARIANT_FAMILIES
    )

    if not families:
        return True

    q_storage = extract_storage_constraints(
        normalized_query
    )
    storage_numbers = {
        str(value)
        for value, _ in q_storage
    }

    q_model_numbers = [
        number
        for number in numeric_tokens(
            normalized_query
        )
        if number not in storage_numbers
    ]

    if not q_model_numbers:
        return True

    q_modifiers = extract_model_modifier_constraints(
        normalized_query
    )
    m_modifiers = extract_model_modifier_constraints(
        normalized_message
    )

    # Если модификация есть в запросе, существующая строгая
    # проверка ниже потребует её в сообщении.
    if q_modifiers:
        return True

    # Базовая модель не должна ловить более специальную.
    return not bool(
        m_modifiers
        & STRICT_MODEL_MODIFIERS
    )


def enforce_exact_base_variant(
    normalized_query: str,
    normalized_message: str,
) -> bool:
    """
    Для Samsung S-series базовая модель считается отдельной версией.

    Запрос:
      S26
      S26 Black

    НЕ должен ловить:
      S26 Ultra
      S26 Plus
      S26+
      S26 Edge

    Но:
      S26 Ultra -> S26 Ultra ✅
      S26 Plus  -> S26 Plus  ✅
    """
    q_codes = samsung_s_series_codes(
        normalized_query
    )

    if not q_codes:
        return True

    m_codes = samsung_s_series_codes(
        normalized_message
    )

    # Должна быть именно та же S-модель.
    if not q_codes.issubset(
        m_codes
    ):
        return False

    q_modifiers = extract_model_modifier_constraints(
        normalized_query
    )
    m_modifiers = extract_model_modifier_constraints(
        normalized_message
    )

    # Если версия явно указана в запросе — она обязательна.
    if q_modifiers:
        return q_modifiers.issubset(
            m_modifiers
        )

    # Если запрос на ОБЫЧНЫЙ S26, то Ultra/Plus/Edge и т.п.
    # уже другой товар и не подходит.
    extra_variant_modifiers = (
        m_modifiers
        & {
            "ultra",
            "plus",
            "edge",
            "pro",
            "max",
            "mini",
            "lite",
        }
    )

    return not extra_variant_modifiers


def extract_model_modifier_constraints(
    normalized_text: str,
) -> set[str]:
    tokens = set(
        normalized_text.split()
    )

    required = (
        tokens
        & STRICT_MODEL_MODIFIERS
    )

    # Сохраняем наше старое специальное поведение:
    # Google Fitbit Air -> Fitbit тоже допустимо.
    for product, optional_modifiers in OPTIONAL_MODIFIERS_BY_PRODUCT.items():
        if product in tokens:
            required -= optional_modifiers

    return required


def extract_connectivity_constraints(
    normalized_text: str,
) -> set[str]:
    tokens = set(
        normalized_text.split()
    )

    return (
        tokens
        & CANONICAL_CONNECTIVITY
    )


def extract_storage_constraints(
    normalized_text: str,
) -> set[tuple[int, str]]:
    """
    Из:
      256 gb
      1 tb
    делаем:
      {(256, "gb")}
      {(1, "tb")}
    """
    matches = re.findall(
        r"\b(\d{1,4})\s*(gb|tb|mb)\b",
        normalized_text,
    )

    return {
        (
            int(value),
            unit,
        )
        for value, unit in matches
    }


def storage_equivalent(
    query_storage: set[tuple[int, str]],
    message_storage: set[tuple[int, str]],
) -> bool:
    if not query_storage:
        return True

    if not message_storage:
        return False

    def to_mb(
        value: int,
        unit: str,
    ) -> int:
        if unit == "tb":
            return value * 1024 * 1024

        if unit == "gb":
            return value * 1024

        return value

    query_mb = {
        to_mb(value, unit)
        for value, unit in query_storage
    }

    message_mb = {
        to_mb(value, unit)
        for value, unit in message_storage
    }

    return bool(
        query_mb
        & message_mb
    )


def product_match_score(
    query: str,
    message: str,
) -> float:
    q = normalize_text(
        query
    )
    m = normalize_text(
        message
    )

    if not q or not m:
        return 0.0

    q_tokens = q.split()
    m_tokens = m.split()

    # --------------------------------------------------------
    # STRICT PRODUCT IDENTITY GATE
    # --------------------------------------------------------
    #
    # Сначала убеждаемся, что это вообще тот бренд/товар.
    # Только после этого запускаем fuzzy-score.
    if not passes_product_identity_gate(
        q,
        m,
    ):
        return 0.0

    # --------------------------------------------------------
    # ОБЯЗАТЕЛЬНЫЕ ХАРАКТЕРИСТИКИ ИЗ ЗАПРОСА
    # --------------------------------------------------------

    # Базовая нумерованная модель != Pro/Plus/Ultra/Max.
    if not enforce_numbered_family_variant(
        q,
        m,
    ):
        return 0.0

    # Samsung S-series:
    # S26 = только обычный S26.
    # S26 Ultra / Plus / Edge — отдельные версии.
    if not enforce_exact_base_variant(
        q,
        m,
    ):
        return 0.0

    # Если запрос содержит короткий код модели вроде S26/A56/M4,
    # в сообщении должен быть тот же код.
    q_model_codes = extract_compact_model_codes(
        q
    )

    if q_model_codes:
        m_model_codes = extract_compact_model_codes(
            m
        )

        if not q_model_codes.issubset(
            m_model_codes
        ):
            return 0.0

    # Цвет:
    # "iphone 14 blue" НЕ должен ловить "iphone 14 red".
    q_colors = extract_color_constraints(
        q
    )

    if q_colors:
        m_colors = extract_color_constraints(
            m
        )

        if not (
            q_colors
            & m_colors
        ):
            return 0.0

    # Состояние:
    # запрос "new" не должен ловить явное "used".
    q_conditions = extract_condition_constraints(
        q
    )

    if q_conditions:
        m_conditions = extract_condition_constraints(
            m
        )

        if not (
            q_conditions
            & m_conditions
        ):
            return 0.0

    # Версия / модификация модели.
    #
    # Это исправляет важный кейс:
    # "iPhone 17 Pro Max" больше НЕ ловит обычный "iPhone 17".
    q_modifiers = extract_model_modifier_constraints(
        q
    )

    if q_modifiers:
        m_modifiers = extract_model_modifier_constraints(
            m
        )

        if not q_modifiers.issubset(
            m_modifiers
        ):
            return 0.0

    # Тип связи / SIM.
    #
    # Если в запросе явно eSIM, обычный SIM без eSIM больше не подходит.
    q_connectivity = extract_connectivity_constraints(
        q
    )

    if q_connectivity:
        m_connectivity = extract_connectivity_constraints(
            m
        )

        if not q_connectivity.issubset(
            m_connectivity
        ):
            return 0.0

    # Накопитель / память.
    q_storage = extract_storage_constraints(
        q
    )

    if q_storage:
        m_storage = extract_storage_constraints(
            m
        )

        if not storage_equivalent(
            q_storage,
            m_storage,
        ):
            return 0.0

    # --------------------------------------------------------
    # ЦИФРЫ МОДЕЛИ
    # --------------------------------------------------------

    # Цифры, относящиеся к памяти, не считаем "моделью",
    # потому что память уже проверили выше.
    storage_numbers = {
        str(value)
        for value, _ in q_storage
    }

    q_numbers = [
        number
        for number in numeric_tokens(q)
        if number not in storage_numbers
    ]

    m_numbers = numeric_tokens(
        m
    )

    # Каждая цифра модели из запроса обязательна.
    # iPhone 17 -> не ловит iPhone 16.
    for number in q_numbers:
        if number not in m_numbers:
            return 0.0

    # После всех строгих проверок точное включение — максимум.
    if q in m:
        return 100.0

    full_partial = fuzz.partial_ratio(
        q,
        m,
    )
    full_token = fuzz.token_set_ratio(
        q,
        m,
    )

    # Если строгий identity gate уже доказал, что это нужный
    # бренд/линейка/семейство, не даём обычному fuzzy-score
    # случайно занизить правильное совпадение.
    identity_floor = 0.0

    if (
        extract_explicit_brand_constraints(q)
        or extract_identity_anchors(q)
        or has_strong_structured_signature(q, m)
    ):
        identity_floor = 90.0

    words = [
        token
        for token in q_tokens
        if not token.isdigit()
    ]

    # Главное название товара.
    # Цвет/память/состояние/модификация/eSIM уже проверены как атрибуты,
    # поэтому не даём им искусственно повышать fuzzy-score.
    core = [
        word
        for word in words
        if word not in BRAND_WORDS
        and word not in MODEL_MODIFIERS
        and word not in ATTRIBUTE_WORDS
        and len(word) >= 3
    ]

    if not core:
        core = [
            word
            for word in words
            if word not in ATTRIBUTE_WORDS
            and len(word) >= 3
        ]

    if not core:
        return max(
            identity_floor,
            full_partial,
            full_token,
        )

    core_scores = [
        token_match_score(
            word,
            m_tokens,
        )
        for word in core
    ]

    best_core = max(
        core_scores
    )
    average_core = (
        sum(core_scores)
        / len(core_scores)
    )

    # Если ядро состоит из одного товара/слова — одного сильного
    # совпадения достаточно.
    #
    # Составные бренды вроде Ray-Ban заранее склеиваются в rayban,
    # чтобы кусок "ban" не совпадал с обычным словом "band".
    # Например Google Fitbit Air -> Fitbit.
    if len(core) == 1:
        if best_core >= 90:
            return max(
                identity_floor,
                90.0,
                full_partial,
                full_token,
            )

        if best_core >= 76:
            return max(
                identity_floor,
                best_core,
                full_partial,
                full_token,
            )

        return max(
            identity_floor,
            full_partial,
            full_token,
        )

    # Для многословного ядра ОДНО совпавшее слово больше не достаточно.
    # Это исправляет:
    # "ray-ban meta" -> Meta Quest  ❌
    #
    # Требуем как минимум два действительно похожих токена.
    strong = sum(
        score >= 78
        for score in core_scores
    )

    if strong >= 2:
        return max(
            identity_floor,
            average_core,
            full_partial,
            full_token,
        )

    # Даже высокий token_set_ratio не должен протащить запрос,
    # если совпало только одно общее слово.
    if identity_floor >= 76:
        return identity_floor

    return min(
        full_partial,
        full_token,
        70.0,
    )



def split_message_into_match_segments(
    message_text: str,
) -> list[str]:
    """
    Разбивает длинный прайс/каталог на отдельные позиции.

    Например:
      S26 black
      S26 blue
      S26 violet
      S26 gold

    будут проверяться как четыре отдельные позиции.

    Также понимаем:
      S26 black / S26 blue
      S26 black | S26 blue
      S26 black ; S26 blue
    """
    text = (
        message_text
        or ""
    ).strip()

    if not text:
        return []

    # Сначала строки.
    raw_lines = re.split(
        r"[\r\n]+",
        text,
    )

    parts = []

    for raw_line in raw_lines:
        line = raw_line.strip(
            " \t•▪▫◦·-–—"
        )

        if not line:
            continue

        # Разделители товарных позиций внутри одной строки.
        subparts = re.split(
            r"\s+(?:\||;|/)\s+",
            line,
        )

        for part in subparts:
            part = part.strip(
                " \t•▪▫◦·-–—"
            )

            if part:
                parts.append(part)

    if not parts:
        return [text]

    # Убираем точные повторы, сохраняя порядок.
    unique = []
    seen = set()

    for part in parts:
        key = part.casefold()

        if key in seen:
            continue

        seen.add(key)
        unique.append(part)

    return unique[:150]


def best_query_segment(
    query: str,
    message_text: str,
) -> tuple[float, str]:
    """
    Ищет товар внутри длинного прайса/сообщения.

    ВАЖНО:
    продавцы часто разбивают ОДНУ позицию на несколько строк:

        iPhone 17 Pro Max
        512GB
        Blue
        eSIM

    Поэтому проверяем окна от 1 до 4 соседних строк.

    При этом не возвращаем всю простыню: выбираем самое короткое
    окно, которое реально проходит строгий matcher.
    """
    parts = split_message_into_match_segments(
        message_text
    )

    if not parts:
        return 0.0, ""

    candidates: list[tuple[int, str]] = []

    # 1..4 соседние строки.
    max_window = min(
        4,
        len(parts),
    )

    for window_size in range(
        1,
        max_window + 1,
    ):
        for index in range(
            0,
            len(parts) - window_size + 1,
        ):
            block_parts = parts[
                index:index + window_size
            ]

            candidate = "\n".join(
                block_parts
            ).strip()

            if not candidate:
                continue

            # Не раздуваем один блок слишком сильно.
            if len(candidate) > 700:
                continue

            candidates.append(
                (
                    window_size,
                    candidate,
                )
            )

    best_score = 0.0
    best_segment = ""
    best_window_size = 999

    for window_size, candidate in candidates:
        score = product_match_score(
            query,
            candidate,
        )

        # Берём более высокий score.
        # При одинаковом score предпочитаем МЕНЬШЕ строк,
        # чтобы не присылать лишние соседние товары.
        if (
            score > best_score
            or (
                score == best_score
                and score >= MATCH_THRESHOLD
                and window_size < best_window_size
            )
        ):
            best_score = score
            best_segment = candidate
            best_window_size = window_size

    # Fallback: если сообщение короткое и каким-то образом
    # line-split не дал хорошего результата, проверяем его целиком.
    # Для длинных каталогов full-message не используем, чтобы
    # не склеивать характеристики разных товаров.
    if len(parts) <= 4:
        full_score = product_match_score(
            query,
            message_text,
        )

        if full_score > best_score:
            best_score = full_score
            best_segment = message_text.strip()

    return (
        best_score,
        best_segment,
    )


# ============================================================
# DIALOGS
# ============================================================

async def get_dialogs_for_account(account_id: int):
    tg_client = clients.get(
        account_id
    )

    if tg_client is None:
        return []

    try:
        if not await tg_client.is_user_authorized():
            return []
    except Exception:
        return []

    result = []

    async for dialog in tg_client.iter_dialogs(
        limit=500
    ):
        if not (
            dialog.is_group
            or dialog.is_channel
        ):
            continue

        result.append({
            "id": int(dialog.id),
            "name": dialog.name or "Без названия",
            "account_id": account_id,
        })

    result.sort(
        key=lambda item: item["name"].casefold()
    )

    return result


async def get_all_dialogs(owner_id: int):
    """
    Один общий список чатов от обоих Telegram-аккаунтов.
    В интерфейсе помечаем их ① и ②.
    """
    account_rows = await get_accounts(
        owner_id
    )

    result = []

    for index, row in enumerate(
        account_rows,
        start=1,
    ):
        account_id = int(row["id"])

        if account_id not in clients:
            continue

        dialogs = await get_dialogs_for_account(
            account_id
        )

        username = row["username"]
        first_name = row["first_name"]

        account_name = (
            f"@{username}"
            if username
            else (first_name or row["label"])
        )

        for dialog in dialogs:
            dialog["account_index"] = index
            dialog["account_name"] = account_name
            result.append(dialog)

    result.sort(
        key=lambda item: (
            item["account_index"],
            item["name"].casefold(),
        )
    )

    return result


# ============================================================
# ANALYTICS
# ============================================================
# ============================================================
# ANALYTICS
# ============================================================

BRAND_RULES = [
    ("Fitbit", ["fitbit"]),
    ("Apple", ["iphone", "ipad", "macbook", "airpods", "imac", "apple watch", "apple"]),
    ("Samsung", ["samsung", "galaxy"]),
    ("Google", ["google", "pixel", "pixelbook", "nest"]),
    ("Sony", ["sony", "playstation", "xperia"]),
    ("Microsoft", ["microsoft", "xbox", "surface"]),
    ("Xiaomi", ["xiaomi", "redmi", "poco"]),
    ("Huawei", ["huawei", "matebook"]),
    ("Honor", ["honor"]),
    ("OnePlus", ["oneplus"]),
    ("Nothing", ["nothing"]),
    ("Lenovo", ["lenovo", "thinkpad", "legion"]),
    ("ASUS", ["asus", "rog", "zenbook", "vivobook"]),
    ("Acer", ["acer", "predator"]),
    ("Dell", ["dell", "alienware", "xps"]),
    ("HP", ["hp", "omen", "spectre", "elitebook"]),
    ("MSI", ["msi"]),
    ("Razer", ["razer"]),
    ("NVIDIA", ["nvidia", "geforce", "rtx", "gtx"]),
    ("AMD", ["amd", "radeon", "ryzen"]),
    ("Intel", ["intel", "core ultra"]),
    ("Garmin", ["garmin"]),
    ("GoPro", ["gopro"]),
    ("DJI", ["dji"]),
    ("Meta", ["meta quest", "oculus"]),
    ("Valve", ["steam deck", "valve"]),
    ("Nintendo", ["nintendo", "switch"]),
]


def detect_brand(found_request: str, full_message: str = "") -> str | None:
    """
    Определяет бренд по реальному запросу из сообщения.
    Приоритет у product-brand: Fitbit -> Fitbit,
    Pixel -> Google, iPhone -> Apple и т.д.
    """
    normalized = normalize_text(
        f"{found_request} {full_message}"
    )

    padded = f" {normalized} "

    for brand, aliases in BRAND_RULES:
        for alias in aliases:
            alias_norm = normalize_text(alias)

            if f" {alias_norm} " in padded:
                return brand

    return None


# ============================================================
# ALL-REQUEST ANALYTICS
# ============================================================

# Аналитика спроса: считаем сообщения, где человек реально
# ищет/покупает/запрашивает товар.
#
# Продажи ("продам", "в наличии", WTS) сюда намеренно НЕ входят.
REQUEST_INTENT_PATTERNS = [
    r"\bищу\b",
    r"\bищем\b",
    r"\bкуплю\b",
    r"\bкупим\b",
    r"\bнужен\b",
    r"\bнужна\b",
    r"\bнужно\b",
    r"\bнужны\b",
    r"\bинтересует\b",
    r"\bинтересуют\b",
    r"\bвозьму\b",
    r"\bвозьмем\b",
    r"\bвозьмём\b",
    r"\bтребуется\b",
    r"\bтребуются\b",
    r"\bпредложите\b",
    r"\bпредлагайте\b",
    r"\bкто\s+продаст\b",
    r"\bесть\s+у\s+кого\b",
    r"\bwtb\b",
    r"\bwanted\b",
    r"\blooking\s+for\b",
    r"\bneed\b",
    r"\bwant\s+to\s+buy\b",
    r"\bbuying\b",
]

REQUEST_INTENT_RE = re.compile(
    "(?i)("
    + "|".join(
        f"(?:{pattern})"
        for pattern in REQUEST_INTENT_PATTERNS
    )
    + r")[\s:,\-–—]*"
)


def extract_all_request_for_analytics(
    message_text: str,
) -> str | None:
    """
    Возвращает реальный покупательский запрос из ЛЮБОГО сообщения
    выбранного чата, независимо от monitor_queries_v2.

    Примеры:
      "Ищу iPhone 17 Pro 256" -> "iPhone 17 Pro 256"
      "Куплю S26 black" -> "S26 black"
      "WTB Fit Bit" -> "Fit Bit"
      "Продам MacBook Pro" -> None
    """
    text = re.sub(
        r"\s+",
        " ",
        (message_text or "").strip(),
    )

    if not text:
        return None

    match = REQUEST_INTENT_RE.search(
        text
    )

    if not match:
        return None

    candidate = text[
        match.end():
    ].strip()

    # Некоторые конструкции удобнее взять целиком после фразы:
    # "есть у кого iPhone 17?"
    if not candidate:
        return None

    # Срезаем типичные хвосты, не относящиеся к названию товара.
    stop_patterns = [
        r"(?i)\s*[,.!?;|]\s*(?:бюджет|цена|город|доставка|срочно|контакт|телефон)\b",
        r"(?i)\s+\b(?:бюджет|цена|город|доставка)\b\s*[:\-]?",
        r"(?i)\s+\b(?:пишите|пишите\s+в\s+лс|в\s+лс|лс)\b",
        r"(?i)\s+\b(?:до|за)\s+\d[\d\s.,]*\s*(?:€|\$|₽|eur|usd|rub)?\b",
    ]

    cut = len(
        candidate
    )

    for pattern in stop_patterns:
        stop = re.search(
            pattern,
            candidate,
        )

        if stop:
            cut = min(
                cut,
                stop.start(),
            )

    candidate = candidate[
        :cut
    ].strip(
        " \t\r\n,.;:!?-–—"
    )

    if not candidate:
        return None

    # Не превращаем огромный текст в "название запроса".
    if len(candidate) > 180:
        candidate = candidate[:180].rstrip()

    return candidate


async def allow_all_request_analytics_event(
    owner_id: int,
    seller_key: str | None,
    found_request: str,
) -> bool:
    """
    Кросс-пост одного и того же запроса одним человеком в несколько
    чатов за 120 секунд считается одним запросом аналитики.

    Если seller_key нет, полагаемся на PK chat_id/message_id.
    """
    if not seller_key:
        return True

    request_key = normalize_text(
        found_request
    )[:220]

    if not request_key:
        return True

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO recent_all_request_analytics_dedup (
                owner_id,
                seller_key,
                request_key,
                first_seen_at
            )
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (
                owner_id,
                seller_key,
                request_key
            )
            DO UPDATE
            SET first_seen_at=EXCLUDED.first_seen_at
            WHERE recent_all_request_analytics_dedup.first_seen_at
                  < NOW() - ($4 * INTERVAL '1 second')
            RETURNING first_seen_at
            """,
            owner_id,
            seller_key,
            request_key,
            DEDUP_WINDOW_SECONDS,
        )

    return row is not None


async def log_all_request_analytics_event(
    owner_id: int,
    chat_id: int,
    message_id: int,
    chat_title: str,
    found_request: str,
    brand: str | None,
    seller_key: str | None,
):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO all_request_analytics_events (
                owner_id,
                chat_id,
                message_id,
                chat_title,
                found_request,
                brand,
                seller_key
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (
                owner_id,
                chat_id,
                message_id
            )
            DO NOTHING
            """,
            owner_id,
            chat_id,
            message_id,
            chat_title,
            found_request,
            brand,
            seller_key,
        )


def analytics_request_key(value: str) -> str:
    """
    Нормализованный ключ, чтобы:
    Google Pixel 9 Pro
    google pixel 9 pro
    считались одним запросом.
    """
    return normalize_text(value)[:160]


async def log_analytics_event(
    owner_id: int,
    chat_id: int,
    message_id: int,
    chat_title: str,
    monitor_query: str,
    found_request: str,
    brand: str | None,
):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO analytics_events (
                owner_id,
                chat_id,
                message_id,
                chat_title,
                monitor_query,
                found_request,
                brand
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (owner_id, chat_id, message_id)
            DO NOTHING
            """,
            owner_id,
            chat_id,
            message_id,
            chat_title,
            monitor_query,
            found_request,
            brand,
        )


def ru_requests(count: int) -> str:
    n = abs(count) % 100
    n1 = n % 10

    if 11 <= n <= 19:
        word = "запросов"
    elif n1 == 1:
        word = "запрос"
    elif 2 <= n1 <= 4:
        word = "запроса"
    else:
        word = "запросов"

    return f"{count} {word}"


def activity_bar(value: int, maximum: int, width: int = 16) -> str:
    if value <= 0 or maximum <= 0:
        return "·"

    length = max(
        1,
        round((value / maximum) * width),
    )

    return "█" * length


def period_start(period: str) -> datetime:
    now_local = datetime.now(ANALYTICS_TZ)

    if period == "today":
        start_local = now_local.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    elif period == "30":
        start_local = now_local - timedelta(days=30)

    else:
        start_local = now_local - timedelta(days=7)

    return start_local.astimezone(timezone.utc)


async def fetch_analytics_rows(owner_id: int, period: str):
    start = period_start(period)

    async with db_pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT
                found_request,
                brand,
                created_at
            FROM all_request_analytics_events
            WHERE owner_id=$1
              AND created_at >= $2
            ORDER BY created_at ASC
            """,
            owner_id,
            start,
        )


def format_period_name(period: str) -> str:
    if period == "today":
        return "СЕГОДНЯ"

    if period == "30":
        return "30 ДНЕЙ"

    return "7 ДНЕЙ"


def analytics_keyboard(period: str = "7"):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Сегодня",
                    callback_data="analytics:today",
                ),
                InlineKeyboardButton(
                    text="7 дней",
                    callback_data="analytics:7",
                ),
                InlineKeyboardButton(
                    text="30 дней",
                    callback_data="analytics:30",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📈 По неделям",
                    callback_data="analytics:weeks",
                )
            ],
            [
                InlineKeyboardButton(
                    text="◀️ Главное меню",
                    callback_data="home",
                )
            ],
        ]
    )


async def analytics_text(owner_id: int, period: str) -> str:
    rows = await fetch_analytics_rows(
        owner_id,
        period,
    )

    total = len(rows)

    if total == 0:
        return (
            f"📊 ВСЕ ЗАПРОСЫ — {format_period_name(period)}\n\n"
            "Пока нет данных.\n\n"
            "Статистика начнёт накапливаться со всех новых запросов в выбранных чатах."
        )

    # ------------------------
    # Время
    # ------------------------

    hourly = [0] * 24

    for row in rows:
        local_dt = row["created_at"].astimezone(
            ANALYTICS_TZ
        )
        hourly[local_dt.hour] += 1

    buckets = [
        ("00–06", sum(hourly[0:6])),
        ("06–12", sum(hourly[6:12])),
        ("12–18", sum(hourly[12:18])),
        ("18–24", sum(hourly[18:24])),
    ]

    max_bucket = max(
        value
        for _, value in buckets
    )

    peak_hour = max(
        range(24),
        key=lambda hour: hourly[hour],
    )

    peak_count = hourly[peak_hour]

    activity_lines = []

    for label, value in buckets:
        activity_lines.append(
            f"{label}  "
            f"{activity_bar(value, max_bucket)}  "
            f"{ru_requests(value)}"
        )

    # ------------------------
    # Топ реальных запросов
    # ------------------------

    request_counts = Counter()
    request_display = {}

    for row in rows:
        found = (row["found_request"] or "").strip()

        if not found:
            continue

        key = analytics_request_key(found)

        if not key:
            continue

        request_counts[key] += 1

        # Оставляем наиболее свежую исходную форму.
        request_display[key] = found

    top_requests = request_counts.most_common(5)

    if top_requests:
        request_lines = [
            f"{index}. {request_display[key]} — {count}"
            for index, (key, count)
            in enumerate(top_requests, start=1)
        ]
    else:
        request_lines = ["—"]

    # ------------------------
    # Топ брендов
    # ------------------------

    brand_counts = Counter(
        row["brand"]
        for row in rows
        if row["brand"]
    )

    top_brands = brand_counts.most_common(5)

    if top_brands:
        brand_lines = [
            f"{index}. {brand} — {count}"
            for index, (brand, count)
            in enumerate(top_brands, start=1)
        ]
    else:
        brand_lines = ["—"]

    # ------------------------
    # Неделя к неделе
    # ------------------------

    now_local = datetime.now(
        ANALYTICS_TZ
    )

    current_week_start_local = (
        now_local
        - timedelta(days=now_local.weekday())
    ).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )

    previous_week_start_local = (
        current_week_start_local
        - timedelta(days=7)
    )

    current_start = current_week_start_local.astimezone(
        timezone.utc
    )

    previous_start = previous_week_start_local.astimezone(
        timezone.utc
    )

    async with db_pool.acquire() as conn:
        week_rows = await conn.fetch(
            """
            SELECT created_at
            FROM all_request_analytics_events
            WHERE owner_id=$1
              AND created_at >= $2
            """,
            owner_id,
            previous_start,
        )

    current_week = 0
    previous_week = 0

    for row in week_rows:
        created = row["created_at"]

        if created >= current_start:
            current_week += 1
        else:
            previous_week += 1

    if previous_week > 0:
        change = (
            (current_week - previous_week)
            / previous_week
            * 100
        )

        arrow = "↑" if change >= 0 else "↓"
        change_text = f"{arrow} {abs(change):.0f}%"

    elif current_week > 0:
        change_text = "↑ новый рост"

    else:
        change_text = "—"

    return (
        f"📊 ВСЕ ЗАПРОСЫ — {format_period_name(period)}\n\n"

        f"🔥 Всего: {ru_requests(total)}\n"
        "📡 Считаются все запросы из выбранных чатов\n\n"

        "⏰ АКТИВНОСТЬ ПО ВРЕМЕНИ\n"
        + "\n".join(activity_lines)
        + "\n\n"

        f"🔥 Пик: "
        f"{peak_hour:02d}:00–{(peak_hour + 1) % 24:02d}:00"
        f" — {ru_requests(peak_count)}\n\n"

        "🔥 ТОП ЗАПРОСОВ\n"
        + "\n".join(request_lines)
        + "\n\n"

        "🏷 ТОП БРЕНДОВ\n"
        + "\n".join(brand_lines)
        + "\n\n"

        "📈 НЕДЕЛЯ К НЕДЕЛЕ\n"
        f"Эта неделя — {current_week}\n"
        f"Прошлая — {previous_week}\n"
        f"Изменение — {change_text}\n\n"

        f"🕒 Часовой пояс: {ANALYTICS_TZ_NAME}"
    )


RU_MONTHS = [
    "",
    "янв",
    "фев",
    "мар",
    "апр",
    "май",
    "июн",
    "июл",
    "авг",
    "сен",
    "окт",
    "ноя",
    "дек",
]


def week_label(start_local: datetime, end_local: datetime) -> str:
    if start_local.month == end_local.month:
        return (
            f"{start_local.day}–{end_local.day} "
            f"{RU_MONTHS[start_local.month]}"
        )

    return (
        f"{start_local.day} {RU_MONTHS[start_local.month]}"
        f"–{end_local.day} {RU_MONTHS[end_local.month]}"
    )


async def weekly_analytics_text(owner_id: int) -> str:
    now_local = datetime.now(
        ANALYTICS_TZ
    )

    current_week_start = (
        now_local
        - timedelta(days=now_local.weekday())
    ).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )

    first_week_start = (
        current_week_start
        - timedelta(weeks=5)
    )

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT created_at
            FROM all_request_analytics_events
            WHERE owner_id=$1
              AND created_at >= $2
            ORDER BY created_at ASC
            """,
            owner_id,
            first_week_start.astimezone(timezone.utc),
        )

    week_counts = [0] * 6

    for row in rows:
        local_dt = row["created_at"].astimezone(
            ANALYTICS_TZ
        )

        delta_days = (
            local_dt.date()
            - first_week_start.date()
        ).days

        index = delta_days // 7

        if 0 <= index < 6:
            week_counts[index] += 1

    lines = []

    previous = None

    for index, count in enumerate(week_counts):
        start = (
            first_week_start
            + timedelta(weeks=index)
        )

        end = min(
            start + timedelta(days=6),
            now_local,
        )

        label = week_label(
            start,
            end,
        )

        if previous is None:
            suffix = ""

        elif previous > 0:
            change = (
                (count - previous)
                / previous
                * 100
            )

            arrow = "↑" if change >= 0 else "↓"

            suffix = (
                f"  {arrow} {abs(change):.0f}%"
            )

        elif count > 0:
            suffix = "  ↑ новый рост"

        else:
            suffix = ""

        lines.append(
            f"{label} — {ru_requests(count)}{suffix}"
        )

        previous = count

    return (
        "📈 ВСЕ ЗАПРОСЫ ПО НЕДЕЛЯМ\n\n"
        + "\n".join(lines)
        + "\n\n"
        f"🕒 Часовой пояс: {ANALYTICS_TZ_NAME}"
    )


# ============================================================
# SELLER + REPLY BUTTON
# ============================================================

async def seller_info(event):
    """
    Возвращает:
    display_name, username_or_none, seller_key_or_none

    seller_key используется только для антидубля.
    Для обычного Telegram User это стабильный user ID.
    Для канала/анонимного админа антидубль по продавцу не применяем.
    """
    try:
        sender = await event.get_sender()

        if sender is None:
            return "Неизвестно", None, None

        # Кнопку "Ответить" и антидубль делаем только для обычного User.
        if isinstance(sender, User):
            username = getattr(
                sender,
                "username",
                None,
            )

            seller_key = f"user:{int(sender.id)}"

            if username:
                return (
                    f"@{username}",
                    username,
                    seller_key,
                )

            name = " ".join(
                part
                for part in [
                    getattr(sender, "first_name", None),
                    getattr(sender, "last_name", None),
                ]
                if part
            ).strip()

            return (
                name or "Пользователь",
                None,
                seller_key,
            )

        # Канал / анонимный админ.
        title = getattr(
            sender,
            "title",
            None,
        )
        username = getattr(
            sender,
            "username",
            None,
        )

        if title:
            return title, None, None

        if username:
            return f"@{username}", None, None

        return "Неизвестно", None, None

    except Exception:
        return "Неизвестно", None, None


def request_dedup_key(found_request: str) -> str:
    """
    Приводит найденный реальный запрос к устойчивому ключу.

    Например:
      "Айфон 17 Pro Max 512 ГБ"
      "iphone 17 pro max 512gb"
    становятся одинаковым ключом.
    """
    normalized = normalize_text(
        found_request
    )

    return normalized[:220]


async def allow_request_after_short_dedup(
    owner_id: int,
    seller_key: str | None,
    found_request: str,
) -> bool:
    """
    True  -> уведомление/аналитику пропускаем.
    False -> это тот же пользователь + тот же запрос
             в пределах DEDUP_WINDOW_SECONDS.

    Важно: время считается от ПЕРВОГО сообщения в залпе.
    Повторы не продлевают окно.
    Поэтому при окне 120 секунд:
      13:00 -> ✅
      13:01 -> ❌
      13:02:01 -> ✅
    """
    if not seller_key:
        # Для анонимного/канального автора не пытаемся угадывать личность.
        return True

    request_key = request_dedup_key(
        found_request
    )

    if not request_key:
        return True

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO recent_request_dedup (
                owner_id,
                seller_key,
                request_key,
                last_seen
            )
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (
                owner_id,
                seller_key,
                request_key
            )
            DO UPDATE
            SET last_seen=EXCLUDED.last_seen
            WHERE recent_request_dedup.last_seen
                  < NOW() - ($4 * INTERVAL '1 second')
            RETURNING last_seen
            """,
            owner_id,
            seller_key,
            request_key,
            DEDUP_WINDOW_SECONDS,
        )

    # INSERT либо разрешённый UPDATE вернут строку.
    # Слишком свежий дубль ничего не обновит и вернёт None.
    return row is not None



def extract_found_request(message_text: str, monitor_query: str) -> str:
    """
    Берёт запрос ИЗ НАЙДЕННОГО СООБЩЕНИЯ, а не наш запрос мониторинга.

    Примеры:
    monitor_query = "google"
    "Ищу Google Pixel" -> "Google Pixel"
    "Куплю Google Pixel 9 Pro, бюджет 500€" -> "Google Pixel 9 Pro"
    "Продам Fitbit Air. Состояние идеал" -> "Fitbit Air"
    """
    text = (message_text or "").strip()

    if not text:
        return monitor_query.strip()

    # Схлопываем переносы и лишние пробелы.
    compact = re.sub(r"\s+", " ", text).strip()

    # Если в сообщении есть явная фраза намерения, берём текст после неё.
    intent_pattern = re.compile(
        r"(?i)\b("
        r"ищу|ищем|куплю|купим|нужен|нужна|нужно|нужны|"
        r"интересует|возьму|возьмём|возьмем|"
        r"продам|продаю|продается|продаётся|"
        r"wtb|wts"
        r")\b[\s:,-]*"
    )

    match = intent_pattern.search(compact)

    if match:
        candidate = compact[match.end():].strip()
    else:
        # Иначе выбираем кусок сообщения, который лучше всего похож
        # на наш запрос мониторинга.
        clauses = [
            part.strip()
            for part in re.split(r"[\n.!?;|]+", compact)
            if part.strip()
        ]

        if clauses:
            candidate = max(
                clauses,
                key=lambda part: product_match_score(
                    monitor_query,
                    part,
                ),
            )
        else:
            candidate = compact

    # Убираем частые хвосты объявления, чтобы в личку вставлялось
    # именно название/модель, а не вся простыня.
    stop_patterns = [
        r"(?i)\s*[,.!?;|]\s*(?:цена|бюджет|состояние|город|доставка|комплект|цвет|память)\b",
        r"(?i)\s+\b(?:бюджет|цена|состояние|город|доставка|комплект)\b\s*[:\-]?",
        r"(?i)\s+\b(?:пишите|предлагайте|в\s+лс|лс)\b",
        r"(?i)\s+\bза\s+\d",
    ]

    cut = len(candidate)

    for pattern in stop_patterns:
        stop_match = re.search(pattern, candidate)

        if stop_match:
            cut = min(cut, stop_match.start())

    candidate = candidate[:cut].strip(" \t\r\n,.;:!?-–—")

    # Если получилось слишком длинно, берём первую осмысленную часть.
    if len(candidate) > 120:
        candidate = re.split(
            r"[\n.!?;|]",
            candidate,
            maxsplit=1,
        )[0].strip()

    # Fallback, если эвристика всё вырезала.
    return candidate or monitor_query.strip()


def reply_keyboard(reply_text: str, seller_username: str | None):
    """
    Одна кнопка: 💬 Ответить

    Используем прямой Telegram deep link tg://resolve вместо https://t.me,
    чтобы Telegram сразу открыл личку и вставил полный текст в поле ввода.
    """
    if not seller_username:
        return None

    draft_text = reply_text.strip()

    encoded_text = quote(
        draft_text,
        safe="",
        encoding="utf-8",
        errors="strict",
    )

    url = (
        f"tg://resolve"
        f"?domain={seller_username}"
        f"&text={encoded_text}"
    )

    print(
        "REPLY LINK | "
        f"draft={draft_text!r} | "
        f"url={url!r}"
    )

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💬 Ответить",
                    url=url,
                )
            ]
        ]
    )


# ============================================================
# MENU
# ============================================================

def main_menu():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔎 Запросы",
                    callback_data="queries",
                ),
                InlineKeyboardButton(
                    text="💬 Чаты",
                    callback_data="chats:0",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📊 Аналитика",
                    callback_data="analytics:7",
                )
            ],
        ]
    )


def queries_menu():
    """
    Legacy-меню для сообщений "добавлено/пусто".
    Основной экран запросов использует пагинацию.
    """
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="➕ Добавить запрос",
                    callback_data="query:add",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🗑 Удалить запрос",
                    callback_data="query:delete:page:0",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📋 Все запросы",
                    callback_data="queries:page:0",
                )
            ],
            [
                InlineKeyboardButton(
                    text="◀️ Главное меню",
                    callback_data="home",
                )
            ],
        ]
    )


# ============================================================
# START
# ============================================================

@dp.message(Command("start"))
async def cmd_start(
    message: Message,
    state: FSMContext,
):
    await state.clear()

    if not await claim_or_check_owner(message.from_user.id):
        await message.answer("⛔ Этот бот закрыт.")
        return

    count = await account_count(
        message.from_user.id
    )

    extra = ""

    if count < MAX_ACCOUNTS:
        extra = (
            f"\n\n👥 Telegram: {count}/{MAX_ACCOUNTS}. "
            "Чтобы добавить аккаунт: /login"
        )

    await message.answer(
        "🔎 Tech Monitor\n\n"
        "Выбери раздел:"
        + extra,
        reply_markup=main_menu(),
    )


@dp.callback_query(F.data == "home")
async def cb_home(
    callback: CallbackQuery,
    state: FSMContext,
):
    await state.clear()

    if not await guard_callback(callback):
        return

    await callback.message.edit_text(
        "🔎 Tech Monitor\n\n"
        "Выбери раздел:",
        reply_markup=main_menu(),
    )

    await callback.answer()


# ============================================================
# STATUS
# ============================================================

async def status_text(owner_id: int):
    account_rows = await get_accounts(
        owner_id
    )

    connected_lines = []

    for index, row in enumerate(
        account_rows,
        start=1,
    ):
        account_id = int(row["id"])
        connected = account_id in clients

        username = row["username"]
        first_name = row["first_name"]

        name = (
            f"@{username}"
            if username
            else (first_name or row["label"])
        )

        connected_lines.append(
            f"{'🟢' if connected else '🔴'} "
            f"{index}. {name}"
        )

    if not connected_lines:
        connected_lines = [
            "🔴 Telegram-аккаунты не подключены"
        ]

    async with db_pool.acquire() as conn:
        query_count = int(
            await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM monitor_queries_v2
                WHERE owner_id=$1
                """,
                owner_id,
            )
            or 0
        )

        selected_count = int(
            await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM selected_chats
                WHERE owner_id=$1
                """,
                owner_id,
            )
            or 0
        )

    available = len(
        await get_all_dialogs(owner_id)
    )

    return (
        "📡 СТАТУС\n\n"
        + "\n".join(connected_lines)
        + "\n\n"
        f"👥 Аккаунтов: {len(clients)}/{MAX_ACCOUNTS}\n"
        f"💬 Доступно чатов: {available}\n"
        f"✅ Выбрано: {selected_count}\n"
        f"🔎 Запросов: {query_count}"
    )


@dp.message(Command("status"))
async def cmd_status(message: Message):
    if not await guard_message(message):
        return

    await message.answer(
        await status_text(
            message.from_user.id
        ),
        reply_markup=main_menu(),
    )


@dp.message(Command("accounts"))
async def cmd_accounts(message: Message):
    """
    Команда скрытая — в системной плашке Menu её нет.
    Нужна только если захочешь проверить оба аккаунта.
    """
    if not await guard_message(message):
        return

    await message.answer(
        await status_text(
            message.from_user.id
        )
    )


# ============================================================
# LOGIN
# ============================================================
# ============================================================
# LOGIN
# ============================================================

@dp.message(Command("login"))
async def cmd_login(
    message: Message,
    state: FSMContext,
):
    """
    Одна и та же /login:
    первый запуск -> добавляет аккаунт 1
    второй запуск -> добавляет аккаунт 2
    Никаких /login1 и /login2.
    """
    if not await guard_message(message):
        return

    owner_id = message.from_user.id
    count = await account_count(
        owner_id
    )

    if count >= MAX_ACCOUNTS:
        await message.answer(
            "✅ Уже подключено 2 из 2 Telegram-аккаунтов.\n\n"
            "Оба могут одновременно собирать сообщения."
        )
        return

    await close_login_client(
        owner_id
    )

    tg_client = TelegramClient(
        StringSession(),
        API_ID,
        API_HASH,
        receive_updates=True,
    )

    try:
        await tg_client.connect()
    except Exception as error:
        await message.answer(
            f"❌ Не удалось запустить авторизацию:\n{error}"
        )
        return

    login_clients[owner_id] = tg_client

    await state.set_state(
        LoginStates.waiting_phone
    )

    await message.answer(
        f"👤 Подключаем аккаунт {count + 1} из {MAX_ACCOUNTS}.\n\n"
        "📱 Отправь номер Telegram.\n\n"
        "Например:\n"
        "+37212345678"
    )


@dp.message(LoginStates.waiting_phone)
async def login_phone(
    message: Message,
    state: FSMContext,
):
    if not await guard_message(message):
        return

    owner_id = message.from_user.id
    tg_client = login_clients.get(
        owner_id
    )

    if tg_client is None:
        await state.clear()
        await message.answer(
            "❌ Авторизация сбросилась. Отправь /login ещё раз."
        )
        return

    phone = (
        message.text
        or ""
    ).strip()

    if not re.fullmatch(
        r"\+\d{7,15}",
        phone,
    ):
        await message.answer(
            "❌ Номер должен выглядеть примерно так:\n"
            "+37212345678"
        )
        return

    try:
        result = await tg_client.send_code_request(
            phone
        )

        await state.update_data(
            phone=phone,
            phone_code_hash=result.phone_code_hash,
        )

        await state.set_state(
            LoginStates.waiting_code
        )

        await message.answer(
            "📨 Telegram отправил код.\n\n"
            "Введи его С ПРОБЕЛАМИ.\n"
            "Например: 1 2 3 4 5"
        )

    except Exception as error:
        await message.answer(
            f"❌ Не удалось запросить код:\n{error}"
        )


async def finish_login(
    message: Message,
    state: FSMContext,
):
    owner_id = message.from_user.id
    tg_client = login_clients.get(
        owner_id
    )

    if tg_client is None:
        await state.clear()
        await message.answer(
            "❌ Авторизация сбросилась. Отправь /login ещё раз."
        )
        return

    try:
        account_id, me = (
            await register_logged_in_client(
                owner_id,
                tg_client,
            )
        )

        # Клиент теперь рабочий и лежит в clients,
        # поэтому из временного словаря просто убираем ссылку,
        # НЕ disconnect.
        login_clients.pop(
            owner_id,
            None,
        )

        await state.clear()

        rows = await get_accounts(
            owner_id
        )

        account_number = next(
            (
                index
                for index, row in enumerate(
                    rows,
                    start=1,
                )
                if int(row["id"]) == account_id
            ),
            len(rows),
        )

        account_name = (
            f"@{me.username}"
            if getattr(me, "username", None)
            else (
                getattr(me, "first_name", None)
                or str(me.id)
            )
        )

        await message.answer(
            f"✅ Аккаунт {account_number} подключён: {account_name}\n\n"
            f"Сейчас работает: {len(clients)}/{MAX_ACCOUNTS}.\n"
            "Открой 💬 Чаты — там будут чаты обоих аккаунтов.",
            reply_markup=main_menu(),
        )

    except RuntimeError as error:
        await close_login_client(
            owner_id
        )
        await state.clear()

        await message.answer(
            f"❌ {error}"
        )

    except Exception as error:
        await close_login_client(
            owner_id
        )
        await state.clear()

        await message.answer(
            f"❌ Не удалось сохранить аккаунт:\n{error}"
        )


@dp.message(LoginStates.waiting_code)
async def login_code(
    message: Message,
    state: FSMContext,
):
    if not await guard_message(message):
        return

    owner_id = message.from_user.id
    tg_client = login_clients.get(
        owner_id
    )

    if tg_client is None:
        await state.clear()
        await message.answer(
            "❌ Авторизация сбросилась. Отправь /login ещё раз."
        )
        return

    code = re.sub(
        r"\D",
        "",
        message.text or "",
    )

    data = await state.get_data()

    try:
        await tg_client.sign_in(
            phone=data["phone"],
            code=code,
            phone_code_hash=data["phone_code_hash"],
        )

        await finish_login(
            message,
            state,
        )

    except SessionPasswordNeededError:
        await state.set_state(
            LoginStates.waiting_password
        )

        await message.answer(
            "🔐 Введи пароль 2FA."
        )

    except PhoneCodeInvalidError:
        await message.answer(
            "❌ Неверный код. Попробуй ещё раз."
        )

    except PhoneCodeExpiredError:
        await close_login_client(
            owner_id
        )

        await state.clear()

        await message.answer(
            "❌ Код истёк. Начни заново: /login"
        )

    except Exception as error:
        await message.answer(
            f"❌ Ошибка авторизации:\n{error}"
        )


@dp.message(LoginStates.waiting_password)
async def login_password(
    message: Message,
    state: FSMContext,
):
    if not await guard_message(message):
        return

    owner_id = message.from_user.id
    tg_client = login_clients.get(
        owner_id
    )

    if tg_client is None:
        await state.clear()
        await message.answer(
            "❌ Авторизация сбросилась. Отправь /login ещё раз."
        )
        return

    try:
        await tg_client.sign_in(
            password=message.text or ""
        )

        await finish_login(
            message,
            state,
        )

    except Exception as error:
        await message.answer(
            f"❌ Пароль не подошёл:\n{error}"
        )


# ============================================================
# QUERIES
# ============================================================
# ============================================================
# QUERIES
# ============================================================

async def query_count(
    owner_id: int,
) -> int:
    async with db_pool.acquire() as conn:
        return int(
            await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM monitor_queries_v2
                WHERE owner_id=$1
                """,
                owner_id,
            )
            or 0
        )


async def get_queries_page(
    owner_id: int,
    page: int,
) -> tuple[list, int, int]:
    """
    Возвращает:
      rows,
      safe_page,
      total_pages

    Работаем напрямую через LIMIT/OFFSET, поэтому даже тысяча
    запросов не пытается влезть в одно Telegram-сообщение.
    """
    total = await query_count(
        owner_id
    )

    total_pages = max(
        1,
        (
            total
            + QUERIES_PER_PAGE
            - 1
        )
        // QUERIES_PER_PAGE,
    )

    safe_page = max(
        0,
        min(
            int(page),
            total_pages - 1,
        ),
    )

    offset = (
        safe_page
        * QUERIES_PER_PAGE
    )

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, query
            FROM monitor_queries_v2
            WHERE owner_id=$1
            ORDER BY id DESC
            LIMIT $2
            OFFSET $3
            """,
            owner_id,
            QUERIES_PER_PAGE,
            offset,
        )

    return (
        list(rows),
        safe_page,
        total_pages,
    )


def queries_page_keyboard(
    page: int,
    total_pages: int,
) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(
                text="➕ Добавить запрос",
                callback_data="query:add",
            ),
            InlineKeyboardButton(
                text="🗑 Удалить",
                callback_data=f"query:delete:page:{page}",
            ),
        ]
    ]

    if total_pages > 1:
        nav = []

        if page > 0:
            nav.append(
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=f"queries:page:{page - 1}",
                )
            )

        nav.append(
            InlineKeyboardButton(
                text=f"{page + 1}/{total_pages}",
                callback_data="query:page:noop",
            )
        )

        if page < total_pages - 1:
            nav.append(
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=f"queries:page:{page + 1}",
                )
            )

        keyboard.append(nav)

    keyboard.append([
        InlineKeyboardButton(
            text="◀️ Главное меню",
            callback_data="home",
        )
    ])

    return InlineKeyboardMarkup(
        inline_keyboard=keyboard
    )


async def render_queries_page(
    owner_id: int,
    page: int = 0,
) -> tuple[str, InlineKeyboardMarkup]:
    rows, page, total_pages = (
        await get_queries_page(
            owner_id,
            page,
        )
    )

    total = await query_count(
        owner_id
    )

    if not rows:
        text = (
            "🔎 ЗАПРОСЫ\n\n"
            "Пока ничего не отслеживается."
        )
    else:
        first_number = (
            page * QUERIES_PER_PAGE
            + 1
        )

        lines = []

        for index, row in enumerate(
            rows,
            start=first_number,
        ):
            lines.append(
                f"{index}. {row['query']}"
            )

        text = (
            "🔎 ЗАПРОСЫ\n\n"
            + "\n".join(lines)
            + "\n\n"
            + f"Всего: {total}"
        )

    return (
        text,
        queries_page_keyboard(
            page,
            total_pages,
        ),
    )


# Оставляем совместимость: старые места в коде могут вызвать queries_text.
async def queries_text(owner_id: int):
    text, _ = await render_queries_page(
        owner_id,
        0,
    )
    return text


@dp.message(Command("queries"))
async def cmd_queries(message: Message):
    if not await guard_message(message):
        return

    text, markup = await render_queries_page(
        message.from_user.id,
        0,
    )

    await message.answer(
        text,
        reply_markup=markup,
    )


@dp.callback_query(F.data == "queries")
async def cb_queries(callback: CallbackQuery):
    if not await guard_callback(callback):
        return

    text, markup = await render_queries_page(
        callback.from_user.id,
        0,
    )

    await callback.message.edit_text(
        text,
        reply_markup=markup,
    )

    await callback.answer()


@dp.callback_query(F.data.startswith("queries:page:"))
async def cb_queries_page(
    callback: CallbackQuery,
):
    if not await guard_callback(callback):
        return

    try:
        page = int(
            callback.data.rsplit(
                ":",
                1,
            )[1]
        )
    except (ValueError, IndexError):
        await callback.answer(
            "Ошибка страницы.",
            show_alert=True,
        )
        return

    text, markup = await render_queries_page(
        callback.from_user.id,
        page,
    )

    await callback.message.edit_text(
        text,
        reply_markup=markup,
    )

    await callback.answer()


@dp.callback_query(F.data == "query:page:noop")
async def cb_query_page_noop(
    callback: CallbackQuery,
):
    await callback.answer()


@dp.message(Command("add"))
async def cmd_add(message: Message, state: FSMContext):
    if not await guard_message(message):
        return

    parts = (message.text or "").split(
        maxsplit=1
    )

    if len(parts) == 2 and parts[1].strip():
        raw_queries = parts[1].strip()

        count, added = await add_queries(
            message.from_user.id,
            raw_queries,
        )

        if count == 0:
            await message.answer(
                "ℹ️ Такие запросы уже есть или список пуст.",
                reply_markup=queries_menu(),
            )
            return

        await message.answer(
            "✅ Добавлено: "
            f"{count}\n\n"
            + "\n".join(
                f"• {query}"
                for query in added
            ),
            reply_markup=queries_menu(),
        )
        return

    await state.set_state(
        QueryStates.waiting_query
    )

    await message.answer(
        "🔎 Напиши, что искать.\n\n"
        "Можно один запрос или несколько строками.\n\n"
        "Например:\n"
        "S25 256 Navy\n"
        "iPhone 17 256 Lavender\n"
        "ray-ban meta"
    )


@dp.callback_query(F.data == "query:add")
async def cb_query_add(callback: CallbackQuery, state: FSMContext):
    if not await guard_callback(callback):
        return

    await state.set_state(
        QueryStates.waiting_query
    )

    await callback.message.answer(
        "🔎 Напиши, что искать.\n\n"
        "Можно один запрос или несколько строками.\n\n"
        "Например:\n"
        "S25 256 Navy\n"
        "iPhone 17 256 Lavender\n"
        "ray-ban meta"
    )

    await callback.answer()


@dp.message(QueryStates.waiting_query)
async def query_input(message: Message, state: FSMContext):
    if not await guard_message(message):
        return

    raw_queries = (
        message.text
        or ""
    ).strip()

    entries = parse_query_entries(
        raw_queries
    )

    if not entries:
        await message.answer(
            "❌ Не вижу ни одного нормального запроса."
        )
        return

    count, added = await add_queries(
        message.from_user.id,
        raw_queries,
    )

    await state.clear()

    if count == 0:
        await message.answer(
            "ℹ️ Эти запросы уже есть.",
            reply_markup=queries_menu(),
        )
        return

    await message.answer(
        "✅ Добавлено: "
        f"{count}\n\n"
        + "\n".join(
            f"• {query}"
            for query in added
        ),
        reply_markup=queries_menu(),
    )


async def build_query_delete_page(
    owner_id: int,
    page: int,
) -> tuple[str, InlineKeyboardMarkup]:
    rows, page, total_pages = (
        await get_queries_page(
            owner_id,
            page,
        )
    )

    total = await query_count(
        owner_id
    )

    keyboard = []

    for row in rows:
        title = str(
            row["query"]
        )

        if len(title) > 42:
            title = (
                title[:39]
                + "..."
            )

        keyboard.append([
            InlineKeyboardButton(
                text=f"❌ {title}",
                callback_data=(
                    f"query:remove:"
                    f"{int(row['id'])}:"
                    f"{page}"
                ),
            )
        ])

    if total_pages > 1:
        nav = []

        if page > 0:
            nav.append(
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=(
                        f"query:delete:page:"
                        f"{page - 1}"
                    ),
                )
            )

        nav.append(
            InlineKeyboardButton(
                text=f"{page + 1}/{total_pages}",
                callback_data="query:page:noop",
            )
        )

        if page < total_pages - 1:
            nav.append(
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=(
                        f"query:delete:page:"
                        f"{page + 1}"
                    ),
                )
            )

        keyboard.append(nav)

    keyboard.append([
        InlineKeyboardButton(
            text="◀️ К списку запросов",
            callback_data=f"queries:page:{page}",
        )
    ])

    text = (
        "🗑 ВЫБЕРИ ЗАПРОС ДЛЯ УДАЛЕНИЯ\n\n"
        f"Всего: {total}\n"
        f"Страница: {page + 1}/{total_pages}\n\n"
        "На экране максимум 8 позиций."
    )

    return (
        text,
        InlineKeyboardMarkup(
            inline_keyboard=keyboard
        ),
    )


# Совместимость со старой кнопкой "query:delete".
@dp.callback_query(F.data == "query:delete")
async def cb_query_delete_menu_legacy(
    callback: CallbackQuery,
):
    if not await guard_callback(callback):
        return

    total = await query_count(
        callback.from_user.id
    )

    if total == 0:
        await callback.answer(
            "Запросов нет.",
            show_alert=True,
        )
        return

    text, markup = await build_query_delete_page(
        callback.from_user.id,
        0,
    )

    await callback.message.edit_text(
        text,
        reply_markup=markup,
    )

    await callback.answer()


@dp.callback_query(F.data.startswith("query:delete:page:"))
async def cb_query_delete_page(
    callback: CallbackQuery,
):
    if not await guard_callback(callback):
        return

    try:
        page = int(
            callback.data.rsplit(
                ":",
                1,
            )[1]
        )
    except (ValueError, IndexError):
        await callback.answer(
            "Ошибка страницы.",
            show_alert=True,
        )
        return

    total = await query_count(
        callback.from_user.id
    )

    if total == 0:
        text, markup = await render_queries_page(
            callback.from_user.id,
            0,
        )

        await callback.message.edit_text(
            text,
            reply_markup=markup,
        )

        await callback.answer(
            "Запросов больше нет."
        )
        return

    text, markup = await build_query_delete_page(
        callback.from_user.id,
        page,
    )

    await callback.message.edit_text(
        text,
        reply_markup=markup,
    )

    await callback.answer()


@dp.callback_query(F.data.startswith("query:remove:"))
async def cb_query_remove(
    callback: CallbackQuery,
):
    if not await guard_callback(callback):
        return

    parts = callback.data.split(
        ":"
    )

    try:
        query_id = int(
            parts[2]
        )

        page = (
            int(parts[3])
            if len(parts) >= 4
            else 0
        )
    except (ValueError, IndexError):
        await callback.answer(
            "Ошибка кнопки.",
            show_alert=True,
        )
        return

    deleted = await delete_query(
        callback.from_user.id,
        query_id,
    )

    # Если нажали старую кнопку из старого сообщения,
    # просто перерисовываем АКТУАЛЬНУЮ страницу.
    if deleted is None:
        total = await query_count(
            callback.from_user.id
        )

        if total == 0:
            text, markup = await render_queries_page(
                callback.from_user.id,
                0,
            )
        else:
            text, markup = await build_query_delete_page(
                callback.from_user.id,
                page,
            )

        await callback.message.edit_text(
            text,
            reply_markup=markup,
        )

        await callback.answer(
            "Эта позиция уже удалена."
        )
        return

    total = await query_count(
        callback.from_user.id
    )

    if total == 0:
        text, markup = await render_queries_page(
            callback.from_user.id,
            0,
        )

        await callback.message.edit_text(
            text,
            reply_markup=markup,
        )

        await callback.answer(
            f"Удалено: {deleted}"
        )
        return

    # После удаления последнего элемента на странице
    # автоматически переходим на предыдущую существующую страницу.
    total_pages = max(
        1,
        (
            total
            + QUERIES_PER_PAGE
            - 1
        )
        // QUERIES_PER_PAGE,
    )

    safe_page = min(
        page,
        total_pages - 1,
    )

    text, markup = await build_query_delete_page(
        callback.from_user.id,
        safe_page,
    )

    await callback.message.edit_text(
        text,
        reply_markup=markup,
    )

    await callback.answer(
        "Удалено ✅"
    )


@dp.message(Command("resetqueries"))
async def cmd_reset_queries(
    message: Message,
):
    """
    Скрытая служебная команда.
    В системное Menu её не добавляем.
    """
    if not await guard_message(message):
        return

    rows = await get_queries(
        message.from_user.id
    )

    if not rows:
        await message.answer(
            "ℹ️ Список отслеживания уже пуст.",
            reply_markup=queries_menu(),
        )
        return

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗑 Да, снести весь список",
                    callback_data="queries:reset:confirm",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Отмена",
                    callback_data="queries",
                )
            ],
        ]
    )

    await message.answer(
        "⚠️ ОЧИСТИТЬ СПИСОК ПОИСКА?\n\n"
        f"Сейчас отображается: {len(rows)} запросов.\n\n"
        "Будут удалены только запросы отслеживания.\n"
        "Чаты, Telegram-аккаунты и аналитика останутся.",
        reply_markup=keyboard,
    )


@dp.callback_query(F.data == "queries:reset:confirm")
async def cb_reset_queries_confirm(
    callback: CallbackQuery,
):
    if not await guard_callback(callback):
        return

    deleted_count = await reset_all_queries(
        callback.from_user.id
    )

    await callback.message.edit_text(
        "✅ СПИСОК ПОИСКА ПОЛНОСТЬЮ ОЧИЩЕН\n\n"
        f"Удалено записей из базы: {deleted_count}\n\n"
        "Теперь добавляй запросы заново — каждый запрос "
        "живёт отдельной записью в чистой таблице v2.",
        reply_markup=queries_menu(),
    )

    await callback.answer(
        "Список очищен ✅"
    )


@dp.message(Command("querycount"))
async def cmd_query_count(
    message: Message,
):
    """
    Скрытая проверка активного списка.
    Старую queries не учитывает вообще.
    """
    if not await guard_message(message):
        return

    owner_id = await get_owner_id()

    if owner_id is None:
        await message.answer(
            "🔎 Активных запросов: 0"
        )
        return

    async with db_pool.acquire() as conn:
        count = int(
            await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM monitor_queries_v2
                WHERE owner_id=$1
                """,
                owner_id,
            )
            or 0
        )

    await message.answer(
        f"🔎 Активных запросов: {count}"
    )


# ============================================================
# CHATS
# ============================================================

def chat_tools_row():
    return [
        InlineKeyboardButton(
            text="🔎 Найти чат",
            callback_data="chatsearch:new",
        ),
        InlineKeyboardButton(
            text="✅ Выбранные",
            callback_data="selectedchats:0",
        ),
    ]


async def build_chats_keyboard(
    owner_id: int,
    page: int,
    dialogs: list,
):
    selected = await get_selected_chat_keys(
        owner_id
    )

    total_pages = max(
        1,
        (
            len(dialogs)
            + CHATS_PER_PAGE
            - 1
        ) // CHATS_PER_PAGE,
    )

    page = max(
        0,
        min(
            page,
            total_pages - 1,
        ),
    )

    start = page * CHATS_PER_PAGE

    current = dialogs[
        start:start + CHATS_PER_PAGE
    ]

    rows = [
        chat_tools_row()
    ]

    for chat in current:
        key = (
            int(chat["account_id"]),
            int(chat["id"]),
        )

        checked = key in selected
        title = chat["name"]
        account_index = chat["account_index"]

        if len(title) > 31:
            title = title[:28] + "..."

        rows.append([
            InlineKeyboardButton(
                text=(
                    f"{'✅' if checked else '⬜'} "
                    f"{account_index}️⃣ {title}"
                ),
                callback_data=(
                    f"chat:toggle:"
                    f"{chat['account_id']}:"
                    f"{chat['id']}:"
                    f"{page}"
                ),
            )
        ])

    nav = []

    if page > 0:
        nav.append(
            InlineKeyboardButton(
                text="◀️",
                callback_data=f"chats:{page - 1}",
            )
        )

    nav.append(
        InlineKeyboardButton(
            text=f"{page + 1}/{total_pages}",
            callback_data="noop",
        )
    )

    if page < total_pages - 1:
        nav.append(
            InlineKeyboardButton(
                text="▶️",
                callback_data=f"chats:{page + 1}",
            )
        )

    rows.append(nav)

    rows.append([
        InlineKeyboardButton(
            text="🔄 Обновить",
            callback_data=f"chats:{page}",
        )
    ])

    rows.append([
        InlineKeyboardButton(
            text="◀️ Главное меню",
            callback_data="home",
        )
    ])

    return InlineKeyboardMarkup(
        inline_keyboard=rows
    )


async def connected_accounts_caption(
    owner_id: int,
) -> str:
    rows = await get_accounts(
        owner_id
    )

    lines = []

    for index, row in enumerate(
        rows,
        start=1,
    ):
        account_id = int(row["id"])

        if account_id not in clients:
            continue

        name = (
            f"@{row['username']}"
            if row["username"]
            else (
                row["first_name"]
                or row["label"]
            )
        )

        lines.append(
            f"{index}️⃣ {name}"
        )

    return "\n".join(lines)


async def render_chats(
    target_message,
    owner_id: int,
    page: int,
    edit: bool,
):
    if not clients:
        text = (
            "🔴 Telegram ещё не подключён.\n\n"
            "Отправь /login."
        )

        if edit:
            await target_message.edit_text(
                text
            )
        else:
            await target_message.answer(
                text
            )

        return

    dialogs = await get_all_dialogs(
        owner_id
    )

    if not dialogs:
        text = (
            "💬 Telegram не вернул группы/каналы.\n\n"
            "Проверь подключение через /status."
        )

        if edit:
            await target_message.edit_text(
                text
            )
        else:
            await target_message.answer(
                text
            )

        return

    accounts_caption = await connected_accounts_caption(
        owner_id
    )

    text = (
        "💬 ВЫБОР ЧАТОВ\n\n"
        f"{accounts_caption}\n\n"
        f"Найдено: {len(dialogs)}\n\n"
        "✅ — отслеживается\n"
        "⬜ — не отслеживается\n\n"
        "1️⃣ / 2️⃣ — с какого аккаунта берётся чат\n\n"
        "Можно листать или нажать 🔎 Найти чат."
    )

    keyboard = await build_chats_keyboard(
        owner_id,
        page,
        dialogs,
    )

    if edit:
        await target_message.edit_text(
            text,
            reply_markup=keyboard,
        )
    else:
        await target_message.answer(
            text,
            reply_markup=keyboard,
        )


def filter_dialogs_by_search(
    dialogs: list,
    search_text: str,
) -> list:
    """
    Поиск по части названия + лёгкая терпимость к опечаткам.
    """
    query = normalize_text(search_text)

    if not query:
        return []

    scored = []

    for chat in dialogs:
        title_normalized = normalize_text(
            chat["name"]
        )

        if not title_normalized:
            continue

        if query in title_normalized:
            score = 100.0
        else:
            score = fuzz.partial_ratio(
                query,
                title_normalized,
            )

        if score >= 78:
            scored.append(
                (
                    score,
                    chat["name"].casefold(),
                    chat,
                )
            )

    scored.sort(
        key=lambda item: (
            -item[0],
            item[1],
        )
    )

    return [
        item[2]
        for item in scored
    ]


async def build_search_results_keyboard(
    owner_id: int,
    search_text: str,
    page: int,
    matches: list,
):
    selected = await get_selected_chat_keys(
        owner_id
    )

    total_pages = max(
        1,
        (
            len(matches)
            + CHATS_PER_PAGE
            - 1
        ) // CHATS_PER_PAGE,
    )

    page = max(
        0,
        min(
            page,
            total_pages - 1,
        ),
    )

    start = page * CHATS_PER_PAGE
    current = matches[
        start:start + CHATS_PER_PAGE
    ]

    rows = []

    for chat in current:
        key = (
            int(chat["account_id"]),
            int(chat["id"]),
        )

        checked = key in selected
        title = chat["name"]

        if len(title) > 31:
            title = title[:28] + "..."

        rows.append([
            InlineKeyboardButton(
                text=(
                    f"{'✅' if checked else '⬜'} "
                    f"{chat['account_index']}️⃣ {title}"
                ),
                callback_data=(
                    f"chatsearch:toggle:"
                    f"{chat['account_id']}:"
                    f"{chat['id']}:"
                    f"{page}"
                ),
            )
        ])

    if len(matches) > CHATS_PER_PAGE:
        nav = []

        if page > 0:
            nav.append(
                InlineKeyboardButton(
                    text="◀️",
                    callback_data=f"chatsearch:page:{page - 1}",
                )
            )

        nav.append(
            InlineKeyboardButton(
                text=f"{page + 1}/{total_pages}",
                callback_data="noop",
            )
        )

        if page < total_pages - 1:
            nav.append(
                InlineKeyboardButton(
                    text="▶️",
                    callback_data=f"chatsearch:page:{page + 1}",
                )
            )

        rows.append(nav)

    rows.append([
        InlineKeyboardButton(
            text="🔎 Новый поиск",
            callback_data="chatsearch:new",
        )
    ])

    rows.append([
        InlineKeyboardButton(
            text="✅ Выбранные чаты",
            callback_data="selectedchats:0",
        ),
        InlineKeyboardButton(
            text="📋 Все чаты",
            callback_data="chatsearch:all",
        ),
    ])

    return InlineKeyboardMarkup(
        inline_keyboard=rows
    )


async def render_search_results(
    target_message,
    owner_id: int,
    search_text: str,
    page: int,
    edit: bool,
):
    dialogs = await get_all_dialogs(
        owner_id
    )

    matches = filter_dialogs_by_search(
        dialogs,
        search_text,
    )

    if not matches:
        text = (
            f"🔎 ПОИСК ЧАТА\n\n"
            f"По запросу «{search_text}» ничего не найдено.\n\n"
            "Попробуй часть названия, например:\n"
            "pixel\n"
            "барах\n"
            "apple"
        )

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="🔎 Новый поиск",
                        callback_data="chatsearch:new",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="📋 Все чаты",
                        callback_data="chatsearch:all",
                    )
                ],
            ]
        )

    else:
        text = (
            "🔎 РЕЗУЛЬТАТЫ ПОИСКА\n\n"
            f"Запрос: {search_text}\n"
            f"Найдено: {len(matches)}\n\n"
            "Нажми на чат, чтобы включить/выключить мониторинг."
        )

        keyboard = await build_search_results_keyboard(
            owner_id,
            search_text,
            page,
            matches,
        )

    if edit:
        await target_message.edit_text(
            text,
            reply_markup=keyboard,
        )
    else:
        await target_message.answer(
            text,
            reply_markup=keyboard,
        )


async def build_selected_chats_keyboard(
    owner_id: int,
    page: int,
    selected_chats: list,
):
    total_pages = max(
        1,
        (
            len(selected_chats)
            + CHATS_PER_PAGE
            - 1
        ) // CHATS_PER_PAGE,
    )

    page = max(
        0,
        min(
            page,
            total_pages - 1,
        ),
    )

    start = page * CHATS_PER_PAGE
    current = selected_chats[
        start:start + CHATS_PER_PAGE
    ]

    rows = []

    for chat in current:
        title = chat["name"]

        if len(title) > 31:
            title = title[:28] + "..."

        rows.append([
            InlineKeyboardButton(
                text=(
                    f"✅ {chat['account_index']}️⃣ {title}"
                ),
                callback_data=(
                    f"selectedchat:toggle:"
                    f"{chat['account_id']}:"
                    f"{chat['id']}:"
                    f"{page}"
                ),
            )
        ])

    if len(selected_chats) > CHATS_PER_PAGE:
        nav = []

        if page > 0:
            nav.append(
                InlineKeyboardButton(
                    text="◀️",
                    callback_data=f"selectedchats:{page - 1}",
                )
            )

        nav.append(
            InlineKeyboardButton(
                text=f"{page + 1}/{total_pages}",
                callback_data="noop",
            )
        )

        if page < total_pages - 1:
            nav.append(
                InlineKeyboardButton(
                    text="▶️",
                    callback_data=f"selectedchats:{page + 1}",
                )
            )

        rows.append(nav)

    rows.append([
        InlineKeyboardButton(
            text="🔎 Найти чат",
            callback_data="chatsearch:new",
        ),
        InlineKeyboardButton(
            text="📋 Все чаты",
            callback_data="chatsearch:all",
        ),
    ])

    rows.append([
        InlineKeyboardButton(
            text="◀️ Главное меню",
            callback_data="home",
        )
    ])

    return InlineKeyboardMarkup(
        inline_keyboard=rows
    )


async def render_selected_chats(
    target_message,
    owner_id: int,
    page: int,
    edit: bool,
):
    selected_chats = await get_selected_chats_details(
        owner_id
    )

    if not selected_chats:
        text = (
            "✅ ВЫБРАННЫЕ ЧАТЫ\n\n"
            "Пока ни один чат не выбран."
        )

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="🔎 Найти чат",
                        callback_data="chatsearch:new",
                    ),
                    InlineKeyboardButton(
                        text="📋 Все чаты",
                        callback_data="chatsearch:all",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        text="◀️ Главное меню",
                        callback_data="home",
                    )
                ],
            ]
        )

    else:
        text = (
            "✅ ВЫБРАННЫЕ ЧАТЫ\n\n"
            f"Сейчас мониторится: {len(selected_chats)}\n\n"
            "Нажми на чат, чтобы убрать его из мониторинга."
        )

        keyboard = await build_selected_chats_keyboard(
            owner_id,
            page,
            selected_chats,
        )

    if edit:
        await target_message.edit_text(
            text,
            reply_markup=keyboard,
        )
    else:
        await target_message.answer(
            text,
            reply_markup=keyboard,
        )


@dp.message(Command("chats"))
async def cmd_chats(
    message: Message,
    state: FSMContext,
):
    if not await guard_message(message):
        return

    await state.clear()

    await render_chats(
        message,
        message.from_user.id,
        0,
        False,
    )


@dp.callback_query(F.data.startswith("chats:"))
async def cb_chats(
    callback: CallbackQuery,
    state: FSMContext,
):
    if not await guard_callback(callback):
        return

    await state.clear()

    page = int(
        callback.data.split(":")[1]
    )

    await render_chats(
        callback.message,
        callback.from_user.id,
        page,
        True,
    )

    await callback.answer()


@dp.callback_query(F.data == "chatsearch:new")
async def cb_chat_search_new(
    callback: CallbackQuery,
    state: FSMContext,
):
    if not await guard_callback(callback):
        return

    await state.set_state(
        ChatSearchStates.waiting_search
    )

    await callback.message.edit_text(
        "🔎 ПОИСК ЧАТА\n\n"
        "Напиши часть названия чата.\n\n"
        "Например:\n"
        "pixel\n"
        "барах\n"
        "apple",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="📋 Все чаты",
                        callback_data="chatsearch:all",
                    )
                ]
            ]
        ),
    )

    await callback.answer()


@dp.message(ChatSearchStates.waiting_search)
async def chat_search_input(
    message: Message,
    state: FSMContext,
):
    if not await guard_message(message):
        return

    search_text = (
        message.text
        or ""
    ).strip()

    if len(search_text) < 2:
        await message.answer(
            "Напиши хотя бы 2 символа."
        )
        return

    await state.update_data(
        chat_search=search_text
    )

    await render_search_results(
        message,
        message.from_user.id,
        search_text,
        0,
        False,
    )


@dp.callback_query(F.data.startswith("chatsearch:page:"))
async def cb_chat_search_page(
    callback: CallbackQuery,
    state: FSMContext,
):
    if not await guard_callback(callback):
        return

    data = await state.get_data()
    search_text = data.get(
        "chat_search"
    )

    if not search_text:
        await callback.answer(
            "Поиск устарел. Нажми «Новый поиск».",
            show_alert=True,
        )
        return

    page = int(
        callback.data.split(":")[2]
    )

    await render_search_results(
        callback.message,
        callback.from_user.id,
        search_text,
        page,
        True,
    )

    await callback.answer()


@dp.callback_query(F.data.startswith("chatsearch:toggle:"))
async def cb_chat_search_toggle(
    callback: CallbackQuery,
    state: FSMContext,
):
    if not await guard_callback(callback):
        return

    parts = callback.data.split(":")

    if len(parts) != 5:
        await callback.answer(
            "Ошибка кнопки.",
            show_alert=True,
        )
        return

    _, _, account_id_raw, chat_id_raw, page_raw = parts

    account_id = int(
        account_id_raw
    )
    chat_id = int(
        chat_id_raw
    )
    page = int(
        page_raw
    )

    data = await state.get_data()
    search_text = data.get(
        "chat_search"
    )

    if not search_text:
        await callback.answer(
            "Поиск устарел. Нажми «Новый поиск».",
            show_alert=True,
        )
        return

    dialogs = await get_all_dialogs(
        callback.from_user.id
    )

    chat = next(
        (
            item
            for item in dialogs
            if int(item["account_id"]) == account_id
            and int(item["id"]) == chat_id
        ),
        None,
    )

    if chat is None:
        await callback.answer(
            "Чат не найден.",
            show_alert=True,
        )
        return

    enabled = await toggle_selected_chat(
        owner_id=callback.from_user.id,
        account_id=account_id,
        chat_id=chat_id,
        title=chat["name"],
    )

    await render_search_results(
        callback.message,
        callback.from_user.id,
        search_text,
        page,
        True,
    )

    await callback.answer(
        "Включено ✅"
        if enabled
        else "Выключено"
    )


@dp.callback_query(F.data == "chatsearch:all")
async def cb_chat_search_all(
    callback: CallbackQuery,
    state: FSMContext,
):
    if not await guard_callback(callback):
        return

    await state.clear()

    await render_chats(
        callback.message,
        callback.from_user.id,
        0,
        True,
    )

    await callback.answer()


@dp.callback_query(F.data.startswith("selectedchats:"))
async def cb_selected_chats(
    callback: CallbackQuery,
    state: FSMContext,
):
    if not await guard_callback(callback):
        return

    await state.clear()

    page = int(
        callback.data.split(":")[1]
    )

    await render_selected_chats(
        callback.message,
        callback.from_user.id,
        page,
        True,
    )

    await callback.answer()


@dp.callback_query(F.data.startswith("selectedchat:toggle:"))
async def cb_selected_chat_toggle(
    callback: CallbackQuery,
):
    if not await guard_callback(callback):
        return

    parts = callback.data.split(":")

    if len(parts) != 5:
        await callback.answer(
            "Ошибка кнопки.",
            show_alert=True,
        )
        return

    _, _, account_id_raw, chat_id_raw, page_raw = parts

    account_id = int(
        account_id_raw
    )
    chat_id = int(
        chat_id_raw
    )
    page = int(
        page_raw
    )

    selected_chats = await get_selected_chats_details(
        callback.from_user.id
    )

    chat = next(
        (
            item
            for item in selected_chats
            if int(item["account_id"]) == account_id
            and int(item["id"]) == chat_id
        ),
        None,
    )

    if chat is None:
        await callback.answer(
            "Чат уже не выбран.",
            show_alert=True,
        )
        return

    await toggle_selected_chat(
        owner_id=callback.from_user.id,
        account_id=account_id,
        chat_id=chat_id,
        title=chat["name"],
    )

    remaining = await get_selected_chats_details(
        callback.from_user.id
    )

    if remaining:
        total_pages = max(
            1,
            (
                len(remaining)
                + CHATS_PER_PAGE
                - 1
            ) // CHATS_PER_PAGE,
        )

        page = min(
            page,
            total_pages - 1,
        )
    else:
        page = 0

    await render_selected_chats(
        callback.message,
        callback.from_user.id,
        page,
        True,
    )

    await callback.answer(
        "Убрано из мониторинга"
    )


@dp.callback_query(F.data.startswith("chat:toggle:"))
async def cb_chat_toggle(
    callback: CallbackQuery,
):
    if not await guard_callback(callback):
        return

    parts = callback.data.split(":")

    if len(parts) != 5:
        await callback.answer(
            "Ошибка кнопки.",
            show_alert=True,
        )
        return

    _, _, account_id_raw, chat_id_raw, page_raw = parts

    account_id = int(
        account_id_raw
    )
    chat_id = int(
        chat_id_raw
    )
    page = int(
        page_raw
    )

    dialogs = await get_all_dialogs(
        callback.from_user.id
    )

    chat = next(
        (
            item
            for item in dialogs
            if int(item["account_id"]) == account_id
            and int(item["id"]) == chat_id
        ),
        None,
    )

    if chat is None:
        await callback.answer(
            "Чат не найден.",
            show_alert=True,
        )
        return

    enabled = await toggle_selected_chat(
        owner_id=callback.from_user.id,
        account_id=account_id,
        chat_id=chat_id,
        title=chat["name"],
    )

    await callback.message.edit_reply_markup(
        reply_markup=await build_chats_keyboard(
            callback.from_user.id,
            page,
            dialogs,
        )
    )

    await callback.answer(
        "Включено ✅"
        if enabled
        else "Выключено"
    )


@dp.callback_query(F.data == "noop")
async def cb_noop(
    callback: CallbackQuery,
):
    await callback.answer()


# ============================================================
# TEST
# ============================================================
# ============================================================
# TEST
# ============================================================

@dp.message(Command("test"))
async def cmd_test(message: Message):
    if not await guard_message(message):
        return

    parts = (message.text or "").split(
        maxsplit=1
    )

    if len(parts) < 2:
        await message.answer(
            "Например:\n"
            "/test продам фитбит почти новый"
        )
        return

    sample = parts[1]

    rows = await get_queries(
        message.from_user.id
    )

    if not rows:
        await message.answer(
            "Сначала добавь запрос."
        )
        return

    scored = [
        (
            row["query"],
            product_match_score(
                row["query"],
                sample,
            ),
        )
        for row in rows
    ]

    scored.sort(
        key=lambda item: item[1],
        reverse=True,
    )

    best_query, best_score = scored[0]

    if best_score >= MATCH_THRESHOLD:
        await message.answer(
            "✅ ПОИСК СРАБОТАЛ\n\n"
            f"Запрос: {best_query}\n"
            f"Совпадение: {best_score:.0f}%"
        )
    else:
        await message.answer(
            "❌ Не совпало.\n\n"
            f"Ближайший запрос: {best_query}\n"
            f"Совпадение: {best_score:.0f}%"
        )



# ============================================================
# ANALYTICS HANDLERS
# ============================================================

@dp.message(Command("analytics"))
async def cmd_analytics(message: Message):
    if not await guard_message(message):
        return

    await message.answer(
        await analytics_text(
            message.from_user.id,
            "7",
        ),
        reply_markup=analytics_keyboard("7"),
    )


@dp.callback_query(F.data.startswith("analytics:"))
async def cb_analytics(callback: CallbackQuery):
    if not await guard_callback(callback):
        return

    period = callback.data.split(":", 1)[1]

    if period == "weeks":
        text = await weekly_analytics_text(
            callback.from_user.id
        )

        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="Сегодня",
                        callback_data="analytics:today",
                    ),
                    InlineKeyboardButton(
                        text="7 дней",
                        callback_data="analytics:7",
                    ),
                    InlineKeyboardButton(
                        text="30 дней",
                        callback_data="analytics:30",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        text="◀️ Главное меню",
                        callback_data="home",
                    )
                ],
            ]
        )

    else:
        if period not in {
            "today",
            "7",
            "30",
        }:
            period = "7"

        text = await analytics_text(
            callback.from_user.id,
            period,
        )

        markup = analytics_keyboard(
            period
        )

    await callback.message.edit_text(
        text,
        reply_markup=markup,
    )

    await callback.answer()


# ============================================================
# MONITOR
# ============================================================

async def monitor_handler(event, account_id: int):
    try:
        text = event.raw_text or ""
        chat_id = event.chat_id

        if not text or chat_id is None:
            return

        owner_id = await get_owner_id()

        if owner_id is None:
            return

        selected = await get_selected_chat_ids(
            owner_id,
            account_id,
        )

        if int(chat_id) not in selected:
            return

        # ----------------------------------------------------
        # АНАЛИТИКА ВСЕХ ЗАПРОСОВ
        # ----------------------------------------------------
        #
        # Это выполняется ДО чтения нашего списка поиска.
        # Поэтому аналитика работает даже если monitor_queries_v2
        # вообще пустая.
        chat = await event.get_chat()

        title = (
            getattr(chat, "title", None)
            or getattr(chat, "username", None)
            or "Telegram"
        )

        (
            seller_display,
            seller_username,
            seller_key,
        ) = await seller_info(event)

        analytics_request = extract_all_request_for_analytics(
            text
        )

        if analytics_request:
            analytics_allowed = (
                await allow_all_request_analytics_event(
                    owner_id=owner_id,
                    seller_key=seller_key,
                    found_request=analytics_request,
                )
            )

            if analytics_allowed:
                analytics_brand = detect_brand(
                    analytics_request,
                    text,
                )

                await log_all_request_analytics_event(
                    owner_id=owner_id,
                    chat_id=int(chat_id),
                    message_id=int(event.id),
                    chat_title=title,
                    found_request=analytics_request,
                    brand=analytics_brand,
                    seller_key=seller_key,
                )

                print(
                    "ALL REQUEST ANALYTICS | "
                    f"request={analytics_request!r} | "
                    f"brand={analytics_brand!r} | "
                    f"chat={chat_id} | "
                    f"message={event.id}"
                )

        # ----------------------------------------------------
        # УВЕДОМЛЕНИЯ ПО НАШЕМУ СПИСКУ
        # ----------------------------------------------------
        query_rows = await get_queries(
            owner_id
        )

        if not query_rows:
            return

        # Берём лучший запрос и КОНКРЕТНУЮ позицию из сообщения.
        #
        # Если в посте:
        # S26 black
        # S26 blue
        # S26 violet
        # S26 gold
        #
        # а запрос S26 black — в уведомление уйдёт именно "S26 black".
        best_query = None
        best_query_id = None
        best_score = 0.0
        best_segment = ""

        for row in query_rows:
            (
                score,
                matched_segment,
            ) = best_query_segment(
                row["query"],
                text,
            )

            if score > best_score:
                best_score = score
                best_query = row["query"]
                best_query_id = int(row["id"])
                best_segment = matched_segment

        if best_query is None or best_query_id is None:
            return

        if best_score < MATCH_THRESHOLD:
            return

        # Не отправляем одно сообщение дважды.
        if not await mark_seen(
            owner_id,
            int(chat_id),
            int(event.id),
        ):
            return

        # chat/title/seller уже получили выше для общей аналитики.

        # Показываем только найденную позицию из каталога.
        body_source = (
            best_segment
            or text
        )

        body = body_source[:3200]

        if len(body_source) > 3200:
            body += "\n\n…"

        notification = (
            f"🔥 ЗАПРОС: {best_query}\n\n"
            f"💬 {title}\n"
            f"👤 {seller_display}\n\n"
            f"{body}"
        )

        found_request = extract_found_request(
            body_source,
            best_query,
        )

        # ----------------------------------------------------
        # ЗАДЕРЖКА + ПОВТОРНАЯ ПРОВЕРКА БАЗЫ
        # ----------------------------------------------------
        #
        # Раньше совпадение могло уже "уснуть" на 15 секунд,
        # затем ты очищал список, а старое уведомление всё равно
        # прилетало. Теперь после сна мы ещё раз спрашиваем БД:
        # существует ли именно этот query_id прямо сейчас.
        if NOTIFICATION_DELAY_SECONDS > 0:
            await asyncio.sleep(
                NOTIFICATION_DELAY_SECONDS
            )

        # Последний жёсткий стоп: если активный список v2 пуст,
        # никакое ранее найденное сообщение не отправляем.
        latest_query_rows = await get_queries(
            owner_id
        )

        if not latest_query_rows:
            print(
                "MATCH CANCELLED | "
                "active query list is empty | "
                f"chat={chat_id} | "
                f"message={event.id}"
            )
            return

        if not await query_still_active(
            owner_id,
            best_query_id,
        ):
            print(
                "MATCH CANCELLED | "
                "query was deleted during delay | "
                f"query_id={best_query_id} | "
                f"query={best_query!r} | "
                f"chat={chat_id} | "
                f"message={event.id}"
            )
            return

        # ----------------------------------------------------
        # АНТИДУБЛЬ ПО ПРОДАВЦУ + РЕАЛЬНОМУ ЗАПРОСУ
        # ----------------------------------------------------
        #
        # Один человек часто кидает один и тот же текст подряд
        # в 10–20 групп. В течение 120 секунд показываем только
        # первое такое совпадение. Через 2 минуты запрос снова
        # считается новым.
        #
        # Дедупликация стоит ДО аналитики, поэтому массовый
        # кросс-постинг не накручивает пики/топы/бренды.
        allowed = await allow_request_after_short_dedup(
            owner_id=owner_id,
            seller_key=seller_key,
            found_request=found_request,
        )

        if not allowed:
            print(
                "CROSSCHAT DUPLICATE SKIPPED | "
                f"seller={seller_key!r} | "
                f"request={request_dedup_key(found_request)!r} | "
                f"chat={chat_id} | "
                f"message={event.id}"
            )
            return

        brand = detect_brand(
            found_request,
            body_source,
        )

        print(
            "FOUND REQUEST | "
            f"monitor={best_query!r} | "
            f"found={found_request!r} | "
            f"brand={brand!r} | "
            f"segment={best_segment!r}"
        )

        await bot.send_message(
            owner_id,
            notification,
            reply_markup=reply_keyboard(
                found_request,
                seller_username,
            ),
            disable_web_page_preview=True,
        )

        print(
            "MATCH SENT | "
            f"query={best_query!r} | "
            f"score={best_score:.0f} | "
            f"chat={chat_id} | "
            f"message={event.id} | "
            f"account_id={account_id}"
        )

    except Exception as error:
        print(
            "MONITOR ERROR:",
            repr(error),
        )


# ============================================================
# COMMANDS
# ============================================================

async def setup_commands():
    # Системная плашка Menu возле строки ввода:
    # только то, что ты просил.
    await bot.set_my_commands([
        BotCommand(
            command="start",
            description="Главное меню",
        ),
        BotCommand(
            command="queries",
            description="Запросы",
        ),
        BotCommand(
            command="chats",
            description="Чаты",
        ),
    ])


# ============================================================
# MAIN
# ============================================================

async def main():
    await init_db()

    # ВАЖНО:
    # старую таблицу queries больше вообще не читаем.
    # monitor_queries_v2 — новый чистый список поиска.
    # Никакой автоматической миграции старых запросов сюда нет.

    # Автоматически переносим старую одиночную сессию
    # в новую таблицу аккаунтов.
    await migrate_legacy_session()

    owner_id = await get_owner_id()

    if owner_id is not None:
        await load_saved_clients(
            owner_id
        )

        await migrate_legacy_selected_chats(
            owner_id
        )

    if clients:
        print(
            f"Telegram accounts connected: "
            f"{len(clients)}/{MAX_ACCOUNTS}"
        )
    else:
        print(
            "Telegram accounts not connected. "
            "Use /login."
        )

    await setup_commands()

    print(
        "Управляющий бот запущен."
    )

    try:
        await dp.start_polling(
            bot
        )

    finally:
        # Временные login-клиенты.
        for tg_client in list(
            login_clients.values()
        ):
            try:
                await tg_client.disconnect()
            except Exception:
                pass

        login_clients.clear()

        # Мониторинговые задачи.
        for task in list(
            telethon_tasks.values()
        ):
            if not task.done():
                task.cancel()

        telethon_tasks.clear()

        # Оба Telegram-клиента.
        for tg_client in list(
            clients.values()
        ):
            try:
                if tg_client.is_connected():
                    await tg_client.disconnect()
            except Exception:
                pass

        clients.clear()

        if db_pool:
            await db_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
