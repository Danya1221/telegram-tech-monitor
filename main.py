import os
import re
import asyncio
import time
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
    AuthKeyDuplicatedError,
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
# FAST PATH / CACHES
# ============================================================

# Интерфейс и Telethon работают в одном asyncio-процессе.
# Поэтому горячие данные не дёргаем из PostgreSQL на каждое
# входящее сообщение и не грузим Telegram dialogs на каждый клик.
DIALOGS_CACHE_SECONDS = 120
ANALYTICS_CACHE_SECONDS = 20
INTEREST_STATS_CACHE_SECONDS = 30

_selected_ids_cache: dict[tuple[int, int], set[int]] = {}
_selected_keys_cache: dict[int, set[tuple[int, int]]] = {}
_selected_details_cache: dict[int, list] = {}
_dialogs_cache: dict[int, tuple[float, list]] = {}
_account_dialogs_cache: dict[int, tuple[float, list]] = {}
_account_dialog_refresh_tasks: dict[int, asyncio.Task] = {}
_account_connect_locks: dict[int, asyncio.Lock] = {}
_account_recovery_tasks: dict[int, asyncio.Task] = {}
_account_last_heartbeat: dict[int, float] = {}
_account_last_error: dict[int, str] = {}
_analytics_rows_cache: dict[tuple[int, str], tuple[float, list]] = {}
_interest_snapshot_cache: dict[tuple[int, str], tuple[float, dict]] = {}
_analytics_dedup_memory: dict[tuple[int, str, str], float] = {}

# CPU-heavy matcher уводим из event loop, чтобы меню/кнопки
# не зависали во время потока сообщений. Одновременно не больше
# двух тяжёлых match-задач, чтобы Railway не забивался на 100%.
monitor_match_semaphore = asyncio.Semaphore(2)


def invalidate_query_cache(owner_id: int):
    # v16: monitor queries are DB-only, no RAM cache exists.
    return


def invalidate_interest_cache(owner_id: int):
    # Interest list is DB-only. Clear only derived stats snapshots.
    for key in list(_interest_snapshot_cache):
        if key[0] == owner_id:
            _interest_snapshot_cache.pop(key, None)


def invalidate_selected_cache(owner_id: int):
    _selected_keys_cache.pop(owner_id, None)
    _selected_details_cache.pop(owner_id, None)

    for key in list(_selected_ids_cache):
        if key[0] == owner_id:
            _selected_ids_cache.pop(key, None)


def invalidate_accounts_cache(owner_id: int):
    # Telegram profiles are DB-only. Dialog UI cache is separate.
    _dialogs_cache.pop(owner_id, None)


def invalidate_dialogs_cache(owner_id: int):
    _dialogs_cache.pop(owner_id, None)


# ============================================================
# FSM
# ============================================================

class LoginStates(StatesGroup):
    waiting_phone = State()
    waiting_code = State()
    waiting_password = State()


class QueryStates(StatesGroup):
    waiting_query = State()


class InterestQueryStates(StatesGroup):
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

        # Отдельный список запросов, которые интересны только
        # для аналитики. Он НИКАК не влияет на уведомления.
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS interest_queries_v1 (
                id BIGSERIAL PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                query TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_interest_queries_v1_owner
            ON interest_queries_v1 (owner_id, id DESC)
        """)

        # ЕДИНЫЙ источник истины для двух списков.
        # account_id здесь намеренно отсутствует:
        # оба Telegram-аккаунта используют один owner_id -> один список.
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS shared_product_lists_v1 (
                id BIGSERIAL PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                list_type TEXT NOT NULL,
                query TEXT NOT NULL,
                query_key TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT chk_shared_product_list_type
                    CHECK (list_type IN ('monitor', 'interest')),
                CONSTRAINT uq_shared_product_query
                    UNIQUE (owner_id, list_type, query_key)
            )
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_shared_product_lists_owner_type
            ON shared_product_lists_v1 (
                owner_id,
                list_type,
                id DESC
            )
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
            CREATE INDEX IF NOT EXISTS idx_selected_chats_owner_account
            ON selected_chats (owner_id, account_id, chat_id)
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

        # Последний известный список чатов каждого Telegram-аккаунта.
        #
        # Нужен для стабильного UI: если Telethon на секунду
        # переподключается или Railway только что перезапустился,
        # аккаунт и его чаты не исчезают из вкладки.
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS telegram_dialog_cache_v1 (
                owner_id BIGINT NOT NULL,
                account_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                title TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (owner_id, account_id, chat_id)
            )
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_telegram_dialog_cache_owner_account
            ON telegram_dialog_cache_v1 (owner_id, account_id)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_telegram_accounts_owner_active
            ON telegram_accounts (owner_id, active, id)
        """)

        # Временное состояние авторизации Telegram-аккаунта.
        #
        # Раньше /login держал client + FSM только в памяти процесса.
        # Если Railway перезапускался (или следующий update попадал
        # в другой процесс), после ввода номера авторизация терялась.
        #
        # Теперь этап, phone_code_hash и временная StringSession
        # переживают перезапуск процесса.
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS telegram_login_flows_v1 (
                owner_id BIGINT PRIMARY KEY,
                stage TEXT NOT NULL,
                target_account_number INTEGER NOT NULL,
                phone TEXT,
                phone_code_hash TEXT,
                session_string TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
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
            CREATE INDEX IF NOT EXISTS idx_seen_messages_created_at
            ON seen_messages (created_at)
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

    value = await get_setting(
        "owner_id"
    )

    return int(value) if value else None


async def claim_or_check_owner(
    user_id: int,
) -> bool:
    if ADMIN_ID:
        return user_id == ADMIN_ID

    owner = await get_owner_id()

    if owner is None:
        await set_setting(
            "owner_id",
            str(user_id),
        )
        owner = await get_owner_id()

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


LIST_TYPE_MONITOR = "monitor"
LIST_TYPE_INTEREST = "interest"


def parse_query_entries(raw_text: str) -> list[str]:
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

        key = canonical_query_key(
            item
        )

        if not key or key in seen:
            continue

        seen.add(key)
        result.append(item)

    return result


async def migrate_legacy_product_lists():
    """
    Одноразово переносим старые два списка в одну общую таблицу.
    Старые таблицы остаются backup, но после миграции больше
    никогда не являются активным источником.
    """
    migration_key = (
        "shared_product_lists_v1_migrated"
    )

    if await get_setting(
        migration_key
    ) == "1":
        return

    async with db_pool.acquire() as conn:
        monitor_rows = await conn.fetch(
            """
            SELECT owner_id, query
            FROM monitor_queries_v2
            ORDER BY id ASC
            """
        )

        interest_rows = await conn.fetch(
            """
            SELECT owner_id, query
            FROM interest_queries_v1
            ORDER BY id ASC
            """
        )

        rows_to_insert = []

        for list_type, rows in (
            (
                LIST_TYPE_MONITOR,
                monitor_rows,
            ),
            (
                LIST_TYPE_INTEREST,
                interest_rows,
            ),
        ):
            for row in rows:
                query = re.sub(
                    r"\s+",
                    " ",
                    str(
                        row["query"]
                        or ""
                    ).strip(),
                )

                query_key = canonical_query_key(
                    query
                )

                if (
                    len(query) < 2
                    or not query_key
                ):
                    continue

                rows_to_insert.append(
                    (
                        int(
                            row["owner_id"]
                        ),
                        list_type,
                        query,
                        query_key,
                    )
                )

        if rows_to_insert:
            await conn.executemany(
                """
                INSERT INTO shared_product_lists_v1 (
                    owner_id,
                    list_type,
                    query,
                    query_key
                )
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (
                    owner_id,
                    list_type,
                    query_key
                )
                DO NOTHING
                """,
                rows_to_insert,
            )

        await conn.execute(
            """
            INSERT INTO app_settings (
                key,
                value
            )
            VALUES ($1, '1')
            ON CONFLICT (key)
            DO UPDATE SET value='1'
            """,
            migration_key,
        )

    print(
        "SHARED PRODUCT LISTS READY | "
        f"migrated rows={len(rows_to_insert)}"
    )


async def _get_shared_product_list(
    owner_id: int,
    list_type: str,
):
    """
    PostgreSQL is the only source of truth.
    Every call reads the current database rows.
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, query
            FROM shared_product_lists_v1
            WHERE owner_id=$1
              AND list_type=$2
            ORDER BY id DESC
            """,
            owner_id,
            list_type,
        )

    return list(rows)


async def get_queries(
    owner_id: int,
):
    """
    Общий monitor-список владельца.
    Оба Telegram-аккаунта используют ровно эти же записи.
    """
    return await _get_shared_product_list(
        owner_id,
        LIST_TYPE_MONITOR,
    )


async def get_interest_queries(
    owner_id: int,
):
    """
    Общий analytics-only список владельца.
    По нему уведомления никогда не отправляются.
    """
    return await _get_shared_product_list(
        owner_id,
        LIST_TYPE_INTEREST,
    )


async def query_still_active(
    owner_id: int,
    query_id: int,
) -> bool:
    async with db_pool.acquire() as conn:
        return bool(
            await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM shared_product_lists_v1
                    WHERE owner_id=$1
                      AND list_type='monitor'
                      AND id=$2
                )
                """,
                owner_id,
                query_id,
            )
        )


async def _add_shared_query(
    owner_id: int,
    list_type: str,
    query: str,
) -> bool:
    query = re.sub(
        r"\s+",
        " ",
        (
            query
            or ""
        ).strip(),
    )

    if len(query) < 2:
        return False

    query_key = canonical_query_key(
        query
    )

    if not query_key:
        return False

    async with db_pool.acquire() as conn:
        inserted_id = await conn.fetchval(
            """
            INSERT INTO shared_product_lists_v1 (
                owner_id,
                list_type,
                query,
                query_key
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (
                owner_id,
                list_type,
                query_key
            )
            DO NOTHING
            RETURNING id
            """,
            owner_id,
            list_type,
            query,
            query_key,
        )

    if inserted_id is None:
        return False

    if list_type == LIST_TYPE_MONITOR:
        invalidate_query_cache(
            owner_id
        )
    else:
        invalidate_interest_cache(
            owner_id
        )

    return True


async def _add_shared_queries(
    owner_id: int,
    list_type: str,
    raw_text: str,
) -> tuple[int, list[str]]:
    entries = parse_query_entries(
        raw_text
    )

    if not entries:
        return 0, []

    existing_rows = await _get_shared_product_list(
        owner_id,
        list_type,
    )

    existing_keys = {
        canonical_query_key(
            row["query"]
        )
        for row in existing_rows
    }

    added = []
    added_keys = set()

    for query in entries:
        key = canonical_query_key(
            query
        )

        if (
            not key
            or key in existing_keys
            or key in added_keys
        ):
            continue

        added_keys.add(
            key
        )
        added.append(
            query
        )

    if not added:
        return 0, []

    async with db_pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO shared_product_lists_v1 (
                owner_id,
                list_type,
                query,
                query_key
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (
                owner_id,
                list_type,
                query_key
            )
            DO NOTHING
            """,
            [
                (
                    owner_id,
                    list_type,
                    query,
                    canonical_query_key(
                        query
                    ),
                )
                for query in added
            ],
        )

    if list_type == LIST_TYPE_MONITOR:
        invalidate_query_cache(
            owner_id
        )
    else:
        invalidate_interest_cache(
            owner_id
        )

    return (
        len(added),
        added,
    )


async def _delete_shared_query(
    owner_id: int,
    list_type: str,
    query_id: int,
) -> str | None:
    async with db_pool.acquire() as conn:
        selected = await conn.fetchrow(
            """
            SELECT id, query, query_key
            FROM shared_product_lists_v1
            WHERE owner_id=$1
              AND list_type=$2
              AND id=$3
            """,
            owner_id,
            list_type,
            query_id,
        )

        if selected is None:
            return None

        selected_query = str(
            selected["query"]
        )

        await conn.execute(
            """
            DELETE FROM shared_product_lists_v1
            WHERE owner_id=$1
              AND list_type=$2
              AND query_key=$3
            """,
            owner_id,
            list_type,
            selected["query_key"],
        )

    if list_type == LIST_TYPE_MONITOR:
        invalidate_query_cache(
            owner_id
        )
    else:
        invalidate_interest_cache(
            owner_id
        )

    return selected_query


async def _reset_shared_queries(
    owner_id: int,
    list_type: str,
) -> int:
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            """
            DELETE FROM shared_product_lists_v1
            WHERE owner_id=$1
              AND list_type=$2
            """,
            owner_id,
            list_type,
        )

    if list_type == LIST_TYPE_MONITOR:
        invalidate_query_cache(
            owner_id
        )
    else:
        invalidate_interest_cache(
            owner_id
        )

    try:
        return int(
            result.split()[-1]
        )
    except Exception:
        return 0


async def add_query(
    owner_id: int,
    query: str,
) -> bool:
    return await _add_shared_query(
        owner_id,
        LIST_TYPE_MONITOR,
        query,
    )


async def add_queries(
    owner_id: int,
    raw_text: str,
) -> tuple[int, list[str]]:
    return await _add_shared_queries(
        owner_id,
        LIST_TYPE_MONITOR,
        raw_text,
    )


async def delete_query(
    owner_id: int,
    query_id: int,
) -> str | None:
    return await _delete_shared_query(
        owner_id,
        LIST_TYPE_MONITOR,
        query_id,
    )


async def reset_all_queries(
    owner_id: int,
) -> int:
    return await _reset_shared_queries(
        owner_id,
        LIST_TYPE_MONITOR,
    )


async def cleanup_duplicate_queries():
    # UNIQUE(owner_id, list_type, query_key) не даёт создать дубли.
    return


async def migrate_multiline_queries():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, owner_id, query
            FROM shared_product_lists_v1
            WHERE list_type='monitor'
              AND (
                    query LIKE '%' || CHR(10) || '%'
                 OR query LIKE '%' || CHR(13) || '%'
              )
            ORDER BY id ASC
            """
        )

    for row in rows:
        entries = parse_query_entries(
            row["query"]
        )

        if len(entries) <= 1:
            continue

        owner_id = int(
            row["owner_id"]
        )

        for entry in entries:
            await add_query(
                owner_id,
                entry,
            )

        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM shared_product_lists_v1
                WHERE owner_id=$1
                  AND list_type='monitor'
                  AND id=$2
                """,
                owner_id,
                int(
                    row["id"]
                ),
            )

        invalidate_query_cache(
            owner_id
        )


async def interest_query_count(
    owner_id: int,
) -> int:
    return len(
        await get_interest_queries(
            owner_id
        )
    )


async def get_interest_queries_page(
    owner_id: int,
    page: int,
) -> tuple[list, int, int]:
    rows = await get_interest_queries(
        owner_id
    )

    total = len(
        rows
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

    start = (
        safe_page
        * QUERIES_PER_PAGE
    )

    return (
        list(
            rows[
                start:
                start + QUERIES_PER_PAGE
            ]
        ),
        safe_page,
        total_pages,
    )


async def add_interest_query(
    owner_id: int,
    query: str,
) -> bool:
    return await _add_shared_query(
        owner_id,
        LIST_TYPE_INTEREST,
        query,
    )


async def add_interest_queries(
    owner_id: int,
    raw_text: str,
) -> tuple[int, list[str]]:
    return await _add_shared_queries(
        owner_id,
        LIST_TYPE_INTEREST,
        raw_text,
    )


async def delete_interest_query(
    owner_id: int,
    query_id: int,
) -> str | None:
    return await _delete_shared_query(
        owner_id,
        LIST_TYPE_INTEREST,
        query_id,
    )


async def reset_all_interest_queries(
    owner_id: int,
) -> int:
    return await _reset_shared_queries(
        owner_id,
        LIST_TYPE_INTEREST,
    )


async def get_accounts(
    owner_id: int,
):
    """
    PostgreSQL is the only source of truth for Telegram profiles.
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
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

    return list(rows)


async def deactivate_invalid_account(
    account_id: int,
    reason: str,
):
    """
    Вызывается ТОЛЬКО когда Telegram однозначно подтвердил,
    что сохранённая user-session больше не авторизована.

    Временный network disconnect сюда не попадает.
    """
    row = await get_account(
        account_id
    )

    if row is None:
        return

    owner_id = int(
        row["owner_id"]
    )

    tg_client = clients.pop(
        account_id,
        None,
    )

    if tg_client is not None:
        try:
            await tg_client.disconnect()
        except Exception:
            pass

    task = telethon_tasks.pop(
        account_id,
        None,
    )

    if (
        task is not None
        and not task.done()
    ):
        task.cancel()

    recovery = _account_recovery_tasks.pop(
        account_id,
        None,
    )

    if (
        recovery is not None
        and not recovery.done()
        and recovery is not asyncio.current_task()
    ):
        recovery.cancel()

    _account_last_error[
        account_id
    ] = reason

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE telegram_accounts
            SET active=FALSE
            WHERE id=$1
            """,
            account_id,
        )

    invalidate_accounts_cache(
        owner_id
    )
    invalidate_dialogs_cache(
        owner_id
    )

    _account_dialogs_cache.pop(
        account_id,
        None,
    )

    print(
        "TELEGRAM ACCOUNT DEACTIVATED | "
        f"account_id={account_id} | "
        f"reason={reason}"
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


async def account_count(
    owner_id: int,
) -> int:
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

            invalidate_accounts_cache(owner_id)
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

        invalidate_accounts_cache(owner_id)
        return int(account_id)


async def delete_account_profile(
    owner_id: int,
    account_id: int,
) -> tuple[bool, str]:
    """
    Полностью удаляет Telegram-профиль ИЗ ЭТОГО БОТА.

    Важно:
    - Telegram-аккаунт пользователя не удаляется;
    - logout в Telegram не вызывается;
    - удаляется только сохранённая StringSession из PostgreSQL;
    - выбранные чаты именно этого профиля снимаются;
    - историческая аналитика сохраняется.
    """
    row = await get_account(
        account_id
    )

    if (
        row is None
        or int(row["owner_id"]) != owner_id
    ):
        return False, "Профиль уже удалён или не найден."

    username = row["username"]
    first_name = row["first_name"]

    display_name = (
        f"@{username}"
        if username
        else (
            first_name
            or row["label"]
            or f"Аккаунт {account_id}"
        )
    )

    # СНАЧАЛА удаляем запись из БД.
    # Тогда даже если callback monitor-task попытается recovery,
    # get_account(account_id) уже вернёт None и reconnect не случится.
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                DELETE FROM selected_chats
                WHERE owner_id=$1
                  AND account_id=$2
                """,
                owner_id,
                account_id,
            )

            await conn.execute(
                """
                DELETE FROM telegram_dialog_cache_v1
                WHERE owner_id=$1
                  AND account_id=$2
                """,
                owner_id,
                account_id,
            )

            await conn.execute(
                """
                DELETE FROM telegram_accounts
                WHERE owner_id=$1
                  AND id=$2
                """,
                owner_id,
                account_id,
            )

            # После удаления перенумеровываем подписи оставшихся
            # профилей для понятного UI: Аккаунт 1 / Аккаунт 2.
            remaining = await conn.fetch(
                """
                SELECT id
                FROM telegram_accounts
                WHERE owner_id=$1
                  AND active=TRUE
                ORDER BY id ASC
                """,
                owner_id,
            )

            for index, remaining_row in enumerate(
                remaining,
                start=1,
            ):
                await conn.execute(
                    """
                    UPDATE telegram_accounts
                    SET label=$1
                    WHERE id=$2
                    """,
                    f"Аккаунт {index}",
                    int(remaining_row["id"]),
                )

    recovery_task = _account_recovery_tasks.pop(
        account_id,
        None,
    )

    if (
        recovery_task is not None
        and not recovery_task.done()
        and recovery_task is not asyncio.current_task()
    ):
        recovery_task.cancel()

    monitor_task = telethon_tasks.pop(
        account_id,
        None,
    )

    if (
        monitor_task is not None
        and not monitor_task.done()
    ):
        monitor_task.cancel()

    tg_client = clients.pop(
        account_id,
        None,
    )

    if tg_client is not None:
        try:
            await tg_client.disconnect()
        except Exception:
            pass

    dialog_refresh = _account_dialog_refresh_tasks.pop(
        account_id,
        None,
    )

    if (
        dialog_refresh is not None
        and not dialog_refresh.done()
    ):
        dialog_refresh.cancel()

    _account_dialogs_cache.pop(
        account_id,
        None,
    )

    _account_last_heartbeat.pop(
        account_id,
        None,
    )

    _account_last_error.pop(
        account_id,
        None,
    )

    invalidate_accounts_cache(
        owner_id
    )

    invalidate_dialogs_cache(
        owner_id
    )

    invalidate_selected_cache(
        owner_id
    )

    return True, display_name


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

    invalidate_accounts_cache(owner_id)


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

    invalidate_selected_cache(owner_id)


async def get_selected_chat_ids(
    owner_id: int,
    account_id: int,
) -> set[int]:
    key = (owner_id, account_id)
    cached = _selected_ids_cache.get(key)

    if cached is not None:
        return cached

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

    result = {int(row["chat_id"]) for row in rows}
    _selected_ids_cache[key] = result
    return result


async def get_selected_chat_keys(owner_id: int) -> set[tuple[int, int]]:
    cached = _selected_keys_cache.get(owner_id)

    if cached is not None:
        return cached

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

    result = {
        (int(row["account_id"]), int(row["chat_id"]))
        for row in rows
    }
    _selected_keys_cache[owner_id] = result
    return result


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
                invalidate_selected_cache(owner_id)
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
            invalidate_selected_cache(owner_id)
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

        invalidate_selected_cache(owner_id)
        return True


async def get_selected_chats_details(owner_id: int):
    cached = _selected_details_cache.get(owner_id)

    if cached is not None:
        return cached

    account_rows = await get_accounts(owner_id)
    account_index = {
        int(row["id"]): index
        for index, row in enumerate(account_rows, start=1)
    }

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT account_id, chat_id, title
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

    _selected_details_cache[owner_id] = result
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
# PERSISTENT TELEGRAM LOGIN FLOW
# ============================================================

LOGIN_CONNECT_TIMEOUT_SECONDS = 20
LOGIN_REQUEST_TIMEOUT_SECONDS = 35


async def get_login_flow(
    owner_id: int,
):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT
                owner_id,
                stage,
                target_account_number,
                phone,
                phone_code_hash,
                session_string,
                updated_at
            FROM telegram_login_flows_v1
            WHERE owner_id=$1
            """,
            owner_id,
        )


async def save_login_flow(
    owner_id: int,
    stage: str,
    target_account_number: int,
    phone: str | None = None,
    phone_code_hash: str | None = None,
    session_string: str | None = None,
):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO telegram_login_flows_v1 (
                owner_id,
                stage,
                target_account_number,
                phone,
                phone_code_hash,
                session_string,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, NOW())
            ON CONFLICT (owner_id)
            DO UPDATE SET
                stage=EXCLUDED.stage,
                target_account_number=EXCLUDED.target_account_number,
                phone=EXCLUDED.phone,
                phone_code_hash=EXCLUDED.phone_code_hash,
                session_string=EXCLUDED.session_string,
                updated_at=NOW()
            """,
            owner_id,
            stage,
            target_account_number,
            phone,
            phone_code_hash,
            session_string,
        )


async def delete_login_flow(
    owner_id: int,
):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            DELETE FROM telegram_login_flows_v1
            WHERE owner_id=$1
            """,
            owner_id,
        )


def make_login_client(
    session_string: str | None = None,
) -> TelegramClient:
    """
    Login-client не слушает updates.
    Он используется только для connect/send_code/sign_in.
    """
    return TelegramClient(
        StringSession(
            session_string or ""
        ),
        API_ID,
        API_HASH,
        receive_updates=False,
        connection_retries=None,
        request_retries=5,
        retry_delay=1,
        auto_reconnect=True,
    )


async def connect_login_client(
    tg_client: TelegramClient,
):
    await asyncio.wait_for(
        tg_client.connect(),
        timeout=LOGIN_CONNECT_TIMEOUT_SECONDS,
    )


async def disconnect_quietly(
    tg_client: TelegramClient | None,
):
    if tg_client is None:
        return

    try:
        await tg_client.disconnect()
    except Exception:
        pass


async def process_login_phone(
    message: Message,
    state: FSMContext,
):
    owner_id = message.from_user.id

    flow = await get_login_flow(
        owner_id
    )

    if (
        flow is None
        or flow["stage"] != "waiting_phone"
    ):
        await state.clear()
        await message.answer(
            "❌ Авторизация не активна. Отправь /login ещё раз."
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

    await message.answer(
        "⏳ Номер принят. Запрашиваю код у Telegram…"
    )

    # Закрываем только старый незавершённый client этого login flow.
    old_client = login_clients.pop(
        owner_id,
        None,
    )

    await disconnect_quietly(
        old_client
    )

    tg_client = None
    keep_client = False

    try:
        tg_client = make_login_client()

        await connect_login_client(
            tg_client
        )

        result = await asyncio.wait_for(
            tg_client.send_code_request(
                phone
            ),
            timeout=LOGIN_REQUEST_TIMEOUT_SECONDS,
        )

        temporary_session = (
            tg_client.session.save()
        )

        if not temporary_session:
            raise RuntimeError(
                "Telegram не создал временную session."
            )

        await save_login_flow(
            owner_id=owner_id,
            stage="waiting_code",
            target_account_number=int(
                flow["target_account_number"]
            ),
            phone=phone,
            phone_code_hash=result.phone_code_hash,
            session_string=temporary_session,
        )

        await state.set_state(
            LoginStates.waiting_code
        )

        # КЛЮЧЕВОЙ FIX:
        # не отключаем client после send_code_request.
        # Код проверяется тем же самым подключением/сессией.
        login_clients[
            owner_id
        ] = tg_client

        keep_client = True

        await message.answer(
            "📨 Код отправлен Telegram.\n\n"
            "Введи его С ПРОБЕЛАМИ.\n"
            "Например: 1 2 3 4 5"
        )

    except asyncio.TimeoutError:
        await delete_login_flow(
            owner_id
        )
        await state.clear()

        await message.answer(
            "❌ Telegram не ответил на запрос кода за "
            f"{LOGIN_REQUEST_TIMEOUT_SECONDS} сек.\n\n"
            "Авторизация сброшена. Отправь /login и попробуй ещё раз."
        )

    except Exception as error:
        await delete_login_flow(
            owner_id
        )
        await state.clear()

        await message.answer(
            "❌ Не удалось запросить код Telegram:\n"
            f"{type(error).__name__}: {error}\n\n"
            "Отправь /login и попробуй ещё раз."
        )

    finally:
        if not keep_client:
            await disconnect_quietly(
                tg_client
            )


async def finish_persistent_login(
    message: Message,
    state: FSMContext,
    tg_client: TelegramClient,
):
    owner_id = message.from_user.id

    try:
        account_id, me = (
            await register_logged_in_client(
                owner_id,
                tg_client,
            )
        )

        await delete_login_flow(
            owner_id
        )
        await state.clear()

        # На случай старого временного клиента из предыдущей версии.
        login_clients.pop(
            owner_id,
            None,
        )

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

        saved_count = await account_count(
            owner_id
        )

        await message.answer(
            f"✅ Аккаунт {account_number} подключён: {account_name}\n\n"
            f"Сохранено: {saved_count}/{MAX_ACCOUNTS}.\n"
            "Открой 💬 Чаты — оба аккаунта останутся там постоянно.",
            reply_markup=main_menu(),
        )

    except RuntimeError as error:
        await disconnect_quietly(
            tg_client
        )
        await delete_login_flow(
            owner_id
        )
        await state.clear()

        await message.answer(
            f"❌ {error}"
        )

    except Exception as error:
        await disconnect_quietly(
            tg_client
        )
        await delete_login_flow(
            owner_id
        )
        await state.clear()

        await message.answer(
            "❌ Не удалось сохранить аккаунт:\n"
            f"{type(error).__name__}: {error}"
        )


async def process_login_code(
    message: Message,
    state: FSMContext,
):
    owner_id = message.from_user.id

    # Сразу подтверждаем получение сообщения ДО БД и Telethon.
    # Если пользователь не видит эту строку, значит сообщение
    # обработал другой/старый Bot API process.
    await message.answer(
        "🔐 Код принят. Проверяю…"
    )

    flow = await get_login_flow(
        owner_id
    )

    if (
        flow is None
        or flow["stage"] != "waiting_code"
        or not flow["phone"]
        or not flow["phone_code_hash"]
        or not flow["session_string"]
    ):
        await state.clear()
        await message.answer(
            "❌ Этап ввода кода потерян. Отправь /login ещё раз."
        )
        return

    code = re.sub(
        r"\D",
        "",
        message.text or "",
    )

    if len(code) < 4:
        await message.answer(
            "❌ Введи код Telegram цифрами.\n"
            "Можно с пробелами: 1 2 3 4 5"
        )
        return

    # Сначала используем ТОТ ЖЕ client, который запросил код.
    tg_client = login_clients.pop(
        owner_id,
        None,
    )

    reconstructed = False
    keep_client = False

    try:
        if tg_client is None:
            # Только fallback после restart Railway.
            reconstructed = True

            tg_client = make_login_client(
                flow["session_string"]
            )

            await connect_login_client(
                tg_client
            )

        elif not tg_client.is_connected():
            await connect_login_client(
                tg_client
            )

        await asyncio.wait_for(
            tg_client.sign_in(
                phone=flow["phone"],
                code=code,
                phone_code_hash=flow["phone_code_hash"],
            ),
            timeout=LOGIN_REQUEST_TIMEOUT_SECONDS,
        )

        await finish_persistent_login(
            message,
            state,
            tg_client,
        )

        # Успешный client теперь принадлежит monitor clients.
        tg_client = None

    except SessionPasswordNeededError:
        if tg_client is None:
            raise

        updated_session = (
            tg_client.session.save()
        )

        await save_login_flow(
            owner_id=owner_id,
            stage="waiting_password",
            target_account_number=int(
                flow["target_account_number"]
            ),
            phone=flow["phone"],
            phone_code_hash=flow["phone_code_hash"],
            session_string=updated_session,
        )

        await state.set_state(
            LoginStates.waiting_password
        )

        # Для 2FA также держим тот же client живым.
        login_clients[
            owner_id
        ] = tg_client

        keep_client = True

        await message.answer(
            "🔐 У аккаунта включён 2FA.\n"
            "Введи пароль Telegram."
        )

    except PhoneCodeInvalidError:
        # Неверный код не должен убивать flow/client.
        if tg_client is not None:
            login_clients[
                owner_id
            ] = tg_client
            keep_client = True

        await message.answer(
            "❌ Неверный код. Попробуй ещё раз."
        )

    except PhoneCodeExpiredError:
        await delete_login_flow(
            owner_id
        )
        await state.clear()

        await message.answer(
            "❌ Код истёк. Начни заново: /login"
        )

    except asyncio.TimeoutError:
        # При timeout сохраняем живой client, если он ещё есть.
        if (
            tg_client is not None
            and tg_client.is_connected()
        ):
            login_clients[
                owner_id
            ] = tg_client
            keep_client = True

        await message.answer(
            "❌ Telegram слишком долго отвечает.\n"
            "Попробуй отправить код ещё раз."
        )

    except Exception as error:
        # Показываем реальную ошибку, но не молчим.
        if (
            tg_client is not None
            and tg_client.is_connected()
        ):
            try:
                latest_session = (
                    tg_client.session.save()
                )

                await save_login_flow(
                    owner_id=owner_id,
                    stage="waiting_code",
                    target_account_number=int(
                        flow["target_account_number"]
                    ),
                    phone=flow["phone"],
                    phone_code_hash=flow["phone_code_hash"],
                    session_string=latest_session,
                )

                login_clients[
                    owner_id
                ] = tg_client
                keep_client = True

            except Exception:
                pass

        await message.answer(
            "❌ Ошибка проверки кода:\n"
            f"{type(error).__name__}: {error}"
        )

    finally:
        if (
            tg_client is not None
            and not keep_client
        ):
            await disconnect_quietly(
                tg_client
            )


async def process_login_password(
    message: Message,
    state: FSMContext,
):
    owner_id = message.from_user.id

    await message.answer(
        "🔐 Пароль принят. Проверяю…"
    )

    flow = await get_login_flow(
        owner_id
    )

    if (
        flow is None
        or flow["stage"] != "waiting_password"
        or not flow["session_string"]
    ):
        await state.clear()
        await message.answer(
            "❌ Этап 2FA потерян. Отправь /login ещё раз."
        )
        return

    password = message.text or ""

    if not password:
        await message.answer(
            "❌ Пароль пустой."
        )
        return

    tg_client = login_clients.pop(
        owner_id,
        None,
    )

    keep_client = False

    try:
        if tg_client is None:
            tg_client = make_login_client(
                flow["session_string"]
            )

            await connect_login_client(
                tg_client
            )

        elif not tg_client.is_connected():
            await connect_login_client(
                tg_client
            )

        await asyncio.wait_for(
            tg_client.sign_in(
                password=password
            ),
            timeout=LOGIN_REQUEST_TIMEOUT_SECONDS,
        )

        await finish_persistent_login(
            message,
            state,
            tg_client,
        )

        tg_client = None

    except asyncio.TimeoutError:
        if (
            tg_client is not None
            and tg_client.is_connected()
        ):
            login_clients[
                owner_id
            ] = tg_client
            keep_client = True

        await message.answer(
            "❌ Telegram слишком долго отвечает.\n"
            "Попробуй пароль ещё раз."
        )

    except Exception as error:
        if (
            tg_client is not None
            and tg_client.is_connected()
        ):
            login_clients[
                owner_id
            ] = tg_client
            keep_client = True

        await message.answer(
            "❌ Пароль/авторизация не прошли:\n"
            f"{type(error).__name__}: {error}\n\n"
            "Попробуй пароль ещё раз."
        )

    finally:
        if (
            tg_client is not None
            and not keep_client
        ):
            await disconnect_quietly(
                tg_client
            )


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


def make_persistent_monitor_client(
    session_string: str,
    account_id: int,
) -> TelegramClient:
    """
    Клиент мониторинга с бесконечными reconnect-попытками.

    Временный login-client и постоянный monitor-client — разные вещи.
    После логина session сохраняется, а для мониторинга создаётся
    отдельный устойчивый клиент.
    """
    tg_client = TelegramClient(
        StringSession(
            session_string
        ),
        API_ID,
        API_HASH,
        receive_updates=True,
        connection_retries=None,
        request_retries=5,
        retry_delay=1,
        auto_reconnect=True,
    )

    tg_client.add_event_handler(
        make_monitor_handler(
            account_id
        ),
        events.NewMessage(),
    )

    return tg_client


def schedule_account_recovery(
    account_id: int,
    delay: float = 0.15,
):
    """
    Запускает максимум ОДИН recovery на аккаунт.
    Можно вызывать хоть из task callback, хоть из supervisor.
    """
    old = _account_recovery_tasks.get(
        account_id
    )

    if old is not None and not old.done():
        return

    async def runner():
        try:
            if delay > 0:
                await asyncio.sleep(
                    delay
                )

            row = await get_account(
                account_id
            )

            if (
                row is None
                or not bool(row["active"])
            ):
                return

            await ensure_saved_account_client(
                row
            )

        except asyncio.CancelledError:
            raise

        except Exception as error:
            _account_last_error[
                account_id
            ] = (
                f"{type(error).__name__}: "
                f"{error}"
            )

            print(
                "ACCOUNT IMMEDIATE RECOVERY ERROR | "
                f"account_id={account_id} | "
                f"{error!r}"
            )

        finally:
            current = _account_recovery_tasks.get(
                account_id
            )

            if current is asyncio.current_task():
                _account_recovery_tasks.pop(
                    account_id,
                    None,
                )

    _account_recovery_tasks[
        account_id
    ] = asyncio.create_task(
        runner()
    )


def start_telethon_monitor(
    account_id: int,
    tg_client: TelegramClient,
):
    old_task = telethon_tasks.get(
        account_id
    )

    if old_task and not old_task.done():
        return

    task = asyncio.create_task(
        tg_client.run_until_disconnected()
    )

    telethon_tasks[
        account_id
    ] = task

    def on_done(
        finished: asyncio.Task,
    ):
        current = telethon_tasks.get(
            account_id
        )

        if current is finished:
            telethon_tasks.pop(
                account_id,
                None,
            )

        try:
            error = finished.exception()
        except asyncio.CancelledError:
            return
        except Exception as callback_error:
            error = callback_error

        if error is not None:
            _account_last_error[
                account_id
            ] = (
                f"{type(error).__name__}: "
                f"{error}"
            )

            print(
                "TELETHON MONITOR STOPPED | "
                f"account_id={account_id} | "
                f"{error!r}"
            )

        # Если приложение ещё живо, monitor-task не должен
        # оставаться мёртвым. Reconnect запускается сразу.
        schedule_account_recovery(
            account_id,
            delay=0.05,
        )

    task.add_done_callback(
        on_done
    )


def account_client_online(
    account_id: int,
) -> bool:
    tg_client = clients.get(
        account_id
    )

    if tg_client is None:
        return False

    try:
        if not tg_client.is_connected():
            return False
    except Exception:
        return False

    monitor_task = telethon_tasks.get(
        account_id
    )

    if (
        monitor_task is None
        or monitor_task.done()
    ):
        return False

    return True


async def heartbeat_account(
    row,
) -> bool:
    """
    Реальный RPC health-check, а не только is_connected().
    """
    account_id = int(
        row["id"]
    )

    tg_client = clients.get(
        account_id
    )

    if tg_client is None:
        schedule_account_recovery(
            account_id
        )
        return False

    try:
        if not tg_client.is_connected():
            schedule_account_recovery(
                account_id
            )
            return False

        await asyncio.wait_for(
            tg_client.get_me(),
            timeout=6,
        )

        _account_last_heartbeat[
            account_id
        ] = time.monotonic()

        _account_last_error.pop(
            account_id,
            None,
        )

        start_telethon_monitor(
            account_id,
            tg_client,
        )

        return True

    except AuthKeyUnregisteredError:
        await deactivate_invalid_account(
            account_id,
            "AuthKeyUnregisteredError during heartbeat",
        )

        return False

    except Exception as error:
        _account_last_error[
            account_id
        ] = (
            f"{type(error).__name__}: "
            f"{error}"
        )

        print(
            "ACCOUNT HEARTBEAT FAILED | "
            f"account_id={account_id} | "
            f"{error!r}"
        )

        try:
            await tg_client.disconnect()
        except Exception:
            pass

        clients.pop(
            account_id,
            None,
        )

        task = telethon_tasks.pop(
            account_id,
            None,
        )

        if (
            task is not None
            and not task.done()
        ):
            task.cancel()

        schedule_account_recovery(
            account_id,
            delay=0.05,
        )

        return False


async def ensure_all_accounts_running(
    owner_id: int,
    wait_timeout: float = 0.0,
):
    rows = await get_accounts(
        owner_id
    )

    offline = [
        row
        for row in rows
        if not account_client_online(
            int(row["id"])
        )
    ]

    if not offline:
        return

    if wait_timeout <= 0:
        for row in offline:
            schedule_account_recovery(
                int(row["id"]),
                delay=0.0,
            )
        return

    try:
        await asyncio.wait_for(
            asyncio.gather(
                *(
                    ensure_saved_account_client(
                        row
                    )
                    for row in offline
                ),
                return_exceptions=True,
            ),
            timeout=wait_timeout,
        )

    except asyncio.TimeoutError:
        for row in offline:
            schedule_account_recovery(
                int(row["id"]),
                delay=0.0,
            )


async def ensure_saved_account_client(
    row,
) -> TelegramClient | None:
    """
    Источник истины — telegram_accounts в PostgreSQL.

    Наличие/отсутствие объекта в clients НЕ означает,
    что аккаунт привязан или отвязан.

    Если connection временно пропал:
      - запись аккаунта в БД остаётся;
      - UI продолжает показывать аккаунт;
      - client автоматически восстанавливается.
    """
    account_id = int(
        row["id"]
    )
    owner_id = int(
        row["owner_id"]
    )

    lock = _account_connect_locks.get(
        account_id
    )

    if lock is None:
        lock = asyncio.Lock()
        _account_connect_locks[
            account_id
        ] = lock

    async with lock:
        existing = clients.get(
            account_id
        )

        if existing is not None:
            try:
                if not existing.is_connected():
                    await asyncio.wait_for(
                        existing.connect(),
                        timeout=12,
                    )

                authorized = await asyncio.wait_for(
                    existing.is_user_authorized(),
                    timeout=8,
                )

                if authorized:
                    await existing.set_receive_updates(
                        True
                    )

                    start_telethon_monitor(
                        account_id,
                        existing,
                    )

                    return existing

            except Exception as error:
                print(
                    "ACCOUNT RECONNECT EXISTING ERROR | "
                    f"account_id={account_id} | "
                    f"{error!r}"
                )

            try:
                await existing.disconnect()
            except Exception:
                pass

            clients.pop(
                account_id,
                None,
            )

        session_string = row["session"]

        if not session_string:
            return None

        tg_client = make_persistent_monitor_client(
            session_string,
            account_id,
        )

        try:
            await asyncio.wait_for(
                tg_client.connect(),
                timeout=12,
            )

            authorized = await asyncio.wait_for(
                tg_client.is_user_authorized(),
                timeout=8,
            )

            if not authorized:
                await tg_client.disconnect()

                await deactivate_invalid_account(
                    account_id,
                    "Telegram session is no longer authorized",
                )

                return None

            me = await asyncio.wait_for(
                tg_client.get_me(),
                timeout=8,
            )

            session_saved = (
                tg_client.session.save()
            )

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
                    getattr(
                        me,
                        "username",
                        None,
                    ),
                    getattr(
                        me,
                        "first_name",
                        None,
                    ),
                    session_saved,
                    account_id,
                )

            clients[
                account_id
            ] = tg_client

            _account_last_heartbeat[
                account_id
            ] = time.monotonic()

            _account_last_error.pop(
                account_id,
                None,
            )

            invalidate_accounts_cache(
                owner_id
            )

            await tg_client.set_receive_updates(
                True
            )

            start_telethon_monitor(
                account_id,
                tg_client,
            )

            print(
                "Telegram account connected/recovered | "
                f"account_id={account_id} | "
                f"tg_id={me.id}"
            )

            return tg_client

        except AuthKeyDuplicatedError as error:
            try:
                await tg_client.disconnect()
            except Exception:
                pass

            _account_last_error[
                account_id
            ] = (
                "AuthKeyDuplicatedError: "
                "эта же Telegram session запущена ещё где-то"
            )

            print(
                "CRITICAL DUPLICATE TELEGRAM SESSION | "
                f"account_id={account_id} | "
                f"{error!r}"
            )

            # Привязку не удаляем. Supervisor продолжит попытки,
            # но исправить duplicate-session можно только оставив
            # один процесс с этой StringSession.
            return None

        except AuthKeyUnregisteredError:
            try:
                await tg_client.disconnect()
            except Exception:
                pass

            await deactivate_invalid_account(
                account_id,
                "AuthKeyUnregisteredError: Telegram session revoked",
            )

            return None

        except Exception as error:
            try:
                await tg_client.disconnect()
            except Exception:
                pass

            print(
                "ACCOUNT CONNECT/RECOVER ERROR | "
                f"account_id={account_id} | "
                f"{error!r}"
            )

            return None


async def connect_saved_account(
    row,
) -> bool:
    return (
        await ensure_saved_account_client(
            row
        )
        is not None
    )


async def load_saved_clients(owner_id: int):
    accounts = await get_accounts(owner_id)

    if not accounts:
        return

    # Два аккаунта подключаем параллельно, а не один за другим.
    await asyncio.gather(
        *(connect_saved_account(row) for row in accounts),
        return_exceptions=True,
    )

    invalidate_accounts_cache(owner_id)
    invalidate_dialogs_cache(owner_id)


async def register_logged_in_client(
    owner_id: int,
    tg_client: TelegramClient,
) -> tuple[int, object]:
    """
    Уже авторизованный login-client сразу становится monitor-client.
    Никакого disconnect/reconnect после кода или 2FA.
    """
    me = await tg_client.get_me()

    session_string = (
        tg_client.session.save()
    )

    if not session_string:
        raise RuntimeError(
            "Не удалось сохранить Telegram session."
        )

    account_id = await save_account(
        owner_id=owner_id,
        session_string=session_string,
        tg_user_id=int(me.id),
        username=getattr(
            me,
            "username",
            None,
        ),
        first_name=getattr(
            me,
            "first_name",
            None,
        ),
    )

    old_client = clients.get(
        account_id
    )

    if (
        old_client is not None
        and old_client is not tg_client
    ):
        try:
            await old_client.disconnect()
        except Exception:
            pass

    old_task = telethon_tasks.pop(
        account_id,
        None,
    )

    if (
        old_task is not None
        and not old_task.done()
    ):
        old_task.cancel()

    tg_client.add_event_handler(
        make_monitor_handler(
            account_id
        ),
        events.NewMessage(),
    )

    await tg_client.set_receive_updates(
        True
    )

    clients[
        account_id
    ] = tg_client

    _account_last_heartbeat[
        account_id
    ] = time.monotonic()

    _account_last_error.pop(
        account_id,
        None,
    )

    start_telethon_monitor(
        account_id,
        tg_client,
    )

    await migrate_legacy_selected_chats(
        owner_id
    )

    invalidate_accounts_cache(
        owner_id
    )

    invalidate_dialogs_cache(
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

async def load_dialog_snapshot(
    owner_id: int,
    account_id: int,
) -> list:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT chat_id, title
            FROM telegram_dialog_cache_v1
            WHERE owner_id=$1
              AND account_id=$2
            ORDER BY LOWER(title), chat_id
            """,
            owner_id,
            account_id,
        )

    return [
        {
            "id": int(
                row["chat_id"]
            ),
            "name": (
                row["title"]
                or "Без названия"
            ),
            "account_id": account_id,
        }
        for row in rows
    ]


async def persist_dialog_snapshot(
    owner_id: int,
    account_id: int,
    dialogs: list,
):
    """
    Обновляет persistent snapshot только после УСПЕШНОГО
    чтения списка диалогов Telegram.
    """
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    DELETE FROM telegram_dialog_cache_v1
                    WHERE owner_id=$1
                      AND account_id=$2
                    """,
                    owner_id,
                    account_id,
                )

                if dialogs:
                    await conn.executemany(
                        """
                        INSERT INTO telegram_dialog_cache_v1 (
                            owner_id,
                            account_id,
                            chat_id,
                            title,
                            updated_at
                        )
                        VALUES ($1, $2, $3, $4, NOW())
                        ON CONFLICT (
                            owner_id,
                            account_id,
                            chat_id
                        )
                        DO UPDATE SET
                            title=EXCLUDED.title,
                            updated_at=NOW()
                        """,
                        [
                            (
                                owner_id,
                                account_id,
                                int(item["id"]),
                                str(
                                    item["name"]
                                    or "Без названия"
                                ),
                            )
                            for item in dialogs
                        ],
                    )

    except Exception as error:
        print(
            "DIALOG SNAPSHOT SAVE ERROR | "
            f"account_id={account_id} | "
            f"{error!r}"
        )


async def refresh_dialogs_for_account(
    row,
) -> list:
    owner_id = int(
        row["owner_id"]
    )
    account_id = int(
        row["id"]
    )

    tg_client = await ensure_saved_account_client(
        row
    )

    if tg_client is None:
        cached = _account_dialogs_cache.get(
            account_id
        )

        if cached is not None:
            return cached[1]

        snapshot = await load_dialog_snapshot(
            owner_id,
            account_id,
        )

        if snapshot:
            _account_dialogs_cache[
                account_id
            ] = (
                time.monotonic(),
                snapshot,
            )

        return snapshot

    result = []

    try:
        async for dialog in tg_client.iter_dialogs(
            limit=500
        ):
            if not (
                dialog.is_group
                or dialog.is_channel
            ):
                continue

            result.append({
                "id": int(
                    dialog.id
                ),
                "name": (
                    dialog.name
                    or "Без названия"
                ),
                "account_id": account_id,
            })

        result.sort(
            key=lambda item: (
                item["name"].casefold()
            )
        )

        _account_dialogs_cache[
            account_id
        ] = (
            time.monotonic(),
            result,
        )

        # Snapshot пишем в фоне, чтобы интерфейс не ждал PostgreSQL.
        asyncio.create_task(
            persist_dialog_snapshot(
                owner_id,
                account_id,
                result,
            )
        )

        return result

    except Exception as error:
        print(
            "DIALOG REFRESH ERROR | "
            f"account_id={account_id} | "
            f"{error!r}"
        )

        cached = _account_dialogs_cache.get(
            account_id
        )

        if cached is not None:
            return cached[1]

        snapshot = await load_dialog_snapshot(
            owner_id,
            account_id,
        )

        if snapshot:
            _account_dialogs_cache[
                account_id
            ] = (
                time.monotonic(),
                snapshot,
            )

        return snapshot


def schedule_dialog_refresh(
    row,
):
    account_id = int(
        row["id"]
    )

    old_task = _account_dialog_refresh_tasks.get(
        account_id
    )

    if (
        old_task is not None
        and not old_task.done()
    ):
        return

    async def runner():
        try:
            await refresh_dialogs_for_account(
                row
            )
        finally:
            _account_dialog_refresh_tasks.pop(
                account_id,
                None,
            )

    _account_dialog_refresh_tasks[
        account_id
    ] = asyncio.create_task(
        runner()
    )


async def get_dialogs_for_account(
    row,
    force_refresh: bool = False,
):
    """
    Stale-while-revalidate:
    - свежий RAM cache -> мгновенно;
    - старый RAM cache -> мгновенно + refresh в фоне;
    - DB snapshot -> мгновенно + refresh в фоне;
    - только если данных нет вообще, ждём Telegram.
    """
    owner_id = int(
        row["owner_id"]
    )
    account_id = int(
        row["id"]
    )
    now = time.monotonic()

    cached = _account_dialogs_cache.get(
        account_id
    )

    if (
        cached is not None
        and not force_refresh
    ):
        age = (
            now
            - cached[0]
        )

        if age < DIALOGS_CACHE_SECONDS:
            return cached[1]

        schedule_dialog_refresh(
            row
        )

        return cached[1]

    if not force_refresh:
        snapshot = await load_dialog_snapshot(
            owner_id,
            account_id,
        )

        if snapshot:
            _account_dialogs_cache[
                account_id
            ] = (
                now,
                snapshot,
            )

            schedule_dialog_refresh(
                row
            )

            return snapshot

    return await refresh_dialogs_for_account(
        row
    )


async def get_all_dialogs(
    owner_id: int,
    force_refresh: bool = False,
):
    """
    ВСЕ сохранённые аккаунты участвуют в списке чатов.

    Раньше здесь был фильтр:
        if account_id in clients

    Из-за него временно disconnected client полностью исчезал
    из интерфейса. Теперь clients — только runtime connection,
    а не источник истины.
    """
    now = time.monotonic()

    cached = _dialogs_cache.get(
        owner_id
    )

    if (
        not force_refresh
        and cached is not None
        and now - cached[0] < DIALOGS_CACHE_SECONDS
    ):
        return cached[1]

    account_rows = await get_accounts(
        owner_id
    )

    if not account_rows:
        _dialogs_cache[
            owner_id
        ] = (
            now,
            [],
        )

        return []

    dialog_lists = await asyncio.gather(
        *(
            get_dialogs_for_account(
                row,
                force_refresh=force_refresh,
            )
            for row in account_rows
        ),
        return_exceptions=True,
    )

    result = []

    account_position = {
        int(row["id"]): index
        for index, row in enumerate(
            account_rows,
            start=1,
        )
    }

    for row, dialogs in zip(
        account_rows,
        dialog_lists,
    ):
        if isinstance(
            dialogs,
            Exception,
        ):
            dialogs = []

        account_id = int(
            row["id"]
        )

        username = row[
            "username"
        ]

        first_name = row[
            "first_name"
        ]

        account_name = (
            f"@{username}"
            if username
            else (
                first_name
                or row["label"]
            )
        )

        for dialog in dialogs:
            item = dict(
                dialog
            )

            item[
                "account_index"
            ] = account_position.get(
                account_id,
                1,
            )

            item[
                "account_name"
            ] = account_name

            result.append(
                item
            )

    result.sort(
        key=lambda item: (
            item["account_index"],
            item["name"].casefold(),
        )
    )

    _dialogs_cache[
        owner_id
    ] = (
        time.monotonic(),
        result,
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
    if not seller_key:
        return True

    request_key = normalize_text(found_request)[:220]

    if not request_key:
        return True

    memory_key = (owner_id, seller_key, request_key)
    now = time.monotonic()

    if len(_analytics_dedup_memory) > 5000:
        cutoff = now - (DEDUP_WINDOW_SECONDS * 2)
        for key, seen_at in list(_analytics_dedup_memory.items()):
            if seen_at < cutoff:
                _analytics_dedup_memory.pop(key, None)

    previous = _analytics_dedup_memory.get(memory_key)

    if previous is not None and now - previous < DEDUP_WINDOW_SECONDS:
        return False

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO recent_all_request_analytics_dedup (
                owner_id, seller_key, request_key, first_seen_at
            )
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (owner_id, seller_key, request_key)
            DO UPDATE
            SET first_seen_at=EXCLUDED.first_seen_at
            WHERE recent_all_request_analytics_dedup.first_seen_at
                  < NOW() - ($4 * INTERVAL '1 second')
            RETURNING first_seen_at
            """,
            owner_id, seller_key, request_key, DEDUP_WINDOW_SECONDS,
        )

    if row is not None:
        _analytics_dedup_memory[memory_key] = now
        return True

    # DB says it's a recent duplicate: remember locally too.
    _analytics_dedup_memory[memory_key] = now
    return False


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
    key = (owner_id, period)
    now = time.monotonic()
    cached = _analytics_rows_cache.get(key)

    if cached is not None and now - cached[0] < ANALYTICS_CACHE_SECONDS:
        return cached[1]

    start = period_start(period)

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT found_request, brand, created_at
            FROM all_request_analytics_events
            WHERE owner_id=$1
              AND created_at >= $2
            ORDER BY created_at ASC
            """,
            owner_id,
            start,
        )

    result = list(rows)
    _analytics_rows_cache[key] = (time.monotonic(), result)
    return result


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
                    text="⭐ Интересующие",
                    callback_data="interests:page:0",
                ),
                InlineKeyboardButton(
                    text="📊 Аналитика",
                    callback_data="analytics:7",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="👥 Профили",
                    callback_data="profiles",
                ),
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
# PROFILES
# ============================================================

def account_display_name(
    row,
) -> str:
    return (
        f"@{row['username']}"
        if row["username"]
        else (
            row["first_name"]
            or row["label"]
            or f"Аккаунт {row['id']}"
        )
    )


async def profiles_text(
    owner_id: int,
) -> str:
    rows = await get_accounts(
        owner_id
    )

    if not rows:
        return (
            "👥 ПРОФИЛИ\n\n"
            "Пока нет подключённых Telegram-аккаунтов.\n\n"
            "Для подключения отправь /login."
        )

    lines = []

    for index, row in enumerate(
        rows,
        start=1,
    ):
        account_id = int(
            row["id"]
        )

        online = account_client_online(
            account_id
        )

        if not online:
            schedule_account_recovery(
                account_id,
                delay=0.0,
            )

        lines.append(
            f"{'🟢' if online else '🟡'} "
            f"{index}. "
            f"{account_display_name(row)}"
        )

    return (
        "👥 ПРОФИЛИ\n\n"
        "Источник: PostgreSQL\n\n"
        + "\n".join(lines)
        + "\n\n"
        f"Сохранено: {len(rows)}/{MAX_ACCOUNTS}\n\n"
        "🟢 — связь активна\n"
        "🟡 — профиль сохранён, идёт автоматическое переподключение\n\n"
        "Удаление профиля удаляет его только из этого бота."
    )


async def profiles_keyboard(
    owner_id: int,
) -> InlineKeyboardMarkup:
    rows = await get_accounts(
        owner_id
    )

    keyboard_rows = []

    for index, row in enumerate(
        rows,
        start=1,
    ):
        account_id = int(
            row["id"]
        )

        name = account_display_name(
            row
        )

        if len(name) > 25:
            name = (
                name[:22]
                + "..."
            )

        keyboard_rows.append([
            InlineKeyboardButton(
                text=(
                    f"🗑 Удалить {index}️⃣ {name}"
                ),
                callback_data=(
                    f"profile:delete:{account_id}"
                ),
            )
        ])

    if len(rows) < MAX_ACCOUNTS:
        keyboard_rows.append([
            InlineKeyboardButton(
                text="➕ Добавить аккаунт",
                callback_data="profile:add",
            )
        ])

    keyboard_rows.append([
        InlineKeyboardButton(
            text="🔄 Проверить связь",
            callback_data="profiles:refresh",
        )
    ])

    keyboard_rows.append([
        InlineKeyboardButton(
            text="◀️ Главное меню",
            callback_data="home",
        )
    ])

    return InlineKeyboardMarkup(
        inline_keyboard=keyboard_rows
    )


async def render_profiles(
    target_message,
    owner_id: int,
    edit: bool,
    force_check: bool = False,
):
    if force_check:
        await ensure_all_accounts_running(
            owner_id,
            wait_timeout=2.0,
        )

    text = await profiles_text(
        owner_id
    )

    keyboard = await profiles_keyboard(
        owner_id
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


@dp.callback_query(F.data == "profiles")
async def cb_profiles(
    callback: CallbackQuery,
    state: FSMContext,
):
    if not await guard_callback(
        callback
    ):
        return

    await state.clear()

    await render_profiles(
        callback.message,
        callback.from_user.id,
        True,
    )

    await callback.answer()


@dp.callback_query(F.data == "profiles:refresh")
async def cb_profiles_refresh(
    callback: CallbackQuery,
    state: FSMContext,
):
    if not await guard_callback(
        callback
    ):
        return

    await state.clear()

    await callback.answer(
        "Проверяю связь…"
    )

    await render_profiles(
        callback.message,
        callback.from_user.id,
        True,
        force_check=True,
    )


@dp.callback_query(F.data == "profile:add")
async def cb_profile_add(
    callback: CallbackQuery,
):
    if not await guard_callback(
        callback
    ):
        return

    count = await account_count(
        callback.from_user.id
    )

    if count >= MAX_ACCOUNTS:
        await callback.answer(
            "Уже подключено 2 из 2.",
            show_alert=True,
        )
        return

    await callback.message.edit_text(
        "➕ ДОБАВЛЕНИЕ ПРОФИЛЯ\n\n"
        "Отправь команду /login.\n"
        "Бот подключит следующий Telegram-аккаунт.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="◀️ Профили",
                        callback_data="profiles",
                    )
                ]
            ]
        ),
    )

    await callback.answer()


@dp.callback_query(F.data.startswith("profile:delete:"))
async def cb_profile_delete_request(
    callback: CallbackQuery,
):
    if not await guard_callback(
        callback
    ):
        return

    try:
        account_id = int(
            callback.data.split(":")[2]
        )
    except Exception:
        await callback.answer(
            "Ошибка профиля.",
            show_alert=True,
        )
        return

    row = await get_account(
        account_id
    )

    if (
        row is None
        or int(row["owner_id"]) != callback.from_user.id
        or not bool(row["active"])
    ):
        await callback.answer(
            "Профиль уже не активен.",
            show_alert=True,
        )

        await render_profiles(
            callback.message,
            callback.from_user.id,
            True,
        )
        return

    name = account_display_name(
        row
    )

    await callback.message.edit_text(
        "⚠️ УДАЛИТЬ ПРОФИЛЬ?\n\n"
        f"{name}\n\n"
        "Будет удалена сохранённая сессия этого профиля "
        "из бота и сняты выбранные чаты этого аккаунта.\n\n"
        "Сам Telegram-аккаунт НЕ удаляется.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✅ Да, удалить",
                        callback_data=(
                            f"profile:confirm:{account_id}"
                        ),
                    ),
                ],
                [
                    InlineKeyboardButton(
                        text="↩️ Отмена",
                        callback_data="profiles",
                    ),
                ],
            ]
        ),
    )

    await callback.answer()


@dp.callback_query(F.data.startswith("profile:confirm:"))
async def cb_profile_delete_confirm(
    callback: CallbackQuery,
    state: FSMContext,
):
    if not await guard_callback(
        callback
    ):
        return

    try:
        account_id = int(
            callback.data.split(":")[2]
        )
    except Exception:
        await callback.answer(
            "Ошибка профиля.",
            show_alert=True,
        )
        return

    await state.clear()

    deleted, name = await delete_account_profile(
        callback.from_user.id,
        account_id,
    )

    if not deleted:
        await callback.answer(
            name,
            show_alert=True,
        )

        await render_profiles(
            callback.message,
            callback.from_user.id,
            True,
        )
        return

    await callback.answer(
        "Профиль удалён"
    )

    await callback.message.edit_text(
        f"✅ Профиль {name} удалён из бота.\n\n"
        "Telegram-аккаунт не затронут.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="👥 Профили",
                        callback_data="profiles",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        text="◀️ Главное меню",
                        callback_data="home",
                    ),
                ],
            ]
        ),
    )


# ============================================================
# STATUS
# ============================================================

async def status_text(owner_id: int):
    (
        account_rows,
        query_rows,
        interest_rows,
        selected_chats,
        dialogs,
    ) = await asyncio.gather(
        get_accounts(owner_id),
        get_queries(owner_id),
        get_interest_queries(owner_id),
        get_selected_chats_details(owner_id),
        get_all_dialogs(owner_id),
    )

    connected_lines = []
    online_count = 0

    for index, row in enumerate(account_rows, start=1):
        account_id = int(row["id"])
        connected = account_client_online(
            account_id
        )

        if connected:
            online_count += 1

        username = row["username"]
        first_name = row["first_name"]
        name = (
            f"@{username}"
            if username
            else (
                first_name
                or row["label"]
            )
        )

        status_icon = (
            "🟢"
            if connected
            else "🟡"
        )

        suffix = ""

        last_error = _account_last_error.get(
            account_id,
            "",
        )

        if "AuthKeyDuplicatedError" in last_error:
            suffix = " ⚠️ duplicate session"

        connected_lines.append(
            f"{status_icon} "
            f"{index}. {name}"
            f"{suffix}"
        )

        if not connected:
            schedule_account_recovery(
                account_id,
                delay=0.0,
            )

    if not connected_lines:
        connected_lines = [
            "🔴 Telegram-аккаунты не подключены"
        ]

    return (
        "📡 СТАТУС\n\n"
        + "\n".join(connected_lines)
        + "\n\n"
        f"👥 Сохранено: {len(account_rows)}/{MAX_ACCOUNTS}\n"
        f"🟢 Сейчас онлайн: {online_count}/{len(account_rows)}\n"
        f"💬 Доступно чатов: {len(dialogs)}\n"
        f"✅ Выбрано: {len(selected_chats)}\n"
        f"🔎 Запросов: {len(query_rows)}\n"
        f"⭐ Интересующих: {len(interest_rows)}"
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


@dp.message(Command("purgeaccounts"))
async def cmd_purge_inactive_accounts(
    message: Message,
):
    """
    Скрытая служебная команда:
    физически удаляет только уже неактивные/отвязанные аккаунты.
    В BotFather Menu не показывается.
    """
    if not await guard_message(
        message
    ):
        return

    owner_id = message.from_user.id

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            DELETE FROM telegram_accounts
            WHERE owner_id=$1
              AND active=FALSE
            RETURNING id
            """,
            owner_id,
        )

    invalidate_accounts_cache(
        owner_id
    )
    invalidate_dialogs_cache(
        owner_id
    )

    await message.answer(
        f"🧹 Удалено старых отвязанных аккаунтов: {len(rows)}"
    )


# ============================================================
# LOGIN
# ============================================================

@dp.message(Command("login"))
async def cmd_login(
    message: Message,
    state: FSMContext,
):
    """
    Одна команда /login:
      1-й аккаунт -> Аккаунт 1
      2-й аккаунт -> Аккаунт 2

    Этап авторизации теперь хранится в PostgreSQL,
    поэтому не зависит от MemoryStorage / restart Railway.
    """
    if not await guard_message(
        message
    ):
        return

    owner_id = message.from_user.id

    count = await account_count(
        owner_id
    )

    if count >= MAX_ACCOUNTS:
        await delete_login_flow(
            owner_id
        )
        await state.clear()

        await message.answer(
            "✅ Уже подключено 2 из 2 Telegram-аккаунтов.\n\n"
            "Оба могут одновременно собирать сообщения."
        )
        return

    # Сбрасываем старую недоделанную авторизацию.
    await close_login_client(
        owner_id
    )
    await delete_login_flow(
        owner_id
    )

    target_number = count + 1

    await save_login_flow(
        owner_id=owner_id,
        stage="waiting_phone",
        target_account_number=target_number,
    )

    await state.set_state(
        LoginStates.waiting_phone
    )

    await message.answer(
        f"👤 Подключаем аккаунт {target_number} из {MAX_ACCOUNTS}.\n\n"
        "📱 Отправь номер Telegram.\n\n"
        "Например:\n"
        "+37212345678"
    )


@dp.message(Command("code"))
async def cmd_login_code_explicit(
    message: Message,
    state: FSMContext,
):
    """
    Скрытый аварийный способ:
      /code 1 2 3 4 5

    Не показывается в Bot Menu.
    """
    if not await guard_message(
        message
    ):
        return

    flow = await get_login_flow(
        message.from_user.id
    )

    if (
        flow is None
        or flow["stage"] != "waiting_code"
    ):
        await message.answer(
            "ℹ️ Сейчас бот не ждёт код Telegram."
        )
        return

    raw = (
        message.text
        or ""
    )

    raw = re.sub(
        r"^/code(?:@\w+)?\s*",
        "",
        raw,
        flags=re.IGNORECASE,
    )

    # Создаём message-копию нельзя/не нужно:
    # process_login_code читает message.text.
    # Поэтому временно кладём код в FSM data и вызываем
    # отдельный helper через простой proxy невозможно.
    # Для команды безопаснее дублировать только parsing:
    code = re.sub(
        r"\D",
        "",
        raw,
    )

    if len(code) < 4:
        await message.answer(
            "❌ Формат: /code 1 2 3 4 5"
        )
        return

    # process_login_code может читать команду целиком:
    # re.sub удалит все нецифровые символы, но цифры в '/code'
    # отсутствуют, поэтому получится именно Telegram code.
    await process_login_code(
        message,
        state,
    )


@dp.message(LoginStates.waiting_phone)
async def login_phone(
    message: Message,
    state: FSMContext,
):
    if not await guard_message(
        message
    ):
        return

    await process_login_phone(
        message,
        state,
    )


@dp.message(LoginStates.waiting_code)
async def login_code(
    message: Message,
    state: FSMContext,
):
    if not await guard_message(
        message
    ):
        return

    await process_login_code(
        message,
        state,
    )


@dp.message(LoginStates.waiting_password)
async def login_password(
    message: Message,
    state: FSMContext,
):
    if not await guard_message(
        message
    ):
        return

    await process_login_password(
        message,
        state,
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
    return len(await get_queries(owner_id))


async def get_queries_page(
    owner_id: int,
    page: int,
) -> tuple[list, int, int]:
    rows = await get_queries(owner_id)
    total = len(rows)
    total_pages = max(
        1,
        (total + QUERIES_PER_PAGE - 1) // QUERIES_PER_PAGE,
    )
    safe_page = max(0, min(int(page), total_pages - 1))
    start = safe_page * QUERIES_PER_PAGE
    return (
        list(rows[start:start + QUERIES_PER_PAGE]),
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

    count = len(
        await get_queries(
            owner_id
        )
    )

    await message.answer(
        f"🔎 Активных запросов: {count}"
    )



# ============================================================
# INTERESTING QUERIES
# ============================================================

def interests_page_keyboard(
    page: int,
    total_pages: int,
) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(
                text="➕ Добавить",
                callback_data="interest:add",
            ),
            InlineKeyboardButton(
                text="🗑 Удалить",
                callback_data=f"interest:delete:page:{page}",
            ),
        ],
        [
            InlineKeyboardButton(
                text="📊 Статистика",
                callback_data="intereststats:7",
            )
        ],
    ]

    if total_pages > 1:
        nav = []

        if page > 0:
            nav.append(
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=f"interests:page:{page - 1}",
                )
            )

        nav.append(
            InlineKeyboardButton(
                text=f"{page + 1}/{total_pages}",
                callback_data="interest:page:noop",
            )
        )

        if page < total_pages - 1:
            nav.append(
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=f"interests:page:{page + 1}",
                )
            )

        keyboard.append(
            nav
        )

    keyboard.append([
        InlineKeyboardButton(
            text="◀️ Главное меню",
            callback_data="home",
        )
    ])

    return InlineKeyboardMarkup(
        inline_keyboard=keyboard
    )


async def render_interests_page(
    owner_id: int,
    page: int = 0,
) -> tuple[str, InlineKeyboardMarkup]:
    rows, page, total_pages = (
        await get_interest_queries_page(
            owner_id,
            page,
        )
    )

    total = await interest_query_count(
        owner_id
    )

    if not rows:
        text = (
            "⭐ ИНТЕРЕСУЮЩИЕ ЗАПРОСЫ\n\n"
            "Отдельный список только для аналитики.\n"
            "Уведомления по нему НЕ отправляются.\n"
            "Можно открыть разбивку по реально запрашиваемым моделям/вариантам.\n\n"
            "Пока список пуст."
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
            "⭐ ИНТЕРЕСУЮЩИЕ ЗАПРОСЫ\n\n"
            "Источник: PostgreSQL\n"
            "Общий список для аккаунтов 1️⃣ и 2️⃣.\n"
            "Только аналитика — уведомления НЕ отправляются.\n\n"
            + "\n".join(lines)
            + "\n\n"
            + f"Всего в списке: {total}"
        )

    return (
        text,
        interests_page_keyboard(
            page,
            total_pages,
        ),
    )


@dp.message(Command("interests"))
async def cmd_interests(
    message: Message,
):
    """
    Скрытая команда. В BotFather Menu её не показываем.
    """
    if not await guard_message(
        message
    ):
        return

    text, markup = await render_interests_page(
        message.from_user.id,
        0,
    )

    await message.answer(
        text,
        reply_markup=markup,
    )


@dp.callback_query(F.data == "interests")
async def cb_interests_legacy(
    callback: CallbackQuery,
):
    if not await guard_callback(
        callback
    ):
        return

    text, markup = await render_interests_page(
        callback.from_user.id,
        0,
    )

    await callback.message.edit_text(
        text,
        reply_markup=markup,
    )

    await callback.answer()


@dp.callback_query(F.data.startswith("interests:page:"))
async def cb_interests_page(
    callback: CallbackQuery,
):
    if not await guard_callback(
        callback
    ):
        return

    try:
        page = int(
            callback.data.rsplit(
                ":",
                1,
            )[1]
        )
    except (
        ValueError,
        IndexError,
    ):
        await callback.answer(
            "Ошибка страницы.",
            show_alert=True,
        )
        return

    text, markup = await render_interests_page(
        callback.from_user.id,
        page,
    )

    await callback.message.edit_text(
        text,
        reply_markup=markup,
    )

    await callback.answer()


@dp.callback_query(F.data == "interest:page:noop")
async def cb_interest_page_noop(
    callback: CallbackQuery,
):
    await callback.answer()


@dp.callback_query(F.data == "interest:add")
async def cb_interest_add(
    callback: CallbackQuery,
    state: FSMContext,
):
    if not await guard_callback(
        callback
    ):
        return

    await state.set_state(
        InterestQueryStates.waiting_query
    )

    await callback.message.answer(
        "⭐ Напиши интересующие запросы.\n\n"
        "Можно один или несколько строками.\n\n"
        "Например:\n"
        "iPhone 17 Pro 256\n"
        "PS5 Pro\n"
        "Google Fitbit Air\n\n"
        "Это НЕ включает уведомления — "
        "позиции нужны только для отдельной статистики."
    )

    await callback.answer()


@dp.message(InterestQueryStates.waiting_query)
async def interest_query_input(
    message: Message,
    state: FSMContext,
):
    if not await guard_message(
        message
    ):
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

    count, added = await add_interest_queries(
        message.from_user.id,
        raw_queries,
    )

    await state.clear()

    if count == 0:
        text, markup = await render_interests_page(
            message.from_user.id,
            0,
        )

        await message.answer(
            "ℹ️ Эти позиции уже есть.\n\n"
            + text,
            reply_markup=markup,
        )
        return

    text, markup = await render_interests_page(
        message.from_user.id,
        0,
    )

    await message.answer(
        "✅ Добавлено в интересующие: "
        f"{count}\n\n"
        + "\n".join(
            f"• {query}"
            for query in added
        )
        + "\n\n"
        + text,
        reply_markup=markup,
    )


async def build_interest_delete_page(
    owner_id: int,
    page: int,
) -> tuple[str, InlineKeyboardMarkup]:
    rows, page, total_pages = (
        await get_interest_queries_page(
            owner_id,
            page,
        )
    )

    total = await interest_query_count(
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
                    f"interest:remove:"
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
                        f"interest:delete:page:"
                        f"{page - 1}"
                    ),
                )
            )

        nav.append(
            InlineKeyboardButton(
                text=f"{page + 1}/{total_pages}",
                callback_data="interest:page:noop",
            )
        )

        if page < total_pages - 1:
            nav.append(
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=(
                        f"interest:delete:page:"
                        f"{page + 1}"
                    ),
                )
            )

        keyboard.append(
            nav
        )

    keyboard.append([
        InlineKeyboardButton(
            text="◀️ К интересующим",
            callback_data=f"interests:page:{page}",
        )
    ])

    text = (
        "🗑 УДАЛЕНИЕ ИНТЕРЕСУЮЩИХ\n\n"
        f"Всего: {total}\n"
        f"Страница: {page + 1}/{total_pages}\n\n"
        "Выбери позицию."
    )

    return (
        text,
        InlineKeyboardMarkup(
            inline_keyboard=keyboard
        ),
    )


@dp.callback_query(F.data.startswith("interest:delete:page:"))
async def cb_interest_delete_page(
    callback: CallbackQuery,
):
    if not await guard_callback(
        callback
    ):
        return

    try:
        page = int(
            callback.data.rsplit(
                ":",
                1,
            )[1]
        )
    except (
        ValueError,
        IndexError,
    ):
        await callback.answer(
            "Ошибка страницы.",
            show_alert=True,
        )
        return

    total = await interest_query_count(
        callback.from_user.id
    )

    if total == 0:
        text, markup = await render_interests_page(
            callback.from_user.id,
            0,
        )

        await callback.message.edit_text(
            text,
            reply_markup=markup,
        )

        await callback.answer(
            "Список пуст."
        )
        return

    text, markup = await build_interest_delete_page(
        callback.from_user.id,
        page,
    )

    await callback.message.edit_text(
        text,
        reply_markup=markup,
    )

    await callback.answer()


@dp.callback_query(F.data.startswith("interest:remove:"))
async def cb_interest_remove(
    callback: CallbackQuery,
):
    if not await guard_callback(
        callback
    ):
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

    except (
        ValueError,
        IndexError,
    ):
        await callback.answer(
            "Ошибка кнопки.",
            show_alert=True,
        )
        return

    deleted = await delete_interest_query(
        callback.from_user.id,
        query_id,
    )

    total = await interest_query_count(
        callback.from_user.id
    )

    if total == 0:
        text, markup = await render_interests_page(
            callback.from_user.id,
            0,
        )

    else:
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

        text, markup = await build_interest_delete_page(
            callback.from_user.id,
            safe_page,
        )

    await callback.message.edit_text(
        text,
        reply_markup=markup,
    )

    await callback.answer(
        (
            f"Удалено: {deleted}"
            if deleted
            else "Позиция уже удалена."
        )
    )


@dp.message(Command("resetinterests"))
async def cmd_reset_interests(
    message: Message,
):
    """
    Скрытая служебная команда.
    """
    if not await guard_message(
        message
    ):
        return

    total = await interest_query_count(
        message.from_user.id
    )

    if total == 0:
        await message.answer(
            "ℹ️ Список интересующих уже пуст."
        )
        return

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗑 Да, очистить",
                    callback_data="interests:reset:confirm",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Отмена",
                    callback_data="interests:page:0",
                )
            ],
        ]
    )

    await message.answer(
        "⚠️ ОЧИСТИТЬ ИНТЕРЕСУЮЩИЕ ЗАПРОСЫ?\n\n"
        f"Сейчас в списке: {total}.\n\n"
        "Обычные запросы, чаты и аналитика не затрагиваются.",
        reply_markup=keyboard,
    )


@dp.callback_query(F.data == "interests:reset:confirm")
async def cb_reset_interests_confirm(
    callback: CallbackQuery,
):
    if not await guard_callback(
        callback
    ):
        return

    deleted = await reset_all_interest_queries(
        callback.from_user.id
    )

    text, markup = await render_interests_page(
        callback.from_user.id,
        0,
    )

    await callback.message.edit_text(
        "✅ Список интересующих очищен.\n"
        f"Удалено: {deleted}\n\n"
        + text,
        reply_markup=markup,
    )

    await callback.answer(
        "Очищено ✅"
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
            callback_data=f"chatsrefresh:{page}",
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
        account_id = int(
            row["id"]
        )

        online = account_client_online(
            account_id
        )

        name = (
            f"@{row['username']}"
            if row["username"]
            else (
                row["first_name"]
                or row["label"]
            )
        )

        lines.append(
            f"{index}️⃣ "
            f"{'🟢' if online else '🟡'} "
            f"{name}"
        )

        if not online:
            # Не блокируем интерфейс ожиданием reconnect.
            asyncio.create_task(
                ensure_saved_account_client(
                    row
                )
            )

    return (
        "\n".join(
            lines
        )
        if lines
        else "🔴 Нет сохранённых Telegram-аккаунтов"
    )


async def render_chats(
    target_message,
    owner_id: int,
    page: int,
    edit: bool,
    force_refresh: bool = False,
):
    account_rows = await get_accounts(
        owner_id
    )

    if account_rows:
        # Обычно возвращается мгновенно. Если один connection умер,
        # даём до 2 сек на немедленный reconnect, затем UI всё равно
        # открывается из snapshot/cache.
        await ensure_all_accounts_running(
            owner_id,
            wait_timeout=2.0,
        )

    if not account_rows:
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
        owner_id,
        force_refresh=force_refresh,
    )

    if not dialogs:
        accounts_caption = await connected_accounts_caption(
            owner_id
        )

        text = (
            "💬 Telegram пока не вернул список групп/каналов.\n\n"
            f"{accounts_caption}\n\n"
            "🟡 = аккаунт сохранён, идёт переподключение.\n"
            "Он НЕ удалён и повторно привязывать его не нужно."
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


@dp.callback_query(F.data.startswith("chatsrefresh:"))
async def cb_chats_refresh(
    callback: CallbackQuery,
    state: FSMContext,
):
    if not await guard_callback(callback):
        return

    await callback.answer("Обновляю…")
    await state.clear()

    page = int(callback.data.split(":")[1])

    await render_chats(
        callback.message,
        callback.from_user.id,
        page,
        True,
        force_refresh=True,
    )


@dp.callback_query(F.data == "chats:0")
async def cb_chats_first_page(
    callback: CallbackQuery,
    state: FSMContext,
):
    """
    Вход из главного меню ВСЕГДА открывает первую страницу.
    """
    if not await guard_callback(
        callback
    ):
        return

    await callback.answer()
    await state.clear()

    await render_chats(
        callback.message,
        callback.from_user.id,
        0,
        True,
    )


@dp.callback_query(
    F.data.startswith("chats:")
    & (F.data != "chats:0")
)
async def cb_chats(
    callback: CallbackQuery,
    state: FSMContext,
):
    if not await guard_callback(callback):
        return

    await callback.answer()

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
# INTEREST MODEL / VARIANT BREAKDOWN
# ============================================================

RAM_VALUES = {
    2, 3, 4, 6, 8, 12, 16, 18, 24, 32, 36, 48, 64
}

STORAGE_VALUES = {
    32, 64, 128, 256, 512, 1024, 2048, 4096
}


def extract_memory_configuration(
    text: str,
) -> tuple[int | None, int | None]:
    """
    Возвращает (RAM, STORAGE) в GB.

    Понимает варианты:
      6/256
      8/256
      12/256
      8+256
      8-256
      8 x 256
      8GB/256GB
      8 ГБ + 256 ГБ
      8 RAM 256
      8GB RAM 256GB
      8 256          (только если числа похожи на RAM/storage)

    1TB / 2TB для накопителя переводятся в 1024 / 2048 GB.
    """
    raw = (text or "").lower()
    raw = raw.replace("гбайт", "gb")
    raw = raw.replace("гб", "gb")
    raw = raw.replace("tb", " tb ")
    raw = raw.replace("тб", " tb ")

    # Normalize common separators.
    raw = re.sub(
        r"\s*[хx×]\s*",
        "/",
        raw,
    )

    # Explicit RAM/storage pair with GB units and separator.
    pair_patterns = [
        r"(?<!\d)(\d{1,2})\s*(?:gb)?\s*[/+]\s*(\d{2,4})\s*(gb|tb)?(?!\d)",
        r"(?<!\d)(\d{1,2})\s*(?:gb)?\s*-\s*(\d{2,4})\s*(gb|tb)?(?!\d)",
        r"(?<!\d)(\d{1,2})\s*(?:gb)?\s*(?:ram|озу)\s*[/+:\-]?\s*(\d{2,4})\s*(gb|tb)?(?!\d)",
        r"(?<!\d)(\d{1,2})\s*gb\s+(?:ram\s+)?(\d{2,4})\s*(gb|tb)?(?!\d)",
    ]

    for pattern in pair_patterns:
        match = re.search(
            pattern,
            raw,
            flags=re.IGNORECASE,
        )

        if not match:
            continue

        ram = int(match.group(1))
        storage = int(match.group(2))
        unit = (
            match.group(3)
            if match.lastindex and match.lastindex >= 3
            else None
        )

        if unit and unit.lower() == "tb":
            storage *= 1024

        if (
            ram in RAM_VALUES
            and storage in STORAGE_VALUES
        ):
            return ram, storage

    # Explicit storage TB may coexist with RAM in forms such as "12/1TB".
    tb_pair = re.search(
        r"(?<!\d)(\d{1,2})\s*(?:gb)?\s*[/+\-]\s*(1|2|4)\s*tb\b",
        raw,
        flags=re.IGNORECASE,
    )

    if tb_pair:
        ram = int(
            tb_pair.group(1)
        )
        storage = int(
            tb_pair.group(2)
        ) * 1024

        if (
            ram in RAM_VALUES
            and storage in STORAGE_VALUES
        ):
            return ram, storage

    # Compact two-number form: "8 256".
    # We only accept it when first number is plausible RAM and
    # second is plausible storage, so model numbers like A57 256
    # cannot become a fake 57/256 configuration.
    compact = re.search(
        r"(?<!\d)(\d{1,2})\s+(\d{2,4})(?!\d)",
        raw,
    )

    if compact:
        ram = int(
            compact.group(1)
        )
        storage = int(
            compact.group(2)
        )

        if (
            ram in RAM_VALUES
            and storage in STORAGE_VALUES
        ):
            return ram, storage

    return None, None


def memory_configuration_label(
    text: str,
) -> str | None:
    ram, storage = extract_memory_configuration(
        text
    )

    if ram is None or storage is None:
        return None

    storage_label = (
        f"{storage // 1024}TB"
        if storage >= 1024
        and storage % 1024 == 0
        else str(storage)
    )

    return f"{ram}/{storage_label}"


def analytics_variant_key(
    found_request: str,
) -> str:
    """
    Группирует реальные запросы.

    Если есть RAM/storage конфигурация, она становится ключевой
    частью группировки, поэтому:
      A57 6/256
      A57 8/256
      A57 12/256
    считаются разными вариантами.

    А:
      8/256
      8+256
      8GB/256GB
    считаются одной конфигурацией.
    """
    normalized = normalize_text(
        found_request
    )

    if not normalized:
        return ""

    memory = memory_configuration_label(
        found_request
    )

    if memory:
        # Remove memory-expression noise from base text before grouping.
        base = normalized

        replacements = [
            r"(?<!\d)\d{1,2}\s*(?:gb)?\s*[/+xх×\-]\s*\d{2,4}\s*(?:gb|tb)?(?!\d)",
            r"(?<!\d)\d{1,2}\s*(?:gb)?\s*(?:ram|озу)\s*[/+:\-]?\s*\d{2,4}\s*(?:gb|tb)?(?!\d)",
            r"(?<!\d)\d{1,2}\s*gb\s+(?:ram\s+)?\d{2,4}\s*(?:gb|tb)?(?!\d)",
        ]

        for pattern in replacements:
            base = re.sub(
                pattern,
                " ",
                base,
                flags=re.IGNORECASE,
            )

        base = re.sub(
            r"\s+",
            " ",
            base,
        ).strip()

        tokens = sorted(
            base.split()
        )

        return (
            " ".join(tokens)
            + " |mem:"
            + memory
        ).strip()

    tokens = normalized.split()

    return " ".join(
        sorted(tokens)
    )


def clean_variant_label(
    found_request: str,
) -> str:
    text = re.sub(
        r"\s+",
        " ",
        (found_request or "").strip(),
    )

    text = text.strip(
        " \t\r\n,.;:!?-–—"
    )

    memory = memory_configuration_label(
        text
    )

    if memory:
        # Keep the actual request wording, but normalize the config
        # visually so all equivalent spellings are obvious.
        text = re.sub(
            r"(?<!\d)\d{1,2}\s*(?:gb)?\s*[/+xх×\-]\s*\d{1,4}\s*(?:gb|tb)?(?!\d)",
            memory,
            text,
            count=1,
            flags=re.IGNORECASE,
        )

    if len(text) > 80:
        text = (
            text[:77].rstrip()
            + "..."
        )

    return text or "—"


def _compute_interest_snapshot_sync(
    interests_data: list[tuple[int, str]],
    found_requests: list[str],
) -> dict:
    assigned_counts = Counter()
    independent_counts = Counter()
    variants_by_id = {interest_id: {} for interest_id, _ in interests_data}
    matched_total = 0

    for found_request in found_requests:
        if not found_request:
            continue

        best_id = None
        best_score = 0.0

        for interest_id, interest_query in interests_data:
            score = product_match_score(interest_query, found_request)

            if score >= MATCH_THRESHOLD:
                independent_counts[interest_id] += 1

                key = analytics_variant_key(found_request)
                if key:
                    grouped = variants_by_id[interest_id]
                    if key not in grouped:
                        grouped[key] = [clean_variant_label(found_request), 0]
                    grouped[key][1] += 1

            if score > best_score:
                best_score = score
                best_id = interest_id

        if best_id is not None and best_score >= MATCH_THRESHOLD:
            matched_total += 1
            assigned_counts[best_id] += 1

    variants_sorted = {}

    for interest_id, grouped in variants_by_id.items():
        variants_sorted[interest_id] = sorted(
            [(value[0], int(value[1])) for value in grouped.values()],
            key=lambda item: (-item[1], item[0].casefold()),
        )

    return {
        "assigned_counts": dict(assigned_counts),
        "independent_counts": dict(independent_counts),
        "variants": variants_sorted,
        "matched_total": matched_total,
    }


async def get_interest_snapshot(owner_id: int, period: str) -> dict:
    cache_key = (owner_id, period)
    now = time.monotonic()
    cached = _interest_snapshot_cache.get(cache_key)

    if cached is not None and now - cached[0] < INTEREST_STATS_CACHE_SECONDS:
        return cached[1]

    interests = await get_interest_queries(owner_id)
    rows = await fetch_analytics_rows(owner_id, period)

    interests_data = [
        (int(row["id"]), str(row["query"]))
        for row in interests
    ]
    found_requests = [
        (row["found_request"] or "").strip()
        for row in rows
    ]

    if interests_data and found_requests:
        computed = await asyncio.to_thread(
            _compute_interest_snapshot_sync,
            interests_data,
            found_requests,
        )
    else:
        computed = {
            "assigned_counts": {},
            "independent_counts": {},
            "variants": {},
            "matched_total": 0,
        }

    snapshot = {
        "interests": interests_data,
        "all_total": len(rows),
        **computed,
    }

    _interest_snapshot_cache[cache_key] = (time.monotonic(), snapshot)
    return snapshot


async def interest_variant_breakdown(
    owner_id: int,
    interest_id: int,
    period: str,
) -> tuple[str | None, int, list[tuple[str, int]]]:
    snapshot = await get_interest_snapshot(owner_id, period)
    interest_query = next(
        (query for item_id, query in snapshot["interests"] if item_id == interest_id),
        None,
    )

    if interest_query is None:
        return None, 0, []

    total = int(snapshot["independent_counts"].get(interest_id, 0))
    variants = snapshot["variants"].get(interest_id, [])
    return interest_query, total, variants


async def interest_counts_for_period(
    owner_id: int,
    period: str,
) -> list[tuple[int, str, int]]:
    snapshot = await get_interest_snapshot(owner_id, period)
    result = [
        (interest_id, query, int(snapshot["independent_counts"].get(interest_id, 0)))
        for interest_id, query in snapshot["interests"]
    ]
    result.sort(key=lambda item: (-item[2], item[1].casefold()))
    return result


def interest_model_select_keyboard(
    rows: list[tuple[int, str, int]],
    period: str,
) -> InlineKeyboardMarkup:
    keyboard = []

    for interest_id, query, count in rows[:20]:
        title = query

        if len(title) > 34:
            title = (
                title[:31]
                + "..."
            )

        keyboard.append([
            InlineKeyboardButton(
                text=f"🧩 {title} — {count}",
                callback_data=(
                    f"interestmodel:"
                    f"{interest_id}:"
                    f"{period}"
                ),
            )
        ])

    keyboard.append([
        InlineKeyboardButton(
            text="◀️ К статистике",
            callback_data=f"intereststats:{period}",
        )
    ])

    return InlineKeyboardMarkup(
        inline_keyboard=keyboard
    )


def interest_model_detail_keyboard(
    interest_id: int,
    period: str,
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Сегодня",
                    callback_data=(
                        f"interestmodel:"
                        f"{interest_id}:today"
                    ),
                ),
                InlineKeyboardButton(
                    text="7 дней",
                    callback_data=(
                        f"interestmodel:"
                        f"{interest_id}:7"
                    ),
                ),
                InlineKeyboardButton(
                    text="30 дней",
                    callback_data=(
                        f"interestmodel:"
                        f"{interest_id}:30"
                    ),
                ),
            ],
            [
                InlineKeyboardButton(
                    text="◀️ Другие модели",
                    callback_data=(
                        f"interestmodels:"
                        f"{period}"
                    ),
                )
            ],
        ]
    )


async def interest_model_detail_text(
    owner_id: int,
    interest_id: int,
    period: str,
) -> str:
    (
        interest_query,
        total,
        variants,
    ) = await interest_variant_breakdown(
        owner_id,
        interest_id,
        period,
    )

    if interest_query is None:
        return (
            "❌ Эта интересующая позиция уже удалена."
        )

    if variants:
        lines = [
            (
                f"{index}. "
                f"{label} — {count}"
            )
            for index, (label, count)
            in enumerate(
                variants[:20],
                start=1,
            )
        ]
    else:
        lines = [
            "— пока ничего"
        ]

    return (
        f"🧩 {interest_query}\n"
        f"📅 {format_period_name(period)}\n\n"
        f"🔥 Всего запросов: {ru_requests(total)}\n\n"
        "📊 КАКИЕ КОНФИГУРАЦИИ ЗАПРАШИВАЛИ\n"
        + "\n".join(lines)
        + "\n\n"
        "RAM/память считаются отдельно: "
        "6/256 ≠ 8/256 ≠ 12/256.\n"
        "А 8/256, 8+256 и 8GB/256GB "
        "склеиваются в одну конфигурацию."
    )



# ============================================================
# INTEREST ANALYTICS
# ============================================================

def interest_analytics_keyboard(
    period: str = "7",
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Сегодня",
                    callback_data="intereststats:today",
                ),
                InlineKeyboardButton(
                    text="7 дней",
                    callback_data="intereststats:7",
                ),
                InlineKeyboardButton(
                    text="30 дней",
                    callback_data="intereststats:30",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🧩 По моделям",
                    callback_data=f"interestmodels:{period}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⚙️ Список интересующих",
                    callback_data="interests:page:0",
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


async def interesting_analytics_text(
    owner_id: int,
    period: str,
) -> str:
    snapshot = await get_interest_snapshot(owner_id, period)
    interests = snapshot["interests"]

    if not interests:
        return (
            f"⭐ ИНТЕРЕСУЮЩИЕ — {format_period_name(period)}\n\n"
            "Список интересующих запросов пока пуст.\n\n"
            "Добавь туда товары или модели, спрос по которым "
            "хочешь считать отдельно."
        )

    display = {interest_id: query for interest_id, query in interests}
    counts = snapshot["assigned_counts"]
    matched_total = int(snapshot["matched_total"])
    all_total = int(snapshot["all_total"])
    share = (matched_total / all_total * 100) if all_total else 0.0

    ordered_positive = sorted(
        [(query_id, int(count)) for query_id, count in counts.items() if count > 0],
        key=lambda item: (-item[1], display.get(item[0], "").casefold()),
    )
    zero_ids = [query_id for query_id in display if counts.get(query_id, 0) == 0]
    ordered = ordered_positive + [(query_id, 0) for query_id in zero_ids]
    top = ordered[:10]

    lines = [
        f"{index}. {display.get(query_id, '—')} — {count}"
        for index, (query_id, count) in enumerate(top, start=1)
    ] or ["—"]

    return (
        f"⭐ ИНТЕРЕСУЮЩИЕ — {format_period_name(period)}\n\n"
        f"🔥 Найдено: {ru_requests(matched_total)}\n"
        f"📡 Всего запросов в чатах: {ru_requests(all_total)}\n"
        f"🎯 Доля интересующих: {share:.0f}%\n"
        f"⭐ Позиций в списке: {len(interests)}\n\n"
        "📊 ПО ИНТЕРЕСУЮЩИМ ПОЗИЦИЯМ\n"
        + "\n".join(lines)
        + "\n\n"
        "ℹ️ Этот список влияет только на статистику.\n"
        "🔎 Уведомления работают отдельно по обычным «Запросам»."
    )


@dp.callback_query(F.data.startswith("interestmodels:"))
async def cb_interest_models(
    callback: CallbackQuery,
):
    if not await guard_callback(
        callback
    ):
        return

    await callback.answer()

    period = callback.data.split(
        ":",
        1,
    )[1]

    if period not in {
        "today",
        "7",
        "30",
    }:
        period = "7"

    rows = await interest_counts_for_period(
        callback.from_user.id,
        period,
    )

    if not rows:
        text = (
            f"🧩 ПО МОДЕЛЯМ — {format_period_name(period)}\n\n"
            "Сначала добавь хотя бы одну позицию "
            "в ⭐ Интересующие."
        )

        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="⭐ К интересующим",
                        callback_data="interests:page:0",
                    )
                ]
            ]
        )

    else:
        text = (
            f"🧩 ПО МОДЕЛЯМ — {format_period_name(period)}\n\n"
            "Выбери интересующую позицию.\n"
            "Справа показано общее количество запросов."
        )

        markup = interest_model_select_keyboard(
            rows,
            period,
        )

    await callback.message.edit_text(
        text,
        reply_markup=markup,
    )



@dp.callback_query(F.data.startswith("interestmodel:"))
async def cb_interest_model_detail(
    callback: CallbackQuery,
):
    if not await guard_callback(
        callback
    ):
        return

    await callback.answer()

    parts = callback.data.split(
        ":"
    )

    try:
        interest_id = int(
            parts[1]
        )
        period = parts[2]

    except (
        ValueError,
        IndexError,
    ):
        await callback.message.answer(
            "❌ Ошибка модели."
        )
        return

    if period not in {
        "today",
        "7",
        "30",
    }:
        period = "7"

    text = await interest_model_detail_text(
        callback.from_user.id,
        interest_id,
        period,
    )

    await callback.message.edit_text(
        text,
        reply_markup=interest_model_detail_keyboard(
            interest_id,
            period,
        ),
    )



@dp.callback_query(F.data.startswith("intereststats:"))
async def cb_interest_stats(
    callback: CallbackQuery,
):
    if not await guard_callback(
        callback
    ):
        return

    await callback.answer()

    period = callback.data.split(
        ":",
        1,
    )[1]

    if period not in {
        "today",
        "7",
        "30",
    }:
        period = "7"

    text = await interesting_analytics_text(
        callback.from_user.id,
        period,
    )

    await callback.message.edit_text(
        text,
        reply_markup=interest_analytics_keyboard(
            period
        ),
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

    await callback.answer()

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



# ============================================================
# MONITOR
# ============================================================

def find_best_monitor_match_sync(query_rows: list, text: str):
    best_query = None
    best_query_id = None
    best_score = 0.0
    best_segment = ""

    for row in query_rows:
        score, matched_segment = best_query_segment(row["query"], text)

        if score > best_score:
            best_score = score
            best_query = row["query"]
            best_query_id = int(row["id"])
            best_segment = matched_segment

    return best_query, best_query_id, best_score, best_segment


async def monitor_handler(event, account_id: int):
    try:
        text = event.raw_text or ""
        chat_id = event.chat_id

        if not text or chat_id is None:
            return

        owner_id = await get_owner_id()

        if owner_id is None:
            return

        # Кэш: больше нет SQL-запроса selected_chats на КАЖДОЕ сообщение.
        selected = await get_selected_chat_ids(owner_id, account_id)

        if int(chat_id) not in selected:
            return

        # Дешёвый regex сначала.
        analytics_request = extract_all_request_for_analytics(text)
        # Кэш: список запросов читается из БД только после его изменения.
        query_rows = await get_queries(owner_id)

        best_query = None
        best_query_id = None
        best_score = 0.0
        best_segment = ""

        if query_rows:
            # CPU matcher не блокирует aiogram UI.
            async with monitor_match_semaphore:
                (
                    best_query,
                    best_query_id,
                    best_score,
                    best_segment,
                ) = await asyncio.to_thread(
                    find_best_monitor_match_sync,
                    query_rows,
                    text,
                )

        has_match = (
            best_query is not None
            and best_query_id is not None
            and best_score >= MATCH_THRESHOLD
        )

        # Самый частый путь: обычное сообщение не является запросом
        # и не совпало с мониторингом. Никаких get_chat/get_sender/SQL.
        if not analytics_request and not has_match:
            return

        # Метаданные Telegram получаем только для реально нужного сообщения.
        chat, seller = await asyncio.gather(
            event.get_chat(),
            seller_info(event),
        )

        title = (
            getattr(chat, "title", None)
            or getattr(chat, "username", None)
            or "Telegram"
        )
        seller_display, seller_username, seller_key = seller

        if analytics_request:
            analytics_allowed = await allow_all_request_analytics_event(
                owner_id=owner_id,
                seller_key=seller_key,
                found_request=analytics_request,
            )

            if analytics_allowed:
                analytics_brand = detect_brand(analytics_request, text)
                await log_all_request_analytics_event(
                    owner_id=owner_id,
                    chat_id=int(chat_id),
                    message_id=int(event.id),
                    chat_title=title,
                    found_request=analytics_request,
                    brand=analytics_brand,
                    seller_key=seller_key,
                )

        if not has_match:
            return

        if not await mark_seen(
            owner_id,
            int(chat_id),
            int(event.id),
        ):
            return

        body_source = best_segment or text
        body = body_source[:3200]

        if len(body_source) > 3200:
            body += "\n\n…"

        notification = (
            f"🔥 ЗАПРОС: {best_query}\n\n"
            f"💬 {title}\n"
            f"👤 {seller_display}\n\n"
            f"{body}"
        )

        found_request = extract_found_request(body_source, best_query)

        if NOTIFICATION_DELAY_SECONDS > 0:
            await asyncio.sleep(NOTIFICATION_DELAY_SECONDS)

        # Одной прямой проверки достаточно: если query_id удалён,
        # значит уведомление отменяется; второй get_queries не нужен.
        if not await query_still_active(owner_id, best_query_id):
            return

        allowed = await allow_request_after_short_dedup(
            owner_id=owner_id,
            seller_key=seller_key,
            found_request=found_request,
        )

        if not allowed:
            return

        await bot.send_message(
            owner_id,
            notification,
            reply_markup=reply_keyboard(found_request, seller_username),
            disable_web_page_preview=True,
        )

    except Exception as error:
        print("MONITOR ERROR:", repr(error))


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
# LOGIN FALLBACK AFTER PROCESS RESTART
# ============================================================

@dp.message()
async def persistent_login_fallback(
    message: Message,
    state: FSMContext,
):
    """
    Catch-all только для незавершённого Telegram login.

    Нужен на случай, если /login был обработан одним Railway
    process, а номер/код пришёл уже после restart и MemoryStorage
    пустая. Этап читается из PostgreSQL.
    """
    if not await guard_message(
        message
    ):
        return

    owner_id = message.from_user.id

    flow = await get_login_flow(
        owner_id
    )

    if flow is None:
        return

    stage = flow["stage"]

    if stage == "waiting_phone":
        await process_login_phone(
            message,
            state,
        )
        return

    if stage == "waiting_code":
        await process_login_code(
            message,
            state,
        )
        return

    if stage == "waiting_password":
        await process_login_password(
            message,
            state,
        )
        return


# ============================================================
# MAIN
# ============================================================

async def prune_runtime_tables():
    """Удаляем только технический мусор, статистику не трогаем."""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM recent_request_dedup "
                "WHERE last_seen < NOW() - INTERVAL '1 day'"
            )
            await conn.execute(
                "DELETE FROM recent_all_request_analytics_dedup "
                "WHERE first_seen_at < NOW() - INTERVAL '1 day'"
            )
            await conn.execute(
                "DELETE FROM seen_messages "
                "WHERE created_at < NOW() - INTERVAL '7 days'"
            )
    except Exception as error:
        print("MAINTENANCE ERROR:", repr(error))


async def maintenance_loop():
    while True:
        await asyncio.sleep(6 * 60 * 60)
        await prune_runtime_tables()


async def account_supervisor_loop(
    owner_id: int,
):
    """
    24/7 supervisor:
    - каждые 3 секунды проверяет runtime-соединения;
    - умерший monitor-task восстанавливает сразу;
    - каждые 25 секунд делает реальный Telegram RPC heartbeat.
    """
    heartbeat_every = 20.0
    last_heartbeat_cycle = 0.0

    while True:
        await asyncio.sleep(
            2
        )

        try:
            rows = await get_accounts(
                owner_id
            )

            for row in rows:
                account_id = int(
                    row["id"]
                )

                if not account_client_online(
                    account_id
                ):
                    schedule_account_recovery(
                        account_id,
                        delay=0.0,
                    )

            now = time.monotonic()

            if (
                now
                - last_heartbeat_cycle
                >= heartbeat_every
            ):
                last_heartbeat_cycle = now

                await asyncio.gather(
                    *(
                        heartbeat_account(
                            row
                        )
                        for row in rows
                    ),
                    return_exceptions=True,
                )

        except asyncio.CancelledError:
            raise

        except Exception as error:
            print(
                "ACCOUNT SUPERVISOR ERROR:",
                repr(error),
            )



async def main():
    await init_db()

    # Один owner_id -> один общий monitor-список и один общий
    # analytics-only interest-список. Оба Telegram-аккаунта
    # используют одни и те же строки.
    await migrate_legacy_product_lists()
    await migrate_multiline_queries()

    # ВАЖНО:
    # shared_product_lists_v1 — единственный активный источник.
    # Старые monitor_queries_v2 / interest_queries_v1 используются
    # только для одноразовой миграции старых данных.

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

    maintenance_task = asyncio.create_task(
        prune_runtime_tables()
    )
    maintenance_loop_task = asyncio.create_task(
        maintenance_loop()
    )

    account_supervisor_task = (
        asyncio.create_task(
            account_supervisor_loop(
                owner_id
            )
        )
        if owner_id is not None
        else None
    )

    print(
        "Управляющий бот запущен."
    )

    try:
        await dp.start_polling(
            bot
        )

    finally:
        for task in [
            maintenance_task,
            maintenance_loop_task,
            account_supervisor_task,
        ]:
            if task and not task.done():
                task.cancel()

        for task in list(
            _account_dialog_refresh_tasks.values()
        ):
            if not task.done():
                task.cancel()

        _account_dialog_refresh_tasks.clear()

        for task in list(
            _account_recovery_tasks.values()
        ):
            if not task.done():
                task.cancel()

        _account_recovery_tasks.clear()

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
