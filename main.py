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
MATCH_THRESHOLD = 76

# Если один и тот же пользователь раскидывает один и тот же запрос
# по нескольким чатам подряд, показываем только первое сообщение.
# Через 120 секунд такой же запрос снова разрешён.
DEDUP_WINDOW_SECONDS = 120

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


async def get_queries(owner_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT id, query
            FROM queries
            WHERE owner_id=$1
            ORDER BY id DESC
            """,
            owner_id,
        )


async def add_query(owner_id: int, query: str):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO queries (owner_id, query) VALUES ($1, $2)",
            owner_id,
            query,
        )


async def delete_query(owner_id: int, query_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM queries WHERE owner_id=$1 AND id=$2",
            owner_id,
            query_id,
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

    # Microsoft / Xbox
    "майкрософт": "microsoft",
    "иксбокс": "xbox",
    "хбокс": "xbox",

    # Fitbit
    "фитбит": "fitbit",

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
        if (
            anchor in KNOWN_PRODUCT_FAMILIES
            and has_strong_structured_signature(
                normalized_query,
                normalized_message,
            )
        ):
            continue

        return False

    return True


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
            FROM analytics_events
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
            f"📊 АНАЛИТИКА — {format_period_name(period)}\n\n"
            "Пока нет данных.\n\n"
            "Статистика начнёт накапливаться с новых найденных сообщений."
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
            FROM analytics_events
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
        f"📊 АНАЛИТИКА — {format_period_name(period)}\n\n"

        f"🔥 Всего: {ru_requests(total)}\n\n"

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
            FROM analytics_events
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
        "📈 ДИНАМИКА ПО НЕДЕЛЯМ\n\n"
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
                    callback_data="query:delete",
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
                FROM queries
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

async def queries_text(owner_id: int):
    rows = await get_queries(owner_id)

    if not rows:
        return (
            "🔎 ЗАПРОСЫ\n\n"
            "Пока ничего не отслеживается."
        )

    return (
        "🔎 ЗАПРОСЫ\n\n"
        + "\n".join(
            f"• {row['query']}"
            for row in rows
        )
    )


@dp.message(Command("queries"))
async def cmd_queries(message: Message):
    if not await guard_message(message):
        return

    await message.answer(
        await queries_text(message.from_user.id),
        reply_markup=queries_menu(),
    )


@dp.callback_query(F.data == "queries")
async def cb_queries(callback: CallbackQuery):
    if not await guard_callback(callback):
        return

    await callback.message.edit_text(
        await queries_text(callback.from_user.id),
        reply_markup=queries_menu(),
    )

    await callback.answer()


@dp.message(Command("add"))
async def cmd_add(message: Message, state: FSMContext):
    if not await guard_message(message):
        return

    parts = (message.text or "").split(
        maxsplit=1
    )

    if len(parts) == 2 and parts[1].strip():
        query = parts[1].strip()

        await add_query(
            message.from_user.id,
            query,
        )

        await message.answer(
            f"✅ Добавил запрос:\n{query}",
            reply_markup=queries_menu(),
        )
        return

    await state.set_state(
        QueryStates.waiting_query
    )

    await message.answer(
        "🔎 Напиши, что искать.\n\n"
        "Например:\n"
        "iPhone 17\n"
        "Google Fitbit Air"
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
        "Например:\n"
        "iPhone 17\n"
        "Google Fitbit Air"
    )

    await callback.answer()


@dp.message(QueryStates.waiting_query)
async def query_input(message: Message, state: FSMContext):
    if not await guard_message(message):
        return

    query = (message.text or "").strip()

    if len(query) < 2:
        await message.answer(
            "❌ Слишком короткий запрос."
        )
        return

    await add_query(
        message.from_user.id,
        query,
    )

    await state.clear()

    await message.answer(
        f"✅ Отслеживаю:\n{query}",
        reply_markup=queries_menu(),
    )


@dp.callback_query(F.data == "query:delete")
async def cb_query_delete_menu(callback: CallbackQuery):
    if not await guard_callback(callback):
        return

    rows = await get_queries(
        callback.from_user.id
    )

    if not rows:
        await callback.answer(
            "Запросов нет.",
            show_alert=True,
        )
        return

    keyboard = []

    for row in rows:
        title = row["query"]

        if len(title) > 42:
            title = title[:39] + "..."

        keyboard.append([
            InlineKeyboardButton(
                text=f"❌ {title}",
                callback_data=f"query:remove:{row['id']}",
            )
        ])

    keyboard.append([
        InlineKeyboardButton(
            text="◀️ Назад",
            callback_data="queries",
        )
    ])

    await callback.message.edit_text(
        "🗑 Выбери запрос для удаления:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=keyboard
        ),
    )

    await callback.answer()


@dp.callback_query(F.data.startswith("query:remove:"))
async def cb_query_remove(callback: CallbackQuery):
    if not await guard_callback(callback):
        return

    query_id = int(
        callback.data.split(":")[2]
    )

    await delete_query(
        callback.from_user.id,
        query_id,
    )

    await callback.message.edit_text(
        await queries_text(callback.from_user.id),
        reply_markup=queries_menu(),
    )

    await callback.answer(
        "Удалено ✅"
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

        query_rows = await get_queries(
            owner_id
        )

        if not query_rows:
            return

        # Берем лучший запрос.
        best_query = None
        best_score = 0.0

        for row in query_rows:
            score = product_match_score(
                row["query"],
                text,
            )

            if score > best_score:
                best_score = score
                best_query = row["query"]

        if best_query is None:
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

        body = text[:3200]

        if len(text) > 3200:
            body += "\n\n…"

        notification = (
            f"🔥 ЗАПРОС: {best_query}\n\n"
            f"💬 {title}\n"
            f"👤 {seller_display}\n\n"
            f"{body}"
        )

        found_request = extract_found_request(
            text,
            best_query,
        )

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
            text,
        )

        await log_analytics_event(
            owner_id=owner_id,
            chat_id=int(chat_id),
            message_id=int(event.id),
            chat_title=title,
            monitor_query=best_query,
            found_request=found_request,
            brand=brand,
        )

        print(
            "FOUND REQUEST | "
            f"monitor={best_query!r} | "
            f"found={found_request!r} | "
            f"brand={brand!r}"
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
