import os
import re
import asyncio
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

bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

db_pool: asyncpg.Pool | None = None
client: TelegramClient | None = None
telethon_task: asyncio.Task | None = None


# ============================================================
# FSM
# ============================================================

class LoginStates(StatesGroup):
    waiting_phone = State()
    waiting_code = State()
    waiting_password = State()


class QueryStates(StatesGroup):
    waiting_query = State()


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

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_messages (
                owner_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (owner_id, chat_id, message_id)
            )
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


async def get_selected_chat_ids(owner_id: int) -> set[int]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT chat_id FROM selected_chats WHERE owner_id=$1",
            owner_id,
        )

    return {int(row["chat_id"]) for row in rows}


async def toggle_selected_chat(owner_id: int, chat_id: int, title: str) -> bool:
    async with db_pool.acquire() as conn:
        exists = await conn.fetchval(
            """
            SELECT 1
            FROM selected_chats
            WHERE owner_id=$1 AND chat_id=$2
            """,
            owner_id,
            chat_id,
        )

        if exists:
            await conn.execute(
                """
                DELETE FROM selected_chats
                WHERE owner_id=$1 AND chat_id=$2
                """,
                owner_id,
                chat_id,
            )
            return False

        await conn.execute(
            """
            INSERT INTO selected_chats (owner_id, chat_id, title)
            VALUES ($1, $2, $3)
            ON CONFLICT (owner_id, chat_id) DO NOTHING
            """,
            owner_id,
            chat_id,
            title,
        )
        return True


async def mark_seen(owner_id: int, chat_id: int, message_id: int) -> bool:
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            """
            INSERT INTO seen_messages (owner_id, chat_id, message_id)
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
            """,
            owner_id,
            chat_id,
            message_id,
        )

    return result.endswith("1")


# ============================================================
# TELETHON SESSION
# ============================================================

async def build_client():
    global client

    saved = await get_setting("telethon_session")
    session = StringSession(saved) if saved else StringSession()

    client = TelegramClient(
        session,
        API_ID,
        API_HASH,
        receive_updates=True,
    )

    client.add_event_handler(
        monitor_handler,
        events.NewMessage(),
    )

    await client.connect()

    try:
        authorized = await client.is_user_authorized()

    except AuthKeyUnregisteredError:
        await set_setting("telethon_session", None)

        try:
            await client.disconnect()
        except Exception:
            pass

        client = TelegramClient(
            StringSession(),
            API_ID,
            API_HASH,
            receive_updates=True,
        )

        client.add_event_handler(
            monitor_handler,
            events.NewMessage(),
        )

        await client.connect()
        authorized = False

    return authorized


async def save_telethon_session():
    if client is None:
        return

    session_string = client.session.save()

    if session_string:
        await set_setting(
            "telethon_session",
            session_string,
        )


def start_telethon_monitor():
    global telethon_task

    if client is None:
        return

    if telethon_task and not telethon_task.done():
        return

    telethon_task = asyncio.create_task(
        client.run_until_disconnected()
    )


# ============================================================
# UNIVERSAL PRODUCT SEARCH
# ============================================================

# Только полезные исключения. Основной поиск универсальный.
PHONETIC_ALIASES = {
    "айфон": "iphone",
    "айфоны": "iphone",
    "айфона": "iphone",
    "айфоне": "iphone",
    "айфоном": "iphone",
    "макбук": "macbook",
    "макбуки": "macbook",
    "макбука": "macbook",
    "плейстейшен": "playstation",
    "плейстейшн": "playstation",
    "плойка": "playstation",
    "иксбокс": "xbox",
    "самсунг": "samsung",
    "хуавей": "huawei",
    "фитбит": "fitbit",
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
}


def normalize_text(text: str) -> str:
    if not text:
        return ""

    text = text.casefold().replace("ё", "е")

    for source, target in PHONETIC_ALIASES.items():
        text = re.sub(
            rf"\b{re.escape(source)}\b",
            target,
            text,
            flags=re.IGNORECASE,
        )

    # Кириллицу и прочие символы приводим к латинице.
    text = unidecode(text)

    # iphone17 -> iphone 17
    text = re.sub(r"([a-z])(\d)", r"\1 \2", text)
    text = re.sub(r"(\d)([a-z])", r"\1 \2", text)

    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def numeric_tokens(text: str) -> list[str]:
    return re.findall(r"\d+", normalize_text(text))


def token_match_score(needle: str, hay_tokens: list[str]) -> float:
    if not needle or not hay_tokens:
        return 0.0

    return max(
        fuzz.ratio(needle, token)
        for token in hay_tokens
    )


def product_match_score(query: str, message: str) -> float:
    q = normalize_text(query)
    m = normalize_text(message)

    if not q or not m:
        return 0.0

    q_tokens = q.split()
    m_tokens = m.split()

    # Цифры модели из запроса обязательны.
    # iPhone 17 не должен ловить iPhone 16.
    q_numbers = numeric_tokens(q)
    m_numbers = numeric_tokens(m)

    for number in q_numbers:
        if number not in m_numbers:
            return 0.0

    if q in m:
        return 100.0

    full_partial = fuzz.partial_ratio(q, m)
    full_token = fuzz.token_set_ratio(q, m)

    words = [
        token
        for token in q_tokens
        if not token.isdigit()
    ]

    # Главное название товара.
    # Google Fitbit Air -> ядро Fitbit.
    core = [
        word
        for word in words
        if word not in BRAND_WORDS
        and word not in MODEL_MODIFIERS
        and len(word) >= 3
    ]

    if not core:
        core = [
            word
            for word in words
            if len(word) >= 3
        ]

    if not core:
        return max(full_partial, full_token)

    core_scores = [
        token_match_score(word, m_tokens)
        for word in core
    ]

    best_core = max(core_scores)
    average_core = sum(core_scores) / len(core_scores)

    # Например Google Fitbit Air -> "Fitbit".
    if best_core >= 90:
        return max(
            90.0,
            full_partial,
            full_token,
        )

    # Опечатки.
    if len(core) == 1 and best_core >= 76:
        return max(
            best_core,
            full_partial,
            full_token,
        )

    if len(core) >= 2:
        strong = sum(
            score >= 78
            for score in core_scores
        )

        if strong >= 2:
            return max(
                average_core,
                full_partial,
                full_token,
            )

    return max(
        full_partial,
        full_token,
    )


# ============================================================
# DIALOGS
# ============================================================

async def get_dialogs():
    if client is None:
        return []

    if not await client.is_user_authorized():
        return []

    result = []

    async for dialog in client.iter_dialogs(limit=500):
        if not (dialog.is_group or dialog.is_channel):
            continue

        result.append({
            "id": int(dialog.id),
            "name": dialog.name or "Без названия",
        })

    result.sort(
        key=lambda item: item["name"].casefold()
    )

    return result


# ============================================================
# SELLER + REPLY BUTTON
# ============================================================

async def seller_info(event):
    """
    Возвращает:
    display_name, username_or_none
    """
    try:
        sender = await event.get_sender()

        if sender is None:
            return "Неизвестно", None

        # Кнопку "Ответить" делаем только для обычного пользователя.
        if isinstance(sender, User):
            username = getattr(sender, "username", None)

            if username:
                return f"@{username}", username

            name = " ".join(
                part
                for part in [
                    getattr(sender, "first_name", None),
                    getattr(sender, "last_name", None),
                ]
                if part
            ).strip()

            return name or "Пользователь", None

        # Канал / анонимный админ.
        title = getattr(sender, "title", None)
        username = getattr(sender, "username", None)

        if title:
            return title, None

        if username:
            return f"@{username}", None

        return "Неизвестно", None

    except Exception:
        return "Неизвестно", None



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
                    text="📡 Статус",
                    callback_data="status",
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
async def cmd_start(message: Message):
    if not await claim_or_check_owner(message.from_user.id):
        await message.answer("⛔ Этот бот закрыт.")
        return

    await message.answer(
        "🔎 Tech Monitor\n\n"
        "Выбери раздел:",
        reply_markup=main_menu(),
    )


@dp.callback_query(F.data == "home")
async def cb_home(callback: CallbackQuery):
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
    authorized = (
        client is not None
        and await client.is_user_authorized()
    )

    async with db_pool.acquire() as conn:
        query_count = await conn.fetchval(
            "SELECT COUNT(*) FROM queries WHERE owner_id=$1",
            owner_id,
        )

        selected_count = await conn.fetchval(
            "SELECT COUNT(*) FROM selected_chats WHERE owner_id=$1",
            owner_id,
        )

    available = 0
    account = "—"

    if authorized:
        try:
            me = await client.get_me()

            account = (
                f"@{me.username}"
                if getattr(me, "username", None)
                else (me.first_name or str(me.id))
            )

            available = len(
                await get_dialogs()
            )

        except Exception:
            pass

    return (
        "📡 СТАТУС\n\n"
        f"Telegram: {'🟢 подключён' if authorized else '🔴 не подключён'}\n"
        f"👤 Аккаунт: {account}\n\n"
        f"💬 Доступно чатов: {available}\n"
        f"✅ Выбрано: {selected_count}\n"
        f"🔎 Запросов: {query_count}"
    )


@dp.message(Command("status"))
async def cmd_status(message: Message):
    if not await guard_message(message):
        return

    await message.answer(
        await status_text(message.from_user.id),
        reply_markup=main_menu(),
    )


@dp.callback_query(F.data == "status")
async def cb_status(callback: CallbackQuery):
    if not await guard_callback(callback):
        return

    await callback.message.edit_text(
        await status_text(callback.from_user.id),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="◀️ Назад",
                        callback_data="home",
                    )
                ]
            ]
        ),
    )

    await callback.answer()


# ============================================================
# LOGIN
# ============================================================

@dp.message(Command("login"))
async def cmd_login(message: Message, state: FSMContext):
    if not await guard_message(message):
        return

    if client is None:
        await message.answer(
            "❌ Telegram-клиент ещё не запущен."
        )
        return

    if await client.is_user_authorized():
        await message.answer(
            "✅ Telegram уже подключён."
        )
        return

    await state.set_state(
        LoginStates.waiting_phone
    )

    await message.answer(
        "📱 Отправь номер Telegram.\n\n"
        "Например:\n"
        "+37212345678"
    )


@dp.message(LoginStates.waiting_phone)
async def login_phone(message: Message, state: FSMContext):
    if not await guard_message(message):
        return

    phone = (message.text or "").strip()

    if not re.fullmatch(r"\+\d{7,15}", phone):
        await message.answer(
            "❌ Номер должен выглядеть примерно так:\n"
            "+37212345678"
        )
        return

    try:
        result = await client.send_code_request(
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


@dp.message(LoginStates.waiting_code)
async def login_code(message: Message, state: FSMContext):
    if not await guard_message(message):
        return

    code = re.sub(
        r"\D",
        "",
        message.text or "",
    )

    data = await state.get_data()

    try:
        await client.sign_in(
            phone=data["phone"],
            code=code,
            phone_code_hash=data["phone_code_hash"],
        )

        await save_telethon_session()
        await state.clear()
        await client.set_receive_updates(True)
        start_telethon_monitor()

        await message.answer(
            "✅ Telegram подключён.\n\n"
            "Теперь открой 💬 Чаты.",
            reply_markup=main_menu(),
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
        await state.clear()

        await message.answer(
            "❌ Код истёк. Начни заново: /login"
        )

    except Exception as error:
        await message.answer(
            f"❌ Ошибка авторизации:\n{error}"
        )


@dp.message(LoginStates.waiting_password)
async def login_password(message: Message, state: FSMContext):
    if not await guard_message(message):
        return

    try:
        await client.sign_in(
            password=message.text or ""
        )

        await save_telethon_session()
        await state.clear()
        await client.set_receive_updates(True)
        start_telethon_monitor()

        await message.answer(
            "✅ Telegram подключён.\n\n"
            "Теперь открой 💬 Чаты.",
            reply_markup=main_menu(),
        )

    except Exception as error:
        await message.answer(
            f"❌ Пароль не подошёл:\n{error}"
        )


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

async def build_chats_keyboard(owner_id: int, page: int, dialogs: list):
    selected = await get_selected_chat_ids(
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

    rows = []

    for chat in current:
        checked = chat["id"] in selected
        title = chat["name"]

        if len(title) > 36:
            title = title[:33] + "..."

        rows.append([
            InlineKeyboardButton(
                text=f"{'✅' if checked else '⬜'} {title}",
                callback_data=f"chat:toggle:{chat['id']}:{page}",
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


async def render_chats(
    target_message,
    owner_id: int,
    page: int,
    edit: bool,
):
    if client is None or not await client.is_user_authorized():
        text = (
            "🔴 Сначала подключи Telegram через /login"
        )

        if edit:
            await target_message.edit_text(text)
        else:
            await target_message.answer(text)

        return

    dialogs = await get_dialogs()

    if not dialogs:
        text = (
            "💬 Telegram не вернул группы/каналы.\n\n"
            "Проверь /status."
        )

        if edit:
            await target_message.edit_text(text)
        else:
            await target_message.answer(text)

        return

    text = (
        "💬 ВЫБОР ЧАТОВ\n\n"
        f"Найдено: {len(dialogs)}\n\n"
        "✅ — отслеживается\n"
        "⬜ — не отслеживается\n\n"
        "Нажми на чат:"
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


@dp.message(Command("chats"))
async def cmd_chats(message: Message):
    if not await guard_message(message):
        return

    await render_chats(
        message,
        message.from_user.id,
        0,
        False,
    )


@dp.callback_query(F.data.startswith("chats:"))
async def cb_chats(callback: CallbackQuery):
    if not await guard_callback(callback):
        return

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


@dp.callback_query(F.data.startswith("chat:toggle:"))
async def cb_chat_toggle(callback: CallbackQuery):
    if not await guard_callback(callback):
        return

    _, _, chat_id_raw, page_raw = (
        callback.data.split(":")
    )

    chat_id = int(chat_id_raw)
    page = int(page_raw)

    dialogs = await get_dialogs()

    chat = next(
        (
            item
            for item in dialogs
            if item["id"] == chat_id
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
        callback.from_user.id,
        chat_id,
        chat["name"],
    )

    await callback.message.edit_reply_markup(
        reply_markup=await build_chats_keyboard(
            callback.from_user.id,
            page,
            dialogs,
        )
    )

    await callback.answer(
        "Включено ✅" if enabled else "Выключено"
    )


@dp.callback_query(F.data == "noop")
async def cb_noop(callback: CallbackQuery):
    await callback.answer()


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
# MONITOR
# ============================================================

async def monitor_handler(event):
    try:
        text = event.raw_text or ""
        chat_id = event.chat_id

        if not text or chat_id is None:
            return

        owner_id = await get_owner_id()

        if owner_id is None:
            return

        selected = await get_selected_chat_ids(
            owner_id
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

        seller_display, seller_username = (
            await seller_info(event)
        )

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

        print(
            "FOUND REQUEST | "
            f"monitor={best_query!r} | "
            f"found={found_request!r}"
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
            f"message={event.id}"
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
    await bot.set_my_commands([
        BotCommand(
            command="start",
            description="Главное меню",
        ),
        BotCommand(
            command="add",
            description="Добавить запрос",
        ),
        BotCommand(
            command="queries",
            description="Мои запросы",
        ),
        BotCommand(
            command="chats",
            description="Выбрать чаты",
        ),
        BotCommand(
            command="status",
            description="Статус мониторинга",
        ),
        BotCommand(
            command="test",
            description="Проверить поиск",
        ),
        BotCommand(
            command="login",
            description="Подключить Telegram",
        ),
    ])


# ============================================================
# MAIN
# ============================================================

async def main():
    await init_db()

    authorized = await build_client()

    if authorized:
        me = await client.get_me()

        await client.set_receive_updates(
            True
        )

        start_telethon_monitor()

        print(
            "Telegram user подключён | "
            f"id={me.id} | "
            f"name={me.first_name}"
        )

    else:
        print(
            "Telegram user НЕ авторизован. "
            "Используй /login."
        )

    await setup_commands()

    print(
        "Управляющий бот запущен."
    )

    try:
        await dp.start_polling(bot)

    finally:
        if telethon_task and not telethon_task.done():
            telethon_task.cancel()

        if client and client.is_connected():
            await client.disconnect()

        if db_pool:
            await db_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
