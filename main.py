import os
import re
import time
import asyncio
from typing import Optional

import asyncpg
from rapidfuzz import fuzz
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    AuthKeyUnregisteredError,
)

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand,
)

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]
OWNER_ID_ENV = int(os.environ.get("OWNER_ID", "0") or "0")

CHATS_PER_PAGE = 8
DIALOG_CACHE_TTL = 60

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

db_pool: Optional[asyncpg.Pool] = None
user_client: Optional[TelegramClient] = None
login_client: Optional[TelegramClient] = None
monitor_task: Optional[asyncio.Task] = None

active_queries: list[tuple[int, str]] = []
selected_chat_ids: set[int] = set()
dialogs_cache: list[dict] = []
dialogs_cache_at = 0.0


class LoginStates(StatesGroup):
    phone = State()
    code = State()
    password = State()


class QueryStates(StatesGroup):
    query = State()


async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS app_config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS queries (
                id BIGSERIAL PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                query TEXT NOT NULL,
                UNIQUE(owner_id, query)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS chats (
                owner_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                title TEXT NOT NULL,
                PRIMARY KEY(owner_id, chat_id)
            )
        """)


async def get_config(key: str):
    async with db_pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT value FROM app_config WHERE key=$1", key
        )


async def set_config(key: str, value: str):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO app_config(key, value)
            VALUES($1, $2)
            ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value
        """, key, value)


async def del_config(key: str):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM app_config WHERE key=$1", key)


async def get_owner_id() -> int:
    if OWNER_ID_ENV:
        return OWNER_ID_ENV
    value = await get_config("owner_id")
    return int(value) if value else 0


async def ensure_owner_message(message: Message) -> bool:
    owner = await get_owner_id()
    if not owner:
        owner = message.from_user.id
        await set_config("owner_id", str(owner))
        print("OWNER CLAIMED:", owner)

    if message.from_user.id != owner:
        await message.answer("⛔ Этот бот приватный.")
        return False
    return True


async def ensure_owner_callback(callback: CallbackQuery) -> bool:
    owner = await get_owner_id()
    if not owner:
        owner = callback.from_user.id
        await set_config("owner_id", str(owner))

    if callback.from_user.id != owner:
        await callback.answer("Этот бот приватный.", show_alert=True)
        return False
    return True


async def reload_cache():
    global active_queries, selected_chat_ids

    owner = await get_owner_id()
    if not owner:
        active_queries = []
        selected_chat_ids = set()
        return

    async with db_pool.acquire() as conn:
        qs = await conn.fetch(
            "SELECT id, query FROM queries WHERE owner_id=$1 ORDER BY id DESC",
            owner,
        )
        cs = await conn.fetch(
            "SELECT chat_id FROM chats WHERE owner_id=$1",
            owner,
        )

    active_queries = [(int(r["id"]), r["query"]) for r in qs]
    selected_chat_ids = {int(r["chat_id"]) for r in cs}


def normalize(text: str) -> str:
    text = (text or "").lower().replace("ё", "е")

    replacements = {
        "айфончик": "iphone",
        "айфоны": "iphone",
        "айфона": "iphone",
        "айфоне": "iphone",
        "айфоном": "iphone",
        "айфон": "iphone",
        "ифон": "iphone",
        "макбуки": "macbook",
        "макбука": "macbook",
        "макбук": "macbook",
        "самсунга": "samsung",
        "самсунг": "samsung",
        "галакси": "galaxy",
        "плейстейшен": "playstation",
        "плейстейшн": "playstation",
        "плойка": "playstation",
        "иксбокс": "xbox",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)

    text = re.sub(r"([a-zа-я])(\d)", r"\1 \2", text)
    text = re.sub(r"(\d)([a-zа-я])", r"\1 \2", text)
    text = re.sub(r"[^a-zа-я0-9]+", " ", text)
    return " ".join(text.split())


def token_match(q: str, message_tokens: list[str]) -> bool:
    if q in message_tokens:
        return True
    threshold = 82 if len(q) <= 3 else 70
    return any(fuzz.ratio(q, token) >= threshold for token in message_tokens)


def matches(query: str, text: str) -> bool:
    q = normalize(query)
    m = normalize(text)
    if not q or not m:
        return False
    if q in m:
        return True

    qt = q.split()
    mt = m.split()

    q_numbers = [x for x in qt if x.isdigit()]
    m_numbers = {x for x in mt if x.isdigit()}

    if any(x not in m_numbers for x in q_numbers):
        return False

    q_words = [x for x in qt if not x.isdigit()]
    if q_words and all(token_match(x, mt) for x in q_words):
        return True

    return max(fuzz.partial_ratio(q, m), fuzz.token_set_ratio(q, m)) >= 78


def make_client(session: str = ""):
    return TelegramClient(
        StringSession(session),
        API_ID,
        API_HASH,
        receive_updates=True,
        auto_reconnect=True,
        connection_retries=10,
        retry_delay=2,
    )


async def authorized() -> bool:
    if user_client is None:
        return False
    try:
        if not user_client.is_connected():
            await user_client.connect()
        return await user_client.is_user_authorized()
    except Exception:
        return False


async def monitor_loop(client: TelegramClient):
    global user_client
    try:
        print("Telethon monitor started.")
        await client.run_until_disconnected()
    except AuthKeyUnregisteredError:
        print("AUTH KEY UNREGISTERED — removing saved session.")
        await del_config("telethon_session")
        if user_client is client:
            user_client = None
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print("MONITOR LOOP ERROR:", repr(e))


async def activate_monitor():
    global monitor_task

    if user_client is None:
        return

    user_client.remove_event_handler(monitor_handler)
    user_client.add_event_handler(monitor_handler, events.NewMessage())
    await user_client.set_receive_updates(True)

    if not monitor_task or monitor_task.done():
        monitor_task = asyncio.create_task(monitor_loop(user_client))


async def load_saved_session():
    global user_client

    session = await get_config("telethon_session")
    if not session:
        print("Saved Telethon session not found.")
        return

    client = make_client(session)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            await del_config("telethon_session")
            await client.disconnect()
            return

        user_client = client
        await activate_monitor()
        me = await user_client.get_me()
        print(f"Telegram user connected: {me.id}")

    except AuthKeyUnregisteredError:
        await del_config("telethon_session")
        try:
            await client.disconnect()
        except Exception:
            pass
    except Exception as e:
        print("LOAD SESSION ERROR:", repr(e))


async def finish_login(client: TelegramClient, state: FSMContext):
    global user_client, login_client, dialogs_cache_at

    await set_config("telethon_session", client.session.save())
    user_client = client
    login_client = None
    dialogs_cache_at = 0

    await state.clear()
    await reload_cache()
    await activate_monitor()


async def get_dialogs(force=False):
    global dialogs_cache, dialogs_cache_at

    if not await authorized():
        return []

    now = time.monotonic()
    if (
        not force
        and dialogs_cache
        and now - dialogs_cache_at < DIALOG_CACHE_TTL
    ):
        return dialogs_cache

    result = []
    async for dialog in user_client.iter_dialogs(limit=500):
        if dialog.is_group or dialog.is_channel:
            result.append({
                "id": int(dialog.id),
                "name": dialog.name or "Без названия",
            })

    result.sort(key=lambda x: x["name"].lower())
    dialogs_cache = result
    dialogs_cache_at = now
    print("Dialogs loaded:", len(result))
    return result


def message_link(event, chat):
    username = getattr(chat, "username", None)
    if username:
        return f"https://t.me/{username}/{event.id}"

    chat_id = event.chat_id
    if chat_id and str(chat_id).startswith("-100"):
        internal = abs(int(chat_id)) - 1_000_000_000_000
        if internal > 0:
            return f"https://t.me/c/{internal}/{event.id}"
    return None


async def sender_name(event):
    try:
        sender = await event.get_sender()
        if sender:
            username = getattr(sender, "username", None)
            if username:
                return f"@{username}"

            title = getattr(sender, "title", None)
            if title:
                return title

            name = " ".join(
                x for x in (
                    getattr(sender, "first_name", None),
                    getattr(sender, "last_name", None),
                )
                if x
            )
            if name:
                return name

        if getattr(event, "post_author", None):
            return event.post_author
    except Exception as e:
        print("SENDER ERROR:", repr(e))

    return "Неизвестно"


def home_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔎 Запросы", callback_data="queries"),
            InlineKeyboardButton(text="💬 Чаты", callback_data="chats:0"),
        ],
        [InlineKeyboardButton(text="📡 Статус", callback_data="status")],
    ])


def queries_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить запрос", callback_data="query:add")],
        [InlineKeyboardButton(text="🗑 Удалить запрос", callback_data="query:delete")],
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="home")],
    ])


@dp.message(Command("whoami"))
async def whoami(message: Message):
    await message.answer(f"Твой Telegram ID: {message.from_user.id}")


@dp.message(Command("start"))
async def start(message: Message):
    if not await ensure_owner_message(message):
        return
    await message.answer(
        "🔎 Tech Monitor\n\nВыбирай:",
        reply_markup=home_keyboard(),
    )


@dp.callback_query(F.data == "home")
async def home(callback: CallbackQuery):
    if not await ensure_owner_callback(callback):
        return
    await callback.message.edit_text(
        "🔎 Tech Monitor\n\nВыбирай:",
        reply_markup=home_keyboard(),
    )
    await callback.answer()


async def status_text():
    owner = await get_owner_id()
    is_auth = await authorized()
    account = "—"
    dialog_count = 0

    if is_auth:
        try:
            me = await user_client.get_me()
            account = f"@{me.username}" if me.username else (me.first_name or str(me.id))
            dialog_count = len(await get_dialogs())
        except Exception as e:
            print("STATUS ERROR:", repr(e))

    async with db_pool.acquire() as conn:
        q_count = await conn.fetchval(
            "SELECT COUNT(*) FROM queries WHERE owner_id=$1", owner
        )
        c_count = await conn.fetchval(
            "SELECT COUNT(*) FROM chats WHERE owner_id=$1", owner
        )

    return (
        "📡 СТАТУС\n\n"
        f"Telegram: {'🟢 подключён' if is_auth else '🔴 не подключён'}\n"
        f"👤 Аккаунт: {account}\n\n"
        f"💬 Доступно чатов: {dialog_count}\n"
        f"✅ Выбрано чатов: {c_count}\n"
        f"🔎 Запросов: {q_count}"
    )


@dp.message(Command("status"))
async def status_cmd(message: Message):
    if not await ensure_owner_message(message):
        return
    await message.answer(await status_text(), reply_markup=home_keyboard())


@dp.callback_query(F.data == "status")
async def status_cb(callback: CallbackQuery):
    if not await ensure_owner_callback(callback):
        return
    await callback.message.edit_text(
        await status_text(),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="◀️ Назад", callback_data="home")
        ]]),
    )
    await callback.answer()


@dp.message(Command("login"))
async def login(message: Message, state: FSMContext):
    global login_client

    if not await ensure_owner_message(message):
        return
    if await authorized():
        await message.answer("✅ Telegram уже подключён.")
        return

    if login_client:
        try:
            await login_client.disconnect()
        except Exception:
            pass

    login_client = make_client()
    await login_client.connect()

    await state.clear()
    await state.set_state(LoginStates.phone)
    await message.answer(
        "📱 Отправь номер Telegram.\n\nНапример:\n+37212345678"
    )


@dp.message(LoginStates.phone)
async def login_phone(message: Message, state: FSMContext):
    if not await ensure_owner_message(message):
        return

    phone = (message.text or "").strip()
    if not phone.startswith("+"):
        await message.answer("Номер должен начинаться с +")
        return

    try:
        sent = await login_client.send_code_request(phone)
        await state.update_data(
            phone=phone,
            phone_code_hash=sent.phone_code_hash,
        )
        await state.set_state(LoginStates.code)
        await message.answer(
            "📨 Код отправлен.\n\n"
            "Введи его С ПРОБЕЛАМИ, например:\n1 2 3 4 5"
        )
    except Exception as e:
        print("SEND CODE ERROR:", repr(e))
        await message.answer(f"❌ Ошибка:\n{e}")


@dp.message(LoginStates.code)
async def login_code(message: Message, state: FSMContext):
    if not await ensure_owner_message(message):
        return

    data = await state.get_data()
    code = re.sub(r"\D", "", message.text or "")

    try:
        await login_client.sign_in(
            phone=data["phone"],
            code=code,
            phone_code_hash=data["phone_code_hash"],
        )
        await finish_login(login_client, state)
        await message.answer(
            "✅ Telegram подключён.\n\nТеперь открой 💬 Чаты.",
            reply_markup=home_keyboard(),
        )

    except SessionPasswordNeededError:
        await state.set_state(LoginStates.password)
        await message.answer("🔐 Введи пароль 2FA.")

    except PhoneCodeInvalidError:
        await message.answer("❌ Неверный код.")

    except PhoneCodeExpiredError:
        await state.clear()
        await message.answer("❌ Код истёк. Начни заново: /login")

    except Exception as e:
        print("LOGIN ERROR:", repr(e))
        await message.answer(f"❌ Ошибка входа:\n{e}")


@dp.message(LoginStates.password)
async def login_password(message: Message, state: FSMContext):
    if not await ensure_owner_message(message):
        return

    try:
        await login_client.sign_in(password=message.text or "")
        await finish_login(login_client, state)
        await message.answer(
            "✅ Telegram подключён.\n\nТеперь открой 💬 Чаты.",
            reply_markup=home_keyboard(),
        )
    except Exception as e:
        print("2FA ERROR:", repr(e))
        await message.answer(f"❌ Ошибка 2FA:\n{e}")


async def queries_text():
    if not active_queries:
        return "🔎 ЗАПРОСЫ\n\nПока запросов нет."
    return "🔎 ЗАПРОСЫ\n\n" + "\n".join(
        f"• {q}" for _, q in active_queries
    )


@dp.message(Command("queries"))
async def queries_cmd(message: Message):
    if not await ensure_owner_message(message):
        return
    await message.answer(await queries_text(), reply_markup=queries_keyboard())


@dp.callback_query(F.data == "queries")
async def queries_cb(callback: CallbackQuery):
    if not await ensure_owner_callback(callback):
        return
    await callback.message.edit_text(
        await queries_text(),
        reply_markup=queries_keyboard(),
    )
    await callback.answer()


async def insert_query(owner: int, query: str):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO queries(owner_id, query)
            VALUES($1, $2)
            ON CONFLICT(owner_id, query) DO NOTHING
        """, owner, query)
    await reload_cache()


@dp.message(Command("add"))
async def add_cmd(message: Message, state: FSMContext):
    if not await ensure_owner_message(message):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 2 and parts[1].strip():
        query = parts[1].strip()
        await insert_query(message.from_user.id, query)
        await message.answer(
            f"✅ Отслеживаю:\n{query}",
            reply_markup=queries_keyboard(),
        )
        return

    await state.set_state(QueryStates.query)
    await message.answer("🔎 Что искать?\n\nНапример:\niPhone 17")


@dp.callback_query(F.data == "query:add")
async def add_cb(callback: CallbackQuery, state: FSMContext):
    if not await ensure_owner_callback(callback):
        return
    await state.set_state(QueryStates.query)
    await callback.message.answer(
        "🔎 Напиши, что отслеживать.\n\nНапример:\niPhone 17"
    )
    await callback.answer()


@dp.message(QueryStates.query)
async def add_query_text(message: Message, state: FSMContext):
    if not await ensure_owner_message(message):
        return

    query = (message.text or "").strip()
    if not query:
        await message.answer("Запрос пустой.")
        return

    await insert_query(message.from_user.id, query)
    await state.clear()
    await message.answer(
        f"✅ Отслеживаю:\n{query}",
        reply_markup=queries_keyboard(),
    )


@dp.callback_query(F.data == "query:delete")
async def delete_query_menu(callback: CallbackQuery):
    if not await ensure_owner_callback(callback):
        return

    if not active_queries:
        await callback.answer("Запросов нет.", show_alert=True)
        return

    rows = [
        [InlineKeyboardButton(
            text=f"❌ {q[:45]}",
            callback_data=f"query:remove:{qid}",
        )]
        for qid, q in active_queries
    ]
    rows.append([
        InlineKeyboardButton(text="◀️ Назад", callback_data="queries")
    ])

    await callback.message.edit_text(
        "🗑 Выбери запрос:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("query:remove:"))
async def remove_query(callback: CallbackQuery):
    if not await ensure_owner_callback(callback):
        return

    qid = int(callback.data.split(":")[2])
    owner = await get_owner_id()

    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM queries WHERE id=$1 AND owner_id=$2",
            qid,
            owner,
        )

    await reload_cache()
    await callback.message.edit_text(
        await queries_text(),
        reply_markup=queries_keyboard(),
    )
    await callback.answer("Удалено ✅")


async def chats_keyboard(page: int, dialogs: list[dict]):
    total_pages = max(1, (len(dialogs) + CHATS_PER_PAGE - 1) // CHATS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    current = dialogs[
        page * CHATS_PER_PAGE:(page + 1) * CHATS_PER_PAGE
    ]

    rows = []
    for chat in current:
        icon = "✅" if chat["id"] in selected_chat_ids else "⬜"
        name = chat["name"]
        if len(name) > 38:
            name = name[:35] + "..."
        rows.append([
            InlineKeyboardButton(
                text=f"{icon} {name}",
                callback_data=f"chat:toggle:{chat['id']}:{page}",
            )
        ])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"chats:{page-1}"))

    nav.append(InlineKeyboardButton(
        text=f"{page+1}/{total_pages}",
        callback_data="noop",
    ))

    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"chats:{page+1}"))

    rows.append(nav)
    rows.append([
        InlineKeyboardButton(
            text="🔄 Обновить список",
            callback_data=f"chats_refresh:{page}",
        )
    ])
    rows.append([
        InlineKeyboardButton(text="◀️ Главное меню", callback_data="home")
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


async def render_chats(message: Message, page=0, edit=False, force=False):
    if not await authorized():
        text = "🔴 Telegram не подключён.\n\nИспользуй /login."
        if edit:
            await message.edit_text(text)
        else:
            await message.answer(text)
        return

    try:
        dialogs = await get_dialogs(force=force)
    except Exception as e:
        print("DIALOG ERROR:", repr(e))
        text = f"❌ Не удалось получить чаты:\n{e}"
        if edit:
            await message.edit_text(text)
        else:
            await message.answer(text)
        return

    text = (
        "💬 ВЫБОР ЧАТОВ\n\n"
        f"Найдено: {len(dialogs)}\n"
        f"Выбрано: {len(selected_chat_ids)}\n\n"
        "✅ — отслеживается\n"
        "⬜ — не отслеживается"
    )

    markup = await chats_keyboard(page, dialogs)

    if edit:
        await message.edit_text(text, reply_markup=markup)
    else:
        await message.answer(text, reply_markup=markup)


@dp.message(Command("chats"))
async def chats_cmd(message: Message):
    if not await ensure_owner_message(message):
        return
    await render_chats(message)


@dp.callback_query(F.data.startswith("chats:"))
async def chats_cb(callback: CallbackQuery):
    if not await ensure_owner_callback(callback):
        return
    page = int(callback.data.split(":")[1])
    await render_chats(callback.message, page, True)
    await callback.answer()


@dp.callback_query(F.data.startswith("chats_refresh:"))
async def chats_refresh(callback: CallbackQuery):
    if not await ensure_owner_callback(callback):
        return
    page = int(callback.data.split(":")[1])
    await render_chats(callback.message, page, True, True)
    await callback.answer("Обновлено ✅")


@dp.callback_query(F.data.startswith("chat:toggle:"))
async def toggle_chat(callback: CallbackQuery):
    if not await ensure_owner_callback(callback):
        return

    _, _, chat_id_raw, page_raw = callback.data.split(":")
    chat_id = int(chat_id_raw)
    page = int(page_raw)

    dialogs = await get_dialogs()
    chat = next((x for x in dialogs if x["id"] == chat_id), None)

    if not chat:
        await callback.answer("Чат не найден.", show_alert=True)
        return

    owner = await get_owner_id()

    async with db_pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM chats WHERE owner_id=$1 AND chat_id=$2",
            owner,
            chat_id,
        )

        if exists:
            await conn.execute(
                "DELETE FROM chats WHERE owner_id=$1 AND chat_id=$2",
                owner,
                chat_id,
            )
            answer = "Выключено"
        else:
            await conn.execute("""
                INSERT INTO chats(owner_id, chat_id, title)
                VALUES($1, $2, $3)
                ON CONFLICT(owner_id, chat_id)
                DO UPDATE SET title=EXCLUDED.title
            """, owner, chat_id, chat["name"])
            answer = "Включено ✅"

    await reload_cache()
    await callback.message.edit_reply_markup(
        reply_markup=await chats_keyboard(page, dialogs)
    )
    await callback.answer(answer)


@dp.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery):
    await callback.answer()


@dp.message(Command("test"))
async def test_cmd(message: Message):
    if not await ensure_owner_message(message):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("/test продам айфон17 256gb")
        return

    hits = [q for _, q in active_queries if matches(q, parts[1])]

    if hits:
        await message.answer(
            "✅ ПОИСК РАБОТАЕТ\n\n"
            + "\n".join(f"• {x}" for x in hits)
        )
    else:
        await message.answer("❌ Совпадений нет.")


async def monitor_handler(event):
    try:
        text = event.raw_text or ""
        chat_id = event.chat_id

        if not text or chat_id is None:
            return
        if int(chat_id) not in selected_chat_ids:
            return

        hit = next(
            (q for _, q in active_queries if matches(q, text)),
            None,
        )
        if not hit:
            return

        chat = await event.get_chat()
        title = (
            getattr(chat, "title", None)
            or getattr(chat, "username", None)
            or "Telegram"
        )
        sender = await sender_name(event)
        link = message_link(event, chat)

        body = text[:3000] + ("\n\n…" if len(text) > 3000 else "")
        notification = (
            f"🔥 ЗАПРОС: {hit}\n\n"
            f"💬 {title}\n"
            f"👤 {sender}\n\n"
            f"{body}"
        )

        markup = None
        if link:
            markup = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="🔗 Открыть оригинальное сообщение",
                    url=link,
                )
            ]])

        await bot.send_message(
            await get_owner_id(),
            notification,
            reply_markup=markup,
            disable_web_page_preview=True,
        )

        print(f"MATCH SENT | chat={chat_id} | msg={event.id} | query={hit!r}")

    except Exception as e:
        print("MONITOR HANDLER ERROR:", repr(e))


async def setup_commands():
    await bot.set_my_commands([
        BotCommand(command="start", description="Главное меню"),
        BotCommand(command="add", description="Добавить запрос"),
        BotCommand(command="queries", description="Мои запросы"),
        BotCommand(command="chats", description="Выбрать чаты"),
        BotCommand(command="status", description="Статус мониторинга"),
        BotCommand(command="test", description="Проверить поиск"),
        BotCommand(command="login", description="Подключить Telegram"),
        BotCommand(command="whoami", description="Мой Telegram ID"),
    ])


async def shutdown():
    if monitor_task and not monitor_task.done():
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass

    for client in (user_client, login_client):
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass

    if db_pool:
        await db_pool.close()


async def main():
    await init_db()
    await reload_cache()
    await load_saved_session()
    await setup_commands()

    print("Control bot started.")

    try:
        await dp.start_polling(bot)
    finally:
        await shutdown()


if __name__ == "__main__":
    asyncio.run(main())
