import os
import re
import asyncio
import aiosqlite

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

from telethon import TelegramClient, events
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
)

from rapidfuzz import fuzz


# =========================================================
# CONFIG
# =========================================================

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]

DATA_DIR = os.environ.get("DATA_DIR", "/data")

SESSION_PATH = f"{DATA_DIR}/telegram_user"
DB_PATH = f"{DATA_DIR}/monitor.db"

CHATS_PER_PAGE = 8

# Если потом понадобится диагностика:
# Railway -> Variables -> DEBUG_EVENTS=1
DEBUG_EVENTS = os.environ.get("DEBUG_EVENTS", "0") == "1"

os.makedirs(DATA_DIR, exist_ok=True)


bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

client = TelegramClient(
    SESSION_PATH,
    API_ID,
    API_HASH,
    receive_updates=True,
)


# =========================================================
# STATES
# =========================================================

class LoginStates(StatesGroup):
    waiting_phone = State()
    waiting_code = State()
    waiting_password = State()


class QueryStates(StatesGroup):
    waiting_query = State()


# =========================================================
# DATABASE
# =========================================================

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute("""
            CREATE TABLE IF NOT EXISTS queries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                query TEXT NOT NULL
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                UNIQUE(user_id, chat_id)
            )
        """)

        await db.commit()


async def get_queries(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT id, query
            FROM queries
            WHERE user_id = ?
            ORDER BY id DESC
            """,
            (user_id,)
        )

        return await cursor.fetchall()


async def get_selected_chat_ids(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT chat_id
            FROM chats
            WHERE user_id = ?
            """,
            (user_id,)
        )

        rows = await cursor.fetchall()

    return {row[0] for row in rows}


# =========================================================
# SEARCH
# =========================================================

def normalize(text: str) -> str:

    if not text:
        return ""

    text = text.lower().strip()

    replacements = {
        # iPhone
        "айфончик": "iphone",
        "айфоны": "iphone",
        "айфона": "iphone",
        "айфоне": "iphone",
        "айфоном": "iphone",
        "айфон": "iphone",
        "айфн": "iphone",
        "ифон": "iphone",

        # MacBook
        "макбуки": "macbook",
        "макбука": "macbook",
        "макбук": "macbook",

        # Samsung
        "самсунг": "samsung",
        "самсунга": "samsung",
        "галакси": "galaxy",

        # PlayStation
        "плейстейшен": "playstation",
        "плейстейшн": "playstation",
        "плойка": "playstation",

        # Xbox
        "иксбокс": "xbox",
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    # iphone17 -> iphone 17
    # 17pro -> 17 pro
    text = re.sub(
        r"([a-zа-яё])(\d)",
        r"\1 \2",
        text
    )

    text = re.sub(
        r"(\d)([a-zа-яё])",
        r"\1 \2",
        text
    )

    # Удаляем тире, точки, запятые и т.д.
    text = re.sub(
        r"[^a-zа-яё0-9]+",
        " ",
        text
    )

    return " ".join(text.split())


def extract_numbers(text: str):
    return re.findall(
        r"\d+",
        normalize(text)
    )


def matches(query: str, message_text: str) -> bool:

    q = normalize(query)
    m = normalize(message_text)

    if not q or not m:
        return False

    # 1. Прямое совпадение
    if q in m:
        return True

    # 2. Цифры модели проверяем отдельно.
    # iPhone 17 не должен срабатывать на iPhone 16.
    query_numbers = extract_numbers(q)

    if query_numbers:

        message_numbers = extract_numbers(m)

        for number in query_numbers:
            if number not in message_numbers:
                return False

    # 3. Нечёткий поиск
    partial_score = fuzz.partial_ratio(
        q,
        m
    )

    token_score = fuzz.token_set_ratio(
        q,
        m
    )

    score = max(
        partial_score,
        token_score
    )

    return score >= 72


# =========================================================
# TELEGRAM DIALOGS
# =========================================================

async def get_dialog_list():

    if not await client.is_user_authorized():
        return []

    result = []

    # Получаем до 500 чатов.
    # Сюда входят и обычные, и архивированные диалоги.
    async for dialog in client.iter_dialogs(
        limit=500
    ):

        if not (
            dialog.is_group
            or dialog.is_channel
        ):
            continue

        result.append({
            "id": dialog.id,
            "name": dialog.name or "Без названия",
        })

    # Сортируем по названию
    result.sort(
        key=lambda x: x["name"].lower()
    )

    return result


# =========================================================
# MESSAGE LINK
# =========================================================

def make_message_link(event, chat):

    username = getattr(
        chat,
        "username",
        None
    )

    # Публичная группа / канал
    if username:
        return (
            f"https://t.me/"
            f"{username}/"
            f"{event.id}"
        )

    chat_id = event.chat_id

    # Приватная супергруппа / канал
    if chat_id and str(chat_id).startswith("-100"):

        internal_id = (
            abs(chat_id)
            - 1_000_000_000_000
        )

        if internal_id > 0:
            return (
                f"https://t.me/c/"
                f"{internal_id}/"
                f"{event.id}"
            )

    return None


# =========================================================
# SENDER
# =========================================================

async def get_sender_name(event):

    try:

        sender = await event.get_sender()

        if not sender:

            if event.post_author:
                return event.post_author

            return "Неизвестно"

        username = getattr(
            sender,
            "username",
            None
        )

        if username:
            return f"@{username}"

        title = getattr(
            sender,
            "title",
            None
        )

        if title:
            return title

        first_name = getattr(
            sender,
            "first_name",
            None
        )

        last_name = getattr(
            sender,
            "last_name",
            None
        )

        name = " ".join(
            x for x in [
                first_name,
                last_name
            ]
            if x
        )

        if name:
            return name

        if event.post_author:
            return event.post_author

        return "Неизвестно"

    except Exception as error:

        print(
            "Sender error:",
            error
        )

        return "Неизвестно"


# =========================================================
# KEYBOARDS
# =========================================================

def main_menu():

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔎 Запросы",
                    callback_data="queries"
                ),
                InlineKeyboardButton(
                    text="💬 Чаты",
                    callback_data="chats:0"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📡 Статус",
                    callback_data="status"
                ),
                InlineKeyboardButton(
                    text="🔄 Обновить",
                    callback_data="refresh"
                ),
            ],
        ]
    )


def queries_menu():

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="➕ Добавить запрос",
                    callback_data="query:add"
                )
            ],
            [
                InlineKeyboardButton(
                    text="🗑 Удалить запрос",
                    callback_data="query:delete"
                )
            ],
            [
                InlineKeyboardButton(
                    text="◀️ Главное меню",
                    callback_data="home"
                )
            ],
        ]
    )


# =========================================================
# START / HOME
# =========================================================

@dp.message(Command("start"))
async def start_handler(message: Message):

    await message.answer(
        "🔎 Tech Monitor\n\n"
        "Выбирай:",
        reply_markup=main_menu()
    )


@dp.callback_query(F.data == "home")
async def home_callback(callback: CallbackQuery):

    await callback.message.edit_text(
        "🔎 Tech Monitor\n\n"
        "Выбирай:",
        reply_markup=main_menu()
    )

    await callback.answer()


@dp.callback_query(F.data == "refresh")
async def refresh_callback(
    callback: CallbackQuery
):

    await callback.message.edit_text(
        "🔎 Tech Monitor\n\n"
        "Данные обновлены.",
        reply_markup=main_menu()
    )

    await callback.answer(
        "Обновлено ✅"
    )


# =========================================================
# STATUS
# =========================================================

async def build_status(user_id: int):

    authorized = (
        await client.is_user_authorized()
    )

    account_text = "не подключён"

    dialogs_count = 0

    if authorized:

        try:
            me = await client.get_me()

            account_text = (
                me.first_name
                or str(me.id)
            )

            dialogs = (
                await get_dialog_list()
            )

            dialogs_count = len(dialogs)

        except Exception as error:

            print(
                "Status error:",
                error
            )

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM queries
            WHERE user_id = ?
            """,
            (user_id,)
        )

        query_count = (
            await cursor.fetchone()
        )[0]

        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM chats
            WHERE user_id = ?
            """,
            (user_id,)
        )

        chat_count = (
            await cursor.fetchone()
        )[0]

    if authorized:
        connection = "🟢 подключён"
    else:
        connection = "🔴 не подключён"

    return (
        "📡 СТАТУС\n\n"
        f"Telegram: {connection}\n"
        f"👤 Аккаунт: {account_text}\n\n"
        f"💬 Доступно чатов: {dialogs_count}\n"
        f"✅ Выбрано чатов: {chat_count}\n"
        f"🔎 Запросов: {query_count}"
    )


@dp.message(Command("status"))
async def status_command(message: Message):

    await message.answer(
        await build_status(
            message.from_user.id
        ),
        reply_markup=main_menu()
    )


@dp.callback_query(F.data == "status")
async def status_callback(
    callback: CallbackQuery
):

    await callback.message.edit_text(
        await build_status(
            callback.from_user.id
        ),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="◀️ Назад",
                        callback_data="home"
                    )
                ]
            ]
        )
    )

    await callback.answer()


# =========================================================
# LOGIN
# =========================================================

@dp.message(Command("login"))
async def login_handler(
    message: Message,
    state: FSMContext
):

    if await client.is_user_authorized():

        me = await client.get_me()

        await message.answer(
            "✅ Уже подключено.\n\n"
            f"Аккаунт: "
            f"{me.first_name or me.id}"
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


@dp.message(
    LoginStates.waiting_phone
)
async def phone_handler(
    message: Message,
    state: FSMContext
):

    phone = (
        message.text
        or ""
    ).strip()

    if not phone.startswith("+"):

        await message.answer(
            "Номер должен начинаться с +"
        )

        return

    try:

        result = (
            await client.send_code_request(
                phone
            )
        )

        await state.update_data(
            phone=phone,
            phone_code_hash=(
                result.phone_code_hash
            )
        )

        await state.set_state(
            LoginStates.waiting_code
        )

        await message.answer(
            "📨 Код отправлен.\n\n"
            "Введи код С ПРОБЕЛАМИ.\n\n"
            "Например:\n"
            "1 2 3 4 5"
        )

    except Exception as error:

        print(
            "Login phone error:",
            repr(error)
        )

        await message.answer(
            f"❌ Ошибка:\n{error}"
        )


@dp.message(
    LoginStates.waiting_code
)
async def code_handler(
    message: Message,
    state: FSMContext
):

    code = re.sub(
        r"\D",
        "",
        message.text or ""
    )

    data = await state.get_data()

    try:

        await client.sign_in(
            phone=data["phone"],
            code=code,
            phone_code_hash=(
                data["phone_code_hash"]
            )
        )

        # После авторизации явно
        # активируем получение updates.
        await client.get_me()

        await client.set_receive_updates(
            True
        )

        await state.clear()

        await message.answer(
            "✅ Telegram подключён!\n\n"
            "Теперь открой 💬 Чаты.",
            reply_markup=main_menu()
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
            "❌ Код неверный.\n"
            "Попробуй ещё раз."
        )

    except PhoneCodeExpiredError:

        await state.clear()

        await message.answer(
            "❌ Код истёк.\n\n"
            "Напиши /login и запроси новый."
        )

    except Exception as error:

        print(
            "Login code error:",
            repr(error)
        )

        await message.answer(
            f"❌ Ошибка:\n{error}"
        )


@dp.message(
    LoginStates.waiting_password
)
async def password_handler(
    message: Message,
    state: FSMContext
):

    try:

        await client.sign_in(
            password=message.text
        )

        await client.get_me()

        await client.set_receive_updates(
            True
        )

        await state.clear()

        await message.answer(
            "✅ Telegram подключён!\n\n"
            "Теперь открой 💬 Чаты.",
            reply_markup=main_menu()
        )

    except Exception as error:

        print(
            "2FA error:",
            repr(error)
        )

        await message.answer(
            f"❌ Ошибка пароля:\n{error}"
        )


# =========================================================
# QUERIES
# =========================================================

async def build_queries_text(
    user_id: int
):

    rows = await get_queries(
        user_id
    )

    if not rows:

        return (
            "🔎 ЗАПРОСЫ\n\n"
            "Пока запросов нет."
        )

    text = "🔎 ЗАПРОСЫ\n\n"

    for _, query in rows:
        text += f"• {query}\n"

    return text


@dp.message(Command("queries"))
async def queries_command(
    message: Message
):

    await message.answer(
        await build_queries_text(
            message.from_user.id
        ),
        reply_markup=queries_menu()
    )


@dp.callback_query(
    F.data == "queries"
)
async def queries_callback(
    callback: CallbackQuery
):

    await callback.message.edit_text(
        await build_queries_text(
            callback.from_user.id
        ),
        reply_markup=queries_menu()
    )

    await callback.answer()


@dp.message(Command("add"))
async def add_command(
    message: Message,
    state: FSMContext
):

    parts = (
        message.text
        or ""
    ).split(
        maxsplit=1
    )

    if len(parts) == 2:

        query = parts[1].strip()

        await save_query_to_db(
            message.from_user.id,
            query
        )

        await message.answer(
            f"✅ Отслеживаю:\n{query}",
            reply_markup=queries_menu()
        )

        return

    await state.set_state(
        QueryStates.waiting_query
    )

    await message.answer(
        "🔎 Что искать?\n\n"
        "Например:\n"
        "iPhone 17"
    )


async def save_query_to_db(
    user_id: int,
    query: str
):

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            INSERT INTO queries
            (user_id, query)
            VALUES (?, ?)
            """,
            (
                user_id,
                query
            )
        )

        await db.commit()


@dp.callback_query(
    F.data == "query:add"
)
async def add_query_callback(
    callback: CallbackQuery,
    state: FSMContext
):

    await state.set_state(
        QueryStates.waiting_query
    )

    await callback.message.answer(
        "🔎 Напиши, что отслеживать.\n\n"
        "Например:\n"
        "iPhone 17"
    )

    await callback.answer()


@dp.message(
    QueryStates.waiting_query
)
async def query_input_handler(
    message: Message,
    state: FSMContext
):

    query = (
        message.text
        or ""
    ).strip()

    if not query:

        await message.answer(
            "Запрос пустой."
        )

        return

    await save_query_to_db(
        message.from_user.id,
        query
    )

    await state.clear()

    await message.answer(
        f"✅ Отслеживаю:\n{query}",
        reply_markup=queries_menu()
    )


@dp.callback_query(
    F.data == "query:delete"
)
async def query_delete_menu(
    callback: CallbackQuery
):

    rows = await get_queries(
        callback.from_user.id
    )

    if not rows:

        await callback.answer(
            "Запросов нет.",
            show_alert=True
        )

        return

    keyboard = []

    for query_id, query in rows:

        name = query

        if len(name) > 45:
            name = (
                name[:42]
                + "..."
            )

        keyboard.append([
            InlineKeyboardButton(
                text=f"❌ {name}",
                callback_data=(
                    f"query:remove:"
                    f"{query_id}"
                )
            )
        ])

    keyboard.append([
        InlineKeyboardButton(
            text="◀️ Назад",
            callback_data="queries"
        )
    ])

    await callback.message.edit_text(
        "🗑 Выбери запрос для удаления:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=keyboard
        )
    )

    await callback.answer()


@dp.callback_query(
    F.data.startswith(
        "query:remove:"
    )
)
async def query_remove_callback(
    callback: CallbackQuery
):

    query_id = int(
        callback.data.split(":")[2]
    )

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            DELETE FROM queries
            WHERE id = ?
            AND user_id = ?
            """,
            (
                query_id,
                callback.from_user.id
            )
        )

        await db.commit()

    await callback.answer(
        "Удалено ✅"
    )

    await callback.message.edit_text(
        await build_queries_text(
            callback.from_user.id
        ),
        reply_markup=queries_menu()
    )


# =========================================================
# CHAT KEYBOARD
# =========================================================

async def build_chats_keyboard(
    user_id: int,
    page: int,
    dialogs: list
):

    selected = (
        await get_selected_chat_ids(
            user_id
        )
    )

    total_pages = max(
        1,
        (
            len(dialogs)
            + CHATS_PER_PAGE
            - 1
        )
        // CHATS_PER_PAGE
    )

    page = max(
        0,
        min(
            page,
            total_pages - 1
        )
    )

    start = (
        page
        * CHATS_PER_PAGE
    )

    end = (
        start
        + CHATS_PER_PAGE
    )

    current = dialogs[
        start:end
    ]

    keyboard = []

    for chat in current:

        enabled = (
            chat["id"]
            in selected
        )

        icon = (
            "✅"
            if enabled
            else "⬜"
        )

        name = chat["name"]

        if len(name) > 38:

            name = (
                name[:35]
                + "..."
            )

        keyboard.append([
            InlineKeyboardButton(
                text=(
                    f"{icon} {name}"
                ),
                callback_data=(
                    "chat:toggle:"
                    f"{chat['id']}:"
                    f"{page}"
                )
            )
        ])

    navigation = []

    if page > 0:

        navigation.append(
            InlineKeyboardButton(
                text="◀️",
                callback_data=(
                    f"chats:"
                    f"{page - 1}"
                )
            )
        )

    navigation.append(
        InlineKeyboardButton(
            text=(
                f"{page + 1}/"
                f"{total_pages}"
            ),
            callback_data="noop"
        )
    )

    if page < total_pages - 1:

        navigation.append(
            InlineKeyboardButton(
                text="▶️",
                callback_data=(
                    f"chats:"
                    f"{page + 1}"
                )
            )
        )

    keyboard.append(
        navigation
    )

    keyboard.append([
        InlineKeyboardButton(
            text="🔄 Обновить список",
            callback_data=(
                f"chats:{page}"
            )
        )
    ])

    keyboard.append([
        InlineKeyboardButton(
            text="◀️ Главное меню",
            callback_data="home"
        )
    ])

    return InlineKeyboardMarkup(
        inline_keyboard=keyboard
    )


async def show_chats(
    message,
    user_id: int,
    page: int,
    edit: bool
):

    if not await client.is_user_authorized():

        text = (
            "🔴 Telegram не подключён.\n\n"
            "Напиши /login"
        )

        if edit:
            await message.edit_text(
                text
            )
        else:
            await message.answer(
                text
            )

        return

    try:

        dialogs = (
            await get_dialog_list()
        )

    except Exception as error:

        print(
            "GET DIALOGS ERROR:",
            repr(error)
        )

        text = (
            "❌ Не удалось получить чаты.\n\n"
            f"{error}"
        )

        if edit:
            await message.edit_text(
                text
            )
        else:
            await message.answer(
                text
            )

        return

    if not dialogs:

        text = (
            "💬 Telegram вернул 0 групп "
            "и каналов.\n\n"
            "Открой 📡 Статус и посмотри, "
            "под каким аккаунтом выполнен вход."
        )

        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="🔄 Повторить",
                        callback_data="chats:0"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="◀️ Назад",
                        callback_data="home"
                    )
                ]
            ]
        )

        if edit:
            await message.edit_text(
                text,
                reply_markup=markup
            )
        else:
            await message.answer(
                text,
                reply_markup=markup
            )

        return

    text = (
        "💬 ВЫБОР ЧАТОВ\n\n"
        f"Найдено: {len(dialogs)}\n\n"
        "✅ — отслеживается\n"
        "⬜ — не отслеживается\n\n"
        "Нажми на чат:"
    )

    markup = await build_chats_keyboard(
        user_id,
        page,
        dialogs
    )

    if edit:

        await message.edit_text(
            text,
            reply_markup=markup
        )

    else:

        await message.answer(
            text,
            reply_markup=markup
        )


# =========================================================
# CHATS
# =========================================================

@dp.message(Command("chats"))
async def chats_command(
    message: Message
):

    await show_chats(
        message,
        message.from_user.id,
        0,
        False
    )


@dp.callback_query(
    F.data.startswith("chats:")
)
async def chats_callback(
    callback: CallbackQuery
):

    page = int(
        callback.data.split(":")[1]
    )

    await show_chats(
        callback.message,
        callback.from_user.id,
        page,
        True
    )

    await callback.answer()


@dp.callback_query(
    F.data.startswith(
        "chat:toggle:"
    )
)
async def chat_toggle_callback(
    callback: CallbackQuery
):

    parts = (
        callback.data
        .split(":")
    )

    chat_id = int(
        parts[2]
    )

    page = int(
        parts[3]
    )

    dialogs = (
        await get_dialog_list()
    )

    chat = next(
        (
            item
            for item in dialogs
            if item["id"]
            == chat_id
        ),
        None
    )

    if chat is None:

        await callback.answer(
            "Чат не найден.",
            show_alert=True
        )

        return

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT id
            FROM chats
            WHERE user_id = ?
            AND chat_id = ?
            """,
            (
                callback.from_user.id,
                chat_id
            )
        )

        existing = (
            await cursor.fetchone()
        )

        if existing:

            await db.execute(
                """
                DELETE FROM chats
                WHERE user_id = ?
                AND chat_id = ?
                """,
                (
                    callback.from_user.id,
                    chat_id
                )
            )

            answer = "Выключено"

        else:

            await db.execute(
                """
                INSERT OR IGNORE INTO chats
                (user_id, chat_id, title)
                VALUES (?, ?, ?)
                """,
                (
                    callback.from_user.id,
                    chat_id,
                    chat["name"]
                )
            )

            answer = "Включено ✅"

        await db.commit()

    await callback.message.edit_reply_markup(
        reply_markup=(
            await build_chats_keyboard(
                callback.from_user.id,
                page,
                dialogs
            )
        )
    )

    await callback.answer(
        answer
    )


@dp.callback_query(
    F.data == "noop"
)
async def noop_callback(
    callback: CallbackQuery
):

    await callback.answer()


# =========================================================
# TEST SEARCH
# =========================================================

@dp.message(Command("test"))
async def test_search_handler(
    message: Message
):

    parts = (
        message.text
        or ""
    ).split(
        maxsplit=1
    )

    if len(parts) < 2:

        await message.answer(
            "Проверка поиска:\n\n"
            "/test продам айфон17 256gb"
        )

        return

    sample = parts[1]

    queries = await get_queries(
        message.from_user.id
    )

    if not queries:

        await message.answer(
            "Сначала добавь запрос."
        )

        return

    matched = []

    for _, query in queries:

        if matches(
            query,
            sample
        ):
            matched.append(query)

    if matched:

        await message.answer(
            "✅ ПОИСК РАБОТАЕТ\n\n"
            "Совпало:\n"
            + "\n".join(
                f"• {x}"
                for x in matched
            )
        )

    else:

        await message.answer(
            "❌ Ни один запрос "
            "не совпал."
        )


# =========================================================
# MONITOR TELETHON
# =========================================================

# Без incoming=True:
# ловим и чужие, и твои тестовые сообщения.
@client.on(
    events.NewMessage()
)
async def monitor_handler(event):

    try:

        text = event.raw_text

        if not text:
            return

        chat_id = event.chat_id

        if chat_id is None:
            return

        if DEBUG_EVENTS:

            print(
                "TG EVENT | "
                f"chat={chat_id} | "
                f"msg={event.id} | "
                f"out={event.out} | "
                f"text={text[:100]!r}"
            )

        # Ищем владельцев,
        # выбравших этот чат
        async with aiosqlite.connect(
            DB_PATH
        ) as db:

            cursor = await db.execute(
                """
                SELECT user_id
                FROM chats
                WHERE chat_id = ?
                """,
                (chat_id,)
            )

            owners = (
                await cursor.fetchall()
            )

        if not owners:

            if DEBUG_EVENTS:
                print(
                    "EVENT IGNORED: "
                    "chat not selected"
                )

            return

        chat = await event.get_chat()

        title = (
            getattr(
                chat,
                "title",
                None
            )
            or getattr(
                chat,
                "username",
                None
            )
            or "Telegram"
        )

        sender = (
            await get_sender_name(
                event
            )
        )

        link = make_message_link(
            event,
            chat
        )

        # Чтобы не упереться
        # в лимит Bot API сообщения
        visible_text = text

        if len(visible_text) > 3000:

            visible_text = (
                visible_text[:3000]
                + "\n\n…"
            )

        for (owner_id,) in owners:

            queries = await get_queries(
                owner_id
            )

            matched_query = None

            for _, query in queries:

                if matches(
                    query,
                    text
                ):

                    matched_query = query
                    break

            if not matched_query:

                if DEBUG_EVENTS:
                    print(
                        "EVENT IGNORED: "
                        "no query match"
                    )

                continue

            notification = (
                f"🔥 ЗАПРОС: "
                f"{matched_query}\n\n"
                f"💬 {title}\n"
                f"👤 {sender}\n\n"
                f"{visible_text}"
            )

            markup = None

            if link:

                markup = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text=(
                                    "🔗 Открыть "
                                    "оригинальное "
                                    "сообщение"
                                ),
                                url=link
                            )
                        ]
                    ]
                )

            try:

                await bot.send_message(
                    owner_id,
                    notification,
                    reply_markup=markup,
                    disable_web_page_preview=True,
                )

                print(
                    "MATCH SENT | "
                    f"query={matched_query!r} | "
                    f"chat={chat_id} | "
                    f"msg={event.id}"
                )

            except Exception as error:

                print(
                    "BOT SEND ERROR:",
                    repr(error)
                )

    except Exception as error:

        print(
            "MONITOR ERROR:",
            repr(error)
        )


# =========================================================
# BOT COMMAND MENU
# =========================================================

async def setup_commands():

    await bot.set_my_commands([
        BotCommand(
            command="start",
            description="Главное меню"
        ),
        BotCommand(
            command="add",
            description="Добавить запрос"
        ),
        BotCommand(
            command="queries",
            description="Мои запросы"
        ),
        BotCommand(
            command="chats",
            description="Выбрать чаты"
        ),
        BotCommand(
            command="status",
            description="Статус мониторинга"
        ),
        BotCommand(
            command="test",
            description="Проверить поиск"
        ),
        BotCommand(
            command="login",
            description="Подключить Telegram"
        ),
    ])


# =========================================================
# MAIN
# =========================================================

async def main():

    await init_db()

    print(
        "Подключаю Telethon..."
    )

    await client.connect()

    if await client.is_user_authorized():

        me = await client.get_me()

        # Явно включаем updates
        await client.set_receive_updates(
            True
        )

        print(
            "Telegram user подключён | "
            f"id={me.id} | "
            f"name={me.first_name}"
        )

        # Проверяем список диалогов
        try:

            dialogs = (
                await get_dialog_list()
            )

            print(
                "Telegram dialogs: "
                f"{len(dialogs)}"
            )

        except Exception as error:

            print(
                "DIALOG STARTUP ERROR:",
                repr(error)
            )

    else:

        print(
            "Telegram user "
            "НЕ авторизован."
        )

    await setup_commands()

    print(
        "Управляющий бот запущен."
    )

    print(
        "Telethon monitor запущен."
    )

    # Главное исправление:
    # bot polling + Telethon receiver
    # работают одновременно.
    await asyncio.gather(
        dp.start_polling(bot),
        client.run_until_disconnected(),
    )


if __name__ == "__main__":
    asyncio.run(main())
