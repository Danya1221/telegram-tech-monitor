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

os.makedirs(DATA_DIR, exist_ok=True)

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

client = TelegramClient(
    SESSION_PATH,
    API_ID,
    API_HASH
)

CHATS_PER_PAGE = 8


# =========================================================
# FSM
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


# =========================================================
# SEARCH
# =========================================================

def normalize(text: str) -> str:
    text = text.lower().strip()

    replacements = {
        "айфон": "iphone",
        "айфона": "iphone",
        "айфоны": "iphone",
        "айфоне": "iphone",
        "айфончик": "iphone",

        "макбук": "macbook",
        "макбука": "macbook",
        "макбуки": "macbook",

        "самсунг": "samsung",
        "галакси": "galaxy",

        "плейстейшен": "playstation",
        "плойка": "playstation",

        "иксбокс": "xbox",
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    # iphone17 -> iphone 17
    text = re.sub(r"([a-zа-яё])(\d)", r"\1 \2", text)
    text = re.sub(r"(\d)([a-zа-яё])", r"\1 \2", text)

    # убираем символы
    text = re.sub(r"[^a-zа-яё0-9]+", " ", text)

    return " ".join(text.split())


def extract_numbers(text: str):
    return re.findall(r"\d+", normalize(text))


def matches(query: str, message: str) -> bool:
    q = normalize(query)
    m = normalize(message)

    if not q or not m:
        return False

    # Точное вхождение
    if q in m:
        return True

    # Если в запросе есть номер модели,
    # стараемся не путать, например, 17 и 16.
    query_numbers = extract_numbers(q)

    if query_numbers:
        message_numbers = extract_numbers(m)

        if not any(number in message_numbers for number in query_numbers):
            return False

    score1 = fuzz.partial_ratio(q, m)
    score2 = fuzz.token_set_ratio(q, m)

    return max(score1, score2) >= 72


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
                )
            ]
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
            ]
        ]
    )


# =========================================================
# HELPERS
# =========================================================

async def selected_chat_ids(user_id: int):
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


async def get_dialog_list():
    dialogs = await client.get_dialogs()

    result = []

    for dialog in dialogs:
        if dialog.is_group or dialog.is_channel:
            result.append(
                {
                    "id": dialog.id,
                    "name": dialog.name or "Без названия"
                }
            )

    result.sort(
        key=lambda x: x["name"].lower()
    )

    return result


def make_message_link(event, chat):
    username = getattr(chat, "username", None)

    if username:
        return f"https://t.me/{username}/{event.id}"

    chat_id = event.chat_id

    # Приватные супергруппы/каналы обычно имеют ID -100...
    if chat_id and str(chat_id).startswith("-100"):
        internal_id = str(chat_id)[4:]
        return f"https://t.me/c/{internal_id}/{event.id}"

    return None


async def sender_display(event):
    try:
        sender = await event.get_sender()

        if not sender:
            return "Неизвестно"

        username = getattr(sender, "username", None)

        if username:
            return f"@{username}"

        first_name = getattr(sender, "first_name", None)
        last_name = getattr(sender, "last_name", None)

        full_name = " ".join(
            part for part in [first_name, last_name]
            if part
        )

        return full_name or "Неизвестно"

    except Exception:
        return "Неизвестно"


# =========================================================
# START
# =========================================================

@dp.message(Command("start"))
async def start_handler(message: Message):
    await message.answer(
        "🔎 Tech Monitor\n\n"
        "Выбирай, что хочешь настроить:",
        reply_markup=main_menu()
    )


@dp.callback_query(F.data == "home")
async def home_callback(callback: CallbackQuery):
    await callback.message.edit_text(
        "🔎 Tech Monitor\n\n"
        "Выбирай, что хочешь настроить:",
        reply_markup=main_menu()
    )

    await callback.answer()


# =========================================================
# STATUS
# =========================================================

async def status_text(user_id: int):
    authorized = await client.is_user_authorized()

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM queries
            WHERE user_id = ?
            """,
            (user_id,)
        )

        query_count = (await cursor.fetchone())[0]

        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM chats
            WHERE user_id = ?
            """,
            (user_id,)
        )

        chat_count = (await cursor.fetchone())[0]

    connection = (
        "🟢 подключён"
        if authorized
        else "🔴 не подключён"
    )

    return (
        "📡 СТАТУС\n\n"
        f"Telegram: {connection}\n"
        f"🔎 Запросов: {query_count}\n"
        f"💬 Чатов: {chat_count}"
    )


@dp.message(Command("status"))
async def status_command(message: Message):
    await message.answer(
        await status_text(message.from_user.id),
        reply_markup=main_menu()
    )


@dp.callback_query(F.data == "status")
async def status_callback(callback: CallbackQuery):
    await callback.message.edit_text(
        await status_text(callback.from_user.id),
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
        await message.answer(
            "✅ Telegram-аккаунт уже подключён."
        )
        return

    await state.set_state(
        LoginStates.waiting_phone
    )

    await message.answer(
        "📱 Отправь номер телефона.\n\n"
        "Например:\n"
        "+37212345678"
    )


@dp.message(LoginStates.waiting_phone)
async def phone_handler(
    message: Message,
    state: FSMContext
):
    phone = message.text.strip()

    if not phone.startswith("+"):
        await message.answer(
            "Номер должен начинаться с +"
        )
        return

    try:
        result = await client.send_code_request(
            phone
        )

        await state.update_data(
            phone=phone,
            phone_code_hash=result.phone_code_hash
        )

        await state.set_state(
            LoginStates.waiting_code
        )

        await message.answer(
            "📨 Код отправлен.\n\n"
            "Введи его С ПРОБЕЛАМИ.\n\n"
            "Например:\n"
            "1 2 3 4 5"
        )

    except Exception as error:
        await message.answer(
            f"❌ Ошибка:\n{error}"
        )


@dp.message(LoginStates.waiting_code)
async def code_handler(
    message: Message,
    state: FSMContext
):
    code = re.sub(
        r"\D",
        "",
        message.text
    )

    data = await state.get_data()

    try:
        await client.sign_in(
            phone=data["phone"],
            code=code,
            phone_code_hash=data["phone_code_hash"]
        )

        await state.clear()

        await message.answer(
            "✅ Telegram подключён!",
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
            "❌ Код неверный."
        )

    except PhoneCodeExpiredError:
        await state.clear()

        await message.answer(
            "❌ Код истёк.\n"
            "Начни заново: /login"
        )

    except Exception as error:
        await message.answer(
            f"❌ Ошибка:\n{error}"
        )


@dp.message(LoginStates.waiting_password)
async def password_handler(
    message: Message,
    state: FSMContext
):
    try:
        await client.sign_in(
            password=message.text
        )

        await state.clear()

        await message.answer(
            "✅ Telegram подключён!",
            reply_markup=main_menu()
        )

    except Exception as error:
        await message.answer(
            f"❌ Ошибка пароля:\n{error}"
        )


# =========================================================
# QUERIES
# =========================================================

async def queries_text(user_id: int):
    rows = await get_queries(user_id)

    if not rows:
        return (
            "🔎 ЗАПРОСЫ\n\n"
            "Пока ничего не отслеживается."
        )

    lines = [
        "🔎 ЗАПРОСЫ",
        ""
    ]

    for _, query in rows:
        lines.append(f"• {query}")

    return "\n".join(lines)


@dp.message(Command("queries"))
async def queries_command(message: Message):
    await message.answer(
        await queries_text(message.from_user.id),
        reply_markup=queries_menu()
    )


@dp.callback_query(F.data == "queries")
async def queries_callback(callback: CallbackQuery):
    await callback.message.edit_text(
        await queries_text(callback.from_user.id),
        reply_markup=queries_menu()
    )

    await callback.answer()


@dp.message(Command("add"))
async def add_command(
    message: Message,
    state: FSMContext
):
    parts = message.text.split(
        maxsplit=1
    )

    if len(parts) == 2:
        query = parts[1].strip()

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT INTO queries
                (user_id, query)
                VALUES (?, ?)
                """,
                (
                    message.from_user.id,
                    query
                )
            )

            await db.commit()

        await message.answer(
            f"✅ Добавил запрос:\n{query}",
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


@dp.callback_query(F.data == "query:add")
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


@dp.message(QueryStates.waiting_query)
async def save_query(
    message: Message,
    state: FSMContext
):
    query = message.text.strip()

    if not query:
        await message.answer(
            "Запрос пустой."
        )
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO queries
            (user_id, query)
            VALUES (?, ?)
            """,
            (
                message.from_user.id,
                query
            )
        )

        await db.commit()

    await state.clear()

    await message.answer(
        f"✅ Отслеживаю:\n{query}",
        reply_markup=queries_menu()
    )


@dp.callback_query(F.data == "query:delete")
async def delete_query_menu(callback: CallbackQuery):
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
        keyboard.append([
            InlineKeyboardButton(
                text=f"❌ {query}",
                callback_data=f"query:remove:{query_id}"
            )
        ])

    keyboard.append([
        InlineKeyboardButton(
            text="◀️ Назад",
            callback_data="queries"
        )
    ])

    await callback.message.edit_text(
        "🗑 Нажми на запрос, который удалить:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=keyboard
        )
    )

    await callback.answer()


@dp.callback_query(
    F.data.startswith("query:remove:")
)
async def remove_query_callback(
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
        await queries_text(callback.from_user.id),
        reply_markup=queries_menu()
    )


# =========================================================
# CHATS
# =========================================================

async def build_chats_keyboard(
    user_id: int,
    page: int
):
    dialogs = await get_dialog_list()

    selected = await selected_chat_ids(
        user_id
    )

    total_pages = max(
        1,
        (len(dialogs) + CHATS_PER_PAGE - 1)
        // CHATS_PER_PAGE
    )

    page = max(
        0,
        min(page, total_pages - 1)
    )

    start = page * CHATS_PER_PAGE
    end = start + CHATS_PER_PAGE

    current = dialogs[start:end]

    keyboard = []

    for chat in current:
        enabled = chat["id"] in selected

        icon = "✅" if enabled else "⬜"

        name = chat["name"]

        if len(name) > 35:
            name = name[:32] + "..."

        keyboard.append([
            InlineKeyboardButton(
                text=f"{icon} {name}",
                callback_data=f"chat:toggle:{chat['id']}:{page}"
            )
        ])

    navigation = []

    if page > 0:
        navigation.append(
            InlineKeyboardButton(
                text="◀️",
                callback_data=f"chats:{page - 1}"
            )
        )

    navigation.append(
        InlineKeyboardButton(
            text=f"{page + 1}/{total_pages}",
            callback_data="noop"
        )
    )

    if page < total_pages - 1:
        navigation.append(
            InlineKeyboardButton(
                text="▶️",
                callback_data=f"chats:{page + 1}"
            )
        )

    keyboard.append(navigation)

    keyboard.append([
        InlineKeyboardButton(
            text="◀️ Главное меню",
            callback_data="home"
        )
    ])

    return InlineKeyboardMarkup(
        inline_keyboard=keyboard
    )


@dp.message(Command("chats"))
async def chats_command(message: Message):
    if not await client.is_user_authorized():
        await message.answer(
            "Сначала подключи Telegram через /login."
        )
        return

    await message.answer(
        "💬 ВЫБОР ЧАТОВ\n\n"
        "✅ — отслеживается\n"
        "⬜ — не отслеживается\n\n"
        "Просто нажимай на нужные чаты:",
        reply_markup=await build_chats_keyboard(
            message.from_user.id,
            0
        )
    )


@dp.callback_query(
    F.data.startswith("chats:")
)
async def chats_callback(
    callback: CallbackQuery
):
    if not await client.is_user_authorized():
        await callback.answer(
            "Сначала /login",
            show_alert=True
        )
        return

    page = int(
        callback.data.split(":")[1]
    )

    await callback.message.edit_text(
        "💬 ВЫБОР ЧАТОВ\n\n"
        "✅ — отслеживается\n"
        "⬜ — не отслеживается\n\n"
        "Просто нажимай на нужные чаты:",
        reply_markup=await build_chats_keyboard(
            callback.from_user.id,
            page
        )
    )

    await callback.answer()


@dp.callback_query(
    F.data.startswith("chat:toggle:")
)
async def toggle_chat_callback(
    callback: CallbackQuery
):
    parts = callback.data.split(":")

    chat_id = int(parts[2])
    page = int(parts[3])

    dialogs = await get_dialog_list()

    chat = next(
        (
            item
            for item in dialogs
            if item["id"] == chat_id
        ),
        None
    )

    if not chat:
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

        existing = await cursor.fetchone()

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

            text = "Выключено"

        else:
            await db.execute(
                """
                INSERT INTO chats
                (user_id, chat_id, title)
                VALUES (?, ?, ?)
                """,
                (
                    callback.from_user.id,
                    chat_id,
                    chat["name"]
                )
            )

            text = "Включено"

        await db.commit()

    await callback.message.edit_reply_markup(
        reply_markup=await build_chats_keyboard(
            callback.from_user.id,
            page
        )
    )

    await callback.answer(text)


@dp.callback_query(F.data == "noop")
async def noop_callback(callback: CallbackQuery):
    await callback.answer()


# =========================================================
# MONITOR
# =========================================================

@client.on(events.NewMessage(incoming=True))
async def monitor_handler(event):
    if not event.raw_text:
        return

    chat_id = event.chat_id

    if not chat_id:
        return

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT user_id
            FROM chats
            WHERE chat_id = ?
            """,
            (chat_id,)
        )

        owners = await cursor.fetchall()

    if not owners:
        return

    chat = await event.get_chat()
    title = getattr(
        chat,
        "title",
        "Telegram"
    )

    sender = await sender_display(event)

    link = make_message_link(
        event,
        chat
    )

    for (owner_id,) in owners:

        queries = await get_queries(
            owner_id
        )

        for _, query in queries:

            if not matches(
                query,
                event.raw_text
            ):
                continue

            notification = (
                f"🔥 ЗАПРОС: {query}\n\n"
                f"💬 {title}\n"
                f"👤 {sender}\n\n"
                f"{event.raw_text}"
            )

            keyboard = None

            if link:
                keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="🔗 Открыть оригинальное сообщение",
                                url=link
                            )
                        ]
                    ]
                )

            try:
                await bot.send_message(
                    owner_id,
                    notification,
                    reply_markup=keyboard
                )

            except Exception as error:
                print(
                    "Ошибка уведомления:",
                    error
                )

            # Одно сообщение = одно уведомление,
            # даже если совпало с несколькими запросами.
            break


# =========================================================
# COMMANDS
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
            command="login",
            description="Подключить Telegram"
        ),
    ])


# =========================================================
# RUN
# =========================================================

async def main():
    await init_db()

    print("Подключаю Telethon...")

    await client.connect()

    if await client.is_user_authorized():
        me = await client.get_me()

        print(
            f"Telegram user подключён: "
            f"{me.id}"
        )
    else:
        print(
            "Telegram user не авторизован."
        )

    await setup_commands()

    print("Управляющий бот запущен.")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
