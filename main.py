import os
import re
import asyncio
import aiosqlite

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message

from telethon import TelegramClient, events
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
)

from rapidfuzz import fuzz


# =========================================================
# НАСТРОЙКИ
# =========================================================

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]

DATA_DIR = "/data"
SESSION_PATH = f"{DATA_DIR}/telegram_user"
DB_PATH = f"{DATA_DIR}/monitor.db"

os.makedirs(DATA_DIR, exist_ok=True)


bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# ВАЖНО:
# Здесь НЕ вызываем client.start(),
# потому что start() пытается спросить телефон через input().
client = TelegramClient(
    SESSION_PATH,
    API_ID,
    API_HASH
)


# =========================================================
# СОСТОЯНИЯ АВТОРИЗАЦИИ
# =========================================================

class LoginStates(StatesGroup):
    waiting_phone = State()
    waiting_code = State()
    waiting_password = State()


# =========================================================
# БАЗА
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
                title TEXT
            )
        """)

        await db.commit()


# =========================================================
# ПОИСК
# =========================================================

def normalize(text: str) -> str:

    text = text.lower()

    replacements = {
        "айфон": "iphone",
        "айфона": "iphone",
        "айфоны": "iphone",
        "айфоне": "iphone",
        "макбук": "macbook",
        "макбука": "macbook",
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    # iphone17 -> iphone 17
    text = re.sub(r"([a-zа-яё])(\d)", r"\1 \2", text)
    text = re.sub(r"(\d)([a-zа-яё])", r"\1 \2", text)

    text = re.sub(
        r"[^a-zа-яё0-9]+",
        " ",
        text
    )

    return " ".join(text.split())


def matches(query: str, message: str) -> bool:

    query = normalize(query)
    message = normalize(message)

    if query in message:
        return True

    score = fuzz.partial_ratio(
        query,
        message
    )

    return score >= 75


# =========================================================
# /START
# =========================================================

@dp.message(Command("start"))
async def start_handler(message: Message):

    authorized = await client.is_user_authorized()

    if authorized:

        status = "🟢 Telegram-аккаунт подключён"

    else:

        status = "🔴 Telegram-аккаунт ещё не подключён"

    await message.answer(
        "🔎 TECH MONITOR\n\n"
        f"{status}\n\n"

        "Авторизация:\n"
        "/login — подключить аккаунт\n"
        "/status — проверить состояние\n\n"

        "Поиск:\n"
        "/add iPhone 17\n"
        "/queries\n"
        "/del 1\n\n"

        "Чаты:\n"
        "/chats\n"
        "/select ID\n"
        "/selected"
    )


# =========================================================
# STATUS
# =========================================================

@dp.message(Command("status"))
async def status_handler(message: Message):

    if await client.is_user_authorized():

        me = await client.get_me()

        await message.answer(
            "🟢 Аккаунт подключён.\n\n"
            f"Имя: {me.first_name or '-'}\n"
            f"ID: {me.id}"
        )

    else:

        await message.answer(
            "🔴 Telegram-аккаунт не подключён.\n\n"
            "Напиши /login"
        )


# =========================================================
# АВТОРИЗАЦИЯ
# =========================================================

@dp.message(Command("login"))
async def login_handler(
    message: Message,
    state: FSMContext
):

    if await client.is_user_authorized():

        await message.answer(
            "✅ Аккаунт уже авторизован."
        )

        return

    await state.set_state(
        LoginStates.waiting_phone
    )

    await message.answer(
        "📱 Отправь номер телефона Telegram.\n\n"
        "Например:\n"
        "+37212345678\n\n"
        "После авторизации это сообщение можешь удалить."
    )


@dp.message(LoginStates.waiting_phone)
async def phone_handler(
    message: Message,
    state: FSMContext
):

    phone = message.text.strip()

    if not phone.startswith("+"):

        await message.answer(
            "Номер должен начинаться с +\n\n"
            "Например:\n"
            "+37212345678"
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
            "📨 Telegram отправил код.\n\n"
            "Отправь его сюда.\n\n"
            "Например:\n"
            "12345"
        )

    except Exception as error:

        await message.answer(
            f"❌ Не удалось отправить код:\n{error}"
        )


@dp.message(LoginStates.waiting_code)
async def code_handler(
    message: Message,
    state: FSMContext
):

    code = (
        message.text
        .replace(" ", "")
        .replace("-", "")
        .strip()
    )

    data = await state.get_data()

    phone = data["phone"]
    phone_code_hash = data["phone_code_hash"]

    try:

        await client.sign_in(
            phone=phone,
            code=code,
            phone_code_hash=phone_code_hash
        )

        await state.clear()

        me = await client.get_me()

        await message.answer(
            "✅ ГОТОВО!\n\n"
            "Telegram-аккаунт подключён.\n"
            f"{me.first_name or ''}\n\n"
            "Теперь попробуй /chats"
        )

    except SessionPasswordNeededError:

        await state.set_state(
            LoginStates.waiting_password
        )

        await message.answer(
            "🔐 На аккаунте включена двухэтапная защита.\n\n"
            "Отправь пароль 2FA."
        )

    except PhoneCodeInvalidError:

        await message.answer(
            "❌ Неверный код.\n"
            "Попробуй ещё раз."
        )

    except PhoneCodeExpiredError:

        await state.clear()

        await message.answer(
            "❌ Код истёк.\n\n"
            "Начни заново: /login"
        )

    except Exception as error:

        await state.clear()

        await message.answer(
            f"❌ Ошибка авторизации:\n{error}"
        )


@dp.message(LoginStates.waiting_password)
async def password_handler(
    message: Message,
    state: FSMContext
):

    password = message.text

    try:

        await client.sign_in(
            password=password
        )

        await state.clear()

        me = await client.get_me()

        await message.answer(
            "✅ ГОТОВО!\n\n"
            "Telegram-аккаунт подключён.\n"
            f"{me.first_name or ''}\n\n"
            "Теперь попробуй /chats"
        )

    except Exception as error:

        await message.answer(
            f"❌ Пароль не подошёл:\n{error}"
        )


# =========================================================
# ЗАПРОСЫ
# =========================================================

@dp.message(Command("add"))
async def add_handler(message: Message):

    parts = message.text.split(
        maxsplit=1
    )

    if len(parts) < 2:

        await message.answer(
            "Например:\n"
            "/add iPhone 17"
        )

        return

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
        f"✅ Отслеживаю:\n{query}"
    )


@dp.message(Command("queries"))
async def queries_handler(message: Message):

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT id, query
            FROM queries
            WHERE user_id = ?
            """,
            (message.from_user.id,)
        )

        rows = await cursor.fetchall()

    if not rows:

        await message.answer(
            "Запросов пока нет."
        )

        return

    text = "🔎 Запросы:\n\n"

    for query_id, query in rows:

        text += (
            f"{query_id}. {query}\n"
        )

    await message.answer(text)


@dp.message(Command("del"))
async def delete_handler(message: Message):

    parts = message.text.split(
        maxsplit=1
    )

    if len(parts) < 2:

        await message.answer(
            "Например:\n"
            "/del 1"
        )

        return

    try:

        query_id = int(parts[1])

    except ValueError:

        await message.answer(
            "После /del нужен номер."
        )

        return

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            DELETE FROM queries
            WHERE id = ?
            AND user_id = ?
            """,
            (
                query_id,
                message.from_user.id
            )
        )

        await db.commit()

    await message.answer(
        "🗑 Запрос удалён."
    )


# =========================================================
# ЧАТЫ
# =========================================================

@dp.message(Command("chats"))
async def chats_handler(message: Message):

    if not await client.is_user_authorized():

        await message.answer(
            "Сначала подключи Telegram:\n"
            "/login"
        )

        return

    dialogs = await client.get_dialogs()

    result = []

    for dialog in dialogs:

        if dialog.is_group or dialog.is_channel:

            result.append(
                (
                    dialog.name,
                    dialog.id
                )
            )

    if not result:

        await message.answer(
            "Группы и каналы не найдены."
        )

        return

    text = "💬 ДОСТУПНЫЕ ЧАТЫ:\n\n"

    for name, chat_id in result:

        line = (
            f"{name}\n"
            f"ID: {chat_id}\n\n"
        )

        if len(text) + len(line) > 3500:

            await message.answer(text)

            text = ""

        text += line

    if text:

        await message.answer(text)


# =========================================================
# ВЫБОР ЧАТА
# =========================================================

@dp.message(Command("select"))
async def select_handler(message: Message):

    if not await client.is_user_authorized():

        await message.answer(
            "Сначала /login"
        )

        return

    parts = message.text.split(
        maxsplit=1
    )

    if len(parts) < 2:

        await message.answer(
            "Например:\n"
            "/select -100123456789"
        )

        return

    try:

        chat_id = int(parts[1])

    except ValueError:

        await message.answer(
            "Неверный ID."
        )

        return

    try:

        entity = await client.get_entity(
            chat_id
        )

    except Exception:

        await message.answer(
            "Не удалось найти чат."
        )

        return

    title = getattr(
        entity,
        "title",
        str(chat_id)
    )

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT id
            FROM chats
            WHERE user_id = ?
            AND chat_id = ?
            """,
            (
                message.from_user.id,
                chat_id
            )
        )

        if await cursor.fetchone():

            await message.answer(
                "Этот чат уже выбран."
            )

            return

        await db.execute(
            """
            INSERT INTO chats
            (user_id, chat_id, title)
            VALUES (?, ?, ?)
            """,
            (
                message.from_user.id,
                chat_id,
                title
            )
        )

        await db.commit()

    await message.answer(
        f"✅ Отслеживаю:\n{title}"
    )


@dp.message(Command("selected"))
async def selected_handler(message: Message):

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT chat_id, title
            FROM chats
            WHERE user_id = ?
            """,
            (message.from_user.id,)
        )

        rows = await cursor.fetchall()

    if not rows:

        await message.answer(
            "Чаты пока не выбраны."
        )

        return

    text = "📡 ОТСЛЕЖИВАЮТСЯ:\n\n"

    for chat_id, title in rows:

        text += (
            f"{title}\n"
            f"{chat_id}\n\n"
        )

    await message.answer(text)


# =========================================================
# МОНИТОРИНГ
# =========================================================

@client.on(events.NewMessage(incoming=True))
async def monitor_handler(event):

    if not event.raw_text:
        return

    chat_id = event.chat_id

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

    for (owner_id,) in owners:

        async with aiosqlite.connect(DB_PATH) as db:

            cursor = await db.execute(
                """
                SELECT query
                FROM queries
                WHERE user_id = ?
                """,
                (owner_id,)
            )

            queries = await cursor.fetchall()

        for (query,) in queries:

            if not matches(
                query,
                event.raw_text
            ):
                continue

            chat = await event.get_chat()

            title = getattr(
                chat,
                "title",
                "Telegram"
            )

            notification = (
                "🔥 НАЙДЕНО\n\n"
                f"🔎 {query}\n\n"
                f"💬 {title}\n\n"
                f"{event.raw_text}"
            )

            try:

                await bot.send_message(
                    owner_id,
                    notification
                )

            except Exception as error:

                print(
                    "Ошибка отправки:",
                    error
                )

            break


# =========================================================
# ЗАПУСК
# =========================================================

async def main():

    await init_db()

    print(
        "Подключаю Telethon..."
    )

    # Просто соединяемся.
    # Никаких input() и запросов телефона здесь нет.
    await client.connect()

    if await client.is_user_authorized():

        print(
            "Telegram user уже авторизован."
        )

    else:

        print(
            "Telegram user не авторизован."
        )

    print(
        "Управляющий бот запущен."
    )

    # aiogram запускает обычный Bot API polling
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
