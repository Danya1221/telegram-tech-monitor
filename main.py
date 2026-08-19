import os
import re
import asyncio
import aiosqlite

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

from telethon import TelegramClient, events
from rapidfuzz import fuzz


# =========================
# НАСТРОЙКИ ИЗ RAILWAY
# =========================

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]

DATA_DIR = os.environ.get("DATA_DIR", "/data")

SESSION_PATH = f"{DATA_DIR}/telegram_user"
DB_PATH = f"{DATA_DIR}/monitor.db"

os.makedirs(DATA_DIR, exist_ok=True)


# =========================
# TELEGRAM
# =========================

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

client = TelegramClient(
    SESSION_PATH,
    API_ID,
    API_HASH
)


# =========================
# ПОИСК
# =========================

def normalize(text: str) -> str:
    text = text.lower()

    # Частые варианты написания техники
    replacements = {
        "айфон": "iphone",
        "айфона": "iphone",
        "айфоны": "iphone",
        "айфоне": "iphone",
        "макбук": "macbook",
        "мак": "macbook",
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    # iphone17 -> iphone 17
    text = re.sub(r"([a-zа-яё])(\d)", r"\1 \2", text)
    text = re.sub(r"(\d)([a-zа-яё])", r"\1 \2", text)

    # Убираем лишние символы
    text = re.sub(r"[^a-zа-яё0-9]+", " ", text)

    return " ".join(text.split())


def matches(query: str, message: str) -> bool:
    query = normalize(query)
    message = normalize(message)

    # Точное вхождение
    if query in message:
        return True

    # Нечёткое совпадение
    score = fuzz.partial_ratio(query, message)

    return score >= 75


# =========================
# БАЗА
# =========================

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


# =========================
# УПРАВЛЯЮЩИЙ БОТ
# =========================

@dp.message(Command("start"))
async def start_handler(message: Message):

    await message.answer(
        "🔎 Tech Monitor\n\n"
        "Команды:\n\n"
        "/add iPhone 17 — добавить запрос\n"
        "/queries — мои запросы\n"
        "/del 1 — удалить запрос\n\n"
        "/chats — показать доступные чаты\n"
        "/select ID — отслеживать чат\n"
        "/selected — выбранные чаты"
    )


@dp.message(Command("add"))
async def add_handler(message: Message):

    parts = message.text.split(maxsplit=1)

    if len(parts) < 2:
        await message.answer(
            "Напиши например:\n\n"
            "/add iPhone 17"
        )
        return

    query = parts[1].strip()

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            INSERT INTO queries (user_id, query)
            VALUES (?, ?)
            """,
            (
                message.from_user.id,
                query
            )
        )

        await db.commit()

    await message.answer(
        f"✅ Добавил:\n{query}"
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

    text = "🔎 Твои запросы:\n\n"

    for query_id, query in rows:
        text += f"{query_id}. {query}\n"

    await message.answer(text)


@dp.message(Command("del"))
async def delete_handler(message: Message):

    parts = message.text.split(maxsplit=1)

    if len(parts) < 2:
        await message.answer(
            "Например:\n/del 1"
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

    await message.answer("🗑 Удалено")


# =========================
# СПИСОК ЧАТОВ
# =========================

@dp.message(Command("chats"))
async def chats_handler(message: Message):

    dialogs = await client.get_dialogs()

    chats = []

    for dialog in dialogs:

        if dialog.is_group or dialog.is_channel:

            chats.append(
                f"{dialog.name}\n"
                f"ID: {dialog.id}"
            )

    if not chats:

        await message.answer(
            "Чаты не найдены."
        )

        return

    # Telegram имеет ограничение длины сообщения
    text = "💬 Доступные чаты:\n\n"

    for chat in chats:

        addition = chat + "\n\n"

        if len(text) + len(addition) > 3500:

            await message.answer(text)

            text = ""

        text += addition

    if text:

        await message.answer(text)


# =========================
# ВЫБОР ЧАТА
# =========================

@dp.message(Command("select"))
async def select_handler(message: Message):

    parts = message.text.split(maxsplit=1)

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

        entity = await client.get_entity(chat_id)

    except Exception:

        await message.answer(
            "Не удалось найти этот чат."
        )

        return

    title = getattr(
        entity,
        "title",
        str(chat_id)
    )

    async with aiosqlite.connect(DB_PATH) as db:

        # Не добавляем один чат дважды
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

        existing = await cursor.fetchone()

        if existing:

            await message.answer(
                "Этот чат уже отслеживается."
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
        f"✅ Теперь отслеживаю:\n{title}"
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
            "Ты ещё не выбрал чаты."
        )

        return

    text = "📡 Отслеживаются:\n\n"

    for chat_id, title in rows:

        text += (
            f"{title}\n"
            f"{chat_id}\n\n"
        )

    await message.answer(text)


# =========================
# МОНИТОРИНГ СООБЩЕНИЙ
# =========================

@client.on(events.NewMessage(incoming=True))
async def new_message_handler(event):

    if not event.raw_text:
        return

    chat_id = event.chat_id

    # Проверяем, выбран ли этот чат
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

            if matches(
                query,
                event.raw_text
            ):

                chat = await event.get_chat()

                title = getattr(
                    chat,
                    "title",
                    "Telegram"
                )

                notification = (
                    f"🔥 НАЙДЕНО\n\n"
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

                # Одно сообщение отправляем один раз
                break


# =========================
# ЗАПУСК
# =========================

async def main():

    await init_db()

    print("Запускаю Telegram client...")

    await client.start()

    print("Telegram client работает.")
    print("Управляющий бот работает.")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
