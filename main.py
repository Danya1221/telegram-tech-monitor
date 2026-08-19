async def main():
    await init_db()

    print("Подключаю Telethon...")

    await client.connect()

    authorized = await client.is_user_authorized()

    if authorized:
        me = await client.get_me()

        print(
            f"Telegram user подключён | "
            f"id={me.id} | "
            f"name={me.first_name}"
        )

        await client.set_receive_updates(True)

    else:
        print("Telegram user НЕ авторизован.")

    await setup_commands()

    print("Управляющий бот запущен.")

    if authorized:
        print("Telethon monitor запущен.")

        await asyncio.gather(
            dp.start_polling(bot),
            client.run_until_disconnected(),
        )
    else:
        print(
            "Telethon monitor ждёт авторизации через /login."
        )

        # Главное:
        # не запускаем run_until_disconnected()
        # на неавторизованной сессии.
        await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
