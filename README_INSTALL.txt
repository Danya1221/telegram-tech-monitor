QUANTUM BOT — FIXED CURRENT VERSION

Files:
- main.py
- requirements.txt
- .python-version

Railway variables required:
API_ID
API_HASH
BOT_TOKEN
DATABASE_URL

Optional:
ADMIN_ID
ANALYTICS_TZ=Europe/Moscow or Europe/Amsterdam

Start command:
python main.py

Important:
Use exactly ONE Railway service and ONE replica for this bot.
The code also uses a PostgreSQL advisory lock and will refuse to start a second copy sharing the same BOT_TOKEN/DATABASE_URL.
