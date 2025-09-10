import asyncio
import os
import sqlite3

import pandas as pd
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # kanal/guruh username yoki chat_id
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# 🔎 Kerakli ustunlar
COLUMNS = [
    "ismingiz?",
    "telefon_raqamingiz?",
    "номер_телефона",
    "xodimlar_soni?",
    "adset_name",
    "ad_name",
]


# === DB Qism ===
def init_db():
    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()
    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS users
                   (
                       id
                       INTEGER
                       PRIMARY
                       KEY
                       AUTOINCREMENT,
                       name
                       TEXT,
                       phone
                       TEXT,
                       raw_phone
                       TEXT
                       UNIQUE,
                       workers
                       TEXT,
                       adset
                       TEXT,
                       ad_name
                       TEXT,
                       sent
                       INTEGER
                       DEFAULT
                       0
                   )
                   """)
    conn.commit()
    conn.close()


def save_users_from_sheet():
    df = pd.read_csv(CSV_URL)
    df = df[COLUMNS]

    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("""
                       INSERT
                       OR IGNORE INTO users (name, phone, raw_phone, workers, adset, ad_name)
        VALUES (?, ?, ?, ?, ?, ?)
                       """, (
                           str(row["ismingiz?"]).strip(),
                           str(row["telefon_raqamingiz?"]).strip(),
                           str(row["номер_телефона"]).strip(),
                           str(row["xodimlar_soni?"]).strip(),
                           str(row["adset_name"]).strip(),
                           str(row["ad_name"]).strip()
                       ))
    conn.commit()
    conn.close()


async def send_unsent_users():
    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, phone, raw_phone, workers, adset, ad_name FROM users WHERE sent = 0 ORDER BY id")
    rows = cursor.fetchall()

    for row in rows:
        user_id, name, phone, raw_phone, workers, adset, ad_name = row

        msg = (
            f"📊 Yangi ma'lumot (ID: {user_id})\n"
            f"👤 Ism: {name}\n"
            f"📞 Telefon: {phone}\n"
            f"📱 Номер телефона: {raw_phone}\n"
            f"👥 Xodimlar soni: {workers}\n"
            f"📌 Adset: {adset}\n"
            f"📢 Reklama: {ad_name}"
        )
        try:
            await bot.send_message(CHAT_ID, msg)

            cursor.execute("UPDATE users SET sent = 1 WHERE id = ?", (user_id,))
            conn.commit()
            await asyncio.sleep(1.2)  # flood control
        except Exception as e:
            print("Yuborishda xato:", e)
            await asyncio.sleep(5)

    conn.close()


async def scheduler(interval=30):
    while True:
        try:
            save_users_from_sheet()
            await send_unsent_users()
        except Exception as e:
            print("Xato:", e)
        await asyncio.sleep(interval)


@dp.message()
async def start(message: Message):
    await message.answer("✅ Bot ishga tushdi. Google Sheets kuzatilyapti...")


async def main():
    init_db()
    save_users_from_sheet()
    await send_unsent_users()

    asyncio.create_task(scheduler(interval=30))
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
