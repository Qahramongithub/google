import asyncio
import json
import os

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

# Yuborilgan qatorlarni saqlash fayli
STATE_FILE = "sent_rows.json"

# 🔎 Kerakli ustunlar ro‘yxati
COLUMNS = [
    "ismingiz?",
    "telefon_raqamingiz?",
    "номер_телефона",
    "xodimlar_soni?",
    "adset_name",
    "ad_name",
]


def load_old_rows():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return set(tuple(x) for x in json.load(f))
    return set()


def save_old_rows(rows):
    with open(STATE_FILE, "w") as f:
        json.dump([list(x) for x in rows], f)


old_rows = load_old_rows()


async def check_updates(process_all=False):
    global old_rows
    df = pd.read_csv(CSV_URL)
    df = df[COLUMNS]  # faqat kerakli ustunlarni olamiz

    new_rows = set(tuple(x) for x in df.astype(str).values.tolist())

    # bir martalik rejimda barcha ma’lumotlarni yuboradi
    if process_all and not old_rows:
        diff = new_rows
    else:
        diff = new_rows - old_rows

    if diff:
        for row in diff:
            row_dict = dict(zip(COLUMNS, row))  # tuple → dict

            msg = (
                f"📊 Yangi ma'lumot:\n"
                f"👤 Ism: {row_dict['ismingiz?']}\n"
                f"📞 Telefon: {row_dict['telefon_raqamingiz?']}\n"
                f"📱 Номер телефона: {row_dict['номер_телефона']}\n"
                f"👥 Xodimlar soni: {row_dict['xodimlar_soni?']}\n"
                f"📌 Adset: {row_dict['adset_name']}\n"
                f"📢 Reklama: {row_dict['ad_name']}"
            )
            await bot.send_message(CHAT_ID, msg)

    old_rows = new_rows
    save_old_rows(old_rows)


async def scheduler(interval=10):
    while True:
        try:
            await check_updates()
        except Exception as e:
            print("Xato:", e)
        await asyncio.sleep(interval)


@dp.message()
async def start(message: Message):
    await message.answer("✅ Bot ishga tushdi. Google Sheets kuzatilyapti...")


async def main():
    # bir martalik barcha ma'lumotlarni yuborish
    await check_updates(process_all=True)

    # kuzatuvchi ishga tushirish
    asyncio.create_task(scheduler(interval=10))

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
