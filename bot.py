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

# Yuborilgan telefon raqamlarni saqlash fayli
STATE_FILE = "sent_phones.json"

# 🔎 Kerakli ustunlar ro‘yxati
COLUMNS = [
    "ismingiz?",
    "telefon_raqamingiz?",
    "номер_телефона",
    "xodimlar_soni?",
    "adset_name",
    "ad_name",
]


def load_sent_phones():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return set(json.load(f))
    return set()


def save_sent_phones(phones):
    with open(STATE_FILE, "w") as f:
        json.dump(list(phones), f)


sent_phones = load_sent_phones()


async def check_updates(process_all=False):
    global sent_phones
    df = pd.read_csv(CSV_URL)
    df = df[COLUMNS]  # faqat kerakli ustunlarni olamiz
    count = 0
    for _, row in df.iterrows():
        phone = str(row["номер_телефона"]).strip()
        count += 1
        # telefon raqam yuborilgan bo‘lsa - tashlab ketamiz
        if phone in sent_phones:
            continue

        row_dict = row.to_dict()

        msg = (
            f"📊 Yangi ma'lumot: sheet number {count}\n"
            f"👤 Ism: {row_dict['ismingiz?']}\n"
            f"📞 Telefon: {row_dict['telefon_raqamingiz?']}\n"
            f"📱 Номер телефона: {row_dict['номер_телефона']}\n"
            f"👥 Xodimlar soni: {row_dict['xodimlar_soni?']}\n"
            f"📌 Adset: {row_dict['adset_name']}\n"
            f"📢 Reklama: {row_dict['ad_name']}"
        )

        try:
            await bot.send_message(CHAT_ID, msg)
            sent_phones.add(phone)  # yuborilgan raqamni qo‘shib qo‘yamiz
            save_sent_phones(sent_phones)
            await asyncio.sleep(1.2)  # flood control uchun delay
        except Exception as e:
            print("Yuborishda xato:", e)
            await asyncio.sleep(5)


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
    # dastlabki barcha ma’lumotlarni tekshirib yuborish
    await check_updates(process_all=True)

    # kuzatuvchi ishga tushirish
    asyncio.create_task(scheduler(interval=10))

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
