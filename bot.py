import argparse
import time
import pandas as pd
import telebot
import os
from dotenv import load_dotenv

load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # kanal/guruh username yoki chat_id

bot = telebot.TeleBot(BOT_TOKEN)

SHEET_ID = os.getenv('GOOGLE_SHEET_ID')
CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv"

old_rows = set()

# 🔎 Kerakli ustunlar ro‘yxati
COLUMNS = [
    "ismingiz?",
    "telefon_raqamingiz?",
    "номер_телефона",
    "xodimlar_soni?",
    "adset_name",
    "ad_name"
]

def check_updates(process_all=False):
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
            bot.send_message(CHAT_ID, msg)

    old_rows = new_rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--watch", action="store_true", help="Google Sheetsni kuzatish rejimi")
    parser.add_argument("--interval", type=int, default=10, help="Tekshirish oralig‘i (sekund)")
    parser.add_argument("--process-once", action="store_true", help="Faqat 1 marta ishlash")

    args = parser.parse_args()

    if args.process_once:
        print("▶️ Bir martalik ishlash...")
        check_updates(process_all=True)
        print("✅ Tugadi.")
    elif args.watch:
        print(f"⏳ Watching Google Sheets (interval {args.interval}s)...")
        while True:
            try:
                check_updates()
            except Exception as e:
                print("Xato:", e)
            time.sleep(args.interval)
    else:
        print("❌ Parametr kiritilmadi. --watch yoki --process-once dan foydalaning.")
