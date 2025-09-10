import asyncio
import os
import re
import sqlite3

import pandas as pd
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv"

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

MAX_LEN = 4000

# DB ustunlar
DB_COLUMNS = ["ismingiz?", "telefon_raqamingiz?", "номер_телефона", "xodimlar_soni?", "adset_name", "ad_name"]


# === DB ===
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


def normalize_columns(cols):
    """Invisible belgi va bo‘sh joylarni olib tashlaydi va kichik harflarga o‘tkazadi"""
    return [c.strip().replace("\ufeff", "").lower() for c in cols]


def save_new_users_from_sheet():
    """Google Sheet'dan faqat yangi foydalanuvchilarni DBga qo‘shadi"""
    try:
        df = pd.read_csv(CSV_URL).fillna("")
        df.columns = normalize_columns(df.columns)

        # DB_COLUMNS bilan moslash
        col_map = {}
        for db_col in DB_COLUMNS:
            norm_db = db_col.strip().lower()
            for sheet_col in df.columns:
                if norm_db == sheet_col:
                    col_map[db_col] = sheet_col
                    break
        # Agar barcha ustun topilmasa xato chiqaradi
        if len(col_map) != len(DB_COLUMNS):
            missing = set(DB_COLUMNS) - set(col_map.keys())
            print("Sheetda quyidagi ustunlar topilmadi:", missing)
            return []

        df = df[[col_map[c] for c in DB_COLUMNS]]

    except Exception as e:
        print("Sheet o‘qishda xato:", e)
        return []

    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()
    new_users = []

    for _, row in df.iterrows():
        try:
            cursor.execute("""
                           INSERT INTO users (name, phone, raw_phone, workers, adset, ad_name)
                           VALUES (?, ?, ?, ?, ?, ?)
                           """, (
                               str(row[col_map["ismingiz?"]]).strip(),
                               str(row[col_map["telefon_raqamingiz?"]]).strip(),
                               str(row[col_map["номер_телефона"]]).strip(),
                               str(row[col_map["xodimlar_soni?"]]).strip(),
                               str(row[col_map["adset_name"]]).strip(),
                               str(row[col_map["ad_name"]]).strip()
                           ))
            new_users.append(row[col_map["номер_телефона"]])
        except sqlite3.IntegrityError:
            continue

    conn.commit()
    conn.close()
    return new_users


async def safe_send_message(chat_id, text):
    """Xabarni alohida qismga bo‘lib yuboradi, flood control bilan"""
    parts = [text[i:i + MAX_LEN] for i in range(0, len(text), MAX_LEN)]
    for part in parts:
        sent = False
        while not sent:
            try:
                await bot.send_message(chat_id, part)
                sent = True
                await asyncio.sleep(1)
            except Exception as e:
                err = str(e).lower()
                if "retry after" in err:
                    retry_sec = int(re.search(r"retry after (\d+)", err).group(1))
                    print(f"Flood control. Kutish {retry_sec} sekund")
                    await asyncio.sleep(retry_sec + 1)
                else:
                    print("Xato yuborishda:", e)
                    await asyncio.sleep(2)


async def send_new_users():
    """DBda hali yuborilmagan foydalanuvchilarni yuboradi"""
    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()
    cursor.execute("""
                   SELECT id, name, phone, raw_phone, workers, adset, ad_name
                   FROM users
                   WHERE sent = 0
                   ORDER BY id
                   """)
    rows = cursor.fetchall()

    for row in rows:
        user_id, name, phone, raw_phone, workers, adset, ad_name = row

        msg = (
            f"📊 <b>ID:</b> {user_id}\n"
            f"👤 <b>Ism:</b> {name}\n"
            f"📞 <b>Telefon:</b> {phone}\n"
            f"📱 <b>Номер телефона:</b> {raw_phone}\n"
            f"👥 <b>Xodimlar soni:</b> {workers}\n"
            f"📌 <b>Adset:</b> {adset}\n"
            f"📢 <b>Reklama:</b> {ad_name}"
        )

        await safe_send_message(CHAT_ID, msg)
        cursor.execute("UPDATE users SET sent = 1 WHERE id = ?", (user_id,))
        conn.commit()

    conn.close()


async def scheduler(interval=30 * 60):
    while True:
        try:
            print("Sheet tekshirilmoqda...")
            new_users = save_new_users_from_sheet()
            if new_users:
                print(f"{len(new_users)} yangi foydalanuvchi topildi")
                await send_new_users()
            else:
                print("Yangi foydalanuvchi yo‘q")
        except Exception as e:
            print("Xato:", e)
        await asyncio.sleep(interval)


async def main():
    init_db()
    await send_new_users()
    asyncio.create_task(scheduler(interval=5 * 60))  # 5 daqiqa
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
