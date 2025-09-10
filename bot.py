# -*- coding: utf-8 -*-
import time
import requests
from b24 import B24
import pandas as pd
import os

# === Налаштування: всі секрети лише з ENV ===
B24_DOMAIN = "ua.zvilnymo.com.ua"
B24_USER_ID = 596

# ID смарт-процесу "Гарантійні листи"
ENTITY_TYPE_ID = 1042

# Куди слати повідомлення (Telegram chat IDs)
TELEGRAM_CHAT_IDS = [-1002345888899]

# Токени з середовища
B24_TOKEN = os.environ.get("B24_TOKEN")
B24_TOKEN_USERS = os.environ.get("B24_TOKEN_USERS")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")

# Опційний DEBUG (будь-яке ненульове значення вмикає друк)
DEBUG = bool(os.environ.get("DEBUG"))

# Поля, які тягнемо з B24
FIELDS = [
    "ID",
    "title",
    "createdTime",
    "createdBy",                 # <- ТВОЯ ОСНОВА: менеджер = той, хто створив
    "assignedById",              # (про запас; не обов'язково використовувати)
    "ufCrm11_1753374261",        # тіло кредита
    "ufCrm11_1753374328",        # сума письма
    "ufCrm11_1753374357",        # коментар
    "ufCrm11_1750708749"         # файл-гарантійний лист
]

# -------------------- Telegram helpers --------------------
def send_telegram_message(text, chat_ids):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chat_id in chat_ids:
        try:
            resp = requests.post(
                url,
                data={
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                },
                timeout=10
            )
            if DEBUG and not resp.ok:
                print("TG sendMessage error:", resp.text)
        except Exception as e:
            if DEBUG:
                print("TG sendMessage exception:", e)

def send_telegram_photo(photo_url, caption, chat_ids):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    for chat_id in chat_ids:
        try:
            resp = requests.post(
                url,
                data={
                    "chat_id": chat_id,
                    "photo": photo_url,   # URL або file_id
                    "caption": caption,
                    "parse_mode": "HTML"
                },
                timeout=15
            )
            if DEBUG and not resp.ok:
                print("TG sendPhoto error:", resp.text)
        except Exception as e:
            if DEBUG:
                print("TG sendPhoto exception:", e)

# -------------------- Bitrix24 helpers --------------------
def load_users_dict():
    """
    Разове завантаження всіх користувачів -> dict з ключами і як str, і як int.
    """
    b24_users = B24(domain=B24_DOMAIN, user_id=B24_USER_ID, token=B24_TOKEN_USERS)
    users = b24_users.get_list("user.get", select=["ID", "NAME", "LAST_NAME", "SECOND_NAME"])
    users_df = pd.DataFrame(users)
    if users_df.empty:
        return {}

    users_df["FIO"] = (
        users_df[["LAST_NAME", "NAME", "SECOND_NAME"]]
        .fillna('')
        .agg(' '.join, axis=1)
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
    )

    users_dict = {}
    for _, row in users_df.iterrows():
        fio = row.get("FIO") or row.get("NAME") or ""
        uid_raw = row.get("ID")
        uid_str = str(uid_raw)
        users_dict[uid_str] = fio
        try:
            users_dict[int(uid_raw)] = fio
        except Exception:
            pass

    return users_dict

def fetch_user_name_by_id(uid, b24_users):
    """
    Онлайн-підтяжка користувача по ID (fallback, коли його не було в кеші).
    Повертає FIO або None.
    """
    if uid is None:
        return None

    user = None
    try:
        data = b24_users.call("user.get", {"ID": uid})
        user = data[0] if isinstance(data, list) else data
    except Exception:
        user = None

    if not user:
        try:
            res = b24_users.get_list("user.search", FILTER={"ID": uid})
            user = res[0] if isinstance(res, list) and res else None
        except Exception:
            user = None

    if not user:
        return None

    last = (user.get("LAST_NAME") or "").strip()
    name = (user.get("NAME") or "").strip()
    mid  = (user.get("SECOND_NAME") or "").strip()
    fio = " ".join([last, name, mid]).replace("  ", " ").strip()
    return fio or None

def resolve_creator_name(item, users_dict, b24_users):
    """
    Менеджер = той, хто створив елемент (createdBy).
    Якщо немає в кеші — онлайн підтягуємо і кешуємо.
    """
    raw_uid = (
        item.get("createdBy")
        or item.get("createdById")
        or item.get("CREATED_BY")
        or item.get("CREATED_BY_ID")
    )

    # спершу шукаємо в кеші (і int, і str)
    for key in (raw_uid, str(raw_uid)):
        if key in users_dict:
            return users_dict[key]

    # якщо не знайшли — підвантажимо онлайном
    fio = fetch_user_name_by_id(raw_uid, b24_users)
    if fio:
        users_dict[str(raw_uid)] = fio
        try:
            users_dict[int(raw_uid)] = fio
        except Exception:
            pass
        return fio

    # останній fallback — показати, що не знайшли (можна додати ID для дебагу)
    return "Невідомо"

def get_new_items(b24, last_checked_iso):
    """
    Тягнемо нові елементи смарт-процесу за createdTime >= last_checked_iso.
    """
    filter_params = {">=createdTime": last_checked_iso}
    items = b24.get_list(
        "crm.item.list",
        entityTypeId=ENTITY_TYPE_ID,
        select=FIELDS,
        b24_filter=filter_params  # використовуємо як у твоєму коді
        # якщо твоя обгортка очікує "filter", можна дублювати:
        # filter=filter_params
    )
    if not isinstance(items, list):
        # на випадок, якщо обгортка повертає dict з ключем 'items'
        items = items.get("items", []) if isinstance(items, dict) else []
    return items

# -------------------- Main loop --------------------
def main():
    if not all([B24_TOKEN, B24_TOKEN_USERS, TELEGRAM_TOKEN]):
        raise RuntimeError("ENV не заповнені: потрібні B24_TOKEN, B24_TOKEN_USERS, TELEGRAM_TOKEN.")

    b24 = B24(domain=B24_DOMAIN, user_id=B24_USER_ID, token=B24_TOKEN)
    b24_users = B24(domain=B24_DOMAIN, user_id=B24_USER_ID, token=B24_TOKEN_USERS)

    users_dict = load_users_dict()
    users_last_refresh = pd.Timestamp.now()

    # Стартове вікно перевірки — останні 5 хв
    last_checked = pd.Timestamp.now() - pd.Timedelta("5min")
    sent_ids = set()

    while True:
        now = pd.Timestamp.now()

        # М'який авто-рефреш кешу користувачів раз на 10 хв
        if (now - users_last_refresh).total_seconds() > 600:
            try:
                users_dict = load_users_dict()
                users_last_refresh = now
                if DEBUG:
                    print("[users] refreshed at", now)
            except Exception as e:
                if DEBUG:
                    print("[users] refresh failed:", e)

        # Отримуємо нові елементи
        try:
            new_items = get_new_items(b24, last_checked.isoformat())
        except Exception as e:
            if DEBUG:
                print("crm.item.list error:", e)
            time.sleep(10)
            last_checked = now
            continue

        for item in new_items:
            item_id = item.get("ID") or item.get("id")
            if not item_id:
                if DEBUG:
                    print("Unknown item format:", item)
                continue
            if item_id in sent_ids:
                continue

            title  = item.get("title", "-")
            body   = item.get("ufCrm11_1753374261", "-")
            amount = item.get("ufCrm11_1753374328", "-")
            comment = item.get("ufCrm11_1753374357", "-")

            # Менеджер = створювач елемента
            manager_name = resolve_creator_name(item, users_dict, b24_users)

            if DEBUG and manager_name == "Невідомо":
                print("createdBy:", item.get("createdBy"), "-> manager not resolved")

            # --- файл (може бути dict або list[dict]) ---
            file_field = item.get("ufCrm11_1750708749")
            url_machine = None
            if isinstance(file_field, dict):
                url_machine = file_field.get("urlMachine")
            elif isinstance(file_field, list) and file_field and isinstance(file_field[0], dict):
                url_machine = file_field[0].get("urlMachine")

            # Формуємо текст повідомлення
            text = (
                "📩 <b>Отримано новий гарантійний лист!</b>\n\n"
                f"👤 <b>Кредитор / Клієнт:</b> <i>{title}</i>\n"
                f"👨‍💼 <b>Менеджер:</b> <b>{manager_name}</b>\n"
                f"💳 <b>Тіло кредиту:</b> <b>{body}</b>\n"
                f"💰 <b>Сума закриття:</b> <b>{amount}</b>\n"
                f"📝 <b>Коментар:</b> <i>{comment}</i>"
            )

            # Надсилаємо: фото якщо є лінк, інакше текст
            if url_machine and str(url_machine).startswith("http"):
                send_telegram_photo(url_machine, text, TELEGRAM_CHAT_IDS)
            else:
                send_telegram_message(text, TELEGRAM_CHAT_IDS)

            sent_ids.add(item_id)

        # зсуваємо "вікно" перевірки
        last_checked = now
        time.sleep(60)

if __name__ == "__main__":
    main()
