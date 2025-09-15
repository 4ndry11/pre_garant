# -*- coding: utf-8 -*-
"""
Гарантійні листи -> Telegram
In-memory watermark по ID (без файлів), safe-відправка з фолбеком на текст,
кеш користувачів, старт із поточного max ID.
Під твою обгортку B24: get_list без order, всі запити з order – через call().
"""
import os
import time
import requests
import pandas as pd
from b24 import B24

# ===== Налаштування =====
B24_DOMAIN = "ua.zvilnymo.com.ua"
B24_USER_ID = 596
ENTITY_TYPE_ID = 1042                         # смарт-процес "Гарантійні листи"
TELEGRAM_CHAT_IDS = [-1002345888899]          # куди шлемо

B24_TOKEN = os.environ.get("B24_TOKEN")
B24_TOKEN_USERS = os.environ.get("B24_TOKEN_USERS")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
DEBUG = bool(os.environ.get("DEBUG"))

FIELDS = [
    "ID",
    "title",
    "createdTime",
    "createdBy",
    "assignedById",
    "ufCrm11_1753374261",   # тіло кредита
    "ufCrm11_1753374328",   # сума письма/закриття
    "ufCrm11_1753374357",   # коментар
    "ufCrm11_1750708749"    # файл-гарантійний лист
]

def log(*a):
    if DEBUG:
        print(*a)

# ---------------- Telegram ----------------
def tg_send_message_safe(text, chat_ids) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    ok_any = False
    for chat_id in chat_ids:
        try:
            resp = requests.post(url, data={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }, timeout=15)
            log("TG sendMessage:", resp.status_code, resp.text[:200])
            ok_any = ok_any or resp.ok
        except Exception as e:
            log("TG sendMessage exception:", e)
    return ok_any

def tg_send_photo_safe(photo_url, caption, chat_ids) -> bool:
    if not (photo_url and str(photo_url).startswith("http")):
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    ok_any = False
    for chat_id in chat_ids:
        try:
            resp = requests.post(url, data={
                "chat_id": chat_id,
                "photo": photo_url,
                "caption": caption,
                "parse_mode": "HTML"
            }, timeout=20)
            log("TG sendPhoto:", resp.status_code, resp.text[:200])
            ok_any = ok_any or resp.ok
        except Exception as e:
            log("TG sendPhoto exception:", e)
    return ok_any

# ---------------- Bitrix24 helpers ----------------
def load_users_dict(b24_users: B24):
    users = b24_users.get_list("user.get", select=["ID","NAME","LAST_NAME","SECOND_NAME"])
    df = pd.DataFrame(users or [])
    if df.empty:
        return {}
    df["FIO"] = (
        df[["LAST_NAME","NAME","SECOND_NAME"]]
        .fillna("").agg(" ".join, axis=1)
        .str.replace(r"\s+"," ", regex=True).str.strip()
    )
    out = {}
    for _, r in df.iterrows():
        uid = r.get("ID")
        fio = r.get("FIO") or r.get("NAME") or ""
        out[str(uid)] = fio
        try:
            out[int(uid)] = fio
        except Exception:
            pass
    return out

def fetch_user_name_by_id(uid, b24_users: B24):
    if uid in (None,"",0):
        return None
    try:
        res = b24_users.call("user.get", {"ID": uid})
        data = (res or {}).get("result")
        user = data[0] if isinstance(data, list) else data
    except Exception:
        user = None

    if not user:
        try:
            res = b24_users.get_list("user.search", b24_filter={"ID": uid})
            user = res[0] if isinstance(res, list) and res else None
        except Exception:
            user = None

    if not user:
        return None

    last = (user.get("LAST_NAME") or "").strip()
    name = (user.get("NAME") or "").strip()
    mid  = (user.get("SECOND_NAME") or "").strip()
    fio = " ".join([last, name, mid]).replace("  "," ").strip()
    return fio or None

def resolve_creator_name(item, users_dict, b24_users: B24):
    raw_uid = (
        item.get("createdBy") or item.get("createdById")
        or item.get("CREATED_BY") or item.get("CREATED_BY_ID")
    )
    for key in (raw_uid, str(raw_uid)):
        if key in users_dict:
            return users_dict[key]
    fio = fetch_user_name_by_id(raw_uid, b24_users)
    if fio:
        users_dict[str(raw_uid)] = fio
        try:
            users_dict[int(raw_uid)] = fio
        except Exception:
            pass
        return fio
    return "Невідомо"

def get_public_file_url(file_field, b24: B24):
    """
    Повертає придатний до TG URL: url/downloadUrl/urlMachine.
    Якщо недоступно — вертає None (тоді шлемо текст).
    (Без disk.file.get, щоб не ускладнювати права/сесії.)
    """
    node = None
    if isinstance(file_field, dict):
        node = file_field
    elif isinstance(file_field, list) and file_field and isinstance(file_field[0], dict):
        node = file_field[0]
    if not isinstance(node, dict):
        return None

    for k in ("url","downloadUrl","DOWNLOAD_URL","DOWNLOAD_URL_SIGN","urlMachine"):
        v = node.get(k)
        if v and str(v).startswith("http"):
            return v
    return None

# ---------- CRM items via call() with order/pagination ----------
def call_crm_item_list(b24: B24, params: dict):
    """
    Обгортка над b24.call для crm.item.list з ручною пагінацією (через start).
    Повертає суцільний список items.
    """
    items = []
    start = 0
    while True:
        payload = dict(params)
        payload["start"] = start
        resp = b24.call("crm.item.list", payload) or {}
        # очікуваний формат: {'result': {'items': [...], 'next': N, 'total': T}}
        result = resp.get("result") or {}
        batch = result.get("items") or []
        items.extend(batch)
        next_pos = result.get("next")
        if next_pos is None:
            break
        start = next_pos
        if DEBUG:
            log(f"crm.item.list page@{start}, got {len(batch)}")
        # невелика пауза, щоб не впертись у ліміти
        time.sleep(0.1)
    return items

def get_current_max_id(b24: B24) -> int:
    """
    Разово на старті беремо поточний максимальний ID у смарт-процесі.
    Через call(), бо потрібен order={"id":"DESC"} і пагінація.
    """
    items = call_crm_item_list(b24, {
        "entityTypeId": ENTITY_TYPE_ID,
        "select": ["ID"],
        "order": {"id": "DESC"}
    })
    if not items:
        return 0
    # Перша позиція після сортування DESC — найбільший ID
    first = items[0]
    return int(first.get("ID") or first.get("id") or 0)

def get_items_after_id(b24: B24, last_id: int):
    """
    Тягнемо елементи з ID > last_id (ASC) через call() + власна пагінація.
    """
    items = call_crm_item_list(b24, {
        "entityTypeId": ENTITY_TYPE_ID,
        "select": FIELDS,
        "filter": {">id": int(last_id)},
        "order": {"id": "ASC"}
    })
    return items or []

# ===== Основний цикл =====
def main():
    if not all([B24_TOKEN, B24_TOKEN_USERS, TELEGRAM_TOKEN]):
        raise RuntimeError("ENV не заповнені: потрібні B24_TOKEN, B24_TOKEN_USERS, TELEGRAM_TOKEN.")

    b24 = B24(domain=B24_DOMAIN, user_id=B24_USER_ID, token=B24_TOKEN)
    b24_users = B24(domain=B24_DOMAIN, user_id=B24_USER_ID, token=B24_TOKEN_USERS)

    users_dict = load_users_dict(b24_users)
    users_last_refresh = pd.Timestamp.utcnow()

    # In-memory watermark: стартуємо з поточного максимуму
    last_id = get_current_max_id(b24)
    log(f"[init] start from current max id = {last_id}")

    while True:
        try:
            # Раз на 10 хв оновлюємо кеш користувачів
            if (pd.Timestamp.utcnow() - users_last_refresh).total_seconds() > 600:
                try:
                    users_dict = load_users_dict(b24_users)
                    users_last_refresh = pd.Timestamp.utcnow()
                    log("[users] refreshed")
                except Exception as e:
                    log("[users] refresh failed:", e)

            # Тягнемо нові елементи
            items = get_items_after_id(b24, last_id)
            if not items:
                time.sleep(30)
                continue

            for item in items:
                item_id = int(item.get("ID") or item.get("id") or 0)
                if not item_id:
                    log("[WARN] bad item:", item)
                    continue

                title   = item.get("title", "-")
                body    = item.get("ufCrm11_1753374261", "-")
                amount  = item.get("ufCrm11_1753374328", "-")
                comment = item.get("ufCrm11_1753374357", "-")
                manager = resolve_creator_name(item, users_dict, b24_users)

                file_url = get_public_file_url(item.get("ufCrm11_1750708749"), b24)

                text = (
                    "📩 <b>Отримано новий гарантійний лист!</b>\n\n"
                    f"👤 <b>Кредитор / Клієнт:</b> <i>{title}</i>\n"
                    f"👨‍💼 <b>Менеджер:</b> <b>{manager}</b>\n"
                    f"💳 <b>Тіло кредиту:</b> <b>{body}</b>\n"
                    f"💰 <b>Сума закриття:</b> <b>{amount}</b>\n"
                    f"📝 <b>Коментар:</b> <i>{comment}</i>"
                )

                sent_ok = False
                if file_url:
                    sent_ok = tg_send_photo_safe(file_url, text, TELEGRAM_CHAT_IDS)
                    if not sent_ok and DEBUG:
                        log("Photo failed, fallback to text")

                if not sent_ok:
                    sent_ok = tg_send_message_safe(text, TELEGRAM_CHAT_IDS)

                # Просуваємо watermark тільки якщо TG підтвердив відправку
                if sent_ok:
                    last_id = max(last_id, item_id)
                    log(f"[sent] id={item_id} ok; last_id -> {last_id}")
                else:
                    log(f"[WARN] TG not sent for id={item_id}; не зсуваємо last_id")

            time.sleep(10)

        except Exception as e:
            log("[loop] exception:", e)
            time.sleep(10)

if __name__ == "__main__":
    main()
