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

# DEBUG: увімкнено за замовчуванням (постав DEBUG=0 щоб вимкнути)
DEBUG = os.environ.get("DEBUG", "1") not in ("0", "", "false", "False")

# Поля, які тягнемо з B24
FIELDS = [
    "ID",
    "title",
    "createdTime",
    "createdBy",                 # <- менеджер = той, хто створив
    "assignedById",
    "ufCrm11_1753374261",        # тіло кредита
    "ufCrm11_1753374328",        # сума письма
    "ufCrm11_1753374357",        # коментар
    "ufCrm11_1750708749"         # файл-гарантійний лист
]

def log(*a):
    if DEBUG:
        try:
            print(*a, flush=True)
        except Exception:
            pass

# -------------------- Telegram helpers --------------------
def send_telegram_message(text, chat_ids) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    ok = False
    for chat_id in chat_ids:
        try:
            log(f"[TG] -> sendMessage chat={chat_id}")
            resp = requests.post(
                url,
                data={
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                },
                timeout=20
            )
            log(f"[TG] <- sendMessage status={resp.status_code} body={resp.text[:500]}")
            ok = ok or resp.ok
        except Exception as e:
            log("[TG] sendMessage exception:", e)
    return ok

def send_telegram_photo(photo_url, caption, chat_ids) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    if not (photo_url and str(photo_url).startswith("http")):
        log("[TG] skip sendPhoto: invalid URL:", photo_url)
        return False
    ok = False
    for chat_id in chat_ids:
        try:
            log(f"[TG] -> sendPhoto chat={chat_id} url={photo_url}")
            resp = requests.post(
                url,
                data={
                    "chat_id": chat_id,
                    "photo": photo_url,   # URL або file_id
                    "caption": caption,
                    "parse_mode": "HTML"
                },
                timeout=25
            )
            log(f"[TG] <- sendPhoto status={resp.status_code} body={resp.text[:500]}")
            ok = ok or resp.ok
        except Exception as e:
            log("[TG] sendPhoto exception:", e)
    return ok

# -------------------- Bitrix24 helpers --------------------
def load_users_dict():
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
    log(f"[users] loaded {len(users_dict)//2} users")
    return users_dict

def fetch_user_name_by_id(uid, b24_users):
    if uid is None:
        return None
    user = None
    try:
        res = b24_users.call("user.get", {"ID": uid}) or {}
        data = res.get("result")
        user = data[0] if isinstance(data, list) else data
    except Exception as e:
        log("[users] user.get exception:", e)
        user = None
    if not user:
        try:
            res = b24_users.get_list("user.search", b24_filter={"ID": uid})
            user = res[0] if isinstance(res, list) and res else None
        except Exception as e:
            log("[users] user.search exception:", e)
            user = None
    if not user:
        return None
    last = (user.get("LAST_NAME") or "").strip()
    name = (user.get("NAME") or "").strip()
    mid  = (user.get("SECOND_NAME") or "").strip()
    fio = " ".join([last, name, mid]).replace("  ", " ").strip()
    return fio or None

def resolve_creator_name(item, users_dict, b24_users):
    raw_uid = (
        item.get("createdBy")
        or item.get("createdById")
        or item.get("CREATED_BY")
        or item.get("CREATED_BY_ID")
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

# ---------- crm.item.list через call() з order/pagination ----------
def call_crm_item_list(b24, params: dict):
    items = []
    start = 0
    page = 0
    while True:
        payload = dict(params)
        payload["start"] = start

        # ✅ фикс логов: собираем preview заранее
        payload_preview = {key: payload.get(key) for key in ("entityTypeId", "select", "filter", "order")}
        log(f"[B24] crm.item.list call page={page} start={start} payload={payload_preview}")

        resp = b24.call("crm.item.list", payload) or {}
        result = resp.get("result") or {}
        batch = result.get("items") or []
        next_pos = result.get("next")
        total = result.get("total")

        log(f"[B24] page={page} got={len(batch)} next={next_pos} total={total}")
        items.extend(batch)

        if next_pos is None:
            break

        start = next_pos
        page += 1
        time.sleep(0.1)

    log(f"[B24] total fetched items={len(items)}")
    return items


def get_current_max_id(b24):
    items = call_crm_item_list(b24, {
        "entityTypeId": ENTITY_TYPE_ID,
        "select": ["ID"],
        "order": {"id": "DESC"}
    })
    if not items:
        return 0
    first = items[0]
    max_id = int(first.get("ID") or first.get("id") or 0)
    log(f"[init] current max ID in SP={max_id}")
    return max_id

def get_items_after_id(b24, last_id: int):
    items = call_crm_item_list(b24, {
        "entityTypeId": ENTITY_TYPE_ID,
        "select": FIELDS,
        "filter": {">id": int(last_id)},
        "order": {"id": "ASC"}
    })
    return items or []

# -------------------- Main loop --------------------
def main():
    print("[boot] GarantBot starting...", flush=True)
    if not all([B24_TOKEN, B24_TOKEN_USERS, TELEGRAM_TOKEN]):
        raise RuntimeError("ENV не заповнені: потрібні B24_TOKEN, B24_TOKEN_USERS, TELEGRAM_TOKEN.")

    b24 = B24(domain=B24_DOMAIN, user_id=B24_USER_ID, token=B24_TOKEN)
    b24_users = B24(domain=B24_DOMAIN, user_id=B24_USER_ID, token=B24_TOKEN_USERS)

    users_dict = load_users_dict()
    users_last_refresh = pd.Timestamp.now()

    # In-memory водорозділ: стартуємо з поточного максимуму ID
    last_id = get_current_max_id(b24)
    sent_ids = set()
    heartbeat = 0
    print(f"[boot] DEBUG={DEBUG} last_id(start)={last_id}", flush=True)

    while True:
        now = pd.Timestamp.now()

        # Heartbeat кожні 30 циклів
        heartbeat += 1
        if heartbeat % 30 == 0:
            print(f"[hb] alive at {now.isoformat()} last_id={last_id}", flush=True)

        # М'який авто-рефреш кешу користувачів раз на 10 хв
        if (now - users_last_refresh).total_seconds() > 600:
            try:
                users_dict = load_users_dict()
                users_last_refresh = now
                log("[users] refreshed at", now)
            except Exception as e:
                log("[users] refresh failed:", e)

        # Отримуємо нові елементи після last_id
        try:
            new_items = get_items_after_id(b24, last_id)
            log(f"[loop] fetched {len(new_items)} new items (> {last_id})")
        except Exception as e:
            log("[B24] crm.item.list exception:", e)
            time.sleep(10)
            continue

        for item in new_items:
            try:
                item_id = item.get("ID") or item.get("id")
                if not item_id:
                    log("[item] Unknown format (no ID):", str(item)[:400])
                    continue
                item_id = int(item_id)
                if item_id in sent_ids:
                    continue

                title  = item.get("title", "-")
                body   = item.get("ufCrm11_1753374261", "-")
                amount = item.get("ufCrm11_1753374328", "-")
                comment = item.get("ufCrm11_1753374357", "-")

                manager_name = resolve_creator_name(item, users_dict, b24_users)

                # --- файл (може бути dict або list[dict]) ---
                file_field = item.get("ufCrm11_1750708749")
                url_machine = None
                if isinstance(file_field, dict):
                    url_machine = file_field.get("urlMachine")
                elif isinstance(file_field, list) and file_field and isinstance(file_field[0], dict):
                    url_machine = file_field[0].get("urlMachine")

                log(f"[item] id={item_id} title={title!r} has_file_url={bool(url_machine)} url={str(url_machine)[:120] if url_machine else None}")

                text = (
                    "📩 <b>Отримано новий гарантійний лист!</b>\n\n"
                    f"👤 <b>Кредитор / Клієнт:</b> <i>{title}</i>\n"
                    f"👨‍💼 <b>Менеджер:</b> <b>{manager_name}</b>\n"
                    f"💳 <b>Тіло кредиту:</b> <b>{body}</b>\n"
                    f"💰 <b>Сума закриття:</b> <b>{amount}</b>\n"
                    f"📝 <b>Коментар:</b> <i>{comment}</i>"
                )

                # Надсилаємо: спершу фото (якщо є URL), інакше текст
                sent_ok = False
                if url_machine and str(url_machine).startswith("http"):
                    sent_ok = send_telegram_photo(url_machine, text, TELEGRAM_CHAT_IDS)
                    if not sent_ok:
                        log("[item] photo failed, fallback to text...")

                if not sent_ok:
                    sent_ok = send_telegram_message(text, TELEGRAM_CHAT_IDS)

                if sent_ok:
                    sent_ids.add(item_id)
                    last_id = max(last_id, item_id)
                    log(f"[sent] id={item_id} OK; last_id -> {last_id}")
                else:
                    log(f"[WARN] TG not sent for id={item_id}; keep last_id={last_id}")

            except Exception as e:
                log(f"[item] exception for id={item.get('ID') or item.get('id')}: {e}")

        time.sleep(10)

if __name__ == "__main__":
    main()
