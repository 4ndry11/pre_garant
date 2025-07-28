import time
import requests
from b24 import B24
import pandas as pd
import os

# === Настройки: все конфиденциальные данные — только из переменных окружения! ===
B24_DOMAIN = "ua.zvilnymo.com.ua"
B24_USER_ID = 596
ENTITY_TYPE_ID = 1042  # ID смарт-процесса Гарантійні листи
TELEGRAM_CHAT_IDS = [-1002345888899]

B24_TOKEN = os.environ.get("B24_TOKEN")
B24_TOKEN_USERS = os.environ.get("B24_TOKEN_USERS")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")

FIELDS = [
    "ID",
    "createdBy",
    "title",
    "ufCrm11_1753374261", # тіло кредита
    "ufCrm11_1753374328", # сума письма
    "ufCrm11_1753374357", # коментар
    "ufCrm11_1750708749", # файл-гарантійний лист
    "createdTime"
]

def load_users_dict():
    """Загружаем всех пользователей сразу и строим словарь user_id -> ФИО"""
    b24_users = B24(domain=B24_DOMAIN, user_id=B24_USER_ID, token=B24_TOKEN_USERS)
    users = b24_users.get_list("user.get", select=["ID", "NAME", "LAST_NAME", "SECOND_NAME"])
    users_df = pd.DataFrame(users)
    # Фамилия Имя Отчество
    users_df["FIO"] = users_df[["LAST_NAME", "NAME", "SECOND_NAME"]].fillna('').agg(' '.join, axis=1).str.strip()
    # user_id (str) -> ФИО
    users_dict = dict(zip(users_df["ID"].astype(str), users_df["FIO"]))
    return users_dict

def send_telegram_message(text, chat_ids):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chat_id in chat_ids:
        requests.post(url, data={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        })

def get_new_items(b24, last_checked):
    filter_params = {
        ">=createdTime": last_checked
    }
    items = b24.get_list(
        "crm.item.list",
        entityTypeId=ENTITY_TYPE_ID,
        select=FIELDS,
        b24_filter=filter_params
    )
    return items

def main():
    b24 = B24(domain=B24_DOMAIN, user_id=B24_USER_ID, token=B24_TOKEN)
    users_dict = load_users_dict()
    last_checked = pd.Timestamp.now() - pd.Timedelta("5min")
    sent_ids = set()

    while True:
        now = pd.Timestamp.now()
        new_items = get_new_items(b24, last_checked.isoformat())
        for item in new_items:
            item_id = item.get('ID') or item.get('id')
            if not item_id:
                print("Неизвестный формат записи:", item)
                continue
            if item_id in sent_ids:
                continue
            title = item.get("title", "-")
            body = item.get("ufCrm11_1753374261", "-")
            amount = item.get("ufCrm11_1753374328", "-")
            comment = item.get("ufCrm11_1753374357", "-")
            created_by = str(item.get("createdBy", ""))
            manager_name = users_dict.get(created_by, "Невідомо")

            # --- Ссылка на файл ---
            file_field = item.get("ufCrm11_1750708749")
            url_machine = None
            if file_field and isinstance(file_field, dict):
                url_machine = file_field.get("urlMachine")

            text = (
                "📩 <b>Отримано новий гарантійний лист!</b>\n\n"
                f"👤 <b>Кредитор / Клієнт:</b> <i>{title}</i>\n"
                f"👨‍💼 <b>Менеджер:</b> <b>{manager_name}</b>\n"
                f"💳 <b>Тіло кредиту:</b> <b>{body}</b>\n"
                f"💰 <b>Сума закриття:</b> <b>{amount}</b>\n"
                f"📝 <b>Коментар:</b> <i>{comment}</i>"
            )
            if url_machine and url_machine.startswith("http"):
                text += f"\n\n🔗 <a href=\"{url_machine}\">Завантажити гарантійний лист</a>"

            send_telegram_message(text, TELEGRAM_CHAT_IDS)
            sent_ids.add(item_id)

        last_checked = now
        time.sleep(60)

if __name__ == "__main__":
    main()
