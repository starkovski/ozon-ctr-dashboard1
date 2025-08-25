import os
import requests
import pandas as pd
from datetime import datetime, timedelta

# 🔑 API ключи из GitHub Secrets
CLIENT_ID = os.getenv("OZON_CLIENT_ID")
API_KEY = os.getenv("OZON_API_KEY")

HEADERS = {
    "Client-Id": CLIENT_ID,
    "Api-Key": API_KEY,
    "Content-Type": "application/json"
}

# === 1. Получение списка товаров ===
def get_products():
    url = "https://api-seller.ozon.ru/v3/product/list"
    products = []
    page = 1

    while True:
        body = {
            "page_size": 1000,
            "page": page,
            "filter": {}
        }
        print(f"📦 Получаем товары (страница {page})...")
        r = requests.post(url, headers=HEADERS, json=body)
        print("👉 Код ответа:", r.status_code)
        r.raise_for_status()
        data = r.json()["result"]

        if not data["items"]:
            break

        products.extend(data["items"])

        if len(data["items"]) < 1000:
            break
        page += 1

    return products

# === 2. Получение аналитики ===
def get_analytics(product_ids, date_from, date_to):
    url = "https://api-seller.ozon.ru/v1/analytics/data"
    body = {
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ["hits_view", "hits_click"],
        "dimension": ["product_id"],
        "filters": [
            {
                "key": "product_id",
                "value": product_ids,
                "operator": "IN"
            }
        ],
        "limit": 1000,
        "offset": 0
    }

    print(f"📊 Получаем аналитику c {date_from} по {date_to}...")
    r = requests.post(url, headers=HEADERS, json=body)
    print("👉 Код ответа:", r.status_code)
    print("👉 Пример ответа:", r.text[:300])
    r.raise_for_status()
    return r.json()

# === 3. Основной скрипт ===
if __name__ == "__main__":
    # 📅 период (последние 30 дней)
    date_to = datetime.today().strftime("%Y-%m-%d")
    date_from = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")

    # 1. получаем список товаров
    products = get_products()
    product_map = {str(p["product_id"]): p for p in products}

    # 2. достаём product_id
    product_ids = list(product_map.keys())

    # ⚡️ если товаров >1000 — делим на чанки
    all_rows = []
    for i in range(0, len(product_ids), 1000):
        chunk = product_ids[i:i+1000]
        analytics = get_analytics(chunk, date_from, date_to)

        for row in analytics.get("result", {}).get("data", []):
            pid = row["dimensions"][0]["id"]
            views = int(row["metrics"][0]) if len(row["metrics"]) > 0 else 0
            clicks = int(row["metrics"][1]) if len(row["metrics"]) > 1 else 0
            ctr = round((clicks / views * 100), 2) if views > 0 else 0

            product = product_map.get(pid, {})
            all_rows.append({
                "product_id": pid,
                "offer_id": product.get("offer_id", ""),
                "name": product.get("name", ""),
                "views": views,
                "clicks": clicks,
                "ctr": ctr
            })

    # 3. таблица + сортировка
    df = pd.DataFrame(all_rows)
    df = df.sort_values(by="ctr", ascending=False)

    print("\n✅ Готовая таблица CTR:")
    print(df.head(20))  # показываем топ-20

    # сохраняем
    df.to_csv("ozon_ctr.csv", index=False, encoding="utf-8-sig")
    print("💾 Файл сохранён: ozon_ctr.csv")
