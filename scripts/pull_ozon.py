import os
import requests
import pandas as pd
from datetime import datetime, timedelta

# 🔑 Данные для авторизации
CLIENT_ID = os.getenv("OZON_CLIENT_ID")
API_KEY = os.getenv("OZON_API_KEY")

HEADERS = {
    "Client-Id": CLIENT_ID,
    "Api-Key": API_KEY,
    "Content-Type": "application/json"
}

# ===== ФУНКЦИИ =====

def get_products():
    """Получаем список всех товаров с product_id"""
    url = "https://api-seller.ozon.ru/v3/product/list"
    products = []
    page = 1

    while True:
        body = {
            "page_size": 1000,
            "page": page,
            "filter": {
                "visibility": "ALL"  # ✅ обязательно указывать
            }
        }
        print(f"📦 Получаем товары (страница {page})...")
        r = requests.post(url, headers=HEADERS, json=body)
        print("👉 Код ответа:", r.status_code)
        if r.status_code != 200:
            print("👉 Ответ:", r.text)
        r.raise_for_status()

        data = r.json()["result"]

        if not data["items"]:
            break

        products.extend(data["items"])

        if len(data["items"]) < 1000:
            break
        page += 1

    return products


def get_analytics(product_ids, date_from, date_to):
    """Получаем аналитику по product_id"""
    url = "https://api-seller.ozon.ru/v1/analytics/data"
    analytics = []

    # Ozon разрешает максимум 1000 ID за раз
    chunk_size = 1000
    for i in range(0, len(product_ids), chunk_size):
        chunk = product_ids[i:i + chunk_size]

        body = {
            "date_from": date_from,
            "date_to": date_to,
            "metrics": ["hits_view", "hits_click", "conv_tocart", "revenue"],
            "dimension": ["sku"],  # группировка по SKU
            "filters": [
                {
                    "key": "product_id",
                    "value": list(map(str, chunk))
                }
            ],
            "limit": 1000,
            "offset": 0
        }

        print(f"📊 Получаем аналитику для {len(chunk)} товаров...")
        r = requests.post(url, headers=HEADERS, json=body)
        print("👉 Код ответа:", r.status_code)
        if r.status_code != 200:
            print("👉 Ответ:", r.text)
        r.raise_for_status()

        data = r.json()["result"]["data"]
        analytics.extend(data)

    return analytics


# ===== ОСНОВНОЙ СКРИПТ =====

if __name__ == "__main__":
    print("📦 Получаем список товаров...")
    products = get_products()
    product_map = {str(p["product_id"]): p["name"] for p in products}
    product_ids = list(product_map.keys())

    print(f"✅ Найдено товаров: {len(product_ids)}")

    # Берем последние 30 дней
    date_to = datetime.now().strftime("%Y-%m-%d")
    date_from = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    print(f"📊 Период: {date_from} → {date_to}")
    analytics = get_analytics(product_ids, date_from, date_to)

    rows = []
    for row in analytics:
        sku = row["dimensions"][0]["id"]
        name = product_map.get(sku, "Неизвестно")
        metrics = row["metrics"]

        views = float(metrics[0]) if len(metrics) > 0 else 0
        clicks = float(metrics[1]) if len(metrics) > 1 else 0
        cart = float(metrics[2]) if len(metrics) > 2 else 0
        revenue = float(metrics[3]) if len(metrics) > 3 else 0

        ctr = (clicks / views * 100) if views > 0 else 0

        rows.append({
            "SKU": sku,
            "Название": name,
            "Показы": views,
            "Клики": clicks,
            "CTR %": round(ctr, 2),
            "В корзину": cart,
            "Выручка": revenue
        })

    df = pd.DataFrame(rows)
    df = df.sort_values(by="CTR %", ascending=False)

    print("📊 Итоговая таблица:")
    print(df.head(20))

    df.to_csv("ozon_report.csv", index=False)
    print("✅ Сохранено в ozon_report.csv")
