import requests
import pandas as pd
from datetime import datetime, timedelta
import os

# --- Константы ---
API_KEY = os.getenv("OZON_API_KEY")
CLIENT_ID = os.getenv("OZON_CLIENT_ID")

HEADERS = {
    "Client-Id": CLIENT_ID,
    "Api-Key": API_KEY,
    "Content-Type": "application/json"
}

PRODUCTS_URL = "https://api-seller.ozon.ru/v3/product/list"
ANALYTICS_URL = "https://api-seller.ozon.ru/v1/analytics/data"

# --- Получение product_id товаров (все страницы) ---
def get_products():
    products = []
    page = 1
    page_size = 1000  # максимум разрешено
    while True:
        body = {
            "page_size": page_size,
            "page": page
        }
        r = requests.post(PRODUCTS_URL, headers=HEADERS, json=body)
        print(f"📦 Получаем товары (страница {page})... Код ответа: {r.status_code}")
        if r.status_code != 200:
            print("👉 Ответ:", r.text)
            r.raise_for_status()
        data = r.json()
        items = data.get("result", {}).get("items", [])
        if not items:
            break
        for it in items:
            products.append({
                "product_id": it["product_id"],
                "name": it["name"]
            })
        if len(items) < page_size:
            break
        page += 1
    return products

# --- Получение аналитики (CTR) ---
def get_analytics(product_ids, date_from, date_to):
    # делим по 1000 ID за раз
    all_rows = []
    chunk_size = 1000
    for i in range(0, len(product_ids), chunk_size):
        chunk = product_ids[i:i+chunk_size]
        body = {
            "date_from": date_from,
            "date_to": date_to,
            "metrics": ["hits_view", "hits_click"],
            "dimension": ["product_id"],
            "filters": [
                {"key": "product_id", "value": chunk}
            ],
            "limit": 1000,
            "offset": 0
        }
        r = requests.post(ANALYTICS_URL, headers=HEADERS, json=body)
        print(f"📊 Получаем аналитику ({i}–{i+chunk_size})... Код ответа: {r.status_code}")
        if r.status_code != 200:
            print("👉 Ответ:", r.text)
            r.raise_for_status()
        data = r.json()
        all_rows.extend(data.get("result", {}).get("data", []))
    return all_rows

# --- Главная логика ---
if __name__ == "__main__":
    # период: последний месяц
    date_to = datetime.today().strftime("%Y-%m-%d")
    date_from = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")

    # товары
    products = get_products()
    product_map = {str(p["product_id"]): p["name"] for p in products}
    product_ids = list(product_map.keys())

    # аналитика
    analytics = get_analytics(product_ids, date_from, date_to)

    # собираем DataFrame
    rows = []
    for row in analytics:
        pid = row["dimensions"][0]["id"]
        views = row["metrics"][0] if len(row["metrics"]) > 0 else 0
        clicks = row["metrics"][1] if len(row["metrics"]) > 1 else 0
        ctr = round(clicks / views * 100, 2) if views > 0 else 0
        rows.append({
            "product_id": pid,
            "name": product_map.get(pid, ""),
            "views": views,
            "clicks": clicks,
            "CTR_%": ctr
        })

    df = pd.DataFrame(rows)
    df = df.sort_values("CTR_%", ascending=False)

    # сохраняем
    out_file = "ozon_ctr.csv"
    df.to_csv(out_file, index=False)
    print(f"✅ Файл сохранён: {out_file}")
    print(df.head(20))
