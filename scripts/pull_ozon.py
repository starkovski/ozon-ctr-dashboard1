import os
import requests
import pandas as pd
from datetime import datetime, timedelta

# Авторизация
CLIENT_ID = os.environ["OZON_CLIENT_ID"]
API_KEY = os.environ["OZON_API_KEY"]

HEADERS = {
    "Client-Id": CLIENT_ID,
    "Api-Key": API_KEY,
    "Content-Type": "application/json"
}

BASE_URL = "https://api-seller.ozon.ru"

# --- Получение списка товаров ---
def get_products():
    url = f"{BASE_URL}/v3/product/list"
    body = {"page_size": 1000, "page": 1}
    r = requests.post(url, headers=HEADERS, json=body)
    r.raise_for_status()
    return r.json()["result"]["items"]

# --- Получение информации по товарам ---
def get_products_info(product_ids):
    url = f"{BASE_URL}/v3/products/info/list"
    body = {"product_id": product_ids}
    r = requests.post(url, headers=HEADERS, json=body)
    r.raise_for_status()
    return r.json()["result"]["items"]

# --- Получение аналитики ---
def get_analytics(product_ids, date_from, date_to):
    url = f"{BASE_URL}/v1/analytics/data"
    body = {
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ["hits_view", "hits_click"],
        "dimension": ["sku"],
        "filters": [{"key": "product_id", "value": [str(pid) for pid in product_ids]}],
        "limit": 1000,
        "offset": 0
    }
    r = requests.post(url, headers=HEADERS, json=body)
    r.raise_for_status()
    return r.json()["result"]["data"]

# --- Основная логика ---
if __name__ == "__main__":
    # Дата (за последние 7 дней)
    date_to = datetime.now().strftime("%Y-%m-%d")
    date_from = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    print("📦 Получаем список товаров...")
    products = get_products()
    product_ids = [p["product_id"] for p in products]

    print("📊 Получаем аналитику...")
    analytics = get_analytics(product_ids, date_from, date_to)

    print("📝 Получаем названия товаров...")
    product_info = get_products_info(product_ids)
    product_map = {p["product_id"]: p["name"] for p in product_info}

    rows = []
    for row in analytics:
        pid = int(row["dimensions"][0]["id"])
        name = product_map.get(pid, "Без названия")
        views = row["metrics"][0] if len(row["metrics"]) > 0 else 0
        clicks = row["metrics"][1] if len(row["metrics"]) > 1 else 0
        ctr = (clicks / views * 100) if views else 0
        rows.append([pid, name, views, clicks, round(ctr, 2)])

    df = pd.DataFrame(rows, columns=["product_id", "Название", "Просмотры", "Клики", "CTR %"])
    df = df.sort_values(by="CTR %", ascending=False)

    print(df.to_string(index=False))
