import requests
import pandas as pd
from datetime import datetime, timedelta

# ======================
# 🔑 Настройки
# ======================
CLIENT_ID = "<ТВОЙ_CLIENT_ID>"
API_KEY = "<ТВОЙ_API_KEY>"

HEADERS = {
    "Client-Id": CLIENT_ID,
    "Api-Key": API_KEY,
    "Content-Type": "application/json"
}

# ======================
# 📦 Получаем список товаров
# ======================
def get_products():
    """Получаем список всех товаров с product_id"""
    url = "https://api-seller.ozon.ru/v3/product/list"
    products = []
    offset = 0
    limit = 1000

    while True:
        body = {
            "limit": limit,
            "offset": offset,
            "filter": {
                "visibility": "ALL"  # важно!
            }
        }
        print(f"📦 Получаем товары (offset={offset})...")
        r = requests.post(url, headers=HEADERS, json=body)
        print("👉 Код ответа:", r.status_code)
        if r.status_code != 200:
            print("👉 Ответ:", r.text)
        r.raise_for_status()

        data = r.json()["result"]

        if not data["items"]:
            break

        products.extend(data["items"])

        if len(data["items"]) < limit:
            break
        offset += limit

    return products

# ======================
# 📊 Получаем аналитику
# ======================
def get_analytics(product_ids, date_from, date_to):
    url = "https://api-seller.ozon.ru/v1/analytics/data"

    body = {
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ["hits_view", "hits_click", "hits_tocart", "hits_tocart_remove", "orders"],
        "dimension": ["sku"],
        "filters": [
            {"key": "product_id", "value": [str(pid) for pid in product_ids]}
        ],
        "limit": 1000,
        "offset": 0
    }

    print("📊 Получаем аналитику...")
    r = requests.post(url, headers=HEADERS, json=body)
    print("👉 Код ответа:", r.status_code)
    if r.status_code != 200:
        print("👉 Ответ:", r.text)
    r.raise_for_status()

    return r.json()["result"]["data"]

# ======================
# 🚀 Основной код
# ======================
if __name__ == "__main__":
    # период: последние 30 дней
    date_to = datetime.today().strftime("%Y-%m-%d")
    date_from = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")

    print("📦 Получаем список товаров...")
    products = get_products()
    print(f"✅ Найдено товаров: {len(products)}")

    product_ids = [p["product_id"] for p in products]

    analytics = get_analytics(product_ids, date_from, date_to)

    rows = []
    for row in analytics:
        product_id = row["dimensions"][0]["id"]
        metrics = row["metrics"]
        name = next((p["name"] for p in products if p["product_id"] == int(product_id)), "N/A")

        rows.append({
            "product_id": product_id,
            "name": name,
            "views": metrics[0] if len(metrics) > 0 else 0,
            "clicks": metrics[1] if len(metrics) > 1 else 0,
            "to_cart": metrics[2] if len(metrics) > 2 else 0,
            "remove_cart": metrics[3] if len(metrics) > 3 else 0,
            "orders": metrics[4] if len(metrics) > 4 else 0
        })

    df = pd.DataFrame(rows)
    df = df.sort_values(by="views", ascending=False)

    output_file = "ozon_analytics.csv"
    df.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(f"✅ Данные сохранены в {output_file}")
