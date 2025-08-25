import os, sys, time, json, datetime as dt
import requests
import pandas as pd

BASE = "https://api-seller.ozon.ru"

CLIENT_ID = os.getenv("OZON_CLIENT_ID")
API_KEY   = os.getenv("OZON_API_KEY")

HEADERS = {
    "Client-Id": CLIENT_ID or "",
    "Api-Key": API_KEY or "",
    "Content-Type": "application/json"
}

if not CLIENT_ID or not API_KEY:
    print("❌ Нет OZON_CLIENT_ID / OZON_API_KEY в Secrets")
    sys.exit(1)

# ==== Дата (последние 30 дней) ====
today = dt.date.today()
date_to = (today - dt.timedelta(days=1)).strftime("%Y-%m-%d")
date_from = (today - dt.timedelta(days=30)).strftime("%Y-%m-%d")

def post_json(url, body):
    r = requests.post(url, headers=HEADERS, json=body, timeout=60)
    if r.status_code >= 400:
        print("👉 Код ответа:", r.status_code)
        print("👉 Ответ:", r.text[:500])
        r.raise_for_status()
    return r.json()

def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

# === 1. Список товаров ===
def get_all_products():
    items, last_id = [], ""
    while True:
        body = {
            "filter": {"visibility": "ALL"},
            "last_id": last_id,
            "limit": 1000
        }
        data = post_json(f"{BASE}/v3/product/list", body)
        result = data.get("result", {})
        batch = result.get("items", [])
        if not batch:
            break
        items.extend(batch)
        last_id = result.get("last_id", "")
        if not last_id:
            break
        time.sleep(0.2)
    return items

# === 2. Имена товаров ===
def get_product_info(product_ids):
    info = {}
    for batch in chunked(product_ids, 1000):
        body = {"product_id": batch}
        data = post_json(f"{BASE}/v3/product/info/list", body)
        items = data.get("result", {}).get("items", [])
        for it in items:
            pid = it.get("product_id")
            if pid:
                info[str(pid)] = {
                    "name": it.get("name", ""),
                    "offer_id": it.get("offer_id", "")
                }
        time.sleep(0.2)
    return info

# === 3. Аналитика (CTR) ===
def get_analytics(product_ids, date_from, date_to):
    rows = []
    for batch in chunked(product_ids, 200):
        body = {
            "date_from": date_from,
            "date_to": date_to,
            "metrics": ["hits_view", "hits_click"],
            "dimension": ["product_id"],
            "filters": [
                {
                    "key": "product_id",
                    "operator": "IN",
                    "value": [str(x) for x in batch]  # 👈 теперь строки
                }
            ],
            "limit": 1000,
            "offset": 0
        }
        data = post_json(f"{BASE}/v1/analytics/data", body)
        for row in data.get("result", {}).get("data", []):
            pid = row["dimensions"][0]["id"]
            m = row["metrics"]
            views = float(m[0]) if len(m) > 0 else 0
            clicks = float(m[1]) if len(m) > 1 else 0
            rows.append((pid, views, clicks))
        time.sleep(0.2)
    return rows

# === MAIN ===
products = get_all_products()
product_ids = [p["product_id"] for p in products]
print(f"✅ Найдено товаров: {len(product_ids)}")

info = get_product_info(product_ids)

analytics = get_analytics(product_ids, date_from, date_to)

rows = []
for pid, views, clicks in analytics:
    ctr = round(clicks / views * 100, 2) if views > 0 else 0
    meta = info.get(str(pid), {})
    rows.append({
        "product_id": pid,
        "sku": meta.get("offer_id", ""),
        "name": meta.get("name", ""),
        "views": int(views),
        "clicks": int(clicks),
        "CTR_%": ctr
    })

df = pd.DataFrame(rows)
df = df.sort_values("CTR_%", ascending=False)

os.makedirs("site", exist_ok=True)
df.to_csv("site/data.csv", index=False, encoding="utf-8-sig")

print("✅ Сохранено в site/data.csv")
print(df.head(20))
