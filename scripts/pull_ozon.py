import os, sys, time, datetime as dt
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

def post_json(url, body, retries=3):
    for attempt in range(retries):
        r = requests.post(url, headers=HEADERS, json=body, timeout=60)
        if r.status_code == 429:  # rate limit
            wait = 2 ** attempt
            print(f"⏳ Rate limit, жду {wait} сек...")
            time.sleep(wait)
            continue
        if r.status_code >= 400:
            print("👉 Код ответа:", r.status_code)
            print("👉 Ответ:", r.text[:200])
            r.raise_for_status()
        return r.json()
    raise Exception("Не удалось получить ответ (после retry)")

def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

# === 1. Список товаров ===
def get_all_products():
    items, last_id = [], ""
    while True:
        body = {"filter": {"visibility": "ALL"}, "last_id": last_id, "limit": 1000}
        data = post_json(f"{BASE}/v3/product/list", body)
        result = data.get("result", {})
        batch = result.get("items", [])
        if not batch: break
        items.extend(batch)
        last_id = result.get("last_id", "")
        if not last_id: break
        time.sleep(0.2)
    return items

# === 2. SKU и имена ===
def get_product_info(product_ids):
    info = {}
    for batch in chunked(product_ids, 100):
        body = {"product_id": batch}
        data = post_json(f"{BASE}/v3/product/info/list", body)
        items = data.get("result", {}).get("items", [])
        for it in items:
            pid = str(it.get("product_id"))
            if pid:
                info[pid] = {
                    "sku": str(it.get("sku", "")),
                    "name": it.get("name", ""),
                    "offer_id": it.get("offer_id", "")
                }
        time.sleep(0.3)
    return info

# === 3. Аналитика ===
def get_analytics(sku_list, date_from, date_to):
    rows = []
    for batch in chunked(sku_list, 50):
        body = {
            "date_from": date_from,
            "date_to": date_to,
            "metrics": ["hits_view", "hits_click"],
            "dimension": ["sku"],
            "filters": [
                {"key": "sku", "operator": "IN", "value": batch}
            ],
            "limit": 1000,
            "offset": 0
        }
        data = post_json(f"{BASE}/v1/analytics/data", body)
        for row in data.get("result", {}).get("data", []):
            sku = row["dimensions"][0]["id"]
            m = row.get("metrics", [])
            views = float(m[0]) if len(m) > 0 else 0
            clicks = float(m[1]) if len(m) > 1 else 0
            ctr = round(clicks / views * 100, 2) if views > 0 else 0
            rows.append((sku, views, clicks, ctr))
        time.sleep(0.5)
    return rows

# === MAIN ===
products = get_all_products()
product_ids = [p["product_id"] for p in products]
print(f"✅ Найдено товаров: {len(product_ids)}")

info = get_product_info(product_ids)
sku_list = [v["sku"] for v in info.values() if v.get("sku")]

analytics = get_analytics(sku_list, date_from, date_to)

rows = []
for sku, views, clicks, ctr in analytics:
    meta = next((v for v in info.values() if v["sku"] == sku), {})
    rows.append({
        "sku": sku,
        "product_id": meta.get("product_id", ""),
        "offer_id": meta.get("offer_id", ""),
        "name": meta.get("name", ""),
        "views": int(views),
        "clicks": int(clicks),
        "CTR_%": ctr
    })

# Если данных нет — создаём пустую таблицу с колонками
if not rows:
    df = pd.DataFrame(columns=["sku","product_id","offer_id","name","views","clicks","CTR_%"])
else:
    df = pd.DataFrame(rows)
    df = df.sort_values("CTR_%", ascending=False)

os.makedirs("site", exist_ok=True)
df.to_csv("site/data.csv", index=False, encoding="utf-8-sig")

print("✅ Сохранено в site/data.csv")
print(df.head(20))
