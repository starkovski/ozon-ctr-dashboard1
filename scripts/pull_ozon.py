#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, time, csv, math
from datetime import datetime, timedelta, timezone
import requests

API_HOST = "https://api-seller.ozon.ru"

CLIENT_ID = os.getenv("OZON_CLIENT_ID")
API_KEY   = os.getenv("OZON_API_KEY")

HEADERS = {
    "Client-Id": CLIENT_ID or "",
    "Api-Key": API_KEY or "",
    "Content-Type": "application/json"
}

def post(url: str, payload: dict, sleep_sec: float = 0.25):
    """Единая обёртка с печатью запроса/ответа и мягким бэкоффом на 429."""
    r = requests.post(url, headers=HEADERS, data=json.dumps(payload))
    if r.status_code == 429:
        time.sleep(1.0)
        r = requests.post(url, headers=HEADERS, data=json.dumps(payload))
    # Лог для диагностики
    print(f"👉 POST {url} | code={r.status_code}")
    if r.status_code >= 400:
        try:
            print("👉 body:", r.text[:500])
        except Exception:
            pass
        r.raise_for_status()
    time.sleep(sleep_sec)
    return r.json()

# ---------- PRODUCTS ----------

def get_all_product_ids() -> list[int]:
    """Тянем все product_id через /v3/product/list (пагинация по last_id)."""
    product_ids = []
    last_id = ""
    page = 1
    while True:
        print(f"📦 Получаем товары (страница {page})...")
        body = {
            "filter": {"visibility": "ALL"},
            "last_id": last_id,
            "limit": 1000
        }
        j = post(f"{API_HOST}/v3/product/list", body)
        result = j.get("result", {})
        items = result.get("items", [])
        if not items:
            break
        for it in items:
            # по спецификации тут есть product_id
            pid = it.get("product_id")
            if pid:
                product_ids.append(int(pid))
        last_id = result.get("last_id") or ""
        page += 1
        if not last_id:
            break
    product_ids = sorted(set(product_ids))
    print(f"✅ Всего товаров (product_id): {len(product_ids)}")
    return product_ids

def chunks(lst, n):
    """Порезка списка на куски по n."""
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def get_products_info(product_ids: list[int]) -> dict:
    """
    Возвращаем словарь по SKU:
      sku -> {"product_id": int, "name": str}
    Берём из /v3/product/info/list батчами до 1000 id.
    """
    sku_map = {}
    for batch in chunks(product_ids, 1000):
        body = {"product_id": batch}
        j = post(f"{API_HOST}/v3/product/info/list", body)
        result = j.get("result", {})
        items = result.get("items", [])
        for it in items:
            pid = it.get("product_id")
            name = (it.get("name") or "").strip()
            # в v3 тут есть поле sku (идентификатор товара на Озон)
            sku = it.get("sku")
            if sku:
                sku_map[str(sku)] = {
                    "product_id": int(pid) if pid else None,
                    "name": name
                }
    print(f"✅ Получено карточек с SKU: {len(sku_map)}")
    return sku_map

# ---------- ANALYTICS ----------

def daterange(period: str):
    """
    Возвращаем (date_from, date_to) в UTC (YYYY-MM-DD)
      day   -> вчера
      week  -> последние 7 дней (включая вчера)
      month -> последние 30 дней (включая вчера)
    """
    today_utc = datetime.now(timezone.utc).date()
    end = today_utc - timedelta(days=1)
    if period == "day":
        start = end
    elif period == "week":
        start = end - timedelta(days=6)
    else:
        start = end - timedelta(days=29)
    return (start.isoformat(), end.isoformat())

def get_analytics_rows(metric: str, sku_list: list[str], date_from: str, date_to: str):
    """
    Тянем строки аналитики для одной метрики (hits_view или hits_click)
    с группировкой по дню и sku. Делаем батч‑фильтрацию по 100 SKU и
    пагинацию по offset/limit.
    """
    rows = []
    for sku_batch in chunks(sku_list, 100):
        offset = 0
        while True:
            body = {
                "date_from": date_from,
                "date_to": date_to,
                "metrics": [metric],
                "dimension": ["day", "sku"],
                "filters": [
                    {"key": "sku", "operator": "IN", "value": [str(x) for x in sku_batch]}
                ],
                "limit": 1000,
                "offset": offset
            }
            j = post(f"{API_HOST}/v1/analytics/data", body, sleep_sec=0.05)
            result = j.get("result", {})
            data = result.get("data", [])
            if not data:
                break
            for row in data:
                dims = row.get("dimensions", [])
                m = row.get("metrics", [])
                if len(dims) >= 2 and m:
                    day_id = dims[0].get("id")  # YYYY-MM-DD
                    sku_id = dims[1].get("id")
                    value = float(m[0] or 0)
                    rows.append((day_id, str(sku_id), value))
            if len(data) < 1000:
                break
            offset += 1000
    return rows

def build_period(period: str, sku_map: dict):
    date_from, date_to = daterange(period)
    print(f"📊 Период {period}: {date_from} … {date_to}")

    all_skus = list(sku_map.keys())
    # Берём просмотры и клики раздельно и склеиваем
    views_rows  = get_analytics_rows("hits_view",  all_skus, date_from, date_to)
    clicks_rows = get_analytics_rows("hits_click", all_skus, date_from, date_to)

    # (day, sku) -> {views, clicks}
    by_day_sku = {}
    for d, s, v in views_rows:
        by_day_sku.setdefault((d, s), {"views": 0.0, "clicks": 0.0})
        by_day_sku[(d, s)]["views"] += v
    for d, s, c in clicks_rows:
        by_day_sku.setdefault((d, s), {"views": 0.0, "clicks": 0.0})
        by_day_sku[(d, s)]["clicks"] += c

    # Пишем деталку по дням
    daily_csv = f"public/daily_{period}.csv"
    os.makedirs("public", exist_ok=True)
    with open(daily_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "sku", "product_id", "name", "views", "clicks", "ctr_percent"])
        for (d, s), vc in by_day_sku.items():
            views = vc["views"]
            clicks = vc["clicks"]
            ctr = (clicks / views * 100.0) if views > 0 else 0.0
            meta = sku_map.get(s, {})
            w.writerow([d, s, meta.get("product_id"), meta.get("name"), int(views), int(clicks), round(ctr, 2)])

    # Агрегируем по SKU за период
    by_sku = {}
    for (d, s), vc in by_day_sku.items():
        x = by_sku.setdefault(s, {"views": 0.0, "clicks": 0.0})
        x["views"]  += vc["views"]
        x["clicks"] += vc["clicks"]

    aggregated = []
    for s, vc in by_sku.items():
        views = vc["views"]
        clicks = vc["clicks"]
        ctr = (clicks / views * 100.0) if views > 0 else 0.0
        meta = sku_map.get(s, {})
        aggregated.append({
            "sku": s,
            "product_id": meta.get("product_id"),
            "name": meta.get("name"),
            "views": int(views),
            "clicks": int(clicks),
            "ctr_percent": round(ctr, 2)
        })

    aggregated.sort(key=lambda x: (-x["ctr_percent"], -x["views"]))
    out_json = f"public/data_{period}.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump({
            "period": period,
            "date_from": date_from,
            "date_to": date_to,
            "generated_at_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "items": aggregated
        }, f, ensure_ascii=False)

    print(f"✅ Сохранено: {out_json} и {daily_csv} (SKU: {len(aggregated)})")

def main():
    if not CLIENT_ID or not API_KEY:
        raise SystemExit("❌ Не заданы OZON_CLIENT_ID / OZON_API_KEY в Secrets")

    # 1) список товаров -> product_id[]
    product_ids = get_all_product_ids()
    if not product_ids:
        print("⚠️ Товары не найдены.")
        return

    # 2) sku + name по product_id
    sku_map = get_products_info(product_ids)
    if not sku_map:
        print("⚠️ SKU не получены.")
        return

    # 3) строим три периода
    for period in ("day", "week", "month"):
        build_period(period, sku_map)

    # 4) кладём простой индекс, если его нет
    idx_path = "public/index.html"
    if not os.path.exists(idx_path):
        with open(idx_path, "w", encoding="utf-8") as f:
            f.write(INDEX_HTML)
        print("🧩 Добавлен public/index.html")

# Минимальный HTML (подменится, если файла нет в репо)
INDEX_HTML = """<!doctype html><meta charset="utf-8">
<title>Ozon CTR Dashboard</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:24px}
h1{margin:0 0 12px}
.controls{display:flex;gap:8px;align-items:center;margin:0 0 12px}
table{border-collapse:collapse;width:100%}
th,td{border-bottom:1px solid #eee;padding:8px 10px;text-align:left;vertical-align:top}
th{position:sticky;top:0;background:#fff}
.badge{display:inline-block;padding:2px 8px;border-radius:12px;background:#f4f4f4}
small{color:#666}
</style>
<h1>CTR по SKU</h1>
<div class="controls">
  <label>Период: 
    <select id="range">
      <option value="day">День</option>
      <option value="week">Неделя</option>
      <option value="month" selected>Месяц</option>
    </select>
  </label>
  <span id="meta" class="badge"></span>
</div>
<table id="t"><thead>
  <tr><th>SKU</th><th>Товар</th><th>Показы</th><th>Клики</th><th>CTR, %</th></tr>
</thead><tbody></tbody></table>
<script>
const $r = document.getElementById('range');
const $meta = document.getElementById('meta');
const $tbody = document.querySelector('#t tbody');
function fmt(n){return (n||0).toLocaleString('ru-RU')}
async function load(){
  const p = $r.value;
  const url = `data_${p}.json`;
  const res = await fetch(url);
  if(!res.ok){ $tbody.innerHTML = `<tr><td colspan="5">Нет данных (${url})</td></tr>`; return; }
  const j = await res.json();
  $meta.textContent = `${j.date_from} — ${j.date_to} | обновлено ${j.generated_at_utc} UTC`;
  const rows = j.items;
  $tbody.innerHTML = rows.map(x => `
    <tr>
      <td>${x.sku}</td>
      <td>${x.name?x.name.replace(/</g,'&lt;'):'—'}<br><small>ID: ${x.product_id||'—'}</small></td>
      <td>${fmt(x.views)}</td>
      <td>${fmt(x.clicks)}</td>
      <td><b>${(x.ctr_percent??0).toFixed(2)}</b></td>
    </tr>
  `).join('');
}
$r.onchange = load; load();
</script>
"""
if __name__ == "__main__":
    main()
