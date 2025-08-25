import os, sys, time, math, json, datetime as dt
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
    print("❌ Нет OZON_CLIENT_ID / OZON_API_KEY в переменных окружения (Secrets).")
    sys.exit(1)

# ---- Настройки периода: последние 30 полных дней ----
today = dt.date.today()
date_to = (today - dt.timedelta(days=1)).strftime("%Y-%m-%d")
date_from = (today - dt.timedelta(days=30)).strftime("%Y-%m-%d")

# ---- Вспомогалки ------------------------------------------------------------

def post_json(url: str, body: dict) -> dict:
    r = requests.post(url, headers=HEADERS, json=body, timeout=60)
    code = r.status_code
    try:
        txt = r.text
        data = r.json() if "application/json" in r.headers.get("Content-Type", "") else {}
    except Exception:
        data = {}
        txt = r.text
    if code >= 400:
        print(f"👉 Код ответа: {code}")
        print(f"👉 Ответ: {txt}")
        r.raise_for_status()
    return data

def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

# ---- Получение списка product_id через /v3/product/list ---------------------
# ВАЖНО: у метода обязательный filter и пагинация через last_id (limit <= 1000)
# Документация/SDK подтверждают схему с filter/last_id/limit.  [oai_citation:3‡pkg.go.dev](https://pkg.go.dev/github.com/diphantxm/ozon-api-client/ozon?utm_source=chatgpt.com)
def get_all_product_ids():
    all_items = []
    last_id = ""
    page = 1
    while True:
        body = {
            "filter": {
                "visibility": "ALL"  # можно сузить: VISIBLE/INVISIBLE/EMPTY_STOCK
            },
            "last_id": last_id,
            "limit": 1000  # максимум 1000
        }
        print(f"📦 Получаем товары (страница {page})...")
        data = post_json(f"{BASE}/v3/product/list", body)
        # разные обёртки встречаются: result.items или items
        result = data.get("result") or data
        items = (result or {}).get("items", [])
        if not items:
            break
        all_items.extend(items)
        last_id = (result or {}).get("last_id", "")
        page += 1
        if not last_id:
            break
        # небольшая вежливая пауза
        time.sleep(0.3)
    if not all_items:
        print("⚠️ Товары не найдены. Проверь видимость и права API-ключа.")
    return all_items

# ---- Подтягиваем названия через /v3/product/info/list -----------------------
# Этот метод — именно v3 (v2 даст 404).  [oai_citation:4‡docs.ozon.ru](https://docs.ozon.ru/global/en/api/?utm_source=chatgpt.com)
def enrich_names(product_ids):
    names = {}
    for batch in chunked(product_ids, 100):
        body = { "product_id": batch }
        data = post_json(f"{BASE}/v3/product/info/list", body)
        result = data.get("result") or {}
        items = result.get("items") or result.get("products") or result.get("result") or []
        # формат бывает разный; ловим популярные поля:
        for it in items:
            pid = it.get("id") or it.get("product_id")
            name = it.get("name") or it.get("title") or ""
            offer = it.get("offer_id") or it.get("sku") or ""
            if pid is not None:
                names[int(pid)] = {"name": name, "offer_id": str(offer)}
        time.sleep(0.2)
    return names

# ---- Аналитика: /v1/analytics/data -----------------------------------------
# У API бывали изменения — иногда возвращалась только одна метрика.
# Поэтому пробуем несколько пар метрик, пока не получим 2 значения в "metrics".
# История «капризности» метрик и изменений метода задокументирована.  [oai_citation:5‡dev.ozon.ru](https://dev.ozon.ru/community/585-Perestal-rabotat-metod-v1-analytics-data?utm_source=chatgpt.com)
METRIC_PAIRS = [
    ("hits_view", "hits_click"),
    ("hits_view_search", "hits_click_search"),
]

def try_fetch_analytics(product_ids, m_view, m_click):
    rows = []
    for batch in chunked(product_ids, 200):  # разумный размер фильтра
        offset = 0
        while True:
            body = {
                "date_from": date_from,
                "date_to": date_to,
                "metrics": [m_view, m_click],
                "dimension": ["product_id"],
                "limit": 1000,
                "offset": offset,
                "filters": [
                    {"key": "product_id", "operator": "IN", "value": [str(x) for x in batch]}
                ]
            }
            data = post_json(f"{BASE}/v1/analytics/data", body)
            result = data.get("result") or {}
            part = result.get("data", [])
            if not part:
                break
            for row in part:
                dims = row.get("dimensions", [])
                mets = row.get("metrics", [])
                if not dims or len(mets) < 2:
                    # как раз тот случай «вернулась одна метрика» — пусть верхний уровень решит
                    rows.append({"_bad_metrics_row": row})
                    continue
                pid = dims[0].get("id")
                try:
                    pid = int(pid)
                except:
                    continue
                views = float(mets[0])
                clicks = float(mets[1])
                rows.append({"product_id": pid, "views": views, "clicks": clicks})
            # пагинация по offset
            if len(part) < 1000:
                break
            offset += 1000
            time.sleep(0.2)
        time.sleep(0.2)
    return rows

def get_analytics_all(product_ids):
    for mv, mc in METRIC_PAIRS:
        print(f"📊 Пробуем метрики: {mv} / {mc}")
        rows = try_fetch_analytics(product_ids, mv, mc)
        good = [r for r in rows if "product_id" in r]
        if good:
            return good, (mv, mc)
        # если совсем пусто — пробуем следующую пару
    return [], None

# ==== MAIN ===================================================================
items = get_all_product_ids()
if not items:
    sys.exit(0)

# Вытаскиваем product_id, offer_id если есть
product_ids = []
offer_by_pid = {}
for it in items:
    pid = it.get("product_id") or it.get("id")
    if pid is None:
        continue
    pid = int(pid)
    product_ids.append(pid)
    offer = it.get("offer_id") or ""
    if offer:
        offer_by_pid[pid] = str(offer)

# Имена
print("🪪 Подтягиваем названия...")
name_map = enrich_names(product_ids)

# Аналитика
analytics_rows, used_metrics = get_analytics_all(product_ids)
if not analytics_rows:
    print("⚠️ Не удалось получить одновременно и показы, и клики. Возможно, по выбранной паре метрик API отдаёт только 1 показатель для вашего аккаунта/периода.")
    print("   Попробуйте позже или скорректируйте METRIC_PAIRS.")
    # Сохраним хоть что-то: пустую таблицу с шапкой, чтобы сайт отдался корректно
    os.makedirs("site", exist_ok=True)
    pd.DataFrame(columns=["product_id","offer_id","name","views","clicks","ctr"]).to_csv("site/data.csv", index=False)
    with open("site/index.html","w",encoding="utf-8") as f:
        f.write("<!doctype html><meta charset='utf-8'><p>Нет данных за период.</p>")
    sys.exit(0)

df = pd.DataFrame(analytics_rows)
# Агрегируем на всякий (если внезапно вернулись дубликаты по product_id)
df = df.groupby("product_id", as_index=False).sum(numeric_only=True)
df["ctr"] = (df["clicks"] / df["views"]).where(df["views"] > 0, 0) * 100

# Присоединим имена и offer_id
def name_of(pid):
    nm = name_map.get(pid, {})
    n = nm.get("name") or ""
    return n

def offer_of(pid):
    nm = name_map.get(pid, {})
    off = nm.get("offer_id") or offer_by_pid.get(pid, "")
    return str(off)

df["name"] = df["product_id"].apply(name_of)
df["offer_id"] = df["product_id"].apply(offer_of)

# Сортировка по CTR убыв.
df = df[["product_id","offer_id","name","views","clicks","ctr"]].sort_values(["ctr","views"], ascending=[False, False])

# Вывод
os.makedirs("site", exist_ok=True)
csv_path = "site/data.csv"
df.to_csv(csv_path, index=False)
print(f"✅ Сохранил {len(df)} строк в {csv_path}")
print(f"ℹ️ Использованные метрики: {used_metrics}")

# Мини-сайт (таблица)
html = f"""<!doctype html>
<html lang="ru">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>CTR за месяц (Ozon)</title>
<style>
body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:24px}}
h1{{margin:0 0 8px}}
small{{color:#555}}
table{{border-collapse:collapse;width:100%;margin-top:16px}}
th,td{{border:1px solid #e5e5e5;padding:8px;text-align:left;vertical-align:top}}
th{{background:#fafafa;cursor:pointer;position:sticky;top:0}}
tr:nth-child(even){{background:#fcfcfc}}
.search{{margin-top:12px}}
input[type=search]{{padding:8px;width:320px;max-width:100%}}
.badge{{display:inline-block;background:#f1f5f9;border:1px solid #e2e8f0;border-radius:6px;padding:2px 6px;margin-left:6px;font-size:12px}}
</style>
<h1>CTR по товарам</h1>
<small>Период: {date_from} → {date_to}. Метрики: {used_metrics[0]} / {used_metrics[1]}.</small>
<div class="search">
  <input id="q" type="search" placeholder="Фильтр по названию или SKU…">
  <span class="badge">клики/показы × 100</span>
</div>
<table id="t">
  <thead>
    <tr>
      <th data-k="product_id">Product ID</th>
      <th data-k="offer_id">SKU</th>
      <th data-k="name">Название</th>
      <th data-k="views">Показы</th>
      <th data-k="clicks">Клики</th>
      <th data-k="ctr">CTR, %</th>
    </tr>
  </thead>
  <tbody></tbody>
</table>
<script>
async function load() {{
  const resp = await fetch('data.csv');
  const text = await resp.text();
  const rows = text.trim().split(/\\r?\\n/).map(l => l.split(/,(?=(?:[^"]*"[^"]*")*[^"]*$)/));
  const head = rows.shift().map(h=>h.replace(/^"|"$/g,''));
  const data = rows.map(r => Object.fromEntries(r.map((v,i)=>[head[i], v.replace(/^"|"$/g,'') ])));
  data.forEach(d => {{
    d.views = +d.views || 0;
    d.clicks = +d.clicks || 0;
    d.ctr = +d.ctr || 0;
  }});
  window._data = data;
  render();
}}
function render(sortKey='ctr', desc=true) {{
  const q = (document.getElementById('q').value||'').toLowerCase().trim();
  let arr = window._data || [];
  if (q) {{
    arr = arr.filter(x => (x.name||'').toLowerCase().includes(q) || (x.offer_id||'').toLowerCase().includes(q));
  }}
  arr = arr.slice().sort((a,b)=> (desc? (b[sortKey]-a[sortKey]) : (a[sortKey]-b[sortKey])) );
  const tb = document.querySelector('#t tbody');
  tb.innerHTML='';
  for (const x of arr) {{
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${{x.product_id}}</td>
      <td>${{x.offer_id||''}}</td>
      <td>${{x.name||''}}</td>
      <td>${{x.views}}</td>
      <td>${{x.clicks}}</td>
      <td>${{(x.ctr).toFixed(2)}}</td>
    `;
    tb.appendChild(tr);
  }}
}}
document.getElementById('q').addEventListener('input', ()=>render());
document.querySelectorAll('th').forEach(th => {{
  th.addEventListener('click', () => {{
    const k = th.dataset.k;
    const desc = !(th.dataset.desc==='true');
    th.dataset.desc = desc;
    render(k, desc);
  }});
}});
load();
</script>
</html>
"""
with open("site/index.html", "w", encoding="utf-8") as f:
    f.write(html)

print("🌐 Сайт собран в папке site/")
