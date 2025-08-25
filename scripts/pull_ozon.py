import os, json, csv, requests
from datetime import datetime, timedelta
from collections import defaultdict

# Берём ключи из GitHub Secrets
CLIENT_ID = os.environ["OZON_CLIENT_ID"]
API_KEY   = os.environ["OZON_API_KEY"]

# Последние 30 дней
date_to   = datetime.utcnow().date() - timedelta(days=1)
date_from = date_to - timedelta(days=29)

# Запрос к Ozon Analytics API
url = "https://api-seller.ozon.ru/v1/analytics/data"
headers = {"Client-Id": CLIENT_ID, "Api-Key": API_KEY}
body = {
    "date_from": str(date_from),
    "date_to":   str(date_to),
    "metrics":   ["shows","clicks","cr"],
    "dimension": ["day","sku"],
    "limit":     5000
}

r = requests.post(url, headers=headers, json=body, timeout=60)
r.raise_for_status()
payload = r.json()

# Обработка результата
by_sku = defaultdict(list)
rows_flat = []

for row in payload.get("result", {}).get("data", []):
    day = row["dimensions"][0]["id"]
    sku = row["dimensions"][1]["id"]
    shows  = float(row["metrics"][0])
    clicks = float(row["metrics"][1])
    cr     = float(row["metrics"][2]) if len(row["metrics"]) > 2 else 0.0
    ctr    = (clicks / shows * 100.0) if shows > 0 else 0.0
    rec = {"date": day, "shows": int(shows), "clicks": int(clicks), "ctr": round(ctr, 3), "cr": round(cr, 3)}
    by_sku[sku].append(rec)
    rows_flat.append([day, sku, int(shows), int(clicks), round(ctr,3), round(cr,3)])

for sku in by_sku:
    by_sku[sku].sort(key=lambda x: x["date"])

# Папка для сайта
os.makedirs("site/data", exist_ok=True)

# JSON
with open("site/data/ctr.json", "w", encoding="utf-8") as f:
    json.dump({"range": {"from": str(date_from), "to": str(date_to)}, "by_sku": by_sku}, f, ensure_ascii=False)

# CSV
with open("site/data/ctr.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["date","sku","shows","clicks","ctr","cr"])
    w.writerows(rows_flat)

# Простая страница
index_html = """<!doctype html><html><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Ozon CTR Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
body{font-family:sans-serif;padding:24px;max-width:1100px;margin:0 auto}
h1{margin:0 0 12px}
.row{display:flex;gap:12px;align-items:center;margin:16px 0}
select,button{padding:8px 10px}
canvas{max-width:100%;}
</style>
</head><body>
<h1>Ozon CTR (по SKU и по дням)</h1>
<div class="row">
  <label for="skuSel">SKU:</label>
  <select id="skuSel"></select>
  <button id="dl">Скачать CSV</button>
</div>
<canvas id="chart" height="120"></canvas>
<script>
async function main(){
  const res = await fetch("data/ctr.json");
  const data = await res.json();
  const sel = document.getElementById("skuSel");
  const keys = Object.keys(data.by_sku).sort();
  if(keys.length===0){ document.body.insertAdjacentHTML('beforeend','<p>Нет данных</p>'); return; }
  for(const k of keys){ const o=document.createElement('option'); o.value=k; o.textContent=k; sel.appendChild(o); }
  const ctx = document.getElementById('chart').getContext('2d');
  let chart;
  function draw(sku){
    const arr = data.by_sku[sku] || [];
    const labels = arr.map(x=>x.date);
    const ctr = arr.map(x=>x.ctr);
    if(chart) chart.destroy();
    chart = new Chart(ctx,{ type:'line',
      data:{ labels, datasets:[{ label:`CTR % (${sku})`, data: ctr, tension:0.3 }]},
      options:{ responsive:true, interaction:{mode:'index', intersect:false},
        scales:{ y:{ ticks:{ callback:(v)=>v+'%' }}}}
    );
  }
  sel.addEventListener('change', e=>draw(e.target.value));
  draw(keys[0]);
  document.getElementById('dl').onclick=()=>{ window.location='data/ctr.csv'; };
}
main();
</script></body></html>"""
with open("site/index.html","w",encoding="utf-8") as f:
    f.write(index_html)

print("✅ Сайт собран: смотри папку site/")
