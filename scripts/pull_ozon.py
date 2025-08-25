import os, json, csv, requests
from datetime import datetime, timedelta
from collections import defaultdict

CLIENT_ID = os.environ["OZON_CLIENT_ID"]
API_KEY   = os.environ["OZON_API_KEY"]

date_to   = datetime.utcnow().date() - timedelta(days=1)
date_from = date_to - timedelta(days=29)

url = "https://api-seller.ozon.ru/v1/analytics/data"
headers = {"Client-Id": CLIENT_ID, "Api-Key": API_KEY}
body = {
    "date_from": str(date_from),
    "date_to":   str(date_to),
    "metrics":   ["hits_view","hits_click"],
    "dimension": ["day","sku"],
    "limit":     1000
}

r = requests.post(url, headers=headers, json=body, timeout=60)
r.raise_for_status()
payload = r.json()

# --- агрегируем ---
daily = defaultdict(lambda: {"shows":0,"clicks":0})
for row in payload.get("result", {}).get("data", []):
    day = row["dimensions"][0]["id"]
    metrics = row.get("metrics", [])
    shows  = float(metrics[0]) if len(metrics) > 0 else 0
    clicks = float(metrics[1]) if len(metrics) > 1 else 0
    daily[day]["shows"]  += shows
    daily[day]["clicks"] += clicks

# считаем CTR
daily_final = []
for day, v in sorted(daily.items()):
    ctr = (v["clicks"]/v["shows"]*100) if v["shows"]>0 else 0
    daily_final.append({"date":day,"shows":int(v["shows"]),"clicks":int(v["clicks"]),"ctr":round(ctr,2)})

# агрегируем по неделям и месяцам
weekly = defaultdict(lambda: {"shows":0,"clicks":0})
monthly = defaultdict(lambda: {"shows":0,"clicks":0})
for d in daily_final:
    dt = datetime.strptime(d["date"], "%Y-%m-%d")
    week = f"{dt.isocalendar().year}-W{dt.isocalendar().week}"
    month = dt.strftime("%Y-%m")
    weekly[week]["shows"] += d["shows"]; weekly[week]["clicks"] += d["clicks"]
    monthly[month]["shows"] += d["shows"]; monthly[month]["clicks"] += d["clicks"]

def finalize(data_dict):
    arr=[]
    for k,v in sorted(data_dict.items()):
        ctr=(v["clicks"]/v["shows"]*100) if v["shows"]>0 else 0
        arr.append({"date":k,"shows":int(v["shows"]),"clicks":int(v["clicks"]),"ctr":round(ctr,2)})
    return arr

weekly_final = finalize(weekly)
monthly_final = finalize(monthly)

os.makedirs("site/data", exist_ok=True)

with open("site/data/ctr.json","w",encoding="utf-8") as f:
    json.dump({"daily":daily_final,"weekly":weekly_final,"monthly":monthly_final},f,ensure_ascii=False)

# --- html ---
index_html = """<!doctype html><html><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Ozon CTR Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
body{font-family:sans-serif;padding:24px;max-width:1100px;margin:0 auto}
h1{margin:0 0 12px}
.row{display:flex;gap:12px;align-items:center;margin:16px 0}
button{padding:6px 12px}
canvas{max-width:100%;}
</style>
</head><body>
<h1>Ozon CTR (суммарно по всем SKU)</h1>
<div class="row">
  <button onclick="draw('daily')">День</button>
  <button onclick="draw('weekly')">Неделя</button>
  <button onclick="draw('monthly')">Месяц</button>
</div>
<canvas id="chart" height="120"></canvas>
<script>
let data,chart,ctx;
async function main(){
  const res = await fetch("data/ctr.json");
  data = await res.json();
  ctx = document.getElementById('chart').getContext('2d');
  draw('daily');
}
function draw(mode){
  const arr = data[mode] || [];
  const labels = arr.map(x=>x.date);
  const ctr = arr.map(x=>x.ctr);
  if(chart) chart.destroy();
  chart = new Chart(ctx,{ type:'line',
    data:{ labels, datasets:[{ label:`CTR % (${mode})`, data: ctr, tension:0.3 }]},
    options:{ responsive:true, interaction:{mode:'index', intersect:false},
      scales:{ y:{ ticks:{ callback:(v)=>v+'%' }}}}
  });
}
main();
</script></body></html>"""

with open("site/index.html","w",encoding="utf-8") as f:
    f.write(index_html)

print("✅ Сайт собран: смотри папку site/")
