import os, json, csv, requests
from datetime import datetime, timedelta
from collections import defaultdict

CLIENT_ID = os.environ["OZON_CLIENT_ID"]
API_KEY   = os.environ["OZON_API_KEY"]

date_to   = datetime.utcnow().date() - timedelta(days=1)
date_from = date_to - timedelta(days=29)

headers = {"Client-Id": CLIENT_ID, "Api-Key": API_KEY}

def fetch(url, body):
    r = requests.post(url, headers=headers, json=body, timeout=60)
    print("👉 Запрос к:", url)
    print("👉 Тело:", json.dumps(body, ensure_ascii=False))
    print("👉 Код ответа:", r.status_code)
    try:
        print("👉 Ответ:", r.json())
    except:
        print("👉 Ответ (текст):", r.text)
    r.raise_for_status()
    return r.json()

# --- 1. CTR по SKU + день ---
url = "https://api-seller.ozon.ru/v1/analytics/data"
body = {
    "date_from": str(date_from),
    "date_to":   str(date_to),
    "metrics":   ["hits_view","hits_click"],
    "dimension": ["day","sku"],
    "limit":     1000,
    "offset":    0
}
payload = fetch(url, body)

# --- агрегируем за весь период по каждому SKU ---
stats = defaultdict(lambda: {"shows":0,"clicks":0,"name":""})

for row in payload.get("result", {}).get("data", []):
    sku = row["dimensions"][1]["id"]   # [0]=day, [1]=sku
    metrics = row.get("metrics", [])
    shows  = float(metrics[0]) if len(metrics) > 0 else 0
    clicks = float(metrics[1]) if len(metrics) > 1 else 0
    stats[sku]["shows"]  += shows
    stats[sku]["clicks"] += clicks

# --- 2. Названия товаров ---
sku_list = list(stats.keys())
names_url = "https://api-seller.ozon.ru/v2/product/info/list"
for i in range(0, len(sku_list), 100):
    batch = sku_list[i:i+100]
    body = {"sku": batch}
    payload = fetch(names_url, body)
    for item in payload.get("result", []):
        s = str(item["id"])
        if s in stats:
            stats[s]["name"] = item.get("name", "")

# --- 3. Считаем CTR ---
rows = []
for sku, v in stats.items():
    shows = v["shows"]
    clicks = v["clicks"]
    ctr = (clicks/shows*100) if shows>0 else 0
    rows.append([sku, v["name"], int(shows), int(clicks), round(ctr,2)])

rows.sort(key=lambda x: x[4], reverse=True)

os.makedirs("site/data", exist_ok=True)

with open("site/data/ctr.csv","w",newline="",encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["sku","name","shows","clicks","ctr"])
    w.writerows(rows)

with open("site/data/ctr.json","w",encoding="utf-8") as f:
    json.dump(rows,f,ensure_ascii=False)

# --- 4. HTML таблица ---
index_html = """<!doctype html><html><head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Ozon CTR Dashboard</title>
<style>
body{font-family:sans-serif;padding:24px;max-width:1200px;margin:0 auto}
h1{margin-bottom:20px}
table{border-collapse:collapse;width:100%}
th,td{border:1px solid #ccc;padding:8px;text-align:left}
th{cursor:pointer;background:#f3f3f3}
</style>
</head><body>
<h1>Ozon CTR по SKU (последние 30 дней)</h1>
<table id="ctrTable">
<thead>
<tr>
  <th onclick="sortTable(0)">SKU</th>
  <th onclick="sortTable(1)">Название</th>
  <th onclick="sortTable(2)">Показы</th>
  <th onclick="sortTable(3)">Клики</th>
  <th onclick="sortTable(4)">CTR %</th>
</tr>
</thead><tbody id="tableBody"></tbody></table>
<script>
async function main(){
  const res = await fetch("data/ctr.json");
  const rows = await res.json();
  const body = document.getElementById("tableBody");
  body.innerHTML = "";
  for(const r of rows){
    const tr=document.createElement("tr");
    tr.innerHTML = `<td>${r[0]}</td><td>${r[1]}</td><td>${r[2]}</td><td>${r[3]}</td><td>${r[4]}</td>`;
    body.appendChild(tr);
  }
}
function sortTable(n){
  const table=document.getElementById("ctrTable");
  let switching=true,dir="desc",switchcount=0;
  while(switching){
    switching=false;
    const rows=table.rows;
    for(let i=1;i<rows.length-1;i++){
      let shouldSwitch=false;
      const x=rows[i].getElementsByTagName("TD")[n];
      const y=rows[i+1].getElementsByTagName("TD")[n];
      let cmpx=isNaN(x.innerHTML)?x.innerHTML.toLowerCase():parseFloat(x.innerHTML)||0;
      let cmpy=isNaN(y.innerHTML)?y.innerHTML.toLowerCase():parseFloat(y.innerHTML)||0;
      if(dir=="asc"?cmpx>cmpy:cmpx<cmpy){shouldSwitch=true;break;}
    }
    if(shouldSwitch){
      rows[i].parentNode.insertBefore(rows[i+1],rows[i]);
      switching=true;switchcount++;
    } else { if(switchcount==0 && dir=="asc"){dir="desc";switching=true;} }
  }
}
main();
</script></body></html>"""

with open("site/index.html","w",encoding="utf-8") as f:
    f.write(index_html)

print("✅ Сайт собран: смотри site/index.html")
