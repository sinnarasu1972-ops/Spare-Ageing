import pandas as pd
from datetime import datetime, timedelta
import uvicorn
from fastapi import FastAPI, Request, UploadFile, File
from fastapi.responses import HTMLResponse, FileResponse
import os
import socket
from typing import Optional
import sys
import numpy as np

def clean_for_json(df):
    df = df.copy()
    df = df.replace([np.inf, -np.inf], None)
    df = df.where(pd.notna(df), None)
    return df

excel_file_path = "./Spares Ageing Report.xlsx"
last_reload_time = None

def parse_date(date_str):
    if pd.isna(date_str) or date_str == "-" or str(date_str).strip() == "":
        return None
    try:
        date_part = str(date_str)[:10].strip()
        date_formats = ['%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d', '%m/%d/%Y', '%d.%m.%Y']
        for fmt in date_formats:
            try:
                return datetime.strptime(date_part, fmt).date()
            except:
                continue
    except:
        pass
    return None

def process_excel_to_csv():
    input_file = excel_file_path
    output_csv = "./Spares Ageing Report_Processed.csv"
    
    if not os.path.exists(input_file):
        return None, 0, None
    
    try:
        df = pd.read_excel(input_file)
    except Exception as e:
        return None, 0, None
    
    today = datetime.now().date()
    
    def categorize_aging(date_str):
        if pd.isna(date_str) or date_str == "-" or str(date_str).strip() == "":
            return "730 and above"
        try:
            date_obj = parse_date(date_str)
            if date_obj is None:
                return "730 and above"
            days_diff = (today - date_obj).days
            if days_diff <= 90:
                return "0 to 90 days"
            elif days_diff <= 180:
                return "91 to 180 days"
            elif days_diff <= 365:
                return "181 to 365 days"
            elif days_diff <= 730:
                return "366 to 730 days"
            else:
                return "730 and above"
        except:
            return "730 and above"
    
    last_issue_col = None
    for col in df.columns:
        if 'last' in str(col).lower() and 'issue' in str(col).lower() and 'date' in str(col).lower():
            last_issue_col = col
            break
    
    last_purchase_col = None
    for col in df.columns:
        if 'last' in str(col).lower() and 'purchase' in str(col).lower() and 'date' in str(col).lower():
            last_purchase_col = col
            break
    
    if last_issue_col is None or last_purchase_col is None:
        return None, 0, None
    
    df['Movement Category I (2)'] = df[last_issue_col].apply(categorize_aging)
    df['Movement Category P (2)'] = df[last_purchase_col].apply(categorize_aging)
    
    gndp_column = None
    for col in df.columns:
        if 'stock' in str(col).lower() and 'gndp' in str(col).lower():
            gndp_column = col
            break
    
    if gndp_column:
        df[gndp_column] = pd.to_numeric(df[gndp_column], errors='coerce').fillna(0)
        total_gndp = df[gndp_column].sum()
    else:
        total_gndp = 0
    
    try:
        df.to_csv(output_csv, index=False)
    except Exception as e:
        return None, 0, None
    
    return output_csv, total_gndp, gndp_column

app = FastAPI()

def format_indian_number(num):
    if num is None or pd.isna(num):
        return "0"
    try:
        actual_value = int(round(float(num)))
        num_str = str(abs(actual_value))
        if len(num_str) <= 3:
            result = num_str
        else:
            last_three = num_str[-3:]
            remaining = num_str[:-3]
            result = ""
            for i, digit in enumerate(reversed(remaining)):
                if i > 0 and i % 2 == 0:
                    result = "," + result
                result = digit + result
            result = result + "," + last_three
        return ("-" + result) if actual_value < 0 else result
    except:
        return "0"

print("\n" + "=" * 70)
print("STARTING SPARE PARTS AGEING DASHBOARD")
print("=" * 70)

csv_file, total_gndp, gndp_column = process_excel_to_csv()

if csv_file is None:
    print("ERROR: Failed to process Excel file")
    sys.exit(1)

try:
    df = pd.read_csv(csv_file)
except Exception as e:
    print(f"ERROR loading CSV: {e}")
    sys.exit(1)

last_reload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

last_issue_col = None
for col in df.columns:
    if 'last' in str(col).lower() and 'issue' in str(col).lower() and 'date' in str(col).lower():
        last_issue_col = col
        break

last_purchase_col = None
for col in df.columns:
    if 'last' in str(col).lower() and 'purchase' in str(col).lower() and 'date' in str(col).lower():
        last_purchase_col = col
        break

location_col = None
for col in df.columns:
    if 'location' in str(col).lower() and 'dealer' not in str(col).lower():
        location_col = col
        break

abc_col = None
for col in df.columns:
    if str(col).upper().strip() == 'ABC':
        abc_col = col
        break

ris_col = None
for col in df.columns:
    if str(col).upper().strip() == 'RIS':
        ris_col = col
        break

part_no_col = None
for col in df.columns:
    if 'part' in str(col).lower() and 'no' in str(col).lower() and 'description' not in str(col).lower():
        part_no_col = col
        break

part_category_col = None
for col in df.columns:
    if 'part' in str(col).lower() and 'category' in str(col).lower():
        part_category_col = col
        break

stock_qty_col = None
for col in df.columns:
    if 'stock' in str(col).lower() and 'qty' in str(col).lower():
        stock_qty_col = col
        break

locations = sorted([x for x in df[location_col].unique().tolist() if pd.notna(x)]) if location_col in df.columns else []
abc_categories = sorted([x for x in df[abc_col].unique().tolist() if pd.notna(x)]) if abc_col and abc_col in df.columns else []
ris_values = sorted([x for x in df[ris_col].unique().tolist() if pd.notna(x)]) if ris_col and ris_col in df.columns else []
part_categories = sorted([x for x in df[part_category_col].unique().tolist() if pd.notna(x)]) if part_category_col in df.columns else []

movement_order = ["0 to 90 days", "91 to 180 days", "181 to 365 days", "366 to 730 days", "730 and above"]
unique_movement = [x for x in df['Movement Category P (2)'].unique().tolist() if pd.notna(x)]
movement_categories = [cat for cat in movement_order if cat in unique_movement]

print(f"Configuration Complete: {len(df):,} records")

if not os.path.exists("static"):
    os.makedirs("static")

css_content = """
* { margin: 0; padding: 0; box-sizing: border-box; }
body { background-color: #f5f7fa; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; padding: 20px 0; }
.container-fluid { max-width: 1400px; margin: 0 auto; }
h1 { color: #2c3e50; font-weight: 700; font-size: 2rem; margin-bottom: 20px; }

.upload-section { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 60px 40px; border-radius: 12px; margin-bottom: 30px; cursor: pointer; transition: all 0.3s ease; box-shadow: 0 4px 15px rgba(102, 126, 234, 0.3); }
.upload-section:hover { transform: translateY(-2px); box-shadow: 0 6px 20px rgba(102, 126, 234, 0.4); }
.upload-section h2 { color: white; font-size: 24px; margin-bottom: 10px; }
.upload-section p { color: rgba(255,255,255,0.9); font-size: 14px; margin-bottom: 5px; }

.dead-stock-monitor { background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%); padding: 30px; border-radius: 12px; margin-bottom: 30px; color: white; }
.dead-stock-monitor h3 { font-size: 20px; margin-bottom: 20px; }
.dead-stock-cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }
.dead-stock-card { background: white; color: #333; padding: 18px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
.dead-stock-card .label { font-size: 12px; color: #666; font-weight: 600; margin-bottom: 8px; }
.dead-stock-card .value { font-size: 26px; font-weight: 700; color: #2c3e50; margin-bottom: 5px; }
.dead-stock-card .sub-value { font-size: 12px; color: #666; margin-bottom: 10px; }
.dead-stock-card .btn { width: 100%; padding: 6px; border: none; border-radius: 6px; color: white; font-weight: 600; cursor: pointer; font-size: 11px; }

.gndp-box { background: linear-gradient(135deg, #3498db 0%, #2980b9 100%); color: white; padding: 30px; border-radius: 12px; margin-bottom: 30px; }
.gndp-value { font-size: 32px; font-weight: 700; margin-bottom: 10px; }
.gndp-label { font-size: 16px; opacity: 0.9; }

.filters-section { background: white; padding: 25px; border-radius: 12px; margin-bottom: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
.filter-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 15px; margin-bottom: 15px; }
.filter-group { display: flex; flex-direction: column; }
.filter-group label { font-size: 11px; font-weight: 600; color: #2c3e50; margin-bottom: 6px; }
.filter-group select, .filter-group input { padding: 8px 10px; border: 1px solid #ddd; border-radius: 6px; font-size: 12px; }
.filter-buttons { display: flex; gap: 10px; }
.btn-apply { background: #3498db; color: white; border: none; padding: 8px 20px; border-radius: 6px; font-weight: 600; cursor: pointer; }
.btn-clear { background: #95a5a6; color: white; border: none; padding: 8px 20px; border-radius: 6px; font-weight: 600; cursor: pointer; }

.data-table { background: white; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); overflow: hidden; }
.data-table .card-header { background: #34495e; color: white; padding: 15px; font-weight: 600; }
.table { font-size: 11px; }
.table thead th { background-color: #34495e; color: white; border: none; padding: 10px; font-weight: 600; }
.table tbody td { padding: 8px 10px; border-bottom: 1px solid #ecf0f1; }
.pagination { margin-top: 15px; }
.page-link { color: #3498db; }
.page-item.active .page-link { background-color: #3498db; border-color: #3498db; }
"""

with open("static/style.css", "w") as f:
    f.write(css_content)

@app.post("/upload-excel")
async def upload_excel(file: UploadFile = File(...)):
    global df, total_gndp, locations, abc_categories, ris_values, part_categories, movement_categories, last_reload_time
    try:
        contents = await file.read()
        with open(excel_file_path, 'wb') as f:
            f.write(contents)
        
        csv_file, total_gndp, gndp_column = process_excel_to_csv()
        if csv_file is None:
            return {"success": False, "message": "Processing failed"}
        
        df = pd.read_csv(csv_file)
        locations = sorted([x for x in df[location_col].unique().tolist() if pd.notna(x)]) if location_col in df.columns else []
        abc_categories = sorted([x for x in df[abc_col].unique().tolist() if pd.notna(x)]) if abc_col and abc_col in df.columns else []
        ris_values = sorted([x for x in df[ris_col].unique().tolist() if pd.notna(x)]) if ris_col and ris_col in df.columns else []
        part_categories = sorted([x for x in df[part_category_col].unique().tolist() if pd.notna(x)]) if part_category_col in df.columns else []
        
        movement_order = ["0 to 90 days", "91 to 180 days", "181 to 365 days", "366 to 730 days", "730 and above"]
        unique_movement = [x for x in df['Movement Category P (2)'].unique().tolist() if pd.notna(x)]
        movement_categories = [cat for cat in movement_order if cat in unique_movement]
        
        last_reload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return {"success": True, "message": "Uploaded! {} records loaded".format(len(df))}
    except Exception as e:
        return {"success": False, "message": "Error: {}".format(str(e))}

def build_html(last_update_time, formatted_gndp, locations, part_categories, movement_categories, abc_categories, ris_values):
    movement_opts = ''.join(['<option value="' + str(cat) + '">' + str(cat) + '</option>' for cat in movement_categories])
    part_cat_opts = ''.join(['<option value="' + str(cat) + '">' + str(cat) + '</option>' for cat in part_categories])
    abc_opts = ''.join(['<option value="' + str(cat) + '">' + str(cat) + '</option>' for cat in abc_categories])
    ris_opts = ''.join(['<option value="' + str(val) + '">' + str(val) + '</option>' for val in ris_values])
    loc_opts = ''.join(['<option value="' + str(loc) + '">' + str(loc) + '</option>' for loc in locations])
    
    html = '<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">'
    html += '<title>Spare Parts Ageing Dashboard</title>'
    html += '<link href="https://cdnjs.cloudflare.com/ajax/libs/bootstrap/5.3.0/css/bootstrap.min.css" rel="stylesheet">'
    html += '<link href="/static/style.css" rel="stylesheet">'
    html += '</head><body><div class="container-fluid p-4">'
    
    html += '<h1>Unnati Motors Mahindra Spare Parts Ageing Dashboard</h1>'
    html += '<p>Last Updated: ' + last_update_time + '</p>'
    
    html += '<div class="upload-section" id="uploadArea" onclick="document.getElementById(\'fileInput\').click();">'
    html += '<h2>📤 Drag & Drop Excel file here</h2>'
    html += '<p>or click to browse | Supports .xlsx and .xls files</p>'
    html += '<input type="file" id="fileInput" accept=".xlsx,.xls" style="display:none;">'
    html += '</div>'
    
    html += '<div class="dead-stock-monitor">'
    html += '<h3>😰 Dead Stock Monitor</h3>'
    html += '<div class="dead-stock-cards">'
    html += '<div class="dead-stock-card"><div class="label">Current Month Complete</div><div class="value" id="cmc">0</div><div class="sub-value">Value: ₹<span id="cmc-val">0</span></div><button class="btn" style="background-color: #e74c3c; color: white;" onclick="downloadCSV()">Export</button></div>'
    html += '<div class="dead-stock-card"><div class="label">Last Month Dead Stock</div><div class="value" id="lm">0</div><div class="sub-value">Value: ₹<span id="lm-val">0</span></div><button class="btn" style="background-color: #3498db; color: white;" onclick="downloadCSV()">Export</button></div>'
    html += '<div class="dead-stock-card"><div class="label">Last to Last Month</div><div class="value" id="ltl">0</div><div class="sub-value">Value: ₹<span id="ltl-val">0</span></div><button class="btn" style="background-color: #27ae60; color: white;" onclick="downloadCSV()">Export</button></div>'
    html += '<div class="dead-stock-card"><div class="label">Total Dead Stock</div><div class="value" id="total">0</div><div class="sub-value">Value: ₹<span id="total-val">0</span></div><button class="btn" style="background-color: #2c3e50; color: white;" onclick="downloadCSV()">Export</button></div>'
    html += '<div class="dead-stock-card"><div class="label">Last Month Liquidation</div><div class="value" id="liq">0</div><div class="sub-value">Value: ₹<span id="liq-val">0</span></div><button class="btn" style="background-color: #f39c12; color: white;" onclick="downloadCSV()">Export</button></div>'
    html += '</div></div>'
    
    html += '<div class="gndp-box"><div class="gndp-value">' + formatted_gndp + '</div><div class="gndp-label">Total Stock at GNDP Value</div></div>'
    
    html += '<div class="filters-section">'
    html += '<div class="filter-grid">'
    html += '<div class="filter-group"><label>Spare Ageing</label><select id="movement" onchange="loadData()"><option value="">All</option>' + movement_opts + '</select></div>'
    html += '<div class="filter-group"><label>Part Category</label><select id="partcat" onchange="loadData()"><option value="">All</option>' + part_cat_opts + '</select></div>'
    html += '<div class="filter-group"><label>ABC Category</label><select id="abc" onchange="loadData()"><option value="">All</option>' + abc_opts + '</select></div>'
    html += '<div class="filter-group"><label>RIS</label><select id="ris" onchange="loadData()"><option value="">All</option>' + ris_opts + '</select></div>'
    html += '<div class="filter-group"><label>Location</label><select id="location" onchange="loadData()"><option value="">All</option>' + loc_opts + '</select></div>'
    html += '<div class="filter-group"><label>Part No.</label><input type="text" id="partno" placeholder="Enter Part No..." onkeyup="loadData()"></div>'
    html += '</div>'
    html += '<div class="filter-buttons"><button class="btn-apply" onclick="loadData()">Apply All</button><button class="btn-clear" onclick="clearAll()">Clear All</button></div>'
    html += '</div>'
    
    html += '<div class="data-table">'
    html += '<div class="card-header">Data Table (<span id="count">0</span> records)</div>'
    html += '<div style="overflow-x: auto;"><table class="table table-sm"><thead><tr>'
    html += '<th>Location</th><th>Part No</th><th>Description</th><th>Category</th><th>Stock Qty</th>'
    html += '<th>Stock GNDP</th><th>Last Issue</th><th>Last Purchase</th><th>Movement P</th><th>Dead Stock</th>'
    html += '</tr></thead><tbody id="tbody"></tbody></table></div>'
    html += '<nav style="padding: 15px;"><ul class="pagination" id="pages"></ul></nav>'
    html += '</div></div>'
    
    html += '<script src="https://cdnjs.cloudflare.com/ajax/libs/bootstrap/5.3.0/js/bootstrap.bundle.min.js"></script>'
    html += '<script>'
    html += 'document.getElementById("uploadArea").addEventListener("dragover", function(e) { e.preventDefault(); this.style.opacity = "0.8"; });'
    html += 'document.getElementById("uploadArea").addEventListener("dragleave", function() { this.style.opacity = "1"; });'
    html += 'document.getElementById("uploadArea").addEventListener("drop", function(e) { e.preventDefault(); if (e.dataTransfer.files.length > 0) uploadFile(e.dataTransfer.files[0]); });'
    html += 'document.getElementById("fileInput").addEventListener("change", function(e) { if (e.target.files.length > 0) uploadFile(e.target.files[0]); });'
    html += 'function uploadFile(file) { const fd = new FormData(); fd.append("file", file); fetch("/upload-excel", {method: "POST", body: fd}).then(r => r.json()).then(d => { alert(d.message); location.reload(); }); }'
    html += 'function loadData(p = 1) { const m = document.getElementById("movement").value; const pc = document.getElementById("partcat").value; const a = document.getElementById("abc").value; const r = document.getElementById("ris").value; const l = document.getElementById("location").value; const pn = document.getElementById("partno").value; const q = new URLSearchParams({page: p, movement_category: m, part_category: pc, abc_category: a, ris: r, location: l, part_number: pn}); fetch("/data?" + q).then(x => x.json()).then(d => { document.getElementById("count").innerText = d.total_records; let h = ""; d.data.forEach(row => { h += "<tr><td>" + (row.Location || "-") + "</td><td>" + (row["Part No"] || "-") + "</td><td>" + (row["Part Description"] || "-") + "</td><td>" + (row["Part Category"] || "-") + "</td><td>" + (row["Stock Qty"] || 0) + "</td><td>" + (row["Stock at GNDP"] || 0) + "</td><td>" + (row["Last Issue Date"] || "-") + "</td><td>" + (row["Last Purchase Date"] || "-") + "</td><td>" + (row["Movement Category P (2)"] || "-") + "</td><td>" + (row["Is Dead Stock"] ? "Yes" : "No") + "</td></tr>"; }); document.getElementById("tbody").innerHTML = h; let pg = ""; for (let i = 1; i <= d.total_pages; i++) { pg += "<li class=\"page-item " + (i === p ? "active" : "") + "\"><a class=\"page-link\" href=\"#\" onclick=\"loadData(" + i + "); return false;\">" + i + "</a></li>"; } document.getElementById("pages").innerHTML = pg; }); }'
    html += 'function clearAll() { document.getElementById("movement").value = ""; document.getElementById("partcat").value = ""; document.getElementById("abc").value = ""; document.getElementById("ris").value = ""; document.getElementById("location").value = ""; document.getElementById("partno").value = ""; loadData(); }'
    html += 'function downloadCSV() { const m = document.getElementById("movement").value; const pc = document.getElementById("partcat").value; const a = document.getElementById("abc").value; const r = document.getElementById("ris").value; const l = document.getElementById("location").value; const pn = document.getElementById("partno").value; const q = new URLSearchParams({movement_category: m, part_category: pc, abc_category: a, ris: r, location: l, part_number: pn}); window.location.href = "/download-csv?" + q; }'
    html += 'loadData();'
    html += '</script>'
    html += '</body></html>'
    
    return html

@app.get("/")
async def dashboard(request: Request):
    formatted_gndp = format_indian_number(total_gndp)
    html = build_html(last_reload_time, formatted_gndp, locations, part_categories, movement_categories, abc_categories, ris_values)
    return HTMLResponse(content=html)

def apply_filters(filtered_df, movement_category, part_category, location, abc_category, ris, part_number):
    if movement_category:
        filtered_df = filtered_df[filtered_df['Movement Category P (2)'] == movement_category]
    if part_category and part_category_col in filtered_df.columns:
        filtered_df = filtered_df[filtered_df[part_category_col] == part_category]
    if location and location_col in filtered_df.columns:
        filtered_df = filtered_df[filtered_df[location_col] == location]
    if abc_category and abc_col in filtered_df.columns:
        filtered_df = filtered_df[filtered_df[abc_col] == abc_category]
    if ris and ris_col in filtered_df.columns:
        filtered_df = filtered_df[filtered_df[ris_col] == ris]
    if part_number and part_no_col in filtered_df.columns:
        filtered_df = filtered_df[filtered_df[part_no_col].astype(str).str.contains(part_number, case=False, na=False)]
    return filtered_df

@app.get("/data")
async def get_data(page: int = 1, per_page: int = 50, movement_category: Optional[str] = None, part_category: Optional[str] = None, location: Optional[str] = None, abc_category: Optional[str] = None, ris: Optional[str] = None, part_number: Optional[str] = None):
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    total_records = len(filtered_df)
    total_pages = (total_records + per_page - 1) // per_page if total_records > 0 else 0
    
    start = (page - 1) * per_page
    end = start + per_page
    page_df = filtered_df.iloc[start:end].copy()
    page_df = clean_for_json(page_df)
    
    return {"data": page_df.to_dict('records'), "page": page, "per_page": per_page, "total_records": total_records, "total_pages": total_pages}

@app.get("/download-csv")
async def download_csv(movement_category: Optional[str] = None, part_category: Optional[str] = None, location: Optional[str] = None, abc_category: Optional[str] = None, ris: Optional[str] = None, part_number: Optional[str] = None):
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    current_datetime = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    filename = "Details_{}.csv".format(current_datetime)
    
    reports_dir = "./Reports"
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)
    
    output_path = os.path.join(reports_dir, filename)
    filtered_df.to_csv(output_path, index=False)
    return FileResponse(path=output_path, filename=filename, media_type='text/csv')

if __name__ == "__main__":
    hostname = socket.gethostname()
    try:
        local_ip = socket.gethostbyname(hostname)
    except:
        local_ip = "127.0.0.1"
    
    port = int(os.getenv("PORT", 8004))
    
    print("\n" + "=" * 70)
    print("Server ready!")
    print("Local: http://localhost:{}".format(port))
    print("Network: http://{}:{}".format(local_ip, port))
    print("=" * 70)
    
    uvicorn.run(app, host="0.0.0.0", port=port)
