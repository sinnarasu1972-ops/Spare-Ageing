import pandas as pd
from datetime import datetime, timedelta
import uvicorn
from fastapi import FastAPI, Request, UploadFile, File
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import os
import socket
from typing import Optional
import sys
import numpy as np

def clean_for_json(df):
    """Clean dataframe for JSON serialization by replacing NaN with None"""
    df = df.copy()
    df = df.replace([np.inf, -np.inf], None)
    df = df.where(pd.notna(df), None)
    return df

# GLOBAL VARIABLES FOR AUTO-UPDATE
excel_file_path = "./Spares Ageing Report.xlsx"
last_file_modified = None
last_reload_time = None

def get_file_modified_time(filepath):
    """Get file modification time"""
    try:
        return os.path.getmtime(filepath)
    except:
        return None

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
    print("Processing Excel file to CSV...")
    
    if not os.path.exists(input_file):
        print(f"ERROR: File not found: {input_file}")
        return None, 0, None
    
    try:
        df = pd.read_excel(input_file)
        print(f"Successfully loaded {len(df)} rows from Excel")
        print(f"Total columns: {len(df.columns)}")
    except Exception as e:
        print(f"ERROR reading Excel file: {e}")
        return None, 0, None
    
    today = datetime.now().date()
    current_month_start = today.replace(day=1)
    last_month_end = current_month_start - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    last_to_last_month_end = last_month_start - timedelta(days=1)
    last_to_last_month_start = last_to_last_month_end.replace(day=1)
    
    def categorize_aging(date_str):
        if pd.isna(date_str) or date_str == "-" or str(date_str).strip() == "":
            return "730 and above"
        try:
            date_obj = parse_date(date_str)
            if date_obj is None:
                return "730 and above"
            days_diff = (today - date_obj).days
            if days_diff < 0:
                return "0 to 90 days"
            elif days_diff <= 90:
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
    
    def categorize_by_month(date_str):
        if pd.isna(date_str) or date_str == "-" or str(date_str).strip() == "":
            return "730 and above"
        try:
            date_obj = parse_date(date_str)
            if date_obj is None:
                return "730 and above"
            if date_obj >= current_month_start:
                return "Current Month"
            elif last_month_start <= date_obj <= last_month_end:
                return "Last Month"
            elif last_to_last_month_start <= date_obj <= last_to_last_month_end:
                return "Last to Last Month"
            else:
                days_diff = (today - date_obj).days
                if days_diff < 0:
                    return "Current Month"
                elif days_diff <= 90:
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
    
    def identify_dead_stock(last_purchase_str, last_issue_str, stock_qty):
        try:
            stock = float(stock_qty) if not pd.isna(stock_qty) else 0
        except:
            stock = 0
        
        if stock <= 0:
            return False, "Not Dead Stock"
        
        if pd.isna(last_issue_str) or last_issue_str == "-" or str(last_issue_str).strip() == "":
            issue_days_diff = 999999
        else:
            try:
                issue_date_obj = parse_date(last_issue_str)
                if issue_date_obj is None:
                    issue_days_diff = 999999
                else:
                    issue_days_diff = (today - issue_date_obj).days
            except:
                issue_days_diff = 999999
        
        if issue_days_diff <= 365:
            return False, "Not Dead Stock"
        
        if pd.isna(last_purchase_str) or last_purchase_str == "-" or str(last_purchase_str).strip() == "":
            return True, "Earlier"
        
        try:
            purchase_date_obj = parse_date(last_purchase_str)
            if purchase_date_obj is None:
                return True, "Earlier"
            
            current_month_last_year_start = current_month_start.replace(year=current_month_start.year - 1)
            current_month_last_year_end = today.replace(year=today.year - 1)
            last_month_last_year_start = last_month_start.replace(year=last_month_start.year - 1)
            last_month_last_year_end = last_month_end.replace(year=last_month_end.year - 1)
            last_to_last_month_last_year_start = last_to_last_month_start.replace(year=last_to_last_month_start.year - 1)
            last_to_last_month_last_year_end = last_to_last_month_end.replace(year=last_to_last_month_end.year - 1)
            
            if current_month_last_year_start <= purchase_date_obj <= current_month_last_year_end:
                return True, "Current Month"
            elif last_month_last_year_start <= purchase_date_obj <= last_month_last_year_end:
                return True, "Last Month"
            elif last_to_last_month_last_year_start <= purchase_date_obj <= last_to_last_month_last_year_end:
                return True, "Last to Last Month"
            else:
                return True, "Earlier"
        except:
            return True, "Earlier"
    
    print("\nSearching for required columns...")
    last_issue_col = None
    for col in df.columns:
        if 'last' in str(col).lower() and 'issue' in str(col).lower() and 'date' in str(col).lower():
            last_issue_col = col
            print(f"Found Last Issue Date: {col}")
            break
    
    last_purchase_col = None
    for col in df.columns:
        if 'last' in str(col).lower() and 'purchase' in str(col).lower() and 'date' in str(col).lower():
            last_purchase_col = col
            print(f"Found Last Purchase Date: {col}")
            break
    
    if last_issue_col is None or last_purchase_col is None:
        print("ERROR: Could not find required columns")
        return None, 0, None
    
    location_col = None
    for col in df.columns:
        if 'location' in str(col).lower() and 'dealer' not in str(col).lower():
            location_col = col
            break
    
    part_category_col = None
    for col in df.columns:
        if 'part' in str(col).lower() and 'category' in str(col).lower():
            part_category_col = col
            break
    
    print("Creating aging categories...")
    df['Movement Category I (2)'] = df[last_issue_col].apply(categorize_aging)
    df['Movement Category P (2)'] = df[last_purchase_col].apply(categorize_aging)
    df['Purchase Month Category'] = df[last_purchase_col].apply(categorize_by_month)
    
    print("Creating Dead Stock categories...")
    stock_qty_col = None
    for col in df.columns:
        if 'stock' in str(col).lower() and 'qty' in str(col).lower():
            stock_qty_col = col
            break
    
    if stock_qty_col:
        dead_stock_results = df.apply(
            lambda row: identify_dead_stock(
                row[last_purchase_col],
                row[last_issue_col],
                row[stock_qty_col] if stock_qty_col in df.columns else 0
            ),
            axis=1
        )
        df['Is Dead Stock'] = dead_stock_results.apply(lambda x: x[0])
        df['Dead Stock Month'] = dead_stock_results.apply(lambda x: x[1])
    
    print(f"Total Dead Stock Parts: {df['Is Dead Stock'].sum()}")
    
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
        print(f"Processed data saved to CSV")
    except Exception as e:
        print(f"ERROR saving CSV: {e}")
        return None, 0, None
    
    return output_csv, total_gndp, gndp_column

app = FastAPI()

def format_indian_number(num):
    if num is None or pd.isna(num):
        return "0"
    try:
        actual_value = int(round(float(num) * 100000))
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
    print("\nERROR: Failed to process Excel file")
    sys.exit(1)

try:
    df = pd.read_csv(csv_file)
    print(f"Successfully loaded {len(df)} rows from processed CSV")
except Exception as e:
    print(f"ERROR loading processed CSV: {e}")
    sys.exit(1)

last_reload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
last_file_modified = get_file_modified_time(excel_file_path)

print("Pre-computing column names...")

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

print(f"Configuration Complete:")
print(f" - Total Records: {len(df):,}")
print(f" - Dead Stock Parts: {df['Is Dead Stock'].sum():,}")
print(f" - Locations: {len(locations)}")

if not os.path.exists("static"):
    os.makedirs("static")

# Create CSS file
css_content = """
body {
    background-color: #f8f9fa;
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
}

.card {
    border-radius: 8px;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    margin-bottom: 10px;
    border: none;
}

#uploadArea {
    position: relative;
    margin-bottom: 20px;
    padding: 20px;
    border: 2px dashed #ccc;
    border-radius: 8px;
    text-align: center;
    background-color: #f5f5f5;
    transition: all 0.3s ease;
}

#uploadArea:hover {
    background-color: #f0f0f0;
    border-color: #007bff;
    cursor: pointer;
}

.table {
    border-radius: 6px;
    font-size: 0.85rem;
}

.table thead th {
    background-color: #343a40;
    color: white;
    border: none;
    font-weight: 500;
    padding: 0.5rem;
}

.table tbody td {
    padding: 0.5rem;
    font-size: 0.85rem;
}

.form-select, .form-control {
    border-radius: 6px;
    border: 1px solid #ced4da;
    font-size: 0.85rem;
}

h1 {
    color: #343a40;
    font-weight: 700;
    font-size: 1.8rem;
}

.btn-sm {
    padding: 0.25rem 0.5rem;
    font-size: 0.85rem;
}

.display-6 {
    font-size: 1.5rem;
    font-weight: 700;
}
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
        print(f"\nNew Excel file uploaded: {file.filename}")
        
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
        
        print(f"File uploaded successfully!")
        return {"success": True, "message": f"Uploaded! {len(df):,} records loaded"}
    except Exception as e:
        print(f"Upload error: {e}")
        return {"success": False, "message": f"Error: {str(e)}"}

@app.get("/")
async def dashboard(request: Request):
    formatted_gndp = format_indian_number(total_gndp)
    
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Spare Parts Ageing Dashboard</title>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/bootstrap/5.3.0/css/bootstrap.min.css" rel="stylesheet">
    <link href="/static/style.css" rel="stylesheet">
</head>
<body>
    <div class="container-fluid p-3">
        <h1 class="mb-2">Unnati Motors Mahindra Spare Parts Ageing Dashboard</h1>
        <p id="lastUpdateTime" class="mb-3">Last Updated: {last_reload_time}</p>
        
        <div id="uploadArea" class="card border-primary mb-4">
            <div class="card-body text-center">
                <p class="mb-1"><strong>Drag & Drop Excel file here or click to browse</strong></p>
                <p class="mb-0 text-muted">Supports .xlsx and .xls files</p>
                <input type="file" id="fileInput" accept=".xlsx,.xls" style="display:none;">
            </div>
        </div>

        <div class="card bg-danger text-white mb-3">
            <div class="card-header">
                <h5 class="mb-0">Dead Stock Monitor</h5>
            </div>
            <div class="card-body">
                <div class="row g-2">
                    <div class="col-md-2">
                        <div class="card bg-light text-dark">
                            <div class="card-body">
                                <div>Current Month</div>
                                <div id="currentMonthCount" style="font-size: 2rem; font-weight: bold;">0</div>
                                <div id="currentMonthValue">Rs. 0</div>
                                <button class="btn btn-danger btn-sm w-100 mt-2" onclick="downloadDeadStock()">Export</button>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-2">
                        <div class="card bg-light text-dark">
                            <div class="card-body">
                                <div>Last Month</div>
                                <div id="lastMonthCount" style="font-size: 2rem; font-weight: bold;">0</div>
                                <div id="lastMonthValue">Rs. 0</div>
                                <button class="btn btn-primary btn-sm w-100 mt-2" onclick="downloadDeadStock('last_month')">Export</button>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-2">
                        <div class="card bg-light text-dark">
                            <div class="card-body">
                                <div>Total Dead Stock</div>
                                <div id="totalCount" style="font-size: 2rem; font-weight: bold;">0</div>
                                <div id="totalValue">Rs. 0</div>
                                <button class="btn btn-dark btn-sm w-100 mt-2" onclick="downloadDeadStock('all')">Export</button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <div class="card bg-primary text-white mb-3">
            <div class="card-body">
                <div style="font-size: 1.5rem; font-weight: bold;">{formatted_gndp}</div>
                <p class="mb-0">Total Stock at GNDP Value</p>
            </div>
        </div>

        <div class="row mb-3">
            <div class="col-md-2">
                <label>Spare Ageing</label>
                <select id="movementCategory" class="form-select" onchange="applyFilters()">
                    <option value="">All</option>
"""
    
    for cat in movement_categories:
        html_content += f'                    <option value="{cat}">{cat}</option>\n'
    
    html_content += f"""
                </select>
            </div>
            <div class="col-md-2">
                <label>Part Category</label>
                <select id="partCategory" class="form-select" onchange="applyFilters()">
                    <option value="">All</option>
"""
    
    for cat in part_categories:
        html_content += f'                    <option value="{cat}">{cat}</option>\n'
    
    html_content += f"""
                </select>
            </div>
            <div class="col-md-2">
                <label>Location</label>
                <select id="location" class="form-select" onchange="applyFilters()">
                    <option value="">All</option>
"""
    
    for loc in locations:
        html_content += f'                    <option value="{loc}">{loc}</option>\n'
    
    html_content += f"""
                </select>
            </div>
        </div>

        <div class="mb-3">
            <button class="btn btn-primary btn-sm" onclick="applyFilters()">Apply All</button>
            <button class="btn btn-secondary btn-sm" onclick="clearFilters()">Clear All</button>
        </div>

        <div class="card">
            <div class="card-header">
                <h6 class="mb-0">Data Table (<span id="recordCount">0</span> records)
                    <button class="btn btn-sm btn-success float-end" onclick="downloadData()">Download CSV</button>
                </h6>
            </div>
            <div class="card-body" style="overflow-x: auto;">
                <table class="table table-bordered table-sm" id="dataTable">
                    <thead>
                        <tr>
                            <th>Location</th>
                            <th>Part No</th>
                            <th>Part Description</th>
                            <th>Part Category</th>
                            <th>Stock Qty</th>
                            <th>Stock at GNDP</th>
                            <th>Last Issue Date</th>
                            <th>Last Purchase Date</th>
                            <th>Movement Category P</th>
                            <th>Dead Stock</th>
                        </tr>
                    </thead>
                    <tbody id="dataTableBody">
                    </tbody>
                </table>
                <nav>
                    <ul class="pagination" id="pagination"></ul>
                </nav>
            </div>
        </div>
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/bootstrap/5.3.0/js/bootstrap.bundle.min.js"></script>
    <script>
        const uploadArea = document.getElementById('uploadArea');
        const fileInput = document.getElementById('fileInput');

        uploadArea.addEventListener('click', () => fileInput.click());
        uploadArea.addEventListener('dragover', (e) => {
            e.preventDefault();
            uploadArea.style.borderColor = '#007bff';
        });
        uploadArea.addEventListener('dragleave', () => {
            uploadArea.style.borderColor = '#ccc';
        });
        uploadArea.addEventListener('drop', (e) => {
            e.preventDefault();
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                uploadFile(files[0]);
            }
        });

        fileInput.addEventListener('change', (e) => {
            if (e.target.files.length > 0) {
                uploadFile(e.target.files[0]);
            }
        });

        function uploadFile(file) {
            const formData = new FormData();
            formData.append('file', file);
            
            fetch('/upload-excel', {{method: 'POST', body: formData}})
                .then(r => r.json())
                .then(data => {{
                    alert(data.message);
                    location.reload();
                }})
                .catch(e => alert('Error: ' + e));
        }

        function getFilters() {
            return {{
                movement_category: document.getElementById('movementCategory').value,
                part_category: document.getElementById('partCategory').value,
                location: document.getElementById('location').value
            }};
        }

        function clearFilters() {
            document.getElementById('movementCategory').value = '';
            document.getElementById('partCategory').value = '';
            document.getElementById('location').value = '';
            applyFilters();
        }

        function applyFilters() {
            updateDataTable(1);
        }

        function updateDataTable(page) {
            const filters = getFilters();
            const params = new URLSearchParams({{...filters, page, per_page: 50}});
            
            fetch(`/data?${{params}}`)
                .then(r => r.json())
                .then(data => {{
                    document.getElementById('recordCount').textContent = data.total_records;
                    let html = '';
                    data.data.forEach(row => {{
                        html += `<tr>
                            <td>${{row.Location || '-'}}</td>
                            <td>${{row['Part No'] || '-'}}</td>
                            <td>${{row['Part Description'] || '-'}}</td>
                            <td>${{row['Part Category'] || '-'}}</td>
                            <td>${{row['Stock Qty'] || 0}}</td>
                            <td>${{formatNumber(row['Stock at GNDP'] || 0)}}</td>
                            <td>${{row['Last Issue Date'] || '-'}}</td>
                            <td>${{row['Last Purchase Date'] || '-'}}</td>
                            <td>${{row['Movement Category P (2)'] || '-'}}</td>
                            <td>${{row['Is Dead Stock'] ? 'Yes' : 'No'}}</td>
                        </tr>`;
                    }});
                    document.getElementById('dataTableBody').innerHTML = html;

                    let paginationHtml = '';
                    for (let i = 1; i <= data.total_pages; i++) {{
                        paginationHtml += `<li class="page-item ${{i === page ? 'active' : ''}}"><a class="page-link" href="#" onclick="updateDataTable(${{i}}); return false;">${{i}}</a></li>`;
                    }}
                    document.getElementById('pagination').innerHTML = paginationHtml;
                }});
        }

        function formatNumber(num) {{
            if (!num || isNaN(num)) return '0';
            return num.toLocaleString('en-IN', {{maximumFractionDigits: 2}});
        }}

        function downloadDeadStock(category) {{
            const filters = getFilters();
            const params = new URLSearchParams(filters);
            window.location.href = `/download-csv?${{params}}`;
        }}

        function downloadData() {{
            const filters = getFilters();
            const params = new URLSearchParams(filters);
            window.location.href = `/download-csv?${{params}}`;
        }}

        applyFilters();
    </script>
</body>
</html>
"""
    
    return HTMLResponse(content=html_content)

def apply_filters(filtered_df, movement_category, part_category, location):
    if movement_category:
        filtered_df = filtered_df[filtered_df['Movement Category P (2)'] == movement_category]
    if part_category and part_category_col in filtered_df.columns:
        filtered_df = filtered_df[filtered_df[part_category_col] == part_category]
    if location and location_col in filtered_df.columns:
        filtered_df = filtered_df[filtered_df[location_col] == location]
    return filtered_df

@app.get("/data")
async def get_data(
    page: int = 1,
    per_page: int = 50,
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None
):
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location)
    total_records = len(filtered_df)
    total_pages = (total_records + per_page - 1) // per_page if total_records > 0 else 0
    
    start = (page - 1) * per_page
    end = start + per_page
    page_df = filtered_df.iloc[start:end].copy()
    page_df = clean_for_json(page_df)
    page_data = page_df.to_dict('records')
    
    return {
        "data": page_data,
        "page": page,
        "per_page": per_page,
        "total_records": total_records,
        "total_pages": total_pages
    }

@app.get("/download-csv")
async def download_csv(
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None
):
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location)
    current_datetime = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    filename = f"Details_{current_datetime}.csv"
    
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
    print(f"Server ready!")
    print(f"Local: http://localhost:{port}")
    print(f"Network: http://{local_ip}:{port}")
    print("=" * 70)
    
    uvicorn.run(app, host="0.0.0.0", port=port)
