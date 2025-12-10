import schedule
import threading
import time
import os
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
import uvicorn
from fastapi import FastAPI, Request, UploadFile, File
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import sys
from typing import Optional
from jinja2 import Template

print("\n" + "="*80)
print("🚀 ULTRA-FAST DASHBOARD WITH AUTO-UPDATE")
print("="*80)

# ============================================================================
# GLOBAL VARIABLES FOR AUTO-UPDATE
# ============================================================================
last_reload_time = None
excel_file_path = "./Spares Ageing Report.xlsx"
last_file_modified = None

def get_file_modified_time(filepath):
    """Get file modification time"""
    try:
        return os.path.getmtime(filepath)
    except:
        return None

# ============================================================================
# STEP 1: CONVERT XLSX TO CSV (Only once at startup + reload time)
# ============================================================================

def process_excel_to_csv_fast():
    """Fast Excel to CSV conversion with minimal processing"""
    input_file = excel_file_path
    output_csv = "./Spares_Ageing_Processed.csv"
    
    print("\n📊 Converting XLSX → CSV (one-time operation)...")
    
    if not os.path.exists(input_file):
        print(f"❌ ERROR: File not found: {input_file}")
        return None
    
    try:
        # Read XLSX
        df = pd.read_excel(input_file)
        print(f"   ✓ Loaded {len(df):,} rows")
        
        today = datetime.now().date()
        current_month_start = today.replace(day=1)
        last_month_end = current_month_start - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        last_to_last_month_end = last_month_start - timedelta(days=1)
        last_to_last_month_start = last_to_last_month_end.replace(day=1)
        
        # Find columns ONCE
        last_issue_col = next((col for col in df.columns if 'last' in str(col).lower() and 'issue' in str(col).lower() and 'date' in str(col).lower()), None)
        last_purchase_col = next((col for col in df.columns if 'last' in str(col).lower() and 'purchase' in str(col).lower() and 'date' in str(col).lower()), None)
        last_issue_qty_col = next((col for col in df.columns if 'last' in str(col).lower() and 'issue' in str(col).lower() and 'qty' in str(col).lower()), None)
        stock_qty_col = next((col for col in df.columns if 'stock' in str(col).lower() and 'qty' in str(col).lower()), None)
        gndp_column = next((col for col in df.columns if 'stock' in str(col).lower() and 'gndp' in str(col).lower()), None)
        
        if not all([last_issue_col, last_purchase_col]):
            print("❌ ERROR: Required columns not found")
            return None
        
        # ====== FAST: Add calculated columns ======
        def parse_date_fast(date_str):
            if pd.isna(date_str) or date_str == "-":
                return None
            try:
                date_part = str(date_str)[:10].strip()
                for fmt in ['%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d', '%m/%d/%Y']:
                    try:
                        return datetime.strptime(date_part, fmt).date()
                    except:
                        continue
            except:
                pass
            return None
        
        # Vectorized aging category
        def categorize_aging(date_val):
            if pd.isna(date_val):
                return "730 and above"
            try:
                date_obj = parse_date_fast(date_val)
                if date_obj is None:
                    return "730 and above"
                days = (today - date_obj).days
                if days <= 90: return "0 to 90 days"
                elif days <= 180: return "91 to 180 days"
                elif days <= 365: return "181 to 365 days"
                elif days <= 730: return "366 to 730 days"
                else: return "730 and above"
            except:
                return "730 and above"
        
        print("   ✓ Computing aging categories...")
        df['Movement Category P (2)'] = df[last_purchase_col].apply(categorize_aging)
        df['Movement Category I (2)'] = df[last_issue_col].apply(categorize_aging)
        
        # Dead stock calculation - FAST
        print("   ✓ Computing dead stock...")
        def is_dead_stock_fast(row):
            try:
                stock = float(row[stock_qty_col]) if not pd.isna(row[stock_qty_col]) else 0
                if stock <= 0:
                    return False
                
                last_issue_str = row[last_issue_col]
                if pd.isna(last_issue_str) or last_issue_str == "-":
                    issue_date = None
                else:
                    issue_date = parse_date_fast(last_issue_str)
                
                if issue_date:
                    issue_days = (today - issue_date).days
                    if issue_days <= 365:
                        return False
                
                purchase_date = parse_date_fast(row[last_purchase_col])
                if purchase_date is None:
                    return True
                
                # One year ago
                one_year_ago = today.replace(year=today.year - 1)
                return purchase_date < one_year_ago
            except:
                return False
        
        df['Is Dead Stock'] = df.apply(is_dead_stock_fast, axis=1)
        print(f"   ✓ Dead Stock Parts: {df['Is Dead Stock'].sum():,}")
        
        # GNDP calculation
        if gndp_column:
            df[gndp_column] = pd.to_numeric(df[gndp_column], errors='coerce').fillna(0)
        
        # Save to CSV
        df.to_csv(output_csv, index=False)
        print(f"   ✓ Saved to: {output_csv}")
        
        return output_csv
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return None

# ============================================================================
# STEP 2: LOAD FROM CSV (SUPER FAST - Read once, keep in memory)
# ============================================================================

csv_file = process_excel_to_csv_fast()

if not csv_file:
    print("❌ Failed to create CSV")
    sys.exit(1)

print("\n⚡ Loading CSV into memory (FAST)...")
try:
    df = pd.read_csv(csv_file, dtype={'Stock Qty': 'float64'})
    print(f"   ✓ Loaded {len(df):,} rows in memory")
except Exception as e:
    print(f"❌ ERROR loading CSV: {e}")
    sys.exit(1)

# Find columns ONCE (cached)
location_col = next((col for col in df.columns if 'location' in str(col).lower() and 'dealer' not in str(col).lower()), None)
part_category_col = next((col for col in df.columns if 'part' in str(col).lower() and 'category' in str(col).lower()), None)
abc_col = next((col for col in df.columns if str(col).upper().strip() == 'ABC'), None)
ris_col = next((col for col in df.columns if str(col).upper().strip() == 'RIS'), None)
part_no_col = next((col for col in df.columns if 'part' in str(col).lower() and 'no' in str(col).lower() and 'description' not in str(col).lower()), None)
stock_qty_col = next((col for col in df.columns if 'stock' in str(col).lower() and 'qty' in str(col).lower()), None)
gndp_column = next((col for col in df.columns if 'stock' in str(col).lower() and 'gndp' in str(col).lower()), None)

# Pre-compute all unique values (CACHED)
print("\n🔧 Pre-computing filter options...")
locations = sorted([x for x in df[location_col].dropna().unique() if pd.notna(x)]) if location_col in df.columns else []
abc_categories = sorted([x for x in df[abc_col].dropna().unique() if pd.notna(x)]) if abc_col and abc_col in df.columns else []
ris_values = sorted([x for x in df[ris_col].dropna().unique() if pd.notna(x)]) if ris_col and ris_col in df.columns else []
part_categories = sorted([x for x in df[part_category_col].dropna().unique() if pd.notna(x)]) if part_category_col in df.columns else []

movement_order = ["0 to 90 days", "91 to 180 days", "181 to 365 days", "366 to 730 days", "730 and above"]
unique_movement = [x for x in df['Movement Category P (2)'].unique() if pd.notna(x)]
movement_categories = [cat for cat in movement_order if cat in unique_movement]

total_gndp = df[gndp_column].sum() if gndp_column in df.columns else 0

print(f"   ✓ Locations: {len(locations)}")
print(f"   ✓ Part Categories: {len(part_categories)}")
print(f"   ✓ Dead Stock: {df['Is Dead Stock'].sum():,}")
print(f"   ✓ Total GNDP: ₹{total_gndp:,.2f} Lac")

# Set last reload time and file modified time
last_reload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
last_file_modified = get_file_modified_time(excel_file_path)

# ============================================================================
# STEP 3: ULTRA-FAST FILTERING (Vectorized operations)
# ============================================================================

def apply_filters_vectorized_fast(filtered_df, movement_category, part_category, location, abc_category, ris, part_number):
    """Fast vectorized filtering"""
    if movement_category:
        cats = movement_category.split(',')
        filtered_df = filtered_df[filtered_df['Movement Category P (2)'].isin(cats)]
    
    if part_category and part_category_col in filtered_df.columns:
        cats = part_category.split(',')
        filtered_df = filtered_df[filtered_df[part_category_col].isin(cats)]
    
    if location and location_col in filtered_df.columns:
        locs = location.split(',')
        filtered_df = filtered_df[filtered_df[location_col].isin(locs)]
    
    if abc_category and abc_col in filtered_df.columns:
        cats = abc_category.split(',')
        filtered_df = filtered_df[filtered_df[abc_col].isin(cats)]
    
    if ris and ris_col in filtered_df.columns:
        ris_list = ris.split(',')
        filtered_df = filtered_df[filtered_df[ris_col].isin(ris_list)]
    
    if part_number and part_no_col in filtered_df.columns:
        filtered_df = filtered_df[filtered_df[part_no_col].astype(str).str.contains(part_number, case=False, na=False)]
    
    return filtered_df

# ============================================================================
# STEP 4: FAST NUMBER FORMATTING
# ============================================================================

def format_indian_number(num):
    """Fast Indian number formatting"""
    if num is None or pd.isna(num):
        return "0"
    try:
        actual_value = int(round(float(num) * 100000))
        num_str = str(abs(actual_value))
        if len(num_str) <= 3:
            return num_str
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

# ============================================================================
# STEP 5: FASTAPI APP
# ============================================================================

app = FastAPI()

if not os.path.exists("static"):
    os.makedirs("static")

# CSS
with open("static/style.css", "w") as f:
    f.write("""
    body { background-color: #f8f9fa; font-family: 'Segoe UI'; font-size: 13px; }
    .card { border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 8px; border: none; }
    .table { font-size: 11px; }
    .table thead th { background-color: #343a40; color: white; padding: 4px 6px; font-weight: 600; }
    .table tbody td { padding: 3px 4px; }
    .table tbody tr:nth-of-type(odd) { background-color: rgba(0,0,0,0.02); }
    .table tbody tr:hover { background-color: rgba(0,123,255,0.1); }
    h1 { color: #333; font-weight: 700; }
    .btn-sm { padding: 4px 8px; font-size: 11px; }
    .form-select { font-size: 12px; padding: 4px 6px; }
    .upload-area { border: 2px dashed #007bff; border-radius: 8px; padding: 20px; text-align: center; cursor: pointer; transition: all 0.3s; }
    .upload-area:hover { background-color: rgba(0,123,255,0.1); border-color: #0056b3; }
    .upload-area.dragover { background-color: rgba(0,123,255,0.2); border-color: #0056b3; }
    #lastUpdateTime { font-size: 11px; color: #666; }
    """)

HTML_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Spare Parts Ageing Dashboard</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="/static/style.css">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-select@1.14.0-beta2/dist/css/bootstrap-select.min.css">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.0/font/bootstrap-icons.css">
</head>
<body>
    <div class="container-fluid p-2">
        <div class="row mb-2">
            <div class="col-12">
                <h1 class="text-center mb-1">Spare Parts Ageing Dashboard</h1>
                <div class="text-center" id="lastUpdateTime" style="color: #666; font-size: 11px;">
                    Last Updated: {{ last_reload_time }}
                </div>
            </div>
        </div>

        <!-- FILE UPLOAD SECTION -->
        <div class="row mb-2">
            <div class="col-12">
                <div class="card">
                    <div class="card-body p-3">
                        <h6 class="mb-2">📤 Upload New Excel File (Auto-Updates Dashboard)</h6>
                        <div class="upload-area" id="uploadArea">
                            <p class="mb-2"><i class="bi bi-cloud-arrow-up"></i> Drag & Drop Excel file here or click to browse</p>
                            <input type="file" id="fileInput" accept=".xlsx,.xls" style="display:none;">
                            <small class="text-muted">Supports .xlsx and .xls files</small>
                        </div>
                        <div id="uploadStatus" class="mt-2"></div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="row mb-2">
            <div class="col-lg-2 col-md-4">
                <div class="card bg-primary text-white">
                    <div class="card-body">
                        <h6 class="mb-1">Total GNDP Value</h6>
                        <p class="mb-0" id="totalGndp" style="font-size: 16px; font-weight: bold;">{{ formatted_gndp }}</p>
                    </div>
                </div>
            </div>
            <div class="col-lg-2 col-md-4">
                <div class="card">
                    <div class="card-body">
                        <h6 class="mb-2">Ageing</h6>
                        <select class="form-select form-select-sm selectpicker" id="movementCategory" multiple title="Select...">
                            {% for cat in movement_categories %}<option>{{ cat }}</option>{% endfor %}
                        </select>
                    </div>
                </div>
            </div>
            <div class="col-lg-2 col-md-4">
                <div class="card">
                    <div class="card-body">
                        <h6 class="mb-2">Part Category</h6>
                        <select class="form-select form-select-sm selectpicker" id="partCategory" multiple title="Select...">
                            {% for pc in part_categories %}<option>{{ pc }}</option>{% endfor %}
                        </select>
                    </div>
                </div>
            </div>
            <div class="col-lg-2 col-md-4">
                <div class="card">
                    <div class="card-body">
                        <h6 class="mb-2">Location</h6>
                        <select class="form-select form-select-sm selectpicker" id="location" multiple title="Select...">
                            {% for loc in locations %}<option>{{ loc }}</option>{% endfor %}
                        </select>
                    </div>
                </div>
            </div>
            <div class="col-lg-2 col-md-4">
                <div class="card">
                    <div class="card-body">
                        <h6 class="mb-2">ABC</h6>
                        <select class="form-select form-select-sm selectpicker" id="abcCategory" multiple title="Select...">
                            {% for abc in abc_categories %}<option>{{ abc }}</option>{% endfor %}
                        </select>
                    </div>
                </div>
            </div>
            <div class="col-lg-2 col-md-4">
                <div class="card">
                    <div class="card-body">
                        <h6 class="mb-2">Part Number</h6>
                        <input type="text" class="form-control form-control-sm" id="partNumber" placeholder="Search...">
                    </div>
                </div>
            </div>
        </div>
        
        <div class="row mb-2">
            <div class="col-12">
                <button class="btn btn-primary btn-sm" id="applyFilters">Apply Filters</button>
                <button class="btn btn-warning btn-sm" id="clearFilters">Clear All</button>
                <button class="btn btn-success btn-sm" id="downloadCsv">Download CSV</button>
                <button class="btn btn-info btn-sm" id="reloadData">🔄 Reload Data (Manual)</button>
            </div>
        </div>
        
        <div class="row">
            <div class="col-12">
                <div class="card">
                    <div class="card-body p-2">
                        <h6 class="mb-2">Data (<span id="recordCount">0</span> records)</h6>
                        <div class="table-responsive">
                            <table class="table table-striped table-hover table-sm" id="dataTable">
                                <thead>
                                    <tr>
                                        <th>Zone</th>
                                        <th>Dealer</th>
                                        <th>Location</th>
                                        <th>Part No.</th>
                                        <th>Description</th>
                                        <th>ABC</th>
                                        <th>RIS</th>
                                        <th>Category</th>
                                        <th>Stock Qty</th>
                                        <th>GNDP Value</th>
                                        <th>Last Issue</th>
                                        <th>Last Purchase</th>
                                        <th>Movement P</th>
                                        <th>Dead Stock</th>
                                    </tr>
                                </thead>
                                <tbody></tbody>
                            </table>
                        </div>
                        <nav aria-label="Page navigation">
                            <ul class="pagination pagination-sm justify-content-center" id="pagination"></ul>
                        </nav>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap-select@1.14.0-beta2/dist/js/bootstrap-select.min.js"></script>
    <script>
        $(document).ready(function() {
            let currentPage = 1;
            const perPage = 100;
            let searchTimeout;
            
            $('.selectpicker').selectpicker();
            
            // ===== FILE UPLOAD =====
            const uploadArea = document.getElementById('uploadArea');
            const fileInput = document.getElementById('fileInput');
            const uploadStatus = document.getElementById('uploadStatus');
            
            uploadArea.addEventListener('click', () => fileInput.click());
            
            uploadArea.addEventListener('dragover', (e) => {
                e.preventDefault();
                uploadArea.classList.add('dragover');
            });
            
            uploadArea.addEventListener('dragleave', () => {
                uploadArea.classList.remove('dragover');
            });
            
            uploadArea.addEventListener('drop', (e) => {
                e.preventDefault();
                uploadArea.classList.remove('dragover');
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
                if (!file.name.match(/\\.(xlsx|xls)$/)) {
                    uploadStatus.innerHTML = '<div class="alert alert-danger alert-sm" role="alert">❌ Please upload .xlsx or .xls file</div>';
                    return;
                }
                
                uploadStatus.innerHTML = '<div class="alert alert-info alert-sm" role="alert">⏳ Uploading...</div>';
                
                const formData = new FormData();
                formData.append('file', file);
                
                $.ajax({
                    url: '/upload-excel',
                    type: 'POST',
                    data: formData,
                    processData: false,
                    contentType: false,
                    success: function(res) {
                        uploadStatus.innerHTML = '<div class="alert alert-success alert-sm" role="alert">✅ ' + res.message + '</div>';
                        setTimeout(() => {
                            location.reload();
                        }, 2000);
                    },
                    error: function(err) {
                        uploadStatus.innerHTML = '<div class="alert alert-danger alert-sm" role="alert">❌ Upload failed</div>';
                    }
                });
            }
            
            // ===== FILTERS =====
            function getFilters() {
                return {
                    movementCategory: $('#movementCategory').val()?.join(',') || '',
                    partCategory: $('#partCategory').val()?.join(',') || '',
                    location: $('#location').val()?.join(',') || '',
                    abcCategory: $('#abcCategory').val()?.join(',') || '',
                    ris: $('#ris').val()?.join(',') || '',
                    partNumber: $('#partNumber').val().trim()
                };
            }
            
            function buildQueryString(filters) {
                const params = new URLSearchParams();
                if (filters.movementCategory) params.append('movement_category', filters.movementCategory);
                if (filters.partCategory) params.append('part_category', filters.partCategory);
                if (filters.location) params.append('location', filters.location);
                if (filters.abcCategory) params.append('abc_category', filters.abcCategory);
                if (filters.partNumber) params.append('part_number', filters.partNumber);
                return params.toString();
            }
            
            function loadData() {
                const filters = getFilters();
                const qs = buildQueryString(filters);
                
                $.ajax({
                    url: `/data?page=${currentPage}&per_page=${perPage}&${qs}`,
                    method: 'GET',
                    success: function(res) {
                        $('#dataTable tbody').empty();
                        res.data.forEach(row => {
                            const deadStockBadge = row['Is Dead Stock'] ? '<span class="badge bg-danger">YES</span>' : '<span class="badge bg-success">NO</span>';
                            $('#dataTable tbody').append(`
                                <tr>
                                    <td>${row.Zone || ''}</td>
                                    <td>${row['Dealer Name'] || ''}</td>
                                    <td>${row.Location || ''}</td>
                                    <td>${row['Part No.'] || ''}</td>
                                    <td>${row['Part Description'] || ''}</td>
                                    <td>${row.ABC || ''}</td>
                                    <td>${row.RIS || ''}</td>
                                    <td>${row['Part Category'] || ''}</td>
                                    <td>${row['Stock Qty'] || ''}</td>
                                    <td>${row['Stock  at GNDP (Rs.) (In Lac)  '] || ''}</td>
                                    <td>${row['Last Issue Date'] || ''}</td>
                                    <td>${row['Last Purchase Date'] || ''}</td>
                                    <td>${row['Movement Category P (2)'] || ''}</td>
                                    <td>${deadStockBadge}</td>
                                </tr>
                            `);
                        });
                        
                        $('#recordCount').text(res.total_records);
                        updatePagination(res.total_pages);
                        updateGNDP();
                    },
                    error: function() {
                        console.log('Error loading data');
                    }
                });
            }
            
            function updateGNDP() {
                const filters = getFilters();
                const qs = buildQueryString(filters);
                $.ajax({
                    url: `/calculate-gndp?${qs}`,
                    method: 'GET',
                    success: function(res) {
                        $('#totalGndp').text('₹' + res.formatted_value);
                    }
                });
            }
            
            function updatePagination(totalPages) {
                $('#pagination').empty();
                if (totalPages === 0) return;
                
                $('#pagination').append(`<li class="page-item ${currentPage === 1 ? 'disabled' : ''}"><a class="page-link" href="#" data-page="${currentPage - 1}">Prev</a></li>`);
                
                for (let i = 1; i <= totalPages; i++) {
                    if (i === 1 || i === totalPages || (i >= currentPage - 1 && i <= currentPage + 1)) {
                        $('#pagination').append(`<li class="page-item ${i === currentPage ? 'active' : ''}"><a class="page-link" href="#" data-page="${i}">${i}</a></li>`);
                    }
                }
                
                $('#pagination').append(`<li class="page-item ${currentPage === totalPages ? 'disabled' : ''}"><a class="page-link" href="#" data-page="${currentPage + 1}">Next</a></li>`);
            }
            
            $('#applyFilters').click(() => { currentPage = 1; loadData(); });
            
            $('#clearFilters').click(() => {
                $('.selectpicker').selectpicker('deselectAll');
                $('#partNumber').val('');
                currentPage = 1;
                loadData();
            });
            
            $('#downloadCsv').click(() => {
                const filters = getFilters();
                const qs = buildQueryString(filters);
                window.location.href = `/download-csv?${qs}`;
            });
            
            $('#reloadData').click(() => {
                location.reload();
            });
            
            $(document).on('click', '.page-link', function(e) {
                e.preventDefault();
                const page = parseInt($(this).data('page'));
                if (page > 0) {
                    currentPage = page;
                    loadData();
                }
            });
            
            loadData();
        });
    </script>
</body>
</html>"""

# HTML endpoint
@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    tmpl = Template(HTML_TEMPLATE)
    return tmpl.render(
        formatted_gndp=format_indian_number(total_gndp),
        movement_categories=movement_categories,
        part_categories=part_categories,
        locations=locations,
        abc_categories=abc_categories,
        ris_values=ris_values,
        last_reload_time=last_reload_time
    )

# FILE UPLOAD ENDPOINT
@app.post("/upload-excel")
async def upload_excel(file: UploadFile = File(...)):
    """Handle Excel file upload"""
    global df, total_gndp, locations, abc_categories, ris_values, part_categories, movement_categories, last_reload_time, last_file_modified
    
    try:
        # Save uploaded file
        contents = await file.read()
        with open(excel_file_path, 'wb') as f:
            f.write(contents)
        
        print(f"\n📤 New Excel file uploaded: {file.filename}")
        
        # Reload data
        csv_file = process_excel_to_csv_fast()
        if csv_file is None:
            return {"success": False, "message": "Processing failed"}
        
        df = pd.read_csv(csv_file)
        
        total_gndp = df[gndp_column].sum() if gndp_column in df.columns else 0
        locations = sorted([x for x in df[location_col].dropna().unique() if pd.notna(x)]) if location_col in df.columns else []
        abc_categories = sorted([x for x in df[abc_col].dropna().unique() if pd.notna(x)]) if abc_col and abc_col in df.columns else []
        ris_values = sorted([x for x in df[ris_col].dropna().unique() if pd.notna(x)]) if ris_col and ris_col in df.columns else []
        part_categories = sorted([x for x in df[part_category_col].dropna().unique() if pd.notna(x)]) if part_category_col in df.columns else []
        
        last_reload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        last_file_modified = get_file_modified_time(excel_file_path)
        
        print(f"✅ File uploaded and processed successfully!")
        print(f"   Records: {len(df):,}")
        
        return {"success": True, "message": f"✅ Uploaded! {len(df):,} records loaded"}
        
    except Exception as e:
        print(f"❌ Upload error: {e}")
        return {"success": False, "message": f"Error: {str(e)}"}

# Data endpoint (FAST - vectorized)
@app.get("/data")
async def get_data(page: int = 1, per_page: int = 100, movement_category: Optional[str] = None, part_category: Optional[str] = None, location: Optional[str] = None, abc_category: Optional[str] = None, ris: Optional[str] = None, part_number: Optional[str] = None):
    filtered_df = apply_filters_vectorized_fast(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    
    total_records = len(filtered_df)
    total_pages = (total_records + per_page - 1) // per_page if total_records > 0 else 0
    start = (page - 1) * per_page
    end = start + per_page
    
    page_df = filtered_df.iloc[start:end]
    page_df = page_df.replace([np.inf, -np.inf], None).where(pd.notna(page_df), None)
    
    return {
        "data": page_df.to_dict('records'),
        "page": page,
        "per_page": per_page,
        "total_records": total_records,
        "total_pages": total_pages
    }

# GNDP calculation (FAST)
@app.get("/calculate-gndp")
async def calculate_gndp(movement_category: Optional[str] = None, part_category: Optional[str] = None, location: Optional[str] = None, abc_category: Optional[str] = None, ris: Optional[str] = None, part_number: Optional[str] = None):
    filtered_df = apply_filters_vectorized_fast(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    total = filtered_df[gndp_column].sum() if gndp_column in filtered_df.columns else 0
    return {
        "total_gndp": total,
        "formatted_value": format_indian_number(total)
    }

# Download CSV
@app.get("/download-csv")
async def download_csv(movement_category: Optional[str] = None, part_category: Optional[str] = None, location: Optional[str] = None, abc_category: Optional[str] = None, ris: Optional[str] = None, part_number: Optional[str] = None):
    filtered_df = apply_filters_vectorized_fast(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    current_datetime = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    filename = f"Details_{current_datetime}.csv"
    os.makedirs("./Reports", exist_ok=True)
    output_path = f"./Reports/{filename}"
    filtered_df.to_csv(output_path, index=False)
    return FileResponse(path=output_path, filename=filename, media_type='text/csv')

# AUTO-RELOAD CHECK (Every 5 minutes)
def check_file_changes():
    """Check if Excel file has been modified and reload if needed"""
    global df, total_gndp, locations, abc_categories, ris_values, part_categories, movement_categories, last_reload_time, last_file_modified
    
    try:
        current_modified = get_file_modified_time(excel_file_path)
        
        # If file modification time changed, reload
        if current_modified and last_file_modified and current_modified > last_file_modified:
            print(f"\n{'='*80}")
            print(f"⏰ AUTO-DETECT: Excel file modified, reloading...")
            print(f"{'='*80}")
            
            csv_file = process_excel_to_csv_fast()
            if csv_file is None:
                print("❌ Reload failed")
                return
            
            df = pd.read_csv(csv_file)
            
            total_gndp = df[gndp_column].sum() if gndp_column in df.columns else 0
            locations = sorted([x for x in df[location_col].dropna().unique() if pd.notna(x)]) if location_col in df.columns else []
            abc_categories = sorted([x for x in df[abc_col].dropna().unique() if pd.notna(x)]) if abc_col and abc_col in df.columns else []
            ris_values = sorted([x for x in df[ris_col].dropna().unique() if pd.notna(x)]) if ris_col and ris_col in df.columns else []
            part_categories = sorted([x for x in df[part_category_col].dropna().unique() if pd.notna(x)]) if part_category_col in df.columns else []
            
            last_reload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            last_file_modified = current_modified
            
            print(f"✅ Auto-reload complete!")
            print(f"   Records: {len(df):,}")
            print(f"{'='*80}\n")
    except Exception as e:
        print(f"⚠️  Auto-detect error: {e}")

# Background scheduler thread
def scheduler_loop():
    print("\n🎯 Auto-detect scheduler running (checks every 5 minutes)\n")
    while True:
        check_file_changes()
        time.sleep(300)  # Check every 5 minutes

scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
scheduler_thread.start()

print("\n" + "="*80)
print("✅ DASHBOARD READY - WITH AUTO-UPDATE!")
print("="*80)
print(f"📍 Features:")
print(f"   ✓ Upload Excel files directly (drag & drop)")
print(f"   ✓ Auto-detects file changes every 5 minutes")
print(f"   ✓ Manual reload button available")
print(f"   ✓ Super fast CSV-based processing")
print("="*80 + "\n")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=10000)
