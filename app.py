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
import threading
import time

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

# OPTIMIZATION: Parse date once at startup
def parse_date_vectorized(date_series):
    """Vectorized date parsing - much faster than row-by-row parsing"""
    result = pd.Series([None] * len(date_series), index=date_series.index)
    date_formats = ['%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d', '%m/%d/%Y', '%d.%m.%Y']
    for fmt in date_formats:
        mask = result.isna()
        try:
            result[mask] = pd.to_datetime(date_series[mask].astype(str).str[:10], format=fmt, errors='coerce')
        except:
            continue
    return result

# OPTIMIZATION: Single date parser function
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

# Part 1: Process Excel to CSV with enhanced logic
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
        print("Make sure the file is not open in Excel and openpyxl is installed:")
        print("pip install openpyxl")
        return None, 0, None
    
    today = datetime.now().date()
    current_month_start = today.replace(day=1)
    last_month_end = current_month_start - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    last_to_last_month_end = last_month_start - timedelta(days=1)
    last_to_last_month_start = last_to_last_month_end.replace(day=1)
    
    print(f"\nDate Calculations:")
    print(f"Today: {today}")
    print(f"Current Month Start: {current_month_start}")
    print(f"Last Month: {last_month_start} to {last_month_end}")
    print(f"Last to Last Month: {last_to_last_month_start} to {last_to_last_month_end}")
    
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
    
    def identify_dead_stock(last_purchase_str, last_issue_str, last_issue_qty, stock_qty):
        """Dead Stock = No issue in last 365 days AND Stock Qty > 0"""
        try:
            stock = float(stock_qty) if not pd.isna(stock_qty) else 0
        except:
            stock = 0
        
        if stock <= 0:
            return False, "Not Dead Stock (No Stock)"
        
        if pd.isna(last_issue_str) or last_issue_str == "-" or str(last_issue_str).strip() == "":
            issue_date_obj = None
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
            return False, "Not Dead Stock (Recent Issue)"
        
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
            elif purchase_date_obj < current_month_last_year_start:
                return True, "Earlier"
            else:
                return True, "Earlier"
        except:
            return True, "Earlier"
    
    print("\nSearching for required columns...")
    last_issue_col = None
    for col in df.columns:
        if 'last' in str(col).lower() and 'issue' in str(col).lower() and 'date' in str(col).lower():
            last_issue_col = col
            print(f"✓ Found Last Issue Date: '{col}'")
            break
    
    last_purchase_col = None
    for col in df.columns:
        if 'last' in str(col).lower() and 'purchase' in str(col).lower() and 'date' in str(col).lower():
            last_purchase_col = col
            print(f"✓ Found Last Purchase Date: '{col}'")
            break
    
    last_issue_qty_col = None
    for col in df.columns:
        if 'last' in str(col).lower() and 'issue' in str(col).lower() and 'qty' in str(col).lower():
            last_issue_qty_col = col
            print(f"✓ Found Last Issue Qty: '{col}'")
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
    
    print("\nCreating aging categories...")
    df['Movement Category I (2)'] = df[last_issue_col].apply(categorize_aging)
    df['Movement Category P (2)'] = df[last_purchase_col].apply(categorize_aging)
    df['Purchase Month Category'] = df[last_purchase_col].apply(categorize_by_month)
    
    print("\nCreating Dead Stock categories for ALL part categories...")
    stock_qty_col = None
    for col in df.columns:
        if 'stock' in str(col).lower() and 'qty' in str(col).lower():
            stock_qty_col = col
            break
    
    dead_stock_results = df.apply(
        lambda row: identify_dead_stock(
            row[last_purchase_col],
            row[last_issue_col],
            row[last_issue_qty_col] if last_issue_qty_col in df.columns else 0,
            row[stock_qty_col] if stock_qty_col and stock_qty_col in df.columns else 0
        ),
        axis=1
    )
    df['Is Dead Stock'] = dead_stock_results.apply(lambda x: x[0])
    df['Dead Stock Month'] = dead_stock_results.apply(lambda x: x[1])
    
    print(f"✓ Dead Stock calculation applied to ALL part categories")
    print(f"\nTotal Dead Stock Parts (All Categories): {df['Is Dead Stock'].sum()}")
    
    gndp_column = None
    for col in df.columns:
        if 'stock' in str(col).lower() and 'gndp' in str(col).lower():
            gndp_column = col
            break
    
    if gndp_column:
        df[gndp_column] = pd.to_numeric(df[gndp_column], errors='coerce').fillna(0)
        total_gndp = df[gndp_column].sum()
        print(f"✓ Total Stock at GNDP Value: {total_gndp:.2f} Lac")
    else:
        total_gndp = 0
    
    try:
        df.to_csv(output_csv, index=False)
        print(f"\n✓ Processed data saved to CSV: {output_csv}")
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
print("STARTING SPARE PARTS AGEING DASHBOARD (WITH AUTO-UPDATE)")
print("=" * 70)

csv_file, total_gndp, gndp_column = process_excel_to_csv()

if csv_file is None:
    print("\n" + "=" * 70)
    print("ERROR: Failed to process Excel file")
    print("=" * 70)
    sys.exit(1)

try:
    df = pd.read_csv(csv_file)
    print(f"\n✓ Successfully loaded {len(df)} rows from processed CSV")
except Exception as e:
    print(f"\nERROR loading processed CSV: {e}")
    sys.exit(1)

# Set initial reload time and file modified time
last_reload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
last_file_modified = get_file_modified_time(excel_file_path)

# OPTIMIZATION: Find all columns ONCE at startup
print("\n🚀 OPTIMIZATION: Pre-computing column names at startup...")
print(" (This happens only once, not on every request)")

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

last_issue_qty_col = None
for col in df.columns:
    if 'last' in str(col).lower() and 'issue' in str(col).lower() and 'qty' in str(col).lower():
        last_issue_qty_col = col
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

# OPTIMIZATION: Create lookup sets for faster filtering
print("✓ Pre-computing unique values for filters...")

locations = sorted([x for x in df[location_col].unique().tolist() if pd.notna(x)]) if location_col in df.columns else []
locations_set = set(locations)

abc_categories = sorted([x for x in df[abc_col].unique().tolist() if pd.notna(x)]) if abc_col and abc_col in df.columns else []
abc_categories_set = set(abc_categories)

ris_values = sorted([x for x in df[ris_col].unique().tolist() if pd.notna(x)]) if ris_col and ris_col in df.columns else []
ris_values_set = set(ris_values)

part_categories = sorted([x for x in df[part_category_col].unique().tolist() if pd.notna(x)]) if part_category_col in df.columns else []
part_categories_set = set(part_categories)

movement_order = ["0 to 90 days", "91 to 180 days", "181 to 365 days", "366 to 730 days", "730 and above"]
unique_movement = [x for x in df['Movement Category P (2)'].unique().tolist() if pd.notna(x)]
movement_categories = [cat for cat in movement_order if cat in unique_movement]
movement_categories_set = set(movement_categories)

print(f"\n✓ Configuration Complete:")
print(f" - Total Records: {len(df):,}")
print(f" - Dead Stock Parts: {df['Is Dead Stock'].sum():,}")
print(f" - Locations: {len(locations)}")
print(f" - Part Categories: {len(part_categories)}")

if not os.path.exists("static"):
    os.makedirs("static")

with open("static/style.css", "w") as f:
    f.write("""
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

.card.bg-primary {
    background: linear-gradient(45deg, #007bff, #0056b3) !important;
}

.card.bg-danger {
    background: linear-gradient(45deg, #dc3545, #a71d2a) !important;
}

.card-body {
    padding: 0.5rem;
}

.card-title {
    font-weight: 600;
    font-size: 0.65rem;
    margin-bottom: 0.25rem;
}

/* FIXED: Upload area should scroll with page, not be sticky */
#uploadArea {
    transition: all 0.3s ease;
    position: relative !important;
    margin-bottom: 20px;
    padding: 20px;
    border: 2px dashed #ccc;
    border-radius: 8px;
    text-align: center;
    background-color: #f5f5f5;
}

#uploadArea:hover {
    background-color: #f0f0f0 !important;
    border-color: #007bff !important;
    cursor: pointer;
}

.table {
    border-radius: 6px;
    overflow: hidden;
    font-size: 0.42rem;
}

.table thead th {
    background-color: #343a40;
    color: white;
    border: none;
    font-weight: 500;
    position: sticky;
    top: 0;
    padding: 0.08rem 0.08rem;
    font-size: 0.42rem;
}

.table tbody td {
    padding: 0.08rem 0.08rem;
    font-size: 0.42rem;
}

.table tbody tr:nth-of-type(odd) {
    background-color: rgba(0, 0, 0, 0.02);
}

.table tbody tr:hover {
    background-color: rgba(0, 123, 255, 0.1);
}

.table-bordered th, .table-bordered td {
    border: 1px solid #dee2e6;
}

#summaryTable {
    font-size: 0.42rem;
}

#summaryTable th {
    font-size: 0.42rem;
    padding: 0.08rem 0.08rem;
    text-align: center;
}

#summaryTable td {
    padding: 0.08rem 0.08rem;
    font-size: 0.42rem;
}

#summaryTable tfoot tr {
    background-color: #e9ecef;
    font-weight: bold;
}

#partCategoryTable {
    font-size: 0.42rem;
}

#partCategoryTable th {
    font-size: 0.42rem;
    padding: 0.08rem 0.08rem;
    text-align: center;
    font-weight: 600;
    background-color: #343a40;
    color: white;
}

#partCategoryTable td {
    padding: 0.08rem 0.08rem;
    text-align: right;
    font-size: 0.42rem;
}

#partCategoryTable td:first-child {
    text-align: left;
    font-weight: 600;
}

#partCategoryTable tbody tr:nth-child(odd) {
    background-color: rgba(0, 0, 0, 0.02);
}

#partCategoryTable tbody tr:hover {
    background-color: rgba(102, 126, 234, 0.08);
}

#partCategoryTable tfoot {
    display: table-footer-group !important;
    background-color: #ff9800 !important;
    visibility: visible !important;
}

#partCategoryTable tfoot tr {
    background-color: #ff9800 !important;
    display: table-row !important;
    visibility: visible !important;
}

#partCategoryTable tfoot td {
    background-color: #ff9800 !important;
    color: white !important;
    font-weight: bold !important;
    padding: 0.10rem 0.08rem;
    font-size: 0.42rem;
    border: 1px solid #ff5500;
    text-align: right;
    display: table-cell !important;
    visibility: visible !important;
}

#partCategoryTable tfoot td:first-child {
    text-align: left;
}

.form-select, .form-control {
    border-radius: 6px;
    border: 1px solid #ced4da;
    font-size: 0.85rem;
}

.form-select {
    padding: 0.25rem 1.75rem 0.25rem 0.5rem;
}

.form-select:focus, .form-control:focus {
    border-color: #007bff;
    box-shadow: 0 0 0 0.2rem rgba(0, 123, 255, 0.25);
}

h1 {
    color: #343a40;
    font-weight: 700;
    font-size: 1.8rem;
}

.page-link {
    color: #007bff;
    font-size: 0.85rem;
    padding: 0.25rem 0.5rem;
}

.page-item.active .page-link {
    background-color: #007bff;
    border-color: #007bff;
}

.btn-sm {
    padding: 0.25rem 0.5rem;
    font-size: 0.85rem;
}

.display-6 {
    font-size: 1.5rem;
    font-weight: 700;
}

.bootstrap-select .dropdown-menu {
    max-height: 300px;
    overflow-y: auto;
}

.badge {
    font-size: 0.75rem;
}

#lastUpdateTime {
    font-size: 0.9rem;
    color: #666;
}
""")

# HTML_TEMPLATE with proper upload section positioning
HTML_TEMPLATE = r"""
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
        <p id="lastUpdateTime" class="mb-3">Last Updated: {{ last_reload_time }}</p>
        
        <!-- Upload Section - Now scrolls with page -->
        <div id="uploadArea" class="card border-primary mb-4">
            <div class="card-body text-center">
                <div class="mb-2">☁️</div>
                <p class="mb-1"><strong>Drag & Drop Excel file here or click to browse</strong></p>
                <p class="mb-0 text-muted">Supports .xlsx and .xls files</p>
                <input type="file" id="fileInput" accept=".xlsx,.xls" style="display:none;">
            </div>
        </div>

        <!-- Dead Stock Monitor -->
        <div class="row">
            <div class="col-12">
                <div class="card bg-danger text-white">
                    <div class="card-header">
                        <h5 class="mb-0">😰 Dead Stock Monitor</h5>
                    </div>
                    <div class="card-body">
                        <div class="row g-2">
                            <!-- Current Month As on Date -->
                            <div class="col-md-2 col-sm-4 col-6">
                                <div class="card bg-light text-dark">
                                    <div class="card-body p-2">
                                        <div class="card-title">Current Month Complete</div>
                                        <div class="card-title">As on Date</div>
                                        <div class="display-6" id="currentMonthCompleteCount">0</div>
                                        <div style="font-size: 0.75rem;">Value: ₹<span id="currentMonthCompleteValue">0</span></div>
                                        <div class="card-title mt-2">Complete Month</div>
                                        <div class="display-6" id="currentMonthAsOnCount">0</div>
                                        <div style="font-size: 0.75rem;">Value: ₹<span id="currentMonthAsOnValue">0</span></div>
                                        <button class="btn btn-danger btn-sm w-100 mt-2" onclick="downloadDeadStock('current_month_complete')">Export</button>
                                    </div>
                                </div>
                            </div>

                            <!-- Last Month Dead Stock -->
                            <div class="col-md-2 col-sm-4 col-6">
                                <div class="card bg-light text-dark">
                                    <div class="card-body p-2">
                                        <div class="card-title">Last Month Dead Stock</div>
                                        <div class="display-6" id="lastMonthDeadStockCount">0</div>
                                        <div style="font-size: 0.75rem;">Value: ₹<span id="lastMonthDeadStockValue">0</span></div>
                                        <button class="btn btn-primary btn-sm w-100 mt-3" onclick="downloadDeadStock('last_month')">Export</button>
                                    </div>
                                </div>
                            </div>

                            <!-- Last to Last Month -->
                            <div class="col-md-2 col-sm-4 col-6">
                                <div class="card bg-light text-dark">
                                    <div class="card-body p-2">
                                        <div class="card-title">Last to Last Month</div>
                                        <div class="display-6" id="lastToLastMonthCount">0</div>
                                        <div style="font-size: 0.75rem;">Value: ₹<span id="lastToLastMonthValue">0</span></div>
                                        <button class="btn btn-success btn-sm w-100 mt-3" onclick="downloadDeadStock('last_to_last_month')">Export</button>
                                    </div>
                                </div>
                            </div>

                            <!-- Total Dead Stock -->
                            <div class="col-md-2 col-sm-4 col-6">
                                <div class="card bg-light text-dark">
                                    <div class="card-body p-2">
                                        <div class="card-title">Total Dead Stock</div>
                                        <div class="display-6" id="totalDeadStockCount">0</div>
                                        <div style="font-size: 0.75rem;">Value: ₹<span id="totalDeadStockValue">0</span></div>
                                        <button class="btn btn-dark btn-sm w-100 mt-3" onclick="downloadDeadStock('all')">Export</button>
                                    </div>
                                </div>
                            </div>

                            <!-- Last Month Liquidation -->
                            <div class="col-md-2 col-sm-4 col-6">
                                <div class="card bg-light text-dark">
                                    <div class="card-body p-2">
                                        <div class="card-title">🟠 Last Month Liquidation</div>
                                        <div class="display-6" id="lastMonthLiquidationCount">0</div>
                                        <div style="font-size: 0.75rem;">Value: ₹<span id="lastMonthLiquidationValue">0</span></div>
                                        <button class="btn btn-warning btn-sm w-100 mt-2" onclick="downloadLiquidation()">Export</button>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Total Stock at GNDP Value -->
        <div class="card bg-primary text-white mt-3">
            <div class="card-body">
                <div class="display-6">{{ formatted_gndp }}</div>
                <p class="mb-0">Total Stock at GNDP Value</p>
            </div>
        </div>

        <!-- Filters -->
        <div class="row mt-3">
            <div class="col-md-2">
                <label>Spare Ageing</label>
                <select id="movementCategory" class="form-select" onchange="applyFilters()">
                    <option value="">Select categories...</option>
                    {% for category in movement_categories %}
                    <option value="{{ category }}">{{ category }}</option>
                    {% endfor %}
                </select>
            </div>
            <div class="col-md-2">
                <label>Part Category</label>
                <select id="partCategory" class="form-select" onchange="applyFilters()">
                    <option value="">Select Part Category</option>
                    {% for pc in part_categories %}
                    <option value="{{ pc }}">{{ pc }}</option>
                    {% endfor %}
                </select>
            </div>
            <div class="col-md-2">
                <label>ABC Category</label>
                <select id="abcCategory" class="form-select" onchange="applyFilters()">
                    <option value="">Select ABC...</option>
                    {% for abc in abc_categories %}
                    <option value="{{ abc }}">{{ abc }}</option>
                    {% endfor %}
                </select>
            </div>
            <div class="col-md-2">
                <label>RIS</label>
                <select id="ris" class="form-select" onchange="applyFilters()">
                    <option value="">Select RIS...</option>
                    {% for ris_val in ris_values %}
                    <option value="{{ ris_val }}">{{ ris_val }}</option>
                    {% endfor %}
                </select>
            </div>
            <div class="col-md-2">
                <label>Location</label>
                <select id="location" class="form-select" onchange="applyFilters()">
                    <option value="">Select locations...</option>
                    {% for loc in locations %}
                    <option value="{{ loc }}">{{ loc }}</option>
                    {% endfor %}
                </select>
            </div>
            <div class="col-md-2">
                <label>Part No.</label>
                <input type="text" id="partNumber" class="form-control" placeholder="Enter Part Number..." onkeyup="applyFilters()">
            </div>
        </div>

        <!-- Active Filters -->
        <div class="mt-3">
            <button class="btn btn-primary btn-sm" onclick="applyFilters()">Apply All</button>
            <button class="btn btn-secondary btn-sm" onclick="clearFilters()">Clear All</button>
        </div>

        <!-- Summary Table -->
        <div class="card mt-3">
            <div class="card-header">
                <h6 class="mb-0">Location Wise Spare Aging Summary
                    <button class="btn btn-sm btn-success float-end" onclick="downloadSummary()">Download Summary</button>
                </h6>
            </div>
            <div class="card-body" style="overflow-x: auto;">
                <table id="summaryTable" class="table table-bordered table-sm">
                    <thead>
                        <tr>
                            <th>Location</th>
                            <th colspan="2">0 to 90 Days</th>
                            <th colspan="2">91 to 180 Days</th>
                            <th colspan="2">181 to 365 Days</th>
                            <th colspan="2">366 to 730 Days</th>
                            <th colspan="2">730+ Days</th>
                            <th colspan="2">Last to Last Month</th>
                            <th colspan="2">Last Month</th>
                            <th colspan="2">Current Month</th>
                            <th colspan="2">Column Total</th>
                        </tr>
                        <tr>
                            <th>Location</th>
                            <th>Count</th><th>Value (Rs.)</th>
                            <th>Count</th><th>Value (Rs.)</th>
                            <th>Count</th><th>Value (Rs.)</th>
                            <th>Count</th><th>Value (Rs.)</th>
                            <th>Count</th><th>Value (Rs.)</th>
                            <th>Count</th><th>Value (Rs.)</th>
                            <th>Count</th><th>Value (Rs.)</th>
                            <th>Count</th><th>Value (Rs.)</th>
                            <th>Count</th><th>Value (Rs.)</th>
                        </tr>
                    </thead>
                    <tbody id="summaryTableBody">
                    </tbody>
                    <tfoot id="summaryTableFooter">
                    </tfoot>
                </table>
            </div>
        </div>

        <!-- Part Category Wise Table -->
        <div class="card mt-3">
            <div class="card-header">
                <h6 class="mb-0">Location Wise Part Category
                    <button class="btn btn-sm btn-success float-end" onclick="downloadPartCategory()">Download Part Category</button>
                </h6>
            </div>
            <div class="card-body" style="overflow-x: auto;">
                <table id="partCategoryTable" class="table table-bordered table-sm">
                    <thead id="partCategoryHead"></thead>
                    <tbody id="partCategoryBody"></tbody>
                    <tfoot id="partCategoryFoot"></tfoot>
                </table>
            </div>
        </div>

        <!-- Data Table -->
        <div class="card mt-3">
            <div class="card-header">
                <h6 class="mb-0">Data Table (<span id="recordCount">0</span> Part Line Item)
                    <button class="btn btn-sm btn-success float-end" onclick="downloadData()">Download Details</button>
                </h6>
            </div>
            <div class="card-body" style="overflow-x: auto;">
                <table id="dataTable" class="table table-bordered table-sm">
                    <thead>
                        <tr>
                            <th>Zone AO</th><th>Dealer Name</th><th>Location</th><th>Part No.</th>
                            <th>Part Description</th><th>Division</th><th>ABC</th><th>HML</th>
                            <th>RIS</th><th>Part Category</th><th>Part Division</th><th>STOR_CD</th>
                            <th>STOR_DESC</th><th>Dealer Ship to Code</th><th>GNDP</th>
                            <th>Stock Qty</th><th>Stock at GNDP (Rs.)</th><th>Stock at MDW (Rs.)</th>
                            <th>Cumm. Issue Qty</th><th>Last Issue Date</th><th>Last Issue Qty</th>
                            <th>Last Purchase Date</th><th>Last Purchase Qty</th><th>Movement Category I</th>
                            <th>Movement Category P</th><th>Dead Stock</th><th>Dead Stock Month</th>
                        </tr>
                    </thead>
                    <tbody id="dataTableBody"></tbody>
                </table>
                <nav>
                    <ul class="pagination" id="pagination"></ul>
                </nav>
            </div>
        </div>
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/bootstrap/5.3.0/js/bootstrap.bundle.min.js"></script>
    <script>
        // Upload functionality
        const uploadArea = document.getElementById('uploadArea');
        const fileInput = document.getElementById('fileInput');

        uploadArea.addEventListener('click', () => fileInput.click());
        uploadArea.addEventListener('dragover', (e) => {
            e.preventDefault();
            uploadArea.style.borderColor = '#007bff';
            uploadArea.style.backgroundColor = '#e7f3ff';
        });
        uploadArea.addEventListener('dragleave', () => {
            uploadArea.style.borderColor = '#ccc';
            uploadArea.style.backgroundColor = '#f5f5f5';
        });
        uploadArea.addEventListener('drop', (e) => {
            e.preventDefault();
            uploadArea.style.borderColor = '#ccc';
            uploadArea.style.backgroundColor = '#f5f5f5';
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
            
            fetch('/upload-excel', {
                method: 'POST',
                body: formData
            })
            .then(response => response.json())
            .then(data => {
                alert(data.message);
                location.reload();
            })
            .catch(error => {
                alert('Error uploading file: ' + error);
            });
        }

        // Initialize
        function init() {
            applyFilters();
        }

        function getFilters() {
            return {
                movement_category: document.getElementById('movementCategory').value,
                part_category: document.getElementById('partCategory').value,
                location: document.getElementById('location').value,
                abc_category: document.getElementById('abcCategory').value,
                ris: document.getElementById('ris').value,
                part_number: document.getElementById('partNumber').value
            };
        }

        function clearFilters() {
            document.getElementById('movementCategory').value = '';
            document.getElementById('partCategory').value = '';
            document.getElementById('location').value = '';
            document.getElementById('abcCategory').value = '';
            document.getElementById('ris').value = '';
            document.getElementById('partNumber').value = '';
            applyFilters();
        }

        function applyFilters() {
            updateDeadStockSummary();
            updateSummaryTable();
            updatePartCategoryTable();
            updateDataTable(1);
        }

        function updateDeadStockSummary() {
            const filters = getFilters();
            const params = new URLSearchParams(filters);
            
            fetch(`/dead-stock-summary?${params}`)
                .then(r => r.json())
                .then(data => {
                    document.getElementById('currentMonthAsOnCount').textContent = data.current_month_as_on_date.count;
                    document.getElementById('currentMonthAsOnValue').textContent = formatNumber(data.current_month_as_on_date.value);
                    document.getElementById('currentMonthCompleteCount').textContent = data.current_month_complete.count;
                    document.getElementById('currentMonthCompleteValue').textContent = formatNumber(data.current_month_complete.value);
                    document.getElementById('lastMonthDeadStockCount').textContent = data.last_month.count;
                    document.getElementById('lastMonthDeadStockValue').textContent = formatNumber(data.last_month.value);
                    document.getElementById('lastToLastMonthCount').textContent = data.last_to_last_month.count;
                    document.getElementById('lastToLastMonthValue').textContent = formatNumber(data.last_to_last_month.value);
                    document.getElementById('totalDeadStockCount').textContent = data.total.count;
                    document.getElementById('totalDeadStockValue').textContent = formatNumber(data.total.value);
                    document.getElementById('lastMonthLiquidationCount').textContent = data.last_month_liquidation.count;
                    document.getElementById('lastMonthLiquidationValue').textContent = formatNumber(data.last_month_liquidation.value);
                });
        }

        function updateSummaryTable() {
            const filters = getFilters();
            const params = new URLSearchParams(filters);
            
            fetch(`/summary?${params}`)
                .then(r => r.json())
                .then(data => {
                    let html = '';
                    data.summary.forEach(row => {
                        html += `<tr>
                            <td>${row.location}</td>
                            <td>${row.aging_0_90_count}</td><td>${formatNumber(row.aging_0_90_value)}</td>
                            <td>${row.aging_91_180_count}</td><td>${formatNumber(row.aging_91_180_value)}</td>
                            <td>${row.aging_181_365_count}</td><td>${formatNumber(row.aging_181_365_value)}</td>
                            <td>${row.aging_366_730_count}</td><td>${formatNumber(row.aging_366_730_value)}</td>
                            <td>${row.aging_730_plus_count}</td><td>${formatNumber(row.aging_730_plus_value)}</td>
                            <td>${row.last_to_last_month_count}</td><td>${formatNumber(row.last_to_last_month_value)}</td>
                            <td>${row.last_month_count}</td><td>${formatNumber(row.last_month_value)}</td>
                            <td>${row.current_month_count}</td><td>${formatNumber(row.current_month_value)}</td>
                            <td>${row.aging_0_90_count + row.aging_91_180_count + row.aging_181_365_count + row.aging_366_730_count + row.aging_730_plus_count}</td>
                            <td>${formatNumber(row.aging_0_90_value + row.aging_91_180_value + row.aging_181_365_value + row.aging_366_730_value + row.aging_730_plus_value)}</td>
                        </tr>`;
                    });

                    const total = data.total;
                    let footerHtml = `<tr>
                        <th>TOTAL</th>
                        <th>${total.aging_0_90_count}</th><th>${formatNumber(total.aging_0_90_value)}</th>
                        <th>${total.aging_91_180_count}</th><th>${formatNumber(total.aging_91_180_value)}</th>
                        <th>${total.aging_181_365_count}</th><th>${formatNumber(total.aging_181_365_value)}</th>
                        <th>${total.aging_366_730_count}</th><th>${formatNumber(total.aging_366_730_value)}</th>
                        <th>${total.aging_730_plus_count}</th><th>${formatNumber(total.aging_730_plus_value)}</th>
                        <th>${total.last_to_last_month_count}</th><th>${formatNumber(total.last_to_last_month_value)}</th>
                        <th>${total.last_month_count}</th><th>${formatNumber(total.last_month_value)}</th>
                        <th>${total.current_month_count}</th><th>${formatNumber(total.current_month_value)}</th>
                        <th>${total.aging_0_90_count + total.aging_91_180_count + total.aging_181_365_count + total.aging_366_730_count + total.aging_730_plus_count}</th>
                        <th>${formatNumber(total.aging_0_90_value + total.aging_91_180_value + total.aging_181_365_value + total.aging_366_730_value + total.aging_730_plus_value)}</th>
                    </tr>`;

                    document.getElementById('summaryTableBody').innerHTML = html;
                    document.getElementById('summaryTableFooter').innerHTML = footerHtml;
                });
        }

        function updatePartCategoryTable() {
            const filters = getFilters();
            const params = new URLSearchParams(filters);
            
            fetch(`/location-part-category-summary?${params}`)
                .then(r => r.json())
                .then(data => {
                    let headerHtml = '<tr><th>Location</th>';
                    data.part_categories.forEach(cat => {
                        headerHtml += `<th>${cat}</th>`;
                    });
                    headerHtml += '<th>Total</th></tr>';
                    document.getElementById('partCategoryHead').innerHTML = headerHtml;

                    let bodyHtml = '';
                    data.summary.forEach(row => {
                        bodyHtml += `<tr><td>${row.location}</td>`;
                        data.part_categories.forEach(cat => {
                            bodyHtml += `<td>${formatNumber(row[cat] || 0)}</td>`;
                        });
                        bodyHtml += `<td>${formatNumber(row.total)}</td></tr>`;
                    });
                    document.getElementById('partCategoryBody').innerHTML = bodyHtml;

                    let footerHtml = `<tr><td>Total</td>`;
                    data.part_categories.forEach(cat => {
                        footerHtml += `<td>${formatNumber(data.total[cat] || 0)}</td>`;
                    });
                    footerHtml += `<td>${formatNumber(data.total.total)}</td></tr>`;
                    document.getElementById('partCategoryFoot').innerHTML = footerHtml;
                });
        }

        function updateDataTable(page) {
            const filters = getFilters();
            const params = new URLSearchParams({...filters, page, per_page: 50});
            
            fetch(`/data?${params}`)
                .then(r => r.json())
                .then(data => {
                    document.getElementById('recordCount').textContent = data.total_records;
                    let html = '';
                    data.data.forEach(row => {
                        html += `<tr>`;
                        ['Zone', 'Dealer Name', 'Location', 'Part No', 'Part Description', 'Division', 'ABC', 'HML', 'RIS', 'Part Category', 'Part Division', 'STOR_CD', 'STOR_DESC', 'Dealer Ship to Code', 'GNDP', 'Stock Qty', 'Stock at GNDP', 'Stock at MDW', 'Cumm. Issue Qty', 'Last Issue Date', 'Last Issue Qty', 'Last Purchase Date', 'Last Purchase Qty', 'Movement Category I', 'Movement Category P', 'Is Dead Stock', 'Dead Stock Month'].forEach(field => {
                            html += `<td>${row[field] || '-'}</td>`;
                        });
                        html += `</tr>`;
                    });
                    document.getElementById('dataTableBody').innerHTML = html;

                    // Pagination
                    let paginationHtml = '';
                    for (let i = 1; i <= data.total_pages; i++) {
                        paginationHtml += `<li class="page-item ${i === page ? 'active' : ''}"><a class="page-link" href="#" onclick="updateDataTable(${i}); return false;">${i}</a></li>`;
                    }
                    document.getElementById('pagination').innerHTML = paginationHtml;
                });
        }

        function formatNumber(num) {
            if (!num || isNaN(num)) return '0';
            return num.toLocaleString('en-IN', {maximumFractionDigits: 2});
        }

        function downloadDeadStock(category) {
            const filters = getFilters();
            const params = new URLSearchParams({...filters, dead_stock_category: category});
            window.location.href = `/download-dead-stock-csv?${params}`;
        }

        function downloadLiquidation() {
            const filters = getFilters();
            const params = new URLSearchParams(filters);
            window.location.href = `/download-last-month-liquidation-csv?${params}`;
        }

        function downloadSummary() {
            const filters = getFilters();
            const params = new URLSearchParams(filters);
            window.location.href = `/download-summary-csv?${params}`;
        }

        function downloadPartCategory() {
            const filters = getFilters();
            const params = new URLSearchParams(filters);
            window.location.href = `/download-part-category-csv?${params}`;
        }

        function downloadData() {
            const filters = getFilters();
            const params = new URLSearchParams(filters);
            window.location.href = `/download-csv?${params}`;
        }

        // Initialize on load
        init();
    </script>
</body>
</html>
""")

@app.post("/upload-excel")
async def upload_excel(file: UploadFile = File(...)):
    """Handle Excel file upload"""
    global df, total_gndp, locations, abc_categories, ris_values, part_categories, movement_categories, last_reload_time, last_file_modified
    try:
        contents = await file.read()
        with open(excel_file_path, 'wb') as f:
            f.write(contents)
        print(f"\n📤 New Excel file uploaded: {file.filename}")
        
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
        last_file_modified = get_file_modified_time(excel_file_path)
        
        print(f"✅ File uploaded and processed successfully!")
        return {"success": True, "message": f"✅ Uploaded! {len(df):,} records loaded"}
    except Exception as e:
        print(f"❌ Upload error: {e}")
        return {"success": False, "message": f"Error: {str(e)}"}

@app.get("/")
async def dashboard(request: Request):
    from jinja2 import Template
    formatted_gndp = format_indian_number(total_gndp)
    template = Template(HTML_TEMPLATE)
    rendered_html = template.render(
        formatted_gndp=formatted_gndp,
        movement_categories=movement_categories,
        part_categories=part_categories,
        locations=locations,
        abc_categories=abc_categories,
        ris_values=ris_values,
        last_reload_time=last_reload_time
    )
    return HTMLResponse(content=rendered_html)

def apply_filters_vectorized(filtered_df, movement_category, part_category, location, abc_category, ris, part_number):
    """Apply all filters at once using vectorized operations"""
    if movement_category:
        categories_list = movement_category.split(',')
        filtered_df = filtered_df[filtered_df['Movement Category P (2)'].isin(categories_list)]
    
    if part_category and part_category_col in filtered_df.columns:
        categories_list = part_category.split(',')
        filtered_df = filtered_df[filtered_df[part_category_col].isin(categories_list)]
    
    if location and location_col in filtered_df.columns:
        locations_list = location.split(',')
        filtered_df = filtered_df[filtered_df[location_col].isin(locations_list)]
    
    if abc_category and abc_col in filtered_df.columns:
        categories_list = abc_category.split(',')
        filtered_df = filtered_df[filtered_df[abc_col].isin(categories_list)]
    
    if ris and ris_col in filtered_df.columns:
        ris_list = ris.split(',')
        filtered_df = filtered_df[filtered_df[ris_col].isin(ris_list)]
    
    if part_number and part_no_col in filtered_df.columns:
        filtered_df = filtered_df[filtered_df[part_no_col].astype(str).str.contains(part_number, case=False, na=False)]
    
    return filtered_df

@app.get("/summary")
async def get_summary(
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None,
    abc_category: Optional[str] = None,
    ris: Optional[str] = None,
    part_number: Optional[str] = None
):
    filtered_df = apply_filters_vectorized(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    summary_data = []
    
    if location_col in filtered_df.columns:
        for loc in sorted(filtered_df[location_col].dropna().unique()):
            loc_df = filtered_df[filtered_df[location_col] == loc]
            summary_row = {
                'location': loc,
                'aging_0_90_count': len(loc_df[loc_df['Movement Category P (2)'] == '0 to 90 days']),
                'aging_0_90_value': loc_df[loc_df['Movement Category P (2)'] == '0 to 90 days'][gndp_column].sum() if gndp_column in loc_df.columns else 0,
                'aging_91_180_count': len(loc_df[loc_df['Movement Category P (2)'] == '91 to 180 days']),
                'aging_91_180_value': loc_df[loc_df['Movement Category P (2)'] == '91 to 180 days'][gndp_column].sum() if gndp_column in loc_df.columns else 0,
                'aging_181_365_count': len(loc_df[loc_df['Movement Category P (2)'] == '181 to 365 days']),
                'aging_181_365_value': loc_df[loc_df['Movement Category P (2)'] == '181 to 365 days'][gndp_column].sum() if gndp_column in loc_df.columns else 0,
                'aging_366_730_count': len(loc_df[loc_df['Movement Category P (2)'] == '366 to 730 days']),
                'aging_366_730_value': loc_df[loc_df['Movement Category P (2)'] == '366 to 730 days'][gndp_column].sum() if gndp_column in loc_df.columns else 0,
                'aging_730_plus_count': len(loc_df[loc_df['Movement Category P (2)'] == '730 and above']),
                'aging_730_plus_value': loc_df[loc_df['Movement Category P (2)'] == '730 and above'][gndp_column].sum() if gndp_column in loc_df.columns else 0,
                'last_to_last_month_count': len(loc_df[loc_df['Purchase Month Category'] == 'Last to Last Month']),
                'last_to_last_month_value': loc_df[loc_df['Purchase Month Category'] == 'Last to Last Month'][gndp_column].sum() if gndp_column in loc_df.columns else 0,
                'last_month_count': len(loc_df[loc_df['Purchase Month Category'] == 'Last Month']),
                'last_month_value': loc_df[loc_df['Purchase Month Category'] == 'Last Month'][gndp_column].sum() if gndp_column in loc_df.columns else 0,
                'current_month_count': len(loc_df[loc_df['Purchase Month Category'] == 'Current Month']),
                'current_month_value': loc_df[loc_df['Purchase Month Category'] == 'Current Month'][gndp_column].sum() if gndp_column in loc_df.columns else 0,
            }
            summary_data.append(summary_row)
    
    total_row = {
        'aging_0_90_count': sum(row['aging_0_90_count'] for row in summary_data),
        'aging_0_90_value': sum(row['aging_0_90_value'] for row in summary_data),
        'aging_91_180_count': sum(row['aging_91_180_count'] for row in summary_data),
        'aging_91_180_value': sum(row['aging_91_180_value'] for row in summary_data),
        'aging_181_365_count': sum(row['aging_181_365_count'] for row in summary_data),
        'aging_181_365_value': sum(row['aging_181_365_value'] for row in summary_data),
        'aging_366_730_count': sum(row['aging_366_730_count'] for row in summary_data),
        'aging_366_730_value': sum(row['aging_366_730_value'] for row in summary_data),
        'aging_730_plus_count': sum(row['aging_730_plus_count'] for row in summary_data),
        'aging_730_plus_value': sum(row['aging_730_plus_value'] for row in summary_data),
        'last_to_last_month_count': sum(row['last_to_last_month_count'] for row in summary_data),
        'last_to_last_month_value': sum(row['last_to_last_month_value'] for row in summary_data),
        'last_month_count': sum(row['last_month_count'] for row in summary_data),
        'last_month_value': sum(row['last_month_value'] for row in summary_data),
        'current_month_count': sum(row['current_month_count'] for row in summary_data),
        'current_month_value': sum(row['current_month_value'] for row in summary_data),
    }
    
    return {"summary": summary_data, "total": total_row}

@app.get("/dead-stock-summary")
async def get_dead_stock_summary(
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None,
    abc_category: Optional[str] = None,
    ris: Optional[str] = None,
    part_number: Optional[str] = None
):
    filtered_df = apply_filters_vectorized(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    
    today = datetime.now().date()
    current_month_start = today.replace(day=1)
    last_month_end = current_month_start - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    last_to_last_month_end = last_month_start - timedelta(days=1)
    last_to_last_month_start = last_to_last_month_end.replace(day=1)
    
    # Get last day of current month from previous year
    if current_month_start.month == 12:
        current_month_last_year_complete_end = current_month_start.replace(year=current_month_start.year, month=12, day=31)
    else:
        current_month_last_year_complete_end = current_month_start.replace(month=current_month_start.month + 1, day=1) - timedelta(days=1)
    
    current_month_last_year_start = current_month_start.replace(year=current_month_start.year - 1)
    current_month_last_year_end = today.replace(year=today.year - 1)
    last_month_last_year_start = last_month_start.replace(year=last_month_start.year - 1)
    last_month_last_year_end = last_month_end.replace(year=last_month_end.year - 1)
    last_to_last_month_last_year_start = last_to_last_month_start.replace(year=last_to_last_month_start.year - 1)
    last_to_last_month_last_year_end = last_to_last_month_end.replace(year=last_to_last_month_end.year - 1)
    
    def get_dead_stock_mask(df_temp, date_range_start, date_range_end):
        """Get mask for dead stock items in a date range"""
        try:
            stock_mask = pd.to_numeric(df_temp[stock_qty_col], errors='coerce').fillna(0) > 0
            purchase_dates = pd.to_datetime(df_temp[last_purchase_col].astype(str).str[:10], errors='coerce')
            issue_dates = pd.to_datetime(df_temp[last_issue_col].astype(str).str[:10], errors='coerce')
            date_range_mask = (purchase_dates >= date_range_start) & (purchase_dates <= date_range_end)
            no_issue_mask = issue_dates.isna() | (issue_dates < purchase_dates)
            return stock_mask & date_range_mask & no_issue_mask
        except:
            return pd.Series([False] * len(df_temp), index=df_temp.index)
    
    # Current Month Complete - items purchased in current month last year, complete the month
    current_month_complete_mask = get_dead_stock_mask(filtered_df, pd.Timestamp(current_month_last_year_start), pd.Timestamp(current_month_last_year_complete_end))
    current_month_complete_df = filtered_df[current_month_complete_mask]
    
    # Current Month As On Date - items purchased in current month last year, up to today
    current_month_as_on_date_mask = get_dead_stock_mask(filtered_df, pd.Timestamp(current_month_last_year_start), pd.Timestamp(current_month_last_year_end))
    current_month_as_on_date_df = filtered_df[current_month_as_on_date_mask]
    
    # Last Month
    last_month_mask = get_dead_stock_mask(filtered_df, pd.Timestamp(last_month_last_year_start), pd.Timestamp(last_month_last_year_end))
    last_month_df = filtered_df[last_month_mask]
    
    # Last to Last Month
    last_to_last_month_mask = get_dead_stock_mask(filtered_df, pd.Timestamp(last_to_last_month_last_year_start), pd.Timestamp(last_to_last_month_last_year_end))
    last_to_last_month_df = filtered_df[last_to_last_month_mask]
    
    # Total Dead Stock
    dead_stock_df = filtered_df[filtered_df['Is Dead Stock'] == True]
    
    # Last Month Liquidation
    try:
        stock_mask = pd.to_numeric(filtered_df[stock_qty_col], errors='coerce').fillna(0) > 0
        purchase_dates = pd.to_datetime(filtered_df[last_purchase_col].astype(str).str[:10], errors='coerce')
        issue_dates = pd.to_datetime(filtered_df[last_issue_col].astype(str).str[:10], errors='coerce')
        old_purchase_mask = purchase_dates < pd.Timestamp(last_month_last_year_start)
        last_month_issue_mask = (issue_dates >= pd.Timestamp(last_month_start)) & (issue_dates <= pd.Timestamp(last_month_end))
        lml_mask = stock_mask & old_purchase_mask & last_month_issue_mask
        lml_df = filtered_df[lml_mask]
    except:
        lml_df = pd.DataFrame()
    
    return {
        "current_month_as_on_date": {
            "count": len(current_month_as_on_date_df),
            "value": current_month_as_on_date_df[gndp_column].sum() if gndp_column in current_month_as_on_date_df.columns and len(current_month_as_on_date_df) > 0 else 0
        },
        "current_month_complete": {
            "count": len(current_month_complete_df),
            "value": current_month_complete_df[gndp_column].sum() if gndp_column in current_month_complete_df.columns and len(current_month_complete_df) > 0 else 0
        },
        "last_month": {
            "count": len(last_month_df),
            "value": last_month_df[gndp_column].sum() if gndp_column in last_month_df.columns and len(last_month_df) > 0 else 0
        },
        "last_to_last_month": {
            "count": len(last_to_last_month_df),
            "value": last_to_last_month_df[gndp_column].sum() if gndp_column in last_to_last_month_df.columns and len(last_to_last_month_df) > 0 else 0
        },
        "total": {
            "count": len(dead_stock_df),
            "value": dead_stock_df[gndp_column].sum() if gndp_column in dead_stock_df.columns and len(dead_stock_df) > 0 else 0
        },
        "last_month_liquidation": {
            "count": len(lml_df),
            "value": lml_df[gndp_column].sum() if gndp_column in lml_df.columns and len(lml_df) > 0 else 0
        }
    }

# Include all other endpoints from the original code...
# (download endpoints remain the same)

@app.get("/calculate-gndp")
async def calculate_gndp(
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None,
    abc_category: Optional[str] = None,
    ris: Optional[str] = None,
    part_number: Optional[str] = None
):
    filtered_df = apply_filters_vectorized(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    total_gndp_calc = filtered_df[gndp_column].sum() if gndp_column in filtered_df.columns else 0
    return {"total_gndp": total_gndp_calc}

@app.get("/data")
async def get_data(
    page: int = 1,
    per_page: int = 50,
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None,
    abc_category: Optional[str] = None,
    ris: Optional[str] = None,
    part_number: Optional[str] = None
):
    filtered_df = apply_filters_vectorized(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
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

@app.get("/location-part-category-summary")
async def get_location_part_category_summary(
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None,
    abc_category: Optional[str] = None,
    ris: Optional[str] = None,
    part_number: Optional[str] = None
):
    filtered_df = apply_filters_vectorized(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    all_part_categories = sorted(filtered_df[part_category_col].dropna().unique().tolist()) if part_category_col and part_category_col in filtered_df.columns else []
    summary_data = []
    
    if location_col in filtered_df.columns:
        for loc in sorted(filtered_df[location_col].dropna().unique()):
            loc_df = filtered_df[filtered_df[location_col] == loc]
            row_data = {'location': loc}
            total_value = 0
            for part_cat in all_part_categories:
                value = loc_df[loc_df[part_category_col] == part_cat][gndp_column].sum() if gndp_column in loc_df.columns else 0
                row_data[part_cat] = value
                total_value += value
            row_data['total'] = total_value
            summary_data.append(row_data)
    
    total_row = {'location': 'TOTAL'}
    grand_total = 0
    for part_cat in all_part_categories:
        total_value = sum(row.get(part_cat, 0) for row in summary_data)
        total_row[part_cat] = total_value
        grand_total += total_value
    total_row['total'] = grand_total
    
    return {
        "summary": summary_data,
        "total": total_row,
        "part_categories": all_part_categories
    }

@app.get("/download-csv")
async def download_csv(
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None,
    abc_category: Optional[str] = None,
    ris: Optional[str] = None,
    part_number: Optional[str] = None
):
    filtered_df = apply_filters_vectorized(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    current_datetime = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    locations_filter = location.split(',') if location and location.strip() else []
    location_part = "_".join(locations_filter) if locations_filter else "All_Locations"
    location_part = location_part.replace(" ", "_").replace("/", "-").replace("\\", "-")
    filename = f"Details_{location_part}_{current_datetime}.csv"
    reports_dir = "./Reports"
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)
    output_path = os.path.join(reports_dir, filename)
    filtered_df.to_csv(output_path, index=False)
    return FileResponse(path=output_path, filename=filename, media_type='text/csv')

@app.get("/download-summary-csv")
async def download_summary_csv(
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None,
    abc_category: Optional[str] = None,
    ris: Optional[str] = None,
    part_number: Optional[str] = None
):
    filtered_df = apply_filters_vectorized(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    summary_data = []
    if location_col in filtered_df.columns:
        for loc in sorted(filtered_df[location_col].dropna().unique()):
            loc_df = filtered_df[filtered_df[location_col] == loc]
            summary_data.append({
                'Location': loc,
                '0-90 Days Count': len(loc_df[loc_df['Movement Category P (2)'] == '0 to 90 days']),
                '0-90 Days Value (Rs.)': format_indian_number(loc_df[loc_df['Movement Category P (2)'] == '0 to 90 days'][gndp_column].sum() if gndp_column in loc_df.columns else 0),
                '91-180 Days Count': len(loc_df[loc_df['Movement Category P (2)'] == '91 to 180 days']),
                '91-180 Days Value (Rs.)': format_indian_number(loc_df[loc_df['Movement Category P (2)'] == '91 to 180 days'][gndp_column].sum() if gndp_column in loc_df.columns else 0),
                '181-365 Days Count': len(loc_df[loc_df['Movement Category P (2)'] == '181 to 365 days']),
                '181-365 Days Value (Rs.)': format_indian_number(loc_df[loc_df['Movement Category P (2)'] == '181 to 365 days'][gndp_column].sum() if gndp_column in loc_df.columns else 0),
                '366-730 Days Count': len(loc_df[loc_df['Movement Category P (2)'] == '366 to 730 days']),
                '366-730 Days Value (Rs.)': format_indian_number(loc_df[loc_df['Movement Category P (2)'] == '366 to 730 days'][gndp_column].sum() if gndp_column in loc_df.columns else 0),
                '730+ Days Count': len(loc_df[loc_df['Movement Category P (2)'] == '730 and above']),
                '730+ Days Value (Rs.)': format_indian_number(loc_df[loc_df['Movement Category P (2)'] == '730 and above'][gndp_column].sum() if gndp_column in loc_df.columns else 0),
                'Last to Last Month Count': len(loc_df[loc_df['Purchase Month Category'] == 'Last to Last Month']),
                'Last to Last Month Value (Rs.)': format_indian_number(loc_df[loc_df['Purchase Month Category'] == 'Last to Last Month'][gndp_column].sum() if gndp_column in loc_df.columns else 0),
                'Last Month Count': len(loc_df[loc_df['Purchase Month Category'] == 'Last Month']),
                'Last Month Value (Rs.)': format_indian_number(loc_df[loc_df['Purchase Month Category'] == 'Last Month'][gndp_column].sum() if gndp_column in loc_df.columns else 0),
                'Current Month Count': len(loc_df[loc_df['Purchase Month Category'] == 'Current Month']),
                'Current Month Value (Rs.)': format_indian_number(loc_df[loc_df['Purchase Month Category'] == 'Current Month'][gndp_column].sum() if gndp_column in loc_df.columns else 0),
            })
    
    if summary_data:
        def parse_indian_number(value_str):
            try:
                if value_str == '0':
                    return 0
                return int(value_str.replace(',', ''))
            except:
                return 0
        
        total_row = {
            'Location': 'TOTAL',
            '0-90 Days Count': sum(row['0-90 Days Count'] for row in summary_data),
            '0-90 Days Value (Rs.)': format_indian_number(sum(parse_indian_number(row['0-90 Days Value (Rs.)']) for row in summary_data) / 100000),
            '91-180 Days Count': sum(row['91-180 Days Count'] for row in summary_data),
            '91-180 Days Value (Rs.)': format_indian_number(sum(parse_indian_number(row['91-180 Days Value (Rs.)']) for row in summary_data) / 100000),
            '181-365 Days Count': sum(row['181-365 Days Count'] for row in summary_data),
            '181-365 Days Value (Rs.)': format_indian_number(sum(parse_indian_number(row['181-365 Days Value (Rs.)']) for row in summary_data) / 100000),
            '366-730 Days Count': sum(row['366-730 Days Count'] for row in summary_data),
            '366-730 Days Value (Rs.)': format_indian_number(sum(parse_indian_number(row['366-730 Days Value (Rs.)']) for row in summary_data) / 100000),
            '730+ Days Count': sum(row['730+ Days Count'] for row in summary_data),
            '730+ Days Value (Rs.)': format_indian_number(sum(parse_indian_number(row['730+ Days Value (Rs.)']) for row in summary_data) / 100000),
            'Last to Last Month Count': sum(row['Last to Last Month Count'] for row in summary_data),
            'Last to Last Month Value (Rs.)': format_indian_number(sum(parse_indian_number(row['Last to Last Month Value (Rs.)']) for row in summary_data) / 100000),
            'Last Month Count': sum(row['Last Month Count'] for row in summary_data),
            'Last Month Value (Rs.)': format_indian_number(sum(parse_indian_number(row['Last Month Value (Rs.)']) for row in summary_data) / 100000),
            'Current Month Count': sum(row['Current Month Count'] for row in summary_data),
            'Current Month Value (Rs.)': format_indian_number(sum(parse_indian_number(row['Current Month Value (Rs.)']) for row in summary_data) / 100000),
        }
        summary_data.append(total_row)
    
    summary_df = pd.DataFrame(summary_data)
    current_datetime = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    locations_filter = location.split(',') if location and location.strip() else []
    location_part = "_".join(locations_filter) if locations_filter else "All_Locations"
    location_part = location_part.replace(" ", "_").replace("/", "-").replace("\\", "-")
    filename = f"Summary_{location_part}_{current_datetime}.csv"
    reports_dir = "./Reports"
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)
    output_path = os.path.join(reports_dir, filename)
    summary_df.to_csv(output_path, index=False)
    return FileResponse(path=output_path, filename=filename, media_type='text/csv')

@app.get("/download-part-category-csv")
async def download_part_category_csv(
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None,
    abc_category: Optional[str] = None,
    ris: Optional[str] = None,
    part_number: Optional[str] = None
):
    filtered_df = apply_filters_vectorized(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    all_part_categories = sorted(filtered_df[part_category_col].dropna().unique().tolist()) if part_category_col and part_category_col in filtered_df.columns else []
    summary_data = []
    if location_col in filtered_df.columns:
        for loc in sorted(filtered_df[location_col].dropna().unique()):
            loc_df = filtered_df[filtered_df[location_col] == loc]
            row_data = {'Location': loc}
            total_value = 0
            for part_cat in all_part_categories:
                value = loc_df[loc_df[part_category_col] == part_cat][gndp_column].sum() if gndp_column in loc_df.columns else 0
                row_data[part_cat] = value
                total_value += value
            row_data['Total'] = total_value
            summary_data.append(row_data)
        
        total_row = {'Location': 'Column Total'}
        grand_total = 0
        for part_cat in all_part_categories:
            total_value = sum(row.get(part_cat, 0) for row in summary_data)
            total_row[part_cat] = total_value
            grand_total += total_value
        total_row['Total'] = grand_total
        summary_data.append(total_row)
    
    summary_df = pd.DataFrame(summary_data)
    current_datetime = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    locations_filter = location.split(',') if location and location.strip() else []
    location_part = "_".join(locations_filter) if locations_filter else "All_Locations"
    location_part = location_part.replace(" ", "_").replace("/", "-").replace("\\", "-")
    filename = f"Part_Category_{location_part}_{current_datetime}.csv"
    reports_dir = "./Reports"
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)
    output_path = os.path.join(reports_dir, filename)
    summary_df.to_csv(output_path, index=False)
    return FileResponse(path=output_path, filename=filename, media_type='text/csv')

@app.get("/download-dead-stock-csv")
async def download_dead_stock_csv(
    dead_stock_category: str = "all",
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None,
    abc_category: Optional[str] = None,
    ris: Optional[str] = None,
    part_number: Optional[str] = None
):
    filtered_df = apply_filters_vectorized(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    today = datetime.now().date()
    current_month_start = today.replace(day=1)
    last_month_end = current_month_start - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    last_to_last_month_end = last_month_start - timedelta(days=1)
    last_to_last_month_start = last_to_last_month_end.replace(day=1)
    
    current_month_last_year_start = current_month_start.replace(year=current_month_start.year - 1)
    current_month_last_year_end = today.replace(year=today.year - 1)
    last_month_last_year_start = last_month_start.replace(year=last_month_start.year - 1)
    last_month_last_year_end = last_month_end.replace(year=last_month_end.year - 1)
    last_to_last_month_last_year_start = last_to_last_month_start.replace(year=last_to_last_month_start.year - 1)
    last_to_last_month_last_year_end = last_to_last_month_end.replace(year=last_to_last_month_end.year - 1)
    
    def get_dead_stock_mask(df_temp, date_range_start, date_range_end):
        try:
            stock_mask = pd.to_numeric(df_temp[stock_qty_col], errors='coerce').fillna(0) > 0
            purchase_dates = pd.to_datetime(df_temp[last_purchase_col].astype(str).str[:10], errors='coerce')
            issue_dates = pd.to_datetime(df_temp[last_issue_col].astype(str).str[:10], errors='coerce')
            date_range_mask = (purchase_dates >= date_range_start) & (purchase_dates <= date_range_end)
            no_issue_mask = issue_dates.isna() | (issue_dates < purchase_dates)
            return stock_mask & date_range_mask & no_issue_mask
        except:
            return pd.Series([False] * len(df_temp), index=df_temp.index)
    
    result_df = pd.DataFrame()
    category_name = "All"
    
    if dead_stock_category == "current_month_complete":
        if current_month_start.month == 12:
            current_month_last_year_complete_end = current_month_last_year_start.replace(year=current_month_last_year_start.year, month=12, day=31)
        else:
            current_month_last_year_complete_end = current_month_last_year_start.replace(month=current_month_last_year_start.month + 1, day=1) - timedelta(days=1)
        mask = get_dead_stock_mask(filtered_df, pd.Timestamp(current_month_last_year_start), pd.Timestamp(current_month_last_year_complete_end))
        result_df = filtered_df[mask]
        category_name = "Current_Month_Complete"
    elif dead_stock_category == "last_month":
        mask = get_dead_stock_mask(filtered_df, pd.Timestamp(last_month_last_year_start), pd.Timestamp(last_month_last_year_end))
        result_df = filtered_df[mask]
        category_name = "Last_Month"
    elif dead_stock_category == "last_to_last_month":
        mask = get_dead_stock_mask(filtered_df, pd.Timestamp(last_to_last_month_last_year_start), pd.Timestamp(last_to_last_month_last_year_end))
        result_df = filtered_df[mask]
        category_name = "Last_To_Last_Month"
    else:
        result_df = filtered_df[filtered_df['Is Dead Stock'] == True]
        category_name = "All"
    
    current_datetime = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    locations_filter = location.split(',') if location and location.strip() else []
    location_part = "_".join(locations_filter) if locations_filter else "All_Locations"
    location_part = location_part.replace(" ", "_").replace("/", "-").replace("\\", "-")
    filename = f"DeadStock_{location_part}_{category_name}_{current_datetime}.csv"
    reports_dir = "./Reports/Dead_Stock"
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)
    output_path = os.path.join(reports_dir, filename)
    result_df.to_csv(output_path, index=False)
    return FileResponse(path=output_path, filename=filename, media_type='text/csv')

@app.get("/download-last-month-liquidation-csv")
async def download_last_month_liquidation_csv(
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None,
    abc_category: Optional[str] = None,
    ris: Optional[str] = None,
    part_number: Optional[str] = None
):
    filtered_df = apply_filters_vectorized(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    today = datetime.now().date()
    current_month_start = today.replace(day=1)
    last_month_end = current_month_start - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    last_month_last_year_start = last_month_start.replace(year=last_month_start.year - 1)
    
    try:
        stock_mask = pd.to_numeric(filtered_df[stock_qty_col], errors='coerce').fillna(0) > 0
        purchase_dates = pd.to_datetime(filtered_df[last_purchase_col].astype(str).str[:10], errors='coerce')
        issue_dates = pd.to_datetime(filtered_df[last_issue_col].astype(str).str[:10], errors='coerce')
        old_purchase_mask = purchase_dates < pd.Timestamp(last_month_last_year_start)
        last_month_issue_mask = (issue_dates >= pd.Timestamp(last_month_start)) & (issue_dates <= pd.Timestamp(last_month_end))
        lml_mask = stock_mask & old_purchase_mask & last_month_issue_mask
        lml_df = filtered_df[lml_mask]
    except:
        lml_df = pd.DataFrame()
    
    current_datetime = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    locations_filter = location.split(',') if location and location.strip() else []
    location_part = "_".join(locations_filter) if locations_filter else "All_Locations"
    location_part = location_part.replace(" ", "_").replace("/", "-").replace("\\", "-")
    filename = f"LastMonth_Liquidation_{location_part}_{current_datetime}.csv"
    reports_dir = "./Reports/Liquidation"
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)
    output_path = os.path.join(reports_dir, filename)
    lml_df.to_csv(output_path, index=False)
    return FileResponse(path=output_path, filename=filename, media_type='text/csv')

if __name__ == "__main__":
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    port = 8004
    print("\n" + "=" * 70)
    print(f"✅ Server ready! Access the dashboard at:")
    print(f" 🌐 Local: http://localhost:{port}")
    print(f" 🌐 Network: http://{local_ip}:{port}")
    print("=" * 70)
    uvicorn.run(app, host="0.0.0.0", port=port)
