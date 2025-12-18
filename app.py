import pandas as pd
from datetime import datetime, timedelta
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import os
from typing import Optional
import sys
import numpy as np
from pathlib import Path
import threading
import time

# ============= GLOBAL VARIABLES =============
csv_file_path = "./Spares Ageing Report_Processed.csv"
last_reload_time = None
df = None
total_gndp = 0
gndp_column = None
excel_error = None
data_lock = threading.Lock()

# Column references
location_col = None
last_issue_col = None
last_purchase_col = None
last_issue_qty_col = None
abc_col = None
ris_col = None
part_no_col = None
part_category_col = None
stock_qty_col = None

# Filter options
locations = []
abc_categories = []
ris_values = []
part_categories = []
movement_categories = []

# ============= UTILITY FUNCTIONS =============

def clean_for_json(df):
    """Clean dataframe for JSON serialization"""
    df = df.copy()
    df = df.replace([np.inf, -np.inf], None)
    df = df.where(pd.notna(df), None)
    return df

def format_indian_number(num):
    """Format number in Indian numbering system"""
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

# ============= FAST CSV LOADING =============

def load_csv_fast():
    """Load CSV file with minimal processing - ULTRA FAST"""
    global df, total_gndp, gndp_column, excel_error, last_reload_time
    global location_col, last_issue_col, last_purchase_col, abc_col, ris_col
    global part_no_col, part_category_col, stock_qty_col, locations, abc_categories
    global ris_values, part_categories, movement_categories
    
    print("\n⚡ FAST CSV LOADING...")
    start_time = time.time()
    
    if not os.path.exists(csv_file_path):
        print(f"⚠️  CSV not found: {csv_file_path}")
        excel_error = f"CSV file not found: {csv_file_path}"
        return False
    
    try:
        # Fast CSV load
        print("📖 Reading CSV file...")
        df = pd.read_csv(csv_file_path, low_memory=False)
        print(f"✓ Loaded {len(df):,} rows in {time.time() - start_time:.2f}s")
        
        # Find columns quickly
        print("🔍 Finding columns...")
        for col in df.columns:
            if 'location' in str(col).lower() and 'dealer' not in str(col).lower():
                location_col = col
            elif 'last' in str(col).lower() and 'issue' in str(col).lower() and 'date' in str(col).lower():
                last_issue_col = col
            elif 'last' in str(col).lower() and 'purchase' in str(col).lower() and 'date' in str(col).lower():
                last_purchase_col = col
            elif 'last' in str(col).lower() and 'issue' in str(col).lower() and 'qty' in str(col).lower():
                last_issue_qty_col = col
            elif str(col).upper().strip() == 'ABC':
                abc_col = col
            elif str(col).upper().strip() == 'RIS':
                ris_col = col
            elif 'part' in str(col).lower() and 'no' in str(col).lower() and 'description' not in str(col).lower():
                part_no_col = col
            elif 'part' in str(col).lower() and 'category' in str(col).lower():
                part_category_col = col
            elif 'stock' in str(col).lower() and 'qty' in str(col).lower():
                stock_qty_col = col
            elif 'stock' in str(col).lower() and 'gndp' in str(col).lower():
                gndp_column = col
        
        # Calculate totals
        print("📊 Calculating totals...")
        if gndp_column and gndp_column in df.columns:
            df[gndp_column] = pd.to_numeric(df[gndp_column], errors='coerce').fillna(0)
            total_gndp = df[gndp_column].sum()
        
        # Pre-compute filter options (FAST)
        print("⚙️  Pre-computing filters...")
        locations = sorted([x for x in df[location_col].unique().tolist() if pd.notna(x)]) if location_col in df.columns else []
        abc_categories = sorted([x for x in df[abc_col].unique().tolist() if pd.notna(x)]) if abc_col and abc_col in df.columns else []
        ris_values = sorted([x for x in df[ris_col].unique().tolist() if pd.notna(x)]) if ris_col and ris_col in df.columns else []
        part_categories = sorted([x for x in df[part_category_col].unique().tolist() if pd.notna(x)]) if part_category_col in df.columns else []
        
        movement_order = ["0 to 90 days", "91 to 180 days", "181 to 365 days", "366 to 730 days", "730 and above"]
        unique_movement = [x for x in df['Movement Category P (2)'].unique().tolist() if pd.notna(x)]
        movement_categories = [cat for cat in movement_order if cat in unique_movement]
        
        load_time = time.time() - start_time
        print(f"\n✅ CSV LOADED SUCCESSFULLY in {load_time:.2f}s")
        print(f"   - Records: {len(df):,}")
        print(f"   - Dead Stock: {df['Is Dead Stock'].sum():,}")
        print(f"   - Locations: {len(locations)}")
        print(f"   - GNDP Value: ₹{total_gndp:,.0f}")
        
        last_reload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        excel_error = None
        return True
        
    except Exception as e:
        print(f"❌ Error loading CSV: {e}")
        excel_error = f"Error: {str(e)}"
        return False

# ============= FASTAPI APP =============

app = FastAPI(title="Spare Parts Dashboard (FAST CSV)", version="3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if not os.path.exists("static"):
    os.makedirs("static")

with open("static/style.css", "w") as f:
    f.write("""
    body { background-color: #f1f5f9; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
    .card { border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); margin-bottom: 15px; border: none; }
    .card:hover { box-shadow: 0 4px 12px rgba(0,0,0,0.12); }
    .table { border-radius: 6px; overflow: hidden; font-size: 0.9rem; }
    .table thead th { background-color: #1e293b; color: white; border: none; font-weight: 600; position: sticky; top: 0; padding: 12px 8px; }
    .table tbody td { padding: 10px 8px; vertical-align: middle; }
    .table tbody tr:hover { background-color: rgba(37, 99, 235, 0.05); }
    .form-select, .form-control { border-radius: 8px; border: 1px solid #e2e8f0; font-size: 0.95rem; }
    .form-select:focus, .form-control:focus { border-color: #2563eb; box-shadow: 0 0 0 0.2rem rgba(37, 99, 235, 0.1); }
    h1 { color: #1e293b; font-weight: 700; font-size: 1.8rem; }
    """)

app.mount("/static", StaticFiles(directory="static"), name="static")

print("\n" + "=" * 70)
print("🚀 FAST CSV SPARE PARTS AGEING DASHBOARD v3.0")
print("=" * 70)

# Load CSV immediately
if not load_csv_fast():
    print(f"\n⚠️  {excel_error}")
    print("Dashboard will show error message until CSV is available")

# ============= API ENDPOINTS =============

@app.get("/health")
async def health_check():
    """Health check"""
    return {"status": "ok", "records": len(df) if df is not None else 0, "timestamp": datetime.now().isoformat()}

@app.post("/reload-data")
async def reload_data():
    """Manual reload endpoint - for manual refresh"""
    with data_lock:
        success = load_csv_fast()
    if success:
        return {"status": "success", "message": "Data reloaded", "time": last_reload_time}
    else:
        return {"status": "error", "message": excel_error}

@app.get("/")
async def dashboard():
    """Main dashboard"""
    if df is None:
        return HTMLResponse(f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Spare Parts Dashboard - Error</title>
            <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
        </head>
        <body style="display: flex; align-items: center; justify-content: center; min-height: 100vh; background: #f1f5f9;">
            <div class="card" style="width: 500px; box-shadow: 0 4px 15px rgba(0,0,0,0.1);">
                <div class="card-body p-5">
                    <h1 class="text-danger mb-3">⚠️ Data Not Available</h1>
                    <p class="card-text mb-4"><strong>Error:</strong> {excel_error}</p>
                    <div class="alert alert-info">
                        <h6>To fix this:</h6>
                        <ol>
                            <li>Make sure CSV file exists: Spares Ageing Report_Processed.csv</li>
                            <li>Push to GitHub</li>
                            <li>Render will auto-redeploy</li>
                            <li>Refresh this page</li>
                        </ol>
                    </div>
                </div>
            </div>
        </body>
        </html>
        """)
    
    html_file = Path("index.html")
    if not html_file.exists():
        return HTMLResponse("<h1>Error: HTML not found</h1>")
    
    with open(html_file, "r", encoding="utf-8") as f:
        html_content = f.read()
    
    formatted_gndp = format_indian_number(total_gndp)
    movement_options = '\n'.join([f'<option value="{cat}">{cat}</option>' for cat in movement_categories])
    part_cat_options = '\n'.join([f'<option value="{cat}">{cat}</option>' for cat in part_categories])
    abc_options = '\n'.join([f'<option value="{cat}">{cat}</option>' for cat in abc_categories])
    ris_options = '\n'.join([f'<option value="{val}">{val}</option>' for val in ris_values])
    locations_options = '\n'.join([f'<option value="{loc}">{loc}</option>' for loc in locations])
    
    html_content = html_content.replace('{formatted_gndp}', formatted_gndp)
    html_content = html_content.replace('{last_reload_time}', last_reload_time)
    html_content = html_content.replace('{total_records}', str(len(df)))
    html_content = html_content.replace('{movement_categories_options}', movement_options)
    html_content = html_content.replace('{part_categories_options}', part_cat_options)
    html_content = html_content.replace('{abc_categories_options}', abc_options)
    html_content = html_content.replace('{ris_values_options}', ris_options)
    html_content = html_content.replace('{locations_options}', locations_options)
    
    return HTMLResponse(content=html_content)

def apply_filters(filtered_df, movement_category, part_category, location, abc_category, ris, part_number):
    """Apply filters"""
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

@app.get("/data")
async def get_data(
    page: int = 1,
    per_page: int = 25,
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None,
    abc_category: Optional[str] = None,
    ris: Optional[str] = None,
    part_number: Optional[str] = None
):
    """Get paginated data"""
    if df is None:
        return {"data": [], "page": 1, "per_page": per_page, "total_records": 0, "total_pages": 0}
    
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    
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

@app.get("/summary")
async def get_summary(
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None,
    abc_category: Optional[str] = None,
    ris: Optional[str] = None,
    part_number: Optional[str] = None
):
    """Get summary"""
    if df is None:
        return {"summary": [], "total": {}}
    
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    
    summary_data = []
    if location_col in filtered_df.columns:
        for loc in sorted(filtered_df[location_col].dropna().unique()):
            loc_df = filtered_df[filtered_df[location_col] == loc]
            summary_data.append({
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
            })
    
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
    """Get dead stock summary"""
    if df is None:
        return {
            "current_month_as_on_date": {"count": 0, "value": 0},
            "current_month_complete": {"count": 0, "value": 0},
            "last_month": {"count": 0, "value": 0},
            "last_to_last_month": {"count": 0, "value": 0},
            "total": {"count": 0, "value": 0},
            "last_month_liquidation": {"count": 0, "value": 0}
        }
    
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    dead_stock_df = filtered_df[filtered_df['Is Dead Stock'] == True]
    
    return {
        "current_month_as_on_date": {"count": 0, "value": 0},
        "current_month_complete": {"count": 0, "value": 0},
        "last_month": {"count": 0, "value": 0},
        "last_to_last_month": {"count": 0, "value": 0},
        "total": {"count": len(dead_stock_df), "value": dead_stock_df[gndp_column].sum() if gndp_column in dead_stock_df.columns else 0},
        "last_month_liquidation": {"count": 0, "value": 0}
    }

@app.get("/calculate-gndp")
async def calculate_gndp(
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None,
    abc_category: Optional[str] = None,
    ris: Optional[str] = None,
    part_number: Optional[str] = None
):
    """Calculate GNDP"""
    if df is None:
        return {"total_gndp": 0}
    
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    total_gndp_calc = filtered_df[gndp_column].sum() if gndp_column in filtered_df.columns else 0
    return {"total_gndp": total_gndp_calc}

@app.get("/location-part-category-summary")
async def get_location_part_category_summary(
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None,
    abc_category: Optional[str] = None,
    ris: Optional[str] = None,
    part_number: Optional[str] = None
):
    """Get part category summary"""
    if df is None:
        return {"summary": [], "total": {}, "part_categories": []}
    
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
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
    
    return {"summary": summary_data, "total": total_row, "part_categories": all_part_categories}

@app.get("/download-csv")
async def download_csv(
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None,
    abc_category: Optional[str] = None,
    ris: Optional[str] = None,
    part_number: Optional[str] = None
):
    """Download CSV"""
    if df is None:
        return {"error": "Data not available"}
    
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    current_datetime = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    locations_filter = location.split(',') if location and location.strip() else []
    location_part = "_".join(locations_filter) if locations_filter else "All_Locations"
    location_part = location_part.replace(" ", "_").replace("/", "-")
    
    filename = f"Details_{location_part}_{current_datetime}.csv"
    reports_dir = "./Reports"
    os.makedirs(reports_dir, exist_ok=True)
    
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
    """Download summary CSV"""
    if df is None:
        return {"error": "Data not available"}
    
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    summary_data = []
    
    if location_col in filtered_df.columns:
        for loc in sorted(filtered_df[location_col].dropna().unique()):
            loc_df = filtered_df[filtered_df[location_col] == loc]
            summary_data.append({
                'Location': loc,
                '0-90 Count': len(loc_df[loc_df['Movement Category P (2)'] == '0 to 90 days']),
                '91-180 Count': len(loc_df[loc_df['Movement Category P (2)'] == '91 to 180 days']),
                '181-365 Count': len(loc_df[loc_df['Movement Category P (2)'] == '181 to 365 days']),
                '366-730 Count': len(loc_df[loc_df['Movement Category P (2)'] == '366 to 730 days']),
                '730+ Count': len(loc_df[loc_df['Movement Category P (2)'] == '730 and above']),
            })
    
    summary_df = pd.DataFrame(summary_data)
    current_datetime = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    filename = f"Summary_{current_datetime}.csv"
    reports_dir = "./Reports"
    os.makedirs(reports_dir, exist_ok=True)
    
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
    """Download part category CSV"""
    if df is None:
        return {"error": "Data not available"}
    
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    all_part_categories = sorted(filtered_df[part_category_col].dropna().unique().tolist()) if part_category_col and part_category_col in filtered_df.columns else []
    summary_data = []
    
    if location_col in filtered_df.columns:
        for loc in sorted(filtered_df[location_col].dropna().unique()):
            loc_df = filtered_df[filtered_df[location_col] == loc]
            row_data = {'Location': loc}
            for part_cat in all_part_categories:
                value = loc_df[loc_df[part_category_col] == part_cat][gndp_column].sum() if gndp_column in loc_df.columns else 0
                row_data[part_cat] = value
            summary_data.append(row_data)
    
    summary_df = pd.DataFrame(summary_data)
    current_datetime = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    filename = f"Part_Category_{current_datetime}.csv"
    reports_dir = "./Reports"
    os.makedirs(reports_dir, exist_ok=True)
    
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
    """Download dead stock CSV"""
    if df is None:
        return {"error": "Data not available"}
    
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    result_df = filtered_df[filtered_df['Is Dead Stock'] == True]
    
    current_datetime = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    filename = f"DeadStock_{current_datetime}.csv"
    reports_dir = "./Reports/Dead_Stock"
    os.makedirs(reports_dir, exist_ok=True)
    
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
    """Download liquidation CSV"""
    if df is None:
        return {"error": "Data not available"}
    
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    
    current_datetime = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    filename = f"Liquidation_{current_datetime}.csv"
    reports_dir = "./Reports/Liquidation"
    os.makedirs(reports_dir, exist_ok=True)
    
    output_path = os.path.join(reports_dir, filename)
    filtered_df.to_csv(output_path, index=False)
    
    return FileResponse(path=output_path, filename=filename, media_type='text/csv')

# ============= SERVER STARTUP =============

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8004))
    print(f"\n🚀 Starting server on port {port}...")
    uvicorn.run(app, host="0.0.0.0", port=port)
