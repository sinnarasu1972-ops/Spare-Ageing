import pandas as pd
from datetime import datetime, timedelta
import uvicorn
from fastapi import FastAPI, Request, UploadFile, File
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import os
import socket
from typing import Optional
import sys
import numpy as np
import threading
import time
from jinja2 import Template
import gzip
import io

# ============= GLOBAL VARIABLES =============
excel_file_path = "./Spares Ageing Report.xlsx"
csv_file_path = "./Spares Ageing Report_Processed.csv"
last_file_modified = None
last_reload_time = None
df = None
total_gndp = 0
gndp_column = None

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

def get_file_modified_time(filepath):
    """Get file modification time"""
    try:
        return os.path.getmtime(filepath)
    except:
        return None

def parse_date(date_str):
    """Parse date from various formats"""
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

# ============= EXCEL PROCESSING =============

def process_excel_to_csv():
    """Process Excel file to CSV with all calculations"""
    input_file = excel_file_path
    output_csv = csv_file_path
    
    print("Processing Excel file to CSV...")
    
    if not os.path.exists(input_file):
        print(f"ERROR: File not found: {input_file}")
        return None, 0, None
    
    try:
        df = pd.read_excel(input_file)
        print(f"Successfully loaded {len(df)} rows from Excel")
    except Exception as e:
        print(f"ERROR reading Excel file: {e}")
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
        """Categorize by aging days"""
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
        """Categorize by month"""
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
        """Identify dead stock"""
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
    
    # Find required columns
    print("\nSearching for required columns...")
    
    last_issue_col_local = None
    for col in df.columns:
        if 'last' in str(col).lower() and 'issue' in str(col).lower() and 'date' in str(col).lower():
            last_issue_col_local = col
            print(f"✓ Found Last Issue Date: '{col}'")
            break
    
    last_purchase_col_local = None
    for col in df.columns:
        if 'last' in str(col).lower() and 'purchase' in str(col).lower() and 'date' in str(col).lower():
            last_purchase_col_local = col
            print(f"✓ Found Last Purchase Date: '{col}'")
            break
    
    last_issue_qty_col_local = None
    for col in df.columns:
        if 'last' in str(col).lower() and 'issue' in str(col).lower() and 'qty' in str(col).lower():
            last_issue_qty_col_local = col
            print(f"✓ Found Last Issue Qty: '{col}'")
            break
    
    if last_issue_col_local is None or last_purchase_col_local is None:
        print("ERROR: Could not find required columns")
        return None, 0, None
    
    location_col_local = None
    for col in df.columns:
        if 'location' in str(col).lower() and 'dealer' not in str(col).lower():
            location_col_local = col
            break
    
    part_category_col_local = None
    for col in df.columns:
        if 'part' in str(col).lower() and 'category' in str(col).lower():
            part_category_col_local = col
            break
    
    print("\nCreating aging categories...")
    df['Movement Category I (2)'] = df[last_issue_col_local].apply(categorize_aging)
    df['Movement Category P (2)'] = df[last_purchase_col_local].apply(categorize_aging)
    df['Purchase Month Category'] = df[last_purchase_col_local].apply(categorize_by_month)
    
    print("\nCreating Dead Stock categories...")
    
    stock_qty_col_local = None
    for col in df.columns:
        if 'stock' in str(col).lower() and 'qty' in str(col).lower():
            stock_qty_col_local = col
            break
    
    if stock_qty_col_local:
        dead_stock_results = df.apply(
            lambda row: identify_dead_stock(
                row[last_purchase_col_local], 
                row[last_issue_col_local],
                row[last_issue_qty_col_local] if last_issue_qty_col_local in df.columns else 0,
                row[stock_qty_col_local] if stock_qty_col_local and stock_qty_col_local in df.columns else 0
            ), 
            axis=1
        )
        df['Is Dead Stock'] = dead_stock_results.apply(lambda x: x[0])
        df['Dead Stock Month'] = dead_stock_results.apply(lambda x: x[1])
        print(f"✓ Dead Stock calculation applied")
        print(f"\nTotal Dead Stock Parts: {df['Is Dead Stock'].sum()}")
    
    gndp_column_local = None
    for col in df.columns:
        if 'stock' in str(col).lower() and 'gndp' in str(col).lower():
            gndp_column_local = col
            break
    
    if gndp_column_local:
        df[gndp_column_local] = pd.to_numeric(df[gndp_column_local], errors='coerce').fillna(0)
        total_gndp_calc = df[gndp_column_local].sum()
        print(f"✓ Total Stock at GNDP Value: {total_gndp_calc:.2f} Lac")
    else:
        total_gndp_calc = 0
    
    try:
        df.to_csv(output_csv, index=False)
        print(f"\n✓ Processed data saved to CSV: {output_csv}")
    except Exception as e:
        print(f"ERROR saving CSV: {e}")
        return None, 0, None
    
    return output_csv, total_gndp_calc, gndp_column_local

# ============= FASTAPI APP SETUP =============

app = FastAPI(title="Spare Parts Dashboard", version="2.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create static directory
if not os.path.exists("static"):
    os.makedirs("static")

# Create CSS file
with open("static/style.css", "w") as f:
    f.write("""
    body { 
        background-color: #f1f5f9; 
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
    }
    .card { 
        border-radius: 10px; 
        box-shadow: 0 2px 8px rgba(0,0,0,0.08); 
        margin-bottom: 15px; 
        border: none;
        transition: box-shadow 0.3s;
    }
    .card:hover {
        box-shadow: 0 4px 12px rgba(0,0,0,0.12);
    }
    .card.bg-primary { 
        background: linear-gradient(135deg, #2563eb, #1e40af) !important; 
    }
    .card.bg-danger { 
        background: linear-gradient(135deg, #ef4444, #dc2626) !important; 
    }
    .table { 
        border-radius: 6px; 
        overflow: hidden; 
        font-size: 0.9rem; 
    }
    .table thead th { 
        background-color: #1e293b; 
        color: white; 
        border: none; 
        font-weight: 600; 
        position: sticky; 
        top: 0; 
        padding: 12px 8px; 
    }
    .table tbody td { 
        padding: 10px 8px; 
        vertical-align: middle;
    }
    .table tbody tr:hover { 
        background-color: rgba(37, 99, 235, 0.05); 
    }
    .form-select, .form-control { 
        border-radius: 8px; 
        border: 1px solid #e2e8f0; 
        font-size: 0.95rem; 
    }
    .form-select:focus, .form-control:focus { 
        border-color: #2563eb; 
        box-shadow: 0 0 0 0.2rem rgba(37, 99, 235, 0.1); 
    }
    h1 { 
        color: #1e293b; 
        font-weight: 700; 
        font-size: 1.8rem; 
    }
    .page-link { 
        color: #2563eb; 
        font-size: 0.85rem; 
        padding: 0.25rem 0.5rem; 
    }
    .page-item.active .page-link { 
        background-color: #2563eb; 
        border-color: #2563eb; 
    }
    .upload-area { 
        border: 2px dashed #2563eb; 
        border-radius: 8px; 
        padding: 20px; 
        text-align: center; 
        cursor: pointer; 
        transition: all 0.3s; 
        background: #f1f5f9;
    }
    .upload-area:hover { 
        background-color: rgba(37, 99, 235, 0.1); 
        border-color: #1e40af; 
    }
    .upload-area.dragover { 
        background-color: rgba(37, 99, 235, 0.2); 
        border-color: #1e40af; 
    }
    """)

app.mount("/static", StaticFiles(directory="static"), name="static")

print("\n" + "=" * 70)
print("STARTING SPARE PARTS AGEING DASHBOARD")
print("=" * 70)

csv_file, total_gndp, gndp_column = process_excel_to_csv()

if csv_file is None:
    print("\n" + "=" * 70)
    print("ERROR: Failed to process Excel file")
    print("=" * 70)
    sys.exit(1)

try:
    df = pd.read_csv(csv_file)
    print(f"\n✓ Successfully loaded {len(df)} rows from CSV")
except Exception as e:
    print(f"\nERROR loading CSV: {e}")
    sys.exit(1)

last_reload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
last_file_modified = get_file_modified_time(excel_file_path)

# Pre-compute all columns at startup
print("\n🚀 OPTIMIZATION: Pre-computing column names...")

for col in df.columns:
    if 'last' in str(col).lower() and 'issue' in str(col).lower() and 'date' in str(col).lower():
        last_issue_col = col
        break

for col in df.columns:
    if 'last' in str(col).lower() and 'purchase' in str(col).lower() and 'date' in str(col).lower():
        last_purchase_col = col
        break

for col in df.columns:
    if 'last' in str(col).lower() and 'issue' in str(col).lower() and 'qty' in str(col).lower():
        last_issue_qty_col = col
        break

for col in df.columns:
    if 'location' in str(col).lower() and 'dealer' not in str(col).lower():
        location_col = col
        break

for col in df.columns:
    if str(col).upper().strip() == 'ABC':
        abc_col = col
        break

for col in df.columns:
    if str(col).upper().strip() == 'RIS':
        ris_col = col
        break

for col in df.columns:
    if 'part' in str(col).lower() and 'no' in str(col).lower() and 'description' not in str(col).lower():
        part_no_col = col
        break

for col in df.columns:
    if 'part' in str(col).lower() and 'category' in str(col).lower():
        part_category_col = col
        break

for col in df.columns:
    if 'stock' in str(col).lower() and 'qty' in str(col).lower():
        stock_qty_col = col
        break

# Pre-compute unique values
print("✓ Pre-computing unique values for filters...")

locations = sorted([x for x in df[location_col].unique().tolist() if pd.notna(x)]) if location_col in df.columns else []
abc_categories = sorted([x for x in df[abc_col].unique().tolist() if pd.notna(x)]) if abc_col and abc_col in df.columns else []
ris_values = sorted([x for x in df[ris_col].unique().tolist() if pd.notna(x)]) if ris_col and ris_col in df.columns else []
part_categories = sorted([x for x in df[part_category_col].unique().tolist() if pd.notna(x)]) if part_category_col in df.columns else []

movement_order = ["0 to 90 days", "91 to 180 days", "181 to 365 days", "366 to 730 days", "730 and above"]
unique_movement = [x for x in df['Movement Category P (2)'].unique().tolist() if pd.notna(x)]
movement_categories = [cat for cat in movement_order if cat in unique_movement]

print(f"\n✓ Configuration Complete:")
print(f"  - Total Records: {len(df):,}")
print(f"  - Dead Stock Parts: {df['Is Dead Stock'].sum():,}")
print(f"  - Locations: {len(locations)}")
print(f"  - Part Categories: {len(part_categories)}")

# ============= API ENDPOINTS =============

@app.get("/health")
async def health_check():
    """Health check endpoint - keeps app warm on Render"""
    return {"status": "ok", "records": len(df), "timestamp": datetime.now().isoformat()}

@app.post("/upload-excel")
async def upload_excel(file: UploadFile = File(...)):
    """Handle Excel file upload"""
    global df, total_gndp, locations, abc_categories, ris_values, part_categories, movement_categories, last_reload_time
    
    try:
        contents = await file.read()
        with open(excel_file_path, 'wb') as f:
            f.write(contents)
        
        print(f"\n📤 New Excel file uploaded: {file.filename}")
        
        csv_file, total_gndp, gndp_col = process_excel_to_csv()
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
        
        print(f"✅ File uploaded and processed successfully!")
        return {"success": True, "message": f"✅ Uploaded! {len(df):,} records loaded"}
        
    except Exception as e:
        print(f"❌ Upload error: {e}")
        return {"success": False, "message": f"Error: {str(e)}"}

@app.get("/")
async def dashboard():
    """Main dashboard endpoint"""
    formatted_gndp = format_indian_number(total_gndp)
    
    movement_options = '\n'.join([f'<option value="{cat}">{cat}</option>' for cat in movement_categories])
    part_cat_options = '\n'.join([f'<option value="{cat}">{cat}</option>' for cat in part_categories])
    abc_options = '\n'.join([f'<option value="{cat}">{cat}</option>' for cat in abc_categories])
    ris_options = '\n'.join([f'<option value="{val}">{val}</option>' for val in ris_values])
    locations_options = '\n'.join([f'<option value="{loc}">{loc}</option>' for loc in locations])
    
    html = HTML_TEMPLATE.replace('{formatted_gndp}', formatted_gndp)
    html = html.replace('{last_reload_time}', last_reload_time)
    html = html.replace('{total_records}', str(len(df)))
    html = html.replace('{movement_categories_options}', movement_options)
    html = html.replace('{part_categories_options}', part_cat_options)
    html = html.replace('{abc_categories_options}', abc_options)
    html = html.replace('{ris_values_options}', ris_options)
    html = html.replace('{locations_options}', locations_options)
    
    return HTMLResponse(content=html)

def apply_filters(filtered_df, movement_category, part_category, location, abc_category, ris, part_number):
    """Apply all filters"""
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
    """Get summary by location"""
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    
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
    }
    
    return {"summary": summary_data, "total": total_row}

@app.get("/calculate-gndp")
async def calculate_gndp(
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None,
    abc_category: Optional[str] = None,
    ris: Optional[str] = None,
    part_number: Optional[str] = None
):
    """Calculate GNDP for filtered data"""
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    total_gndp_calc = filtered_df[gndp_column].sum() if gndp_column in filtered_df.columns else 0
    return {"total_gndp": total_gndp_calc}

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
    
    return {
        "summary": summary_data,
        "total": total_row,
        "part_categories": all_part_categories
    }

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
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    
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
    
    current_month_complete_mask = get_dead_stock_mask(filtered_df, pd.Timestamp(current_month_last_year_start), pd.Timestamp(current_month_last_year_start.replace(month=current_month_last_year_start.month + 1 if current_month_last_year_start.month < 12 else 1, year=current_month_last_year_start.year + (1 if current_month_last_year_start.month == 12 else 0)) - timedelta(days=1)))
    current_month_complete_df = filtered_df[current_month_complete_mask]
    
    current_month_as_on_date_mask = get_dead_stock_mask(filtered_df, pd.Timestamp(current_month_last_year_start), pd.Timestamp(current_month_last_year_end))
    current_month_as_on_date_df = filtered_df[current_month_as_on_date_mask]
    
    last_month_mask = get_dead_stock_mask(filtered_df, pd.Timestamp(last_month_last_year_start), pd.Timestamp(last_month_last_year_end))
    last_month_df = filtered_df[last_month_mask]
    
    last_to_last_month_mask = get_dead_stock_mask(filtered_df, pd.Timestamp(last_to_last_month_last_year_start), pd.Timestamp(last_to_last_month_last_year_end))
    last_to_last_month_df = filtered_df[last_to_last_month_mask]
    
    dead_stock_df = filtered_df[filtered_df['Is Dead Stock'] == True]
    
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

@app.get("/download-csv")
async def download_csv(
    movement_category: Optional[str] = None,
    part_category: Optional[str] = None,
    location: Optional[str] = None,
    abc_category: Optional[str] = None,
    ris: Optional[str] = None,
    part_number: Optional[str] = None
):
    """Download filtered data as CSV"""
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    
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
    """Download summary as CSV"""
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    
    summary_data = []
    if location_col in filtered_df.columns:
        for loc in sorted(filtered_df[location_col].dropna().unique()):
            loc_df = filtered_df[filtered_df[location_col] == loc]
            summary_data.append({
                'Location': loc,
                '0-90 Days Count': len(loc_df[loc_df['Movement Category P (2)'] == '0 to 90 days']),
                '0-90 Days Value (Rs.)': loc_df[loc_df['Movement Category P (2)'] == '0 to 90 days'][gndp_column].sum() if gndp_column in loc_df.columns else 0,
                '91-180 Days Count': len(loc_df[loc_df['Movement Category P (2)'] == '91 to 180 days']),
                '91-180 Days Value (Rs.)': loc_df[loc_df['Movement Category P (2)'] == '91 to 180 days'][gndp_column].sum() if gndp_column in loc_df.columns else 0,
                '181-365 Days Count': len(loc_df[loc_df['Movement Category P (2)'] == '181 to 365 days']),
                '181-365 Days Value (Rs.)': loc_df[loc_df['Movement Category P (2)'] == '181 to 365 days'][gndp_column].sum() if gndp_column in loc_df.columns else 0,
                '366-730 Days Count': len(loc_df[loc_df['Movement Category P (2)'] == '366 to 730 days']),
                '366-730 Days Value (Rs.)': loc_df[loc_df['Movement Category P (2)'] == '366 to 730 days'][gndp_column].sum() if gndp_column in loc_df.columns else 0,
                '730+ Days Count': len(loc_df[loc_df['Movement Category P (2)'] == '730 and above']),
                '730+ Days Value (Rs.)': loc_df[loc_df['Movement Category P (2)'] == '730 and above'][gndp_column].sum() if gndp_column in loc_df.columns else 0,
            })
    
    if summary_data:
        total_row = {
            'Location': 'TOTAL',
            '0-90 Days Count': sum(row['0-90 Days Count'] for row in summary_data),
            '91-180 Days Count': sum(row['91-180 Days Count'] for row in summary_data),
            '181-365 Days Count': sum(row['181-365 Days Count'] for row in summary_data),
            '366-730 Days Count': sum(row['366-730 Days Count'] for row in summary_data),
            '730+ Days Count': sum(row['730+ Days Count'] for row in summary_data),
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
    """Download part category summary as CSV"""
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    
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
    """Download dead stock data as CSV"""
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    
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
    
    if dead_stock_category == "current_month_as_on_date":
        mask = get_dead_stock_mask(filtered_df, pd.Timestamp(current_month_last_year_start), pd.Timestamp(current_month_last_year_end))
        result_df = filtered_df[mask]
        category_name = "Current_Month_AsOnDate"
    
    elif dead_stock_category == "current_month_complete":
        complete_end = current_month_last_year_start.replace(month=current_month_last_year_start.month + 1 if current_month_last_year_start.month < 12 else 1, year=current_month_last_year_start.year + (1 if current_month_last_year_start.month == 12 else 0)) - timedelta(days=1)
        mask = get_dead_stock_mask(filtered_df, pd.Timestamp(current_month_last_year_start), pd.Timestamp(complete_end))
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
    """Download last month liquidation as CSV"""
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    
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

# ============= HTML TEMPLATE =============
HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Unnati Motors - Spare Parts Ageing Dashboard</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.0/font/bootstrap-icons.css">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-select@1.14.0-beta2/dist/css/bootstrap-select.min.css">
    <link rel="stylesheet" href="/static/style.css">
    <style>
        :root {
            --primary: #2563eb;
            --secondary: #64748b;
            --danger: #ef4444;
            --success: #10b981;
            --warning: #f59e0b;
            --dark: #1e293b;
            --light: #f1f5f9;
        }
        
        .header-upload-container {
            display: flex;
            gap: 20px;
            align-items: stretch;
            margin-bottom: 20px;
            flex-wrap: wrap;
        }
        
        .header-section {
            flex: 1;
            min-width: 300px;
            background: linear-gradient(135deg, var(--primary) 0%, #1e40af 100%);
            color: white;
            padding: 25px;
            border-radius: 12px;
            box-shadow: 0 4px 15px rgba(37, 99, 235, 0.2);
        }
        
        .header-section h1 {
            font-size: 1.8rem;
            margin: 0;
            font-weight: 700;
        }
        
        .header-section .subtitle {
            font-size: 0.9rem;
            opacity: 0.9;
            margin-top: 5px;
        }
        
        .upload-section {
            flex: 0 0 auto;
            width: 350px;
            background: white;
            padding: 20px;
            border-radius: 12px;
            border: 2px dashed var(--primary);
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        
        .upload-section h5 {
            font-size: 0.95rem;
            margin-bottom: 12px;
            color: var(--dark);
        }
        
        .upload-area {
            border: 2px dashed var(--primary);
            border-radius: 8px;
            padding: 15px;
            text-align: center;
            cursor: pointer;
            transition: all 0.3s;
            background: var(--light);
        }
        
        .upload-area:hover {
            background-color: rgba(37, 99, 235, 0.08);
            border-color: #1e40af;
        }
        
        .upload-area.dragover {
            background-color: rgba(37, 99, 235, 0.15);
            border-color: #1e40af;
        }
        
        .dead-stock-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 15px;
        }
        
        .dead-stock-card {
            background: white;
            padding: 18px;
            border-radius: 10px;
            border-left: 5px solid var(--primary);
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }
        
        .dead-stock-card.current { border-left-color: #ef4444; }
        .dead-stock-card.last { border-left-color: #8b5cf6; }
        .dead-stock-card.last-to-last { border-left-color: #10b981; }
        .dead-stock-card.total { border-left-color: #f59e0b; }
        .dead-stock-card.liquidation { border-left-color: #f97316; }
        
        .dead-stock-card h6 {
            font-size: 0.85rem;
            color: var(--secondary);
            margin-bottom: 8px;
            font-weight: 600;
        }
        
        .dead-stock-card .count {
            font-size: 2rem;
            font-weight: 700;
            color: var(--dark);
            margin-bottom: 5px;
        }
        
        .dead-stock-card .value {
            font-size: 0.85rem;
            color: var(--secondary);
        }
        
        .filter-card {
            background: white;
            padding: 15px;
            border-radius: 10px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }
        
        @media (max-width: 768px) {
            .header-upload-container {
                flex-direction: column;
            }
            
            .upload-section {
                width: 100%;
            }
            
            .dead-stock-grid {
                grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            }
        }
    </style>
</head>
<body>
    <div class="container-fluid p-4">
        <!-- HEADER + UPLOAD -->
        <div class="header-upload-container">
            <div class="header-section">
                <h1>📊 Unnati Motors Mahindra</h1>
                <p class="subtitle">Spare Parts Ageing Dashboard</p>
                <small style="opacity: 0.8; margin-top: 5px; display: block;">Last Updated: {last_reload_time}</small>
            </div>
            
            <div class="upload-section">
                <h5>📤 Upload QlikView File</h5>
                <div class="upload-area" id="uploadArea">
                    <i class="bi bi-cloud-arrow-up" style="font-size: 1.5rem; color: var(--primary);"></i>
                    <p style="margin: 8px 0 0 0; font-size: 0.9rem; font-weight: 500;">Drag & Drop or Click</p>
                    <input type="file" id="fileInput" accept=".xlsx,.xls" style="display:none;">
                    <small>Supports .xlsx and .xls</small>
                </div>
                <div id="uploadStatus" style="margin-top: 8px; font-size: 0.85rem;"></div>
            </div>
        </div>
        
        <!-- DEAD STOCK SECTION -->
        <div class="card bg-danger text-white mb-4">
            <div class="card-body">
                <h4 style="margin: 0 0 20px 0; font-weight: 700;">😰 Dead Stock Monitor</h4>
                <div class="dead-stock-grid">
                    <div class="dead-stock-card current">
                        <h6>Current Month (As on Date)</h6>
                        <div class="count" id="deadStockCurrentAsOnCount">0</div>
                        <div class="value">Value: <span id="deadStockCurrentAsOnValue">₹0</span></div>
                        <button class="btn btn-danger btn-sm w-100 mt-2" id="btnDeadStockCurrent">Export</button>
                    </div>
                    <div class="dead-stock-card current">
                        <h6>Current Month (Complete)</h6>
                        <div class="count" id="deadStockCurrentCompleteCount">0</div>
                        <div class="value">Value: <span id="deadStockCurrentCompleteValue">₹0</span></div>
                        <button class="btn btn-danger btn-sm w-100 mt-2" id="btnDeadStockCurrentComplete">Export</button>
                    </div>
                    <div class="dead-stock-card last">
                        <h6>Last Month Dead Stock</h6>
                        <div class="count" id="deadStockLastCount">0</div>
                        <div class="value">Value: <span id="deadStockLastValue">₹0</span></div>
                        <button class="btn btn-sm w-100 mt-2" id="btnDeadStockLast" style="background-color: #8b5cf6; color: white;">Export</button>
                    </div>
                    <div class="dead-stock-card last-to-last">
                        <h6>Last to Last Month</h6>
                        <div class="count" id="deadStockLastToLastCount">0</div>
                        <div class="value">Value: <span id="deadStockLastToLastValue">₹0</span></div>
                        <button class="btn btn-sm w-100 mt-2" id="btnDeadStockLastToLast" style="background-color: #10b981; color: white;">Export</button>
                    </div>
                    <div class="dead-stock-card total">
                        <h6>Total Dead Stock</h6>
                        <div class="count" id="deadStockTotalCount">0</div>
                        <div class="value">Value: <span id="deadStockTotalValue">₹0</span></div>
                        <button class="btn btn-warning btn-sm w-100 mt-2" id="btnDeadStockAll">Export</button>
                    </div>
                    <div class="dead-stock-card liquidation">
                        <h6>🟠 Last Month Liquidation</h6>
                        <div class="count" id="lastMonthLiquidationCount">0</div>
                        <div class="value">Value: <span id="lastMonthLiquidationValue">₹0</span></div>
                        <button class="btn btn-sm w-100 mt-2" id="btnLastMonthLiquidation" style="background-color: #f97316; color: white;">Export</button>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- KPI SECTION -->
        <div class="row mb-4">
            <div class="col-lg-3 col-md-6 mb-3">
                <div class="card bg-primary text-white h-100">
                    <div class="card-body">
                        <h6 class="card-title">Total Stock at GNDP Value</h6>
                        <p style="font-size: 2rem; font-weight: 700; margin: 0;" id="totalGndp">{formatted_gndp}</p>
                    </div>
                </div>
            </div>
            
            <div class="col-lg-3 col-md-6 mb-3">
                <div class="filter-card">
                    <label class="form-label" style="font-weight: 600; font-size: 0.9rem;">Spare Ageing</label>
                    <select class="form-select form-select-sm selectpicker" id="movementCategory" multiple data-live-search="true" title="Select categories...">
                        {movement_categories_options}
                    </select>
                </div>
            </div>
            
            <div class="col-lg-3 col-md-6 mb-3">
                <div class="filter-card">
                    <label class="form-label" style="font-weight: 600; font-size: 0.9rem;">Part Category</label>
                    <select class="form-select form-select-sm selectpicker" id="partCategory" multiple data-live-search="true" title="Select Part Category...">
                        {part_categories_options}
                    </select>
                </div>
            </div>
            
            <div class="col-lg-3 col-md-6 mb-3">
                <div class="filter-card">
                    <label class="form-label" style="font-weight: 600; font-size: 0.9rem;">ABC Category</label>
                    <select class="form-select form-select-sm selectpicker" id="abcCategory" multiple data-live-search="true" title="Select ABC...">
                        {abc_categories_options}
                    </select>
                </div>
            </div>
        </div>
        
        <!-- MORE FILTERS -->
        <div class="row mb-4">
            <div class="col-lg-3 col-md-6 mb-3">
                <div class="filter-card">
                    <label class="form-label" style="font-weight: 600; font-size: 0.9rem;">RIS</label>
                    <select class="form-select form-select-sm selectpicker" id="ris" multiple data-live-search="true" title="Select RIS...">
                        {ris_values_options}
                    </select>
                </div>
            </div>
            
            <div class="col-lg-3 col-md-6 mb-3">
                <div class="filter-card">
                    <label class="form-label" style="font-weight: 600; font-size: 0.9rem;">Location</label>
                    <select class="form-select form-select-sm selectpicker" id="location" multiple data-live-search="true" title="Select locations...">
                        {locations_options}
                    </select>
                </div>
            </div>
            
            <div class="col-lg-3 col-md-6 mb-3">
                <div class="filter-card">
                    <label class="form-label" style="font-weight: 600; font-size: 0.9rem;">Part No.</label>
                    <input type="text" class="form-control form-control-sm" id="partNumber" placeholder="Search Part Number...">
                </div>
            </div>
            
            <div class="col-lg-3 col-md-6 mb-3">
                <div class="filter-card" style="display: flex; flex-direction: column; justify-content: flex-end;">
                    <div class="row g-2">
                        <div class="col-6">
                            <button id="applyFilters" class="btn btn-primary btn-sm w-100">
                                <i class="bi bi-check-circle"></i> Apply
                            </button>
                        </div>
                        <div class="col-6">
                            <button id="clearFilters" class="btn btn-warning btn-sm w-100">
                                <i class="bi bi-x-circle"></i> Clear
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- SUMMARY TABLE -->
        <div class="card mb-4">
            <div class="card-body">
                <div class="d-flex justify-content-between align-items-center mb-3">
                    <h5 class="mb-0">Location Wise Spare Aging Summary</h5>
                    <button id="downloadSummaryCsv" class="btn btn-info btn-sm">
                        <i class="bi bi-download"></i> Download
                    </button>
                </div>
                <div class="table-responsive">
                    <table class="table table-striped table-hover table-sm table-bordered" id="summaryTable">
                        <thead class="table-dark">
                            <tr>
                                <th>Location</th>
                                <th colspan="2" class="text-center">0-90 Days</th>
                                <th colspan="2" class="text-center">91-180 Days</th>
                                <th colspan="2" class="text-center">181-365 Days</th>
                                <th colspan="2" class="text-center">366-730 Days</th>
                                <th colspan="2" class="text-center">730+ Days</th>
                            </tr>
                        </thead>
                        <tbody></tbody>
                        <tfoot class="table-secondary fw-bold"></tfoot>
                    </table>
                </div>
            </div>
        </div>
        
        <!-- LOCATION WISE PART CATEGORY TABLE -->
        <div class="card mb-4">
            <div class="card-body">
                <div class="row align-items-center">
                    <div class="col-md-6">
                        <h5 class="mb-0">Location Wise Part Category</h5>
                    </div>
                    <div class="col-md-6 text-end">
                        <button id="downloadPartCategoryCsv" class="btn btn-info btn-sm">
                            <i class="bi bi-download"></i> Download Part Category
                        </button>
                    </div>
                </div>
                <div class="table-responsive mt-2">
                    <table class="table table-striped table-hover table-sm table-bordered" id="partCategoryTable">
                        <thead class="table-dark">
                            <tr id="partCategoryHeaders">
                                <th>Location</th>
                                <th>Total</th>
                            </tr>
                        </thead>
                        <tbody></tbody>
                        <tfoot style="background-color: #ff9800 !important; color: white !important; font-weight: bold !important;">
                            <tr id="partCategoryTotal"></tr>
                        </tfoot>
                    </table>
                </div>
            </div>
        </div>
        
        <!-- DATA TABLE -->
        <div class="card">
            <div class="card-body">
                <div class="d-flex justify-content-between align-items-center mb-3">
                    <h5 class="mb-0">Details <span id="recordCount" class="text-muted">(0 Records)</span></h5>
                    <button id="downloadCsv" class="btn btn-success btn-sm">
                        <i class="bi bi-download"></i> Download
                    </button>
                </div>
                <div class="table-responsive">
                    <table class="table table-striped table-hover table-sm" id="dataTable">
                        <thead>
                            <tr>
                                <th>Part No.</th>
                                <th>Description</th>
                                <th>Location</th>
                                <th>Stock Qty</th>
                                <th>GNDP Value</th>
                                <th>Movement</th>
                            </tr>
                        </thead>
                        <tbody></tbody>
                    </table>
                </div>
                <nav aria-label="Pagination">
                    <ul class="pagination pagination-sm justify-content-center" id="pagination"></ul>
                </nav>
            </div>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap-select@1.14.0-beta2/dist/js/bootstrap-select.min.js"></script>
    
    <script>
        let currentPage = 1;
        const perPage = 25;
        let isDataLoaded = false;
        
        $(document).ready(function() {
            $('.selectpicker').selectpicker();
            setupFileUpload();
            setupEventListeners();
            
            setTimeout(() => {
                if (!isDataLoaded) {
                    loadAllData();
                    isDataLoaded = true;
                }
            }, 500);
        });
        
        function setupFileUpload() {
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
        }
        
        function uploadFile(file) {
            if (!file.name.match(/\.(xlsx|xls)$/)) {
                document.getElementById('uploadStatus').innerHTML = '<div class="alert alert-danger alert-sm" role="alert">❌ Please upload .xlsx or .xls file</div>';
                return;
            }
            
            document.getElementById('uploadStatus').innerHTML = '<div class="alert alert-info alert-sm" role="alert">⏳ Uploading...</div>';
            
            const formData = new FormData();
            formData.append('file', file);
            
            $.ajax({
                url: '/upload-excel',
                type: 'POST',
                data: formData,
                processData: false,
                contentType: false,
                success: function(res) {
                    document.getElementById('uploadStatus').innerHTML = '<div class="alert alert-success alert-sm" role="alert">✅ ' + res.message + '</div>';
                    setTimeout(() => {
                        location.reload();
                    }, 2000);
                },
                error: function(err) {
                    document.getElementById('uploadStatus').innerHTML = '<div class="alert alert-danger alert-sm" role="alert">❌ Upload failed</div>';
                }
            });
        }
        
        function formatIndianNumber(num) {
            if (num === null || num === undefined || isNaN(num)) return '0';
            const actualValue = Math.round(num * 100000);
            let numStr = Math.abs(actualValue).toString();
            let lastThree = numStr.substring(numStr.length - 3);
            let otherNumbers = numStr.substring(0, numStr.length - 3);
            if (otherNumbers !== '') lastThree = ',' + lastThree;
            let result = otherNumbers.replace(/\B(?=(\d{2})+(?!\d))/g, ',') + lastThree;
            return actualValue < 0 ? '-' + result : result;
        }
        
        function getFilters() {
            return {
                movementCategory: $('#movementCategory').val() || [],
                partCategory: $('#partCategory').val() || [],
                location: $('#location').val() || [],
                abcCategory: $('#abcCategory').val() || [],
                ris: $('#ris').val() || [],
                partNumber: $('#partNumber').val().trim()
            };
        }
        
        function buildQueryString(filters) {
            const params = new URLSearchParams();
            if (filters.movementCategory.length) params.append('movement_category', filters.movementCategory.join(','));
            if (filters.partCategory.length) params.append('part_category', filters.partCategory.join(','));
            if (filters.location.length) params.append('location', filters.location.join(','));
            if (filters.abcCategory.length) params.append('abc_category', filters.abcCategory.join(','));
            if (filters.ris.length) params.append('ris', filters.ris.join(','));
            if (filters.partNumber) params.append('part_number', filters.partNumber);
            return params.toString();
        }
        
        function loadAllData() {
            loadData();
            loadSummary();
            loadPartCategorySummary();
            loadDeadStockSummary();
        }
        
        function loadDeadStockSummary() {
            const filters = getFilters();
            const queryString = buildQueryString(filters);
            
            $.ajax({
                url: `/dead-stock-summary?${queryString}`,
                method: 'GET',
                success: function(response) {
                    $('#deadStockCurrentAsOnCount').text(response.current_month_as_on_date.count);
                    $('#deadStockCurrentAsOnValue').text('₹' + formatIndianNumber(response.current_month_as_on_date.value));
                    $('#deadStockCurrentCompleteCount').text(response.current_month_complete.count);
                    $('#deadStockCurrentCompleteValue').text('₹' + formatIndianNumber(response.current_month_complete.value));
                    $('#deadStockLastCount').text(response.last_month.count);
                    $('#deadStockLastValue').text('₹' + formatIndianNumber(response.last_month.value));
                    $('#deadStockLastToLastCount').text(response.last_to_last_month.count);
                    $('#deadStockLastToLastValue').text('₹' + formatIndianNumber(response.last_to_last_month.value));
                    $('#deadStockTotalCount').text(response.total.count);
                    $('#deadStockTotalValue').text('₹' + formatIndianNumber(response.total.value));
                    if (response.last_month_liquidation) {
                        $('#lastMonthLiquidationCount').text(response.last_month_liquidation.count);
                        $('#lastMonthLiquidationValue').text('₹' + formatIndianNumber(response.last_month_liquidation.value));
                    }
                }
            });
        }
        
        function loadSummary() {
            const filters = getFilters();
            const queryString = buildQueryString(filters);
            
            $.ajax({
                url: `/summary?${queryString}`,
                method: 'GET',
                success: function(response) {
                    $('#summaryTable tbody').empty();
                    $('#summaryTable tfoot').empty();
                    
                    response.summary.forEach(row => {
                        const totalCount = row.aging_0_90_count + row.aging_91_180_count + row.aging_181_365_count + row.aging_366_730_count + row.aging_730_plus_count;
                        const totalValue = row.aging_0_90_value + row.aging_91_180_value + row.aging_181_365_value + row.aging_366_730_value + row.aging_730_plus_value;
                        
                        $('#summaryTable tbody').append(`
                            <tr>
                                <td class="fw-bold">${row.location}</td>
                                <td class="text-end">${row.aging_0_90_count}</td>
                                <td class="text-end">${formatIndianNumber(row.aging_0_90_value)}</td>
                                <td class="text-end">${row.aging_91_180_count}</td>
                                <td class="text-end">${formatIndianNumber(row.aging_91_180_value)}</td>
                                <td class="text-end">${row.aging_181_365_count}</td>
                                <td class="text-end">${formatIndianNumber(row.aging_181_365_value)}</td>
                                <td class="text-end">${row.aging_366_730_count}</td>
                                <td class="text-end">${formatIndianNumber(row.aging_366_730_value)}</td>
                                <td class="text-end">${row.aging_730_plus_count}</td>
                                <td class="text-end">${formatIndianNumber(row.aging_730_plus_value)}</td>
                            </tr>
                        `);
                    });
                    
                    const total = response.total;
                    const grandTotalCount = total.aging_0_90_count + total.aging_91_180_count + total.aging_181_365_count + total.aging_366_730_count + total.aging_730_plus_count;
                    const grandTotalValue = total.aging_0_90_value + total.aging_91_180_value + total.aging_181_365_value + total.aging_366_730_value + total.aging_730_plus_value;
                    
                    $('#summaryTable tfoot').html(`
                        <tr class="table-warning fw-bold">
                            <td class="fw-bold">TOTAL</td>
                            <td class="text-end">${total.aging_0_90_count}</td>
                            <td class="text-end">${formatIndianNumber(total.aging_0_90_value)}</td>
                            <td class="text-end">${total.aging_91_180_count}</td>
                            <td class="text-end">${formatIndianNumber(total.aging_91_180_value)}</td>
                            <td class="text-end">${total.aging_181_365_count}</td>
                            <td class="text-end">${formatIndianNumber(total.aging_181_365_value)}</td>
                            <td class="text-end">${total.aging_366_730_count}</td>
                            <td class="text-end">${formatIndianNumber(total.aging_366_730_value)}</td>
                            <td class="text-end">${total.aging_730_plus_count}</td>
                            <td class="text-end">${formatIndianNumber(total.aging_730_plus_value)}</td>
                        </tr>
                    `);
                }
            });
        }
        
        function loadPartCategorySummary() {
            const filters = getFilters();
            const queryString = buildQueryString(filters);
            
            $.ajax({
                url: `/location-part-category-summary?${queryString}`,
                method: 'GET',
                success: function(response) {
                    const partCategories = response.part_categories;
                    let headerHtml = '<th>Location</th>';
                    partCategories.forEach(cat => {
                        headerHtml += `<th>${cat}</th>`;
                    });
                    headerHtml += '<th style="background-color: #ffc107; color: #000;">Total</th>';
                    $('#partCategoryHeaders').html(headerHtml);
                    
                    $('#partCategoryTable tbody').empty();
                    response.summary.forEach(row => {
                        let rowHtml = `<tr><td class="fw-bold">${row.location}</td>`;
                        let rowTotal = 0;
                        partCategories.forEach(cat => {
                            const value = row[cat] || 0;
                            rowHtml += `<td class="text-end">${formatIndianNumber(value)}</td>`;
                            rowTotal += value;
                        });
                        rowHtml += `<td class="text-end fw-bold" style="background-color: #fff3cd; color: #000;">${formatIndianNumber(rowTotal)}</td></tr>`;
                        $('#partCategoryTable tbody').append(rowHtml);
                    });
                    
                    let footerHtml = '<tr style="background-color: #ff9800; color: white;">';
                    footerHtml += '<td class="fw-bold" style="color: white; text-align: left; background-color: #ff9800; padding: 0.10rem 0.08rem; font-size: 0.85rem; border: 1px solid #ff5500;">Total</td>';
                    let grandTotal = 0;
                    partCategories.forEach(cat => {
                        const total = response.total[cat] || 0;
                        footerHtml += `<td class="fw-bold" style="color: white; text-align: right; background-color: #ff9800; padding: 0.10rem 0.08rem; font-size: 0.85rem; border: 1px solid #ff5500;">${formatIndianNumber(total)}</td>`;
                        grandTotal += total;
                    });
                    footerHtml += `<td class="fw-bold" style="color: white; text-align: right; background-color: #ff9800; padding: 0.10rem 0.08rem; font-size: 0.85rem; border: 1px solid #ff5500;">${formatIndianNumber(grandTotal)}</td></tr>`;
                    $('#partCategoryTable tfoot').html(footerHtml);
                }
            });
        }
        
        function loadData() {
            const filters = getFilters();
            const queryString = buildQueryString(filters);
            
            $.ajax({
                url: `/data?page=${currentPage}&per_page=${perPage}&${queryString}`,
                method: 'GET',
                success: function(response) {
                    $('#dataTable tbody').empty();
                    
                    response.data.forEach(row => {
                        $('#dataTable tbody').append(`
                            <tr>
                                <td>${row['Part No.'] || ''}</td>
                                <td>${row['Part Description'] || ''}</td>
                                <td>${row.Location || ''}</td>
                                <td>${row['Stock Qty'] || ''}</td>
                                <td>${row['Stock  at GNDP (Rs.) (In Lac)  '] || ''}</td>
                                <td>${row['Movement Category P (2)'] || ''}</td>
                            </tr>
                        `);
                    });
                    
                    updatePagination(response.total_pages, response.page);
                    $('#recordCount').text(`(${response.total_records} Records)`);
                    updateGNDP();
                }
            });
        }
        
        function updateGNDP() {
            const filters = getFilters();
            const queryString = buildQueryString(filters);
            $.ajax({
                url: `/calculate-gndp?${queryString}`,
                method: 'GET',
                success: function(response) {
                    $('#totalGndp').text(formatIndianNumber(response.total_gndp));
                }
            });
        }
        
        function updatePagination(totalPages, currentPage) {
            $('#pagination').empty();
            if (totalPages === 0) return;
            
            $('#pagination').append(`
                <li class="page-item ${currentPage === 1 ? 'disabled' : ''}">
                    <a class="page-link" href="#" data-page="${currentPage - 1}">Previous</a>
                </li>
            `);
            
            for (let i = 1; i <= totalPages; i++) {
                if (i === 1 || i === totalPages || (i >= currentPage - 2 && i <= currentPage + 2)) {
                    $('#pagination').append(`
                        <li class="page-item ${i === currentPage ? 'active' : ''}">
                            <a class="page-link" href="#" data-page="${i}">${i}</a>
                        </li>
                    `);
                } else if (i === currentPage - 3 || i === currentPage + 3) {
                    $('#pagination').append(`<li class="page-item disabled"><a class="page-link" href="#">...</a></li>`);
                }
            }
            
            $('#pagination').append(`
                <li class="page-item ${currentPage === totalPages ? 'disabled' : ''}">
                    <a class="page-link" href="#" data-page="${currentPage + 1}">Next</a>
                </li>
            `);
        }
        
        function setupEventListeners() {
            $('#applyFilters').click(function() {
                currentPage = 1;
                loadAllData();
            });
            
            $('#clearFilters').click(function() {
                $('.selectpicker').selectpicker('deselectAll');
                $('#partNumber').val('');
                currentPage = 1;
                loadAllData();
            });
            
            $(document).on('click', '.page-link', function(e) {
                e.preventDefault();
                const page = parseInt($(this).data('page'));
                if (page > 0) {
                    currentPage = page;
                    loadData();
                }
            });
            
            $('#downloadCsv').click(function() {
                const filters = getFilters();
                const queryString = buildQueryString(filters);
                window.location.href = `/download-csv?${queryString}`;
            });
            
            $('#downloadSummaryCsv').click(function() {
                const filters = getFilters();
                const queryString = buildQueryString(filters);
                window.location.href = `/download-summary-csv?${queryString}`;
            });
            
            $('#downloadPartCategoryCsv').click(function() {
                const filters = getFilters();
                const queryString = buildQueryString(filters);
                window.location.href = `/download-part-category-csv?${queryString}`;
            });
            
            $('#btnDeadStockCurrent').click(function() {
                const filters = getFilters();
                const queryString = buildQueryString(filters);
                window.location.href = `/download-dead-stock-csv?dead_stock_category=current_month_as_on_date&${queryString}`;
            });
            
            $('#btnDeadStockCurrentComplete').click(function() {
                const filters = getFilters();
                const queryString = buildQueryString(filters);
                window.location.href = `/download-dead-stock-csv?dead_stock_category=current_month_complete&${queryString}`;
            });
            
            $('#btnDeadStockLast').click(function() {
                const filters = getFilters();
                const queryString = buildQueryString(filters);
                window.location.href = `/download-dead-stock-csv?dead_stock_category=last_month&${queryString}`;
            });
            
            $('#btnDeadStockLastToLast').click(function() {
                const filters = getFilters();
                const queryString = buildQueryString(filters);
                window.location.href = `/download-dead-stock-csv?dead_stock_category=last_to_last_month&${queryString}`;
            });
            
            $('#btnDeadStockAll').click(function() {
                const filters = getFilters();
                const queryString = buildQueryString(filters);
                window.location.href = `/download-dead-stock-csv?dead_stock_category=all&${queryString}`;
            });
            
            $('#btnLastMonthLiquidation').click(function() {
                const filters = getFilters();
                const queryString = buildQueryString(filters);
                window.location.href = `/download-last-month-liquidation-csv?${queryString}`;
            });
        }
    </script>
</body>
</html>
"""

# ============= SERVER STARTUP =============

if __name__ == "__main__":
    hostname = socket.gethostname()
    try:
        local_ip = socket.gethostbyname(hostname)
    except:
        local_ip = "127.0.0.1"
    
    port = int(os.environ.get("PORT", 8004))
    
    print("\n" + "=" * 70)
    print(f"✅ Server ready! Access the dashboard at:")
    print(f"   🌐 Local: http://localhost:{port}")
    print(f"   🌐 Network: http://{local_ip}:{port}")
    print("=" * 70 + "\n")
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=port,
        log_level="info"
    )
