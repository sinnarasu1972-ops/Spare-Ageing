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

# ============= GLOBAL VARIABLES =============
excel_file_path = "./Spares Ageing Report.xlsx"
csv_file_path = "./Spares Ageing Report_Processed.csv"
accessories_model_file = "./Accessories_Model.xlsx"
last_file_modified = None
last_reload_time = None
df = None
total_gndp = 0
gndp_column = None
excel_error = None
accessories_mapping = {}

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

def format_df_for_export(df_to_export):
    """Format dataframe for CSV export - fix scientific notation in GNDP column"""
    export_df = df_to_export.copy()
    
    if gndp_column and gndp_column in export_df.columns:
        export_df[gndp_column] = export_df[gndp_column].apply(
            lambda x: f"{float(x):.7f}" if pd.notna(x) and x != '' else x
        )
    
    return export_df

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

def load_accessories_mapping():
    """Load accessories model mapping from Excel file"""
    global accessories_mapping
    try:
        if os.path.exists(accessories_model_file):
            acc_df = pd.read_excel(accessories_model_file)
            for _, row in acc_df.iterrows():
                part_prefix = str(row['PART NO ']).strip().upper()
                vehicle_details = str(row['Vehicle Details']).strip()
                accessories_mapping[part_prefix] = vehicle_details
            print(f"✓ Loaded {len(accessories_mapping)} accessories model mappings")
            return True
        else:
            print(f"⚠️  Accessories model file not found: {accessories_model_file}")
            return False
    except Exception as e:
        print(f"⚠️  Error loading accessories mapping: {e}")
        return False

def get_model_group(part_no):
    """Get model group based on part number prefix"""
    if pd.isna(part_no) or part_no == "":
        return ""
    
    part_str = str(part_no).strip().upper()
    
    for prefix_len in [4, 3, 2]:
        if len(part_str) >= prefix_len:
            prefix = part_str[:prefix_len]
            if prefix in accessories_mapping:
                return accessories_mapping[prefix]
    
    return ""

# ============= EXCEL PROCESSING =============

def process_excel_to_csv():
    """Process Excel file to CSV with all calculations"""
    input_file = excel_file_path
    output_csv = csv_file_path
    
    print("Processing Excel file to CSV...")
    
    if not os.path.exists(input_file):
        print(f"⚠️  File not found: {input_file}")
        return None, 0, None
    
    try:
        df = pd.read_excel(input_file)
        print(f"Successfully loaded {len(df)} rows from Excel")
    except Exception as e:
        print(f"⚠️  Error reading Excel file: {e}")
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
        print("⚠️  Could not find required columns")
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
    
    print("\nAdding Model Group column...")
    load_accessories_mapping()
    
    part_no_col_local = None
    for col in df.columns:
        if 'part' in str(col).lower() and 'no' in str(col).lower() and 'description' not in str(col).lower():
            part_no_col_local = col
            break
    
    if part_no_col_local:
        df['Model Group'] = df[part_no_col_local].apply(get_model_group)
        print(f"✓ Model Group column added based on {part_no_col_local}")
    else:
        df['Model Group'] = ""
        print("⚠️  Part No column not found, Model Group column will be empty")
    
    try:
        df.to_csv(output_csv, index=False)
        print(f"\n✓ Processed data saved to CSV: {output_csv}")
    except Exception as e:
        print(f"⚠️  Error saving CSV: {e}")
        return None, 0, None
    
    return output_csv, total_gndp_calc, gndp_column_local

# ============= FASTAPI APP SETUP =============

app = FastAPI(title="Spare Parts Dashboard", version="2.1")

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
    """)

app.mount("/static", StaticFiles(directory="static"), name="static")

print("\n" + "=" * 70)
print("STARTING SPARE PARTS AGEING DASHBOARD - VERSION 2.1 (DATE RANGE REMOVED)")
print("=" * 70)

try:
    csv_file, total_gndp, gndp_column = process_excel_to_csv()
    
    if csv_file is None:
        print("\n⚠️  Excel file not processed successfully")
        excel_error = "Excel file not found or has errors. Please add 'Spares Ageing Report.xlsx' to the project root."
        df = None
    else:
        try:
            df = pd.read_csv(csv_file)
            print(f"\n✓ Successfully loaded {len(df)} rows from CSV")
        except Exception as e:
            print(f"\n⚠️  Error loading CSV: {e}")
            excel_error = f"Error loading data: {str(e)}"
            df = None
            
except Exception as e:
    print(f"\n⚠️  Unexpected error: {e}")
    excel_error = f"Error processing Excel file: {str(e)}"
    df = None

last_reload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
last_file_modified = get_file_modified_time(excel_file_path)

if df is not None:
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
    
    # ============= DEBUG: Print all column names =============
    print("\n📋 ============ EXCEL FILE COLUMNS ============")
    print("All columns in your Excel file:")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i}. {col}")
    
    print("\nSearching for key columns:")
    print(f"  ✓ Last Issue Date: {last_issue_col}")
    print(f"  ✓ Last Purchase Date: {last_purchase_col}")
    print(f"  ✓ Location: {location_col}")
    print(f"  ✓ Part No: {part_no_col}")
    print(f"  ✓ Part Category: {part_category_col}")
    print(f"  ✓ ABC Category: {abc_col}")
    print(f"  ✓ RIS: {ris_col}")
    print(f"  ✓ Stock Qty: {stock_qty_col}")
    print(f"  ✓ GNDP Column: {gndp_column}")
    print("==========================================\n")
    # ============= END DEBUG =============
    
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
else:
    print(f"\n⚠️  {excel_error}")

# ============= API ENDPOINTS =============

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok", "records": len(df) if df is not None else 0, "timestamp": datetime.now().isoformat()}

@app.get("/")
async def dashboard():
    """Main dashboard endpoint"""
    
    if df is None:
        return HTMLResponse(content=f"""
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
                            <li>Add <code>Spares Ageing Report.xlsx</code> to your project root</li>
                            <li>Commit and push to GitHub</li>
                            <li>Render will auto-redeploy automatically</li>
                            <li>Refresh this page</li>
                        </ol>
                    </div>
                    <p class="text-muted small">Last check: {last_reload_time}</p>
                </div>
            </div>
        </body>
        </html>
        """)
    
    html_file = Path("index.html")
    if not html_file.exists():
        return HTMLResponse(content="<h1>Error: HTML template not found at index.html</h1>")
    
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
    if df is None:
        return {"error": "Data not available"}
    
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
    """
    Get part category summary with robust error handling
    """
    try:
        if df is None:
            return {"summary": [], "total": {}, "part_categories": []}
        
        if not part_category_col:
            print("⚠️  Part Category column not detected")
            return {
                "summary": [],
                "total": {},
                "part_categories": [],
                "warning": "Part Category column not found - check Excel column names"
            }
        
        if part_category_col not in df.columns:
            print(f"❌ Part Category column '{part_category_col}' not in DataFrame columns")
            return {
                "summary": [],
                "total": {},
                "part_categories": [],
                "error": f"Column not found: {part_category_col}"
            }
        
        try:
            filtered_df = apply_filters(
                df.copy(), 
                movement_category, part_category, location, 
                abc_category, ris, part_number
            )
        except Exception as e:
            print(f"❌ Filter error: {e}")
            filtered_df = df.copy()
        
        try:
            all_part_categories = sorted([
                str(x).strip() 
                for x in filtered_df[part_category_col].dropna().unique() 
                if pd.notna(x) and str(x).strip() != ''
            ])
            print(f"✅ Found {len(all_part_categories)} part categories")
        except Exception as e:
            print(f"❌ Error extracting part categories: {e}")
            all_part_categories = []
        
        if not location_col or location_col not in filtered_df.columns:
            print(f"⚠️  Location column not found: {location_col}")
            return {
                "summary": [],
                "total": {},
                "part_categories": all_part_categories,
                "warning": "Location column not found"
            }
        
        if not gndp_column or gndp_column not in filtered_df.columns:
            print(f"⚠️  GNDP column not found: {gndp_column}")
            return {
                "summary": [],
                "total": {},
                "part_categories": all_part_categories,
                "warning": "GNDP Value column not found"
            }
        
        summary_data = []
        try:
            for loc in sorted(filtered_df[location_col].dropna().unique()):
                if pd.isna(loc):
                    continue
                
                loc_df = filtered_df[filtered_df[location_col] == loc]
                row_data = {'location': str(loc)}
                total_value = 0
                
                for part_cat in all_part_categories:
                    try:
                        cat_df = loc_df[loc_df[part_category_col] == part_cat]
                        value = float(cat_df[gndp_column].sum()) if len(cat_df) > 0 else 0.0
                    except Exception as e:
                        print(f"⚠️  Error for {loc}/{part_cat}: {e}")
                        value = 0.0
                    
                    row_data[part_cat] = value
                    total_value += value
                
                row_data['total'] = total_value
                summary_data.append(row_data)
            
            print(f"✅ Processed {len(summary_data)} locations")
        
        except Exception as e:
            print(f"❌ Error processing locations: {e}")
            import traceback
            traceback.print_exc()
        
        total_row = {'location': 'TOTAL'}
        grand_total = 0.0
        
        try:
            for part_cat in all_part_categories:
                col_total = sum(row.get(part_cat, 0) for row in summary_data)
                total_row[part_cat] = col_total
                grand_total += col_total
            
            total_row['total'] = grand_total
            print(f"✅ Grand Total: {grand_total}")
        
        except Exception as e:
            print(f"❌ Error calculating totals: {e}")
        
        result = {
            "summary": summary_data,
            "total": total_row,
            "part_categories": all_part_categories
        }
        
        print(f"✅ location_part_category_summary returned successfully")
        return result
    
    except Exception as e:
        print(f"❌ CRITICAL ERROR in location_part_category_summary:")
        print(f"   {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            "summary": [],
            "total": {},
            "part_categories": [],
            "error": f"Failed to load Part Category data: {str(e)}"
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
    if df is None:
        return {"error": "Data not available"}
    
    filtered_df = apply_filters(df.copy(), movement_category, part_category, location, abc_category, ris, part_number)
    
    filtered_df = format_df_for_export(filtered_df)
    
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
    if df is None:
        return {"error": "Data not available"}
    
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
            '0-90 Days Value (Rs.)': round(sum(row['0-90 Days Value (Rs.)'] for row in summary_data), 2),
            '91-180 Days Count': sum(row['91-180 Days Count'] for row in summary_data),
            '91-180 Days Value (Rs.)': round(sum(row['91-180 Days Value (Rs.)'] for row in summary_data), 2),
            '181-365 Days Count': sum(row['181-365 Days Count'] for row in summary_data),
            '181-365 Days Value (Rs.)': round(sum(row['181-365 Days Value (Rs.)'] for row in summary_data), 2),
            '366-730 Days Count': sum(row['366-730 Days Count'] for row in summary_data),
            '366-730 Days Value (Rs.)': round(sum(row['366-730 Days Value (Rs.)'] for row in summary_data), 2),
            '730+ Days Count': sum(row['730+ Days Count'] for row in summary_data),
            '730+ Days Value (Rs.)': round(sum(row['730+ Days Value (Rs.)'] for row in summary_data), 2),
        }
        summary_data.append(total_row)
    
    summary_df = pd.DataFrame(summary_data)
    
    value_cols = [col for col in summary_df.columns if 'Value' in col]
    for col in value_cols:
        summary_df[col] = summary_df[col].apply(lambda x: round(float(x), 2) if pd.notna(x) else 0)
    
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
    if df is None:
        return {"error": "Data not available"}
    
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
    if df is None:
        return {"error": "Data not available"}
    
    filtered_df = df.copy()
    
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
    
    if dead_stock_category == "current_month_as_on_date":
        mask = get_dead_stock_mask(filtered_df, pd.Timestamp(current_month_last_year_start), pd.Timestamp(current_month_last_year_end))
        result_df = filtered_df[mask]
        category_name = "CurrentMonth_AsOnDate"
        
    elif dead_stock_category == "current_month_complete":
        current_month_complete_end = current_month_last_year_start.replace(
            month=current_month_last_year_start.month + 1 if current_month_last_year_start.month < 12 else 1, 
            year=current_month_last_year_start.year + (1 if current_month_last_year_start.month == 12 else 0)
        ) - timedelta(days=1)
        mask = get_dead_stock_mask(filtered_df, pd.Timestamp(current_month_last_year_start), pd.Timestamp(current_month_complete_end))
        result_df = filtered_df[mask]
        category_name = "CurrentMonth_Complete"
        
    elif dead_stock_category == "last_month":
        mask = get_dead_stock_mask(filtered_df, pd.Timestamp(last_month_last_year_start), pd.Timestamp(last_month_last_year_end))
        result_df = filtered_df[mask]
        category_name = "LastMonth"
        
    elif dead_stock_category == "last_to_last_month":
        mask = get_dead_stock_mask(filtered_df, pd.Timestamp(last_to_last_month_last_year_start), pd.Timestamp(last_to_last_month_last_year_end))
        result_df = filtered_df[mask]
        category_name = "LastToLastMonth"
        
    else:
        result_df = filtered_df[filtered_df['Is Dead Stock'] == True]
        category_name = "AllDeadStock"
    
    current_datetime = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    locations_filter = location.split(',') if location and location.strip() else []
    location_part = "_".join(locations_filter) if locations_filter else "All_Locations"
    location_part = location_part.replace(" ", "_").replace("/", "-").replace("\\", "-")
    
    filename = f"DeadStock_{category_name}_{location_part}_{current_datetime}.csv"
    reports_dir = "./Reports/Dead_Stock"
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)
    
    result_df = format_df_for_export(result_df)
    
    output_path = os.path.join(reports_dir, filename)
    result_df.to_csv(output_path, index=False)
    
    print(f"✓ Exported {len(result_df)} dead stock records for category: {dead_stock_category}")
    
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
    if df is None:
        return {"error": "Data not available"}
    
    filtered_df = df.copy()
    
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
    except Exception as e:
        print(f"Error calculating last month liquidation: {e}")
        lml_df = pd.DataFrame()
    
    current_datetime = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    locations_filter = location.split(',') if location and location.strip() else []
    location_part = "_".join(locations_filter) if locations_filter else "All_Locations"
    location_part = location_part.replace(" ", "_").replace("/", "-").replace("\\", "-")
    
    filename = f"LastMonth_Liquidation_{location_part}_{current_datetime}.csv"
    reports_dir = "./Reports/Liquidation"
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)
    
    lml_df = format_df_for_export(lml_df)
    
    output_path = os.path.join(reports_dir, filename)
    lml_df.to_csv(output_path, index=False)
    
    print(f"✓ Exported {len(lml_df)} last month liquidation records")
    
    return FileResponse(path=output_path, filename=filename, media_type='text/csv')

# ============= SERVER STARTUP =============

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8004))
    uvicorn.run(app, host="0.0.0.0", port=port)
