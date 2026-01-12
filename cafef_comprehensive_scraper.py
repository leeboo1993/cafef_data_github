# =====================================================
# 0️⃣ UNIVERSAL ENV LOADER
# =====================================================
from pathlib import Path
import os
import sys
import argparse
import json

def load_env_safely():
    """Load .env for local runs, skip if env vars already exist."""
    if not os.getenv("R2_BUCKET"):
        try:
            from dotenv import load_dotenv
            env_path = Path(__file__).resolve().parent / ".env"
            if env_path.exists():
                load_dotenv(env_path)
        except ImportError:
            pass

load_env_safely()

# =====================================================
# 1️⃣ IMPORTS
# =====================================================
import re
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import R2 utils conditionally or mock
try:
    from utils_r2 import upload_to_r2, download_from_r2, list_r2_files, ensure_folder_exists
except ImportError:
    # Mocks for when utils_r2 is missing entirely (unlikely but safe)
    def upload_to_r2(*args): pass
    def download_from_r2(*args): return False
    def list_r2_files(*args): return []
    def ensure_folder_exists(*args): pass

# =====================================================
# 2️⃣ CONFIGURATION
# =====================================================
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://cafef.vn/"
}

CONFIG = {
    # 1. Proprietary Trading (Giao dich Tu Doanh)
    "proprietary": {
        "url": "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/GDTuDoanh.ashx",
        "r2_folder": "cafef_data/proprietary_trading/",
        "file_prefix": "all_proprietary_trading",
        "start_date_default": "01/01/2023",
        "data_key_chain": ["Data", "Data", "ListDataTudoanh"],
    },
    # 2. Insider Trading (Giao dich Co dong & Noi bo)
    "insider": {
        "url": "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/GDCoDong.ashx",
        "r2_folder": "cafef_data/insider_trading/",
        "file_prefix": "all_insider_trading",
        "start_date_default": "01/01/2023",
        "data_key_chain": ["Data", "Data"],
    },
    # 3. Order Statistics (Thong ke Dat lenh -2)
    "order_stats": {
        "url": "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/ThongKeDL.ashx",
        "r2_folder": "cafef_data/order_statistics/",
        "file_prefix": "all_order_statistics",
        "start_date_default": "01/01/2023",
         "data_key_chain": ["Data", "Data"],
    },
    # 4. Tick Data / Price Distribution
    "tick_data": {
        "url": "https://msh-appdata.cafef.vn/rest-api/api/v1/MatchPrice",
        "r2_folder": "cafef_data/tick_data/",
        "file_pattern": "cafef_tick_data_{ddmmyy}.parquet",
    },
    "price_distribution": {
        "url": "https://msh-appdata.cafef.vn/rest-api/api/v1/MatchPrice", # Same Endpoint
        "r2_folder": "cafef_data/price_distribution/",
        "file_pattern": "cafef_price_distribution_{ddmmyy}.parquet",
    }
}

# ... (Helper Functions remain same) ...

def find_latest_master_file(folder, prefix, local_mode=False, bucket=None):
    """Find the latest master file pattern: prefix_upto_YYYYMMDD.parquet"""
    candidates = []
    
    if local_mode:
        if os.path.exists(folder):
            for f in os.listdir(folder):
                if f.startswith(prefix) and f.endswith(".parquet"):
                    candidates.append(os.path.join(folder, f))
    else:
        files = list_r2_files(bucket, folder)
        for f in files:
            # R2 keys include folder path
            fname = os.path.basename(f)
            if fname.startswith(prefix) and fname.endswith(".parquet"):
                candidates.append(f) # f is full key
                
    if not candidates:
        return None
        
    # Sort by name (which includes date if formatted YYYYMMDD) or just modification time
    # Actually, we should try to parse the date from filename if possible, or fallback to simple sort
    # Filename format expected: all_xyz_upto_20250101.parquet
    # Should sort correctly alphabetically if YYYYMMDD is used.
    candidates.sort() 
    return candidates[-1]

def fetch_tick_data_raw(url, symbol, date_obj):
    """Fetch raw JSON from MatchPrice endpoint."""
    try:
        date_str = date_obj.strftime("%Y%m%d")
        params = {"symbol": symbol, "date": date_str}
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        
        if r.status_code != 200: return None
        return r.json()
    except Exception as e:
        return None

# ... (Previous Helper Functions)

def parse_asp_date(x):
    """Parse ASP.NET JSON Date format: /Date(1744909200000)/"""
    if pd.isna(x) or not isinstance(x, str):
        return x
    
    # Check for /Date(123123123)/ format
    m = re.search(r"/Date\((\d+)\)/", x)
    if m:
        try:
            ts = int(m.group(1)) / 1000
            return datetime.fromtimestamp(ts)
        except:
            return pd.NaT
            
    return x

def clean_dataframe_dates(df):
    """Clean all date-like columns in the dataframe."""
    # Columns that definitely contain dates based on Observation
    date_keywords = ["date", "ngay", "time", "day"]
    
    for col in df.columns:
        lower_col = col.lower()
        if any(k in lower_col for k in date_keywords):
            # Try to clean
            # First, if it's object type, try ASP parsing
            if df[col].dtype == object:
                # check first non-null
                first_valid = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if isinstance(first_valid, str) and "/Date(" in first_valid:
                    print(f"   🔧 Parsing ASP.NET dates in column: {col}")
                    df[col] = df[col].apply(parse_asp_date)
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # If it looks like '26.4 ( -0.75 %)', it's not a date, but 'date' might be key.
                # Actually, standard string dates might technically be there too.
                # Let's rely on pd.to_datetime for standard formats if not ASP
                elif "date" in lower_col or "ngay" in lower_col:
                     df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

    return df

# ... (Previous API Fetchers)

# ... (Previous Worker Functions)

def update_range_dataset(data_type, tickers, local_mode=False, max_workers=10):
    config = CONFIG[data_type]
    bucket = os.getenv("R2_BUCKET")
    folder = config["r2_folder"]
    prefix = config["file_prefix"]
    
    # Local folder handling
    if local_mode:
        ensure_folder_exists_local(folder)
        
    # 1. Find and Load Master
    df_master = pd.DataFrame()
    last_update_map = {}
    
    latest_file_path = find_latest_master_file(folder, prefix, local_mode, bucket)
    
    if latest_file_path:
        print(f"📥 Loading existing master: {latest_file_path}")
        local_load_path = latest_file_path
        
        if not local_mode:
            local_load_path = f"temp_master_{data_type}.parquet"
            download_from_r2(bucket, latest_file_path, local_load_path)
            
        try:
            df_master = pd.read_parquet(local_load_path)
            
            # Normalize date col
            if "ngay" in df_master.columns: df_master = df_master.rename(columns={"ngay": "date"})
            if "date" in df_master.columns:
                df_master["date"] = pd.to_datetime(df_master["date"])
                last_update_map = df_master.groupby("ticker")["date"].max().to_dict()
                print(f"✅ Loaded history: {len(df_master)} rows (Latest date: {df_master['date'].max().strftime('%Y-%m-%d')})")
            else:
                print("⚠️ Master file missing date column. Starting fresh.")
                df_master = pd.DataFrame()
        except Exception as e:
            print(f"⚠️ Corrupted master file: {e}. Starting fresh.")
            df_master = pd.DataFrame()
            
        # Cleanup temp download
        if not local_mode and os.path.exists(local_load_path):
            os.remove(local_load_path)
    else:
        print(f"✨ No history found. Starting fresh.")

    # 2. Parallel Fetch
    print(f"🔄 Updating {len(tickers)} tickers for {data_type} (Workers: {max_workers})...")
    new_rows = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_ticker_range_data, t, config, last_update_map): t for t in tickers}
        
        completed_count = 0
        for future in as_completed(futures):
            res = future.result()
            if res:
                new_rows.extend(res)
            
            completed_count += 1
            if completed_count % 100 == 0:
                print(f"   ⏳ Processed {completed_count}/{len(tickers)}...")

    # 3. Merge & Save
    if not new_rows:
        print("✅ No new data found. Master file remains unchanged.")
        return

    print(f"📊 New records found: {len(new_rows)}")
    df_new = pd.DataFrame(new_rows)
    df_new.columns = [c.lower() for c in df_new.columns]
    
    if "ngay" in df_new.columns: df_new = df_new.rename(columns={"ngay": "date"})
    if "date" in df_new.columns: df_new["date"] = pd.to_datetime(df_new["date"], dayfirst=True, errors='coerce')
    
    # CLEAN ASP DATES HERE
    df_new = clean_dataframe_dates(df_new)

    rename_map = {"symbol": "ticker", "giatrirong": "net_value", "khoiluongrong": "net_volume", "nguoithuchien": "executor"}
    df_new = df_new.rename(columns=rename_map)

    if df_master.empty:
        df_final = df_new
    else:
        df_final = pd.concat([df_master, df_new], ignore_index=True)
        
    if "date" in df_final.columns and "ticker" in df_final.columns:
        df_final = df_final.drop_duplicates()
        
    # Determine new filename with latest date coverage
    max_date = datetime.now()
    if "date" in df_final.columns and not df_final.empty:
        # Check if Nat
        valid_dates = df_final["date"].dropna()
        if not valid_dates.empty:
            max_date = valid_dates.max()
        
    new_filename = f"{prefix}_upto_{max_date.strftime('%Y%m%d')}.parquet"
    
    if local_mode:
        new_path = os.path.join(folder, new_filename)
        df_final.to_parquet(new_path, index=False)
        print(f"💾 Updated master saved: {new_path}")
        
        # Cleanup old local file if name changed
        if latest_file_path and latest_file_path != new_path:
            try:
                os.remove(latest_file_path)
                print(f"🗑️ Removed old master: {latest_file_path}")
            except: pass
    else:
        temp_path = f"temp_{new_filename}"
        r2_key = f"{folder}{new_filename}"
        df_final.to_parquet(temp_path, index=False)
        upload_to_r2(temp_path, bucket, r2_key)
        print(f"☁️ Uploaded new master: {r2_key}")
        os.remove(temp_path)
        
        # Cleanup old R2 file
        if latest_file_path and latest_file_path != r2_key:
             pass 

# ... (update_tick_data and run functions remain same) ...

def update_tick_data(tickers, target_date_obj, local_mode=False, max_workers=10, summary_only=False):
    # Select mode
    mode_key = "price_distribution" if summary_only else "tick_data"
    config = CONFIG[mode_key]
    bucket = os.getenv("R2_BUCKET")
    
    date_str = target_date_obj.strftime('%Y-%m-%d')
    ddmmyy = target_date_obj.strftime("%d%m%y")
    filename = config["file_pattern"].format(ddmmyy=ddmmyy)
    r2_key = f"{config['r2_folder']}{filename}"
    local_path = f"cafef_data/{mode_key}/{filename}" if local_mode else f"temp_{filename}"
    
    should_skip = False
    if local_mode:
        if os.path.exists(local_path): should_skip = True
    else:
        if list_r2_files(bucket, r2_key): should_skip = True

    if should_skip:
        print(f"[{date_str}] ✅ {mode_key} already exists.")
        return

    print(f"[{date_str}] ⚡ Fetching {mode_key.upper()} (Workers: {max_workers})...")
    all_rows = []
    
    def process_one(ticker):
        raw = fetch_tick_data_raw(config["url"], ticker, target_date_obj)
        if not raw: return []
        
        if summary_only:
            # Extract Aggregates
            # Keys: price, totalVolume, volPercent
            rows = raw.get("aggregates", [])
        else:
            # Extract Ticks
            # Keys: id, time, price, volume, etc.
            rows = raw.get("data", [])
            
        for r in rows:
            r["ticker"] = ticker
            r["date"] = date_str
        return rows

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_one, t): t for t in tickers}
        
        completed_count = 0
        for future in as_completed(futures):
            res = future.result()
            if res:
                all_rows.extend(res)
            
            completed_count += 1
            if completed_count % 500 == 0:
                print(f"   [{date_str}] ⏱️ Processed {completed_count}/{len(tickers)}...")

    if not all_rows:
        print(f"[{date_str}] 🔸 No data found.")
        return

    print(f"[{date_str}] 📦 Saving {len(all_rows)} records...")
    df = pd.DataFrame(all_rows)
    
    # Standardize columns for distribution
    if summary_only:
        # cafeF returns: price, totalVolume, volPercent
        # let's rename to something nice
        rename_map = {
            "totalVolume": "volume",
            "volPercent": "percentage"
        }
        df = df.rename(columns=rename_map)

    if local_mode:
        ensure_folder_exists_local(os.path.dirname(local_path))
        df.to_parquet(local_path, index=False)
        print(f"[{date_str}] 💾 Saved locally: {local_path}")
    else:
        ensure_folder_exists(bucket, config["r2_folder"])
        df.to_parquet(local_path, index=False)
        upload_to_r2(local_path, bucket, r2_key)
        os.remove(local_path)
        print(f"[{date_str}] ☁️ Uploaded: {r2_key}")

# =====================================================
# 4️⃣ RUNNER
# =====================================================
def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="daily", help="daily or backfill")
    parser.add_argument("--tick-backfill-days", type=int, default=0, help="Days back to check for tick data")
    parser.add_argument("--start-year", type=int, default=2023, help="Start year for Range Data")
    parser.add_argument("--tick-start-date", type=str, default=None, help="YYYY-MM-DD start for Tick Backfill")
    parser.add_argument("--local", action="store_true", help="Run local only (Save to disk, skip R2)")
    parser.add_argument("--workers", type=int, default=10, help="Number of parallel workers per task")
    parser.add_argument("--parallel-days", type=int, default=1, help="Number of tick-data days to run in parallel")
    parser.add_argument("--summary-only", action="store_true", help="Download Price Distribution SUMMARY instead of full ticks")
    
    args = parser.parse_args()

    # Update configs
    start_date_str = f"01/01/{args.start_year}"
    for key in ["proprietary", "insider", "order_stats"]:
        CONFIG[key]["start_date_default"] = start_date_str
    
    if args.local:
        print(f"🏠 RUNNING IN LOCAL MODE (Workers/Task: {args.workers}, Parallel Days: {args.parallel_days})")
        if args.summary_only:
            print("📊 MODE: PRICE DISTRIBUTION SUMMARY (Lightweight)")
        else:
            print("📈 MODE: FULL TICK DATA (Heavy)")

    # 1. Get Tickers
    tickers = get_tickers_from_latest_stock_price(local_mode=args.local)
    if not tickers:
        print("⚠️ No tickers found (and fallback failed). Aborting.")
        return
    print(f"📋 Loaded {len(tickers)} tickers.")

    # 2. Update Range Data (Parallel Categories)
    print("\n=== UPDATING RANGE DATASETS (PARALLEL) ===")
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for dtype in ["proprietary", "insider", "order_stats"]:
            print(f"🚀 Starting task: {dtype}")
            futures.append(executor.submit(update_range_dataset, dtype, tickers, args.local, args.workers))
        
        for future in as_completed(futures):
            # Just wait for completion, exceptions are printed inside
            pass

    # 3. Update Tick Data OR Summary Data
    label = "PRICE DISTRIBUTION" if args.summary_only else "TICK DATA"
    print(f"\n=== UPDATING {label} ===")
    
    dates_to_process = []
    
    if args.tick_start_date:
        print(f"🚨 STARTING DEEP BACKFILL FROM {args.tick_start_date}")
        try:
            start_dt = datetime.strptime(args.tick_start_date, "%Y-%m-%d")
            end_dt = datetime.now()
            delta = end_dt - start_dt
            
            for i in range(delta.days + 1):
                target_date = start_dt + timedelta(days=i)
                if target_date.weekday() < 5:
                    dates_to_process.append(target_date)
        except ValueError:
            print("❌ Invalid date format.")
            return
            
    else:
        # Today
        today = datetime.now()
        if today.weekday() < 5:
            dates_to_process.append(today)
        
        # Short backfill
        if args.tick_backfill_days > 0:
            print(f"🔙 Checking backfill for {args.tick_backfill_days} days...")
            for i in range(1, args.tick_backfill_days + 1):
                past_date = today - timedelta(days=i)
                if past_date.weekday() < 5:
                    dates_to_process.append(past_date)

    if not dates_to_process:
        print("✅ No dates to process.")
        return

    print(f"📅 Processing {len(dates_to_process)} days (Parallel Days: {args.parallel_days})...")
    
    with ThreadPoolExecutor(max_workers=args.parallel_days) as day_executor:
        day_futures = [
            day_executor.submit(update_tick_data, tickers, dt, args.local, args.workers, args.summary_only) 
            for dt in dates_to_process
        ]
        for f in as_completed(day_futures):
            pass

if __name__ == "__main__":
    run()
