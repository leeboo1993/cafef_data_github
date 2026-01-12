# =====================================================
# 0️⃣ UNIVERSAL ENV LOADER
# =====================================================
from pathlib import Path
import os
import sys
import argparse

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
        "r2_path": "cafef_data/proprietary_trading/all_proprietary_trading.parquet",
        "start_date_default": "01/01/2023",
        "type": "range", 
    },
    # 2. Insider Trading (Giao dich Co dong & Noi bo)
    "insider": {
        "url": "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/GDCoDong.ashx",
        "r2_path": "cafef_data/insider_trading/all_insider_trading.parquet",
        "start_date_default": "01/01/2023",
        "type": "range",
    },
    # 3. Order Statistics (Thong ke Dat lenh -2)
    "order_stats": {
        "url": "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/ThongKeDL.ashx",
        "r2_path": "cafef_data/order_statistics/all_order_statistics.parquet",
        "start_date_default": "01/01/2023",
        "type": "range",
    },
    # 4. Tick Data (Khop lenh tung lenh -5)
    "tick_data": {
        "url": "https://msh-appdata.cafef.vn/rest-api/api/v1/MatchPrice",
        "r2_folder": "cafef_data/tick_data/",
        "file_pattern": "cafef_tick_data_{ddmmyy}.parquet",
        "type": "daily_consolidated",
    }
}

# =====================================================
# 3️⃣ HELPER FUNCTIONS
# =====================================================
def ensure_folder_exists_local(folder):
    if not os.path.exists(folder):
        os.makedirs(folder)

def get_tickers_from_latest_stock_price(local_mode=False):
    """Download tickers from R2 or look for local files."""
    bucket = os.getenv("R2_BUCKET")
    
    # 1. Local Mode Strategy
    if local_mode or not bucket:
        print("🕵️ Local Mode: Searching for stock price files in 'cafef_data/'...")
        local_dir = "cafef_data/"
        candidates = []
        if os.path.exists(local_dir):
            for root, dirs, files in os.walk(local_dir):
                for f in files:
                    if f.startswith("cafef_stock_price_") and f.endswith(".parquet"):
                        candidates.append(os.path.join(root, f))
        
        if candidates:
            # Use most recent file
            latest_file = max(candidates, key=lambda x: os.path.getmtime(x))
            print(f"📋 Using local ticker list from: {latest_file}")
            try:
                df = pd.read_parquet(latest_file, columns=["ticker"])
                return sorted(df["ticker"].unique().tolist())
            except Exception as e:
                print(f"⚠️ Error reading local tickers: {e}")
        
        print("⚠️ No local ticker file found. Using Top 30 Fallback.")
        return ["ACB", "BCM", "BID", "BVH", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG", 
                "MBB", "MSN", "MWG", "PLX", "POW", "SAB", "SHB", "SSB", "SSI", "STB", 
                "TCB", "TPB", "VCB", "VHM", "VIB", "VIC", "VJC", "VNM", "VPB", "VRE"]

    # 2. R2 Mode Strategy
    files = list_r2_files(bucket, "cafef_data/cafef_stock_price_")
    files = [f for f in files if f.endswith(".parquet")]
    if not files:
        print("❌ No stock price data found in R2.")
        return []
    
    def parse_date(f):
        m = re.search(r"(\d{6})\.parquet", f)
        if m: return datetime.strptime(m.group(1), "%d%m%y")
        return datetime.min
        
    latest_file = max(files, key=parse_date)
    print(f"📋 Using ticker list from R2: {latest_file}")
    
    local_path = "temp_tickers.parquet"
    if download_from_r2(bucket, latest_file, local_path):
        try:
            df = pd.read_parquet(local_path, columns=["ticker"])
            tickers = df["ticker"].unique().tolist()
            os.remove(local_path)
            return sorted(tickers)
        except Exception as e:
            if os.path.exists(local_path): os.remove(local_path)
            return []
    return []

# -----------------------------------------------------
# API FETCHERS
# -----------------------------------------------------
def fetch_cafef_range_data(url, symbol, start_date, end_date):
    """Fetch data from CafeF AJAX endpoint (for Prop, Insider, OrderStats)."""
    all_data = []
    page_index = 1
    page_size = 1000
    
    while True:
        try:
            params = {
                "Symbol": symbol,
                "StartDate": start_date,
                "EndDate": end_date,
                "PageIndex": page_index,
                "PageSize": page_size
            }
            r = requests.get(url, params=params, headers=HEADERS, timeout=10)
            if r.status_code != 200: break
            
            data = r.json()
            if not data: break
            
            # Common pattern: Data is in "Data" key
            rows = data.get("Data", [])
            if not rows: break

            all_data.extend(rows)
            
            if len(rows) < page_size: break
            page_index += 1
            time.sleep(0.05) 
            
        except Exception as e:
            # print(f"⚠️ Error fetching {symbol}: {e}")
            break
            
    return all_data

def fetch_tick_data(url, symbol, date_obj):
    """Fetch tick data for a specific symbol and date."""
    try:
        date_str = date_obj.strftime("%Y%m%d")
        params = {"symbol": symbol, "date": date_str}
        r = requests.get(url, params=params, headers=HEADERS, timeout=5)
        
        if r.status_code != 200: return None
        data = r.json()
        
        ticks = data.get("data", [])
        if not ticks: return None
        
        for t in ticks:
            t["ticker"] = symbol
            t["date"] = date_obj.strftime("%Y-%m-%d")
            
        return ticks
    except Exception as e:
        return None

# -----------------------------------------------------
# DATASET UPDATERS
# -----------------------------------------------------
def update_range_dataset(data_type, tickers, local_mode=False):
    config = CONFIG[data_type]
    bucket = os.getenv("R2_BUCKET")
    master_key = config["r2_path"]
    
    # Path handling
    local_master_path = master_key if local_mode else f"temp_{data_type}_master.parquet"
    if local_mode:
        ensure_folder_exists_local(os.path.dirname(local_master_path))

    # 1. Load Master
    df_master = pd.DataFrame()
    last_update_map = {}
    
    exists = False
    if local_mode:
        if os.path.exists(local_master_path): exists = True
    else:
        if list_r2_files(bucket, master_key): exists = True

    if exists:
        print(f"📥 Loading master file: {local_master_path if local_mode else master_key}")
        if not local_mode:
            download_from_r2(bucket, master_key, local_master_path)
            
        try:
            df_master = pd.read_parquet(local_master_path)
            
            # Ensure date column exists and is datetime
            date_col = "date"
            if date_col not in df_master.columns and "ngay" in df_master.columns:
                 df_master = df_master.rename(columns={"ngay": "date"})
            
            df_master["date"] = pd.to_datetime(df_master["date"])
            last_update_map = df_master.groupby("ticker")["date"].max().to_dict()
            print(f"✅ Loaded {len(df_master)} rows.")
        except Exception as e:
            print(f"⚠️ Master file corrupted: {e}. Starting fresh.")
            df_master = pd.DataFrame()
    else:
        print(f"✨ No existing file found. Creating new.")

    # 2. Fetch New Data
    print(f"🔄 Updating {len(tickers)} tickers for {data_type}...")
    new_rows = []
    today = datetime.now()
    today_str = today.strftime("%d/%m/%Y")
    
    for i, ticker in enumerate(tickers):
        last_date = last_update_map.get(ticker)
        if last_date:
            start_dt = last_date + timedelta(days=1)
            # Future protection
            if start_dt > today: continue
            start_str = start_dt.strftime("%d/%m/%Y")
        else:
            start_str = config["start_date_default"]
            
        # Only fetch if start <= today
        if datetime.strptime(start_str, "%d/%m/%Y") <= today:
            raw = fetch_cafef_range_data(config["url"], ticker, start_str, today_str)
            if raw:
                for row in raw:
                    row["ticker"] = ticker
                    new_rows.append(row)
        
        if (i + 1) % 50 == 0: print(f"   ⏳ Processed {i + 1}/{len(tickers)}...")

    # 3. Merge & Save
    if not new_rows:
        print("✅ No new data found.")
        if not local_mode and os.path.exists(local_master_path): os.remove(local_master_path)
        return

    print(f"📊 Found {len(new_rows)} new records.")
    df_new = pd.DataFrame(new_rows)
    df_new.columns = [c.lower() for c in df_new.columns]
    
    if "ngay" in df_new.columns: df_new = df_new.rename(columns={"ngay": "date"})
    if "date" in df_new.columns: df_new["date"] = pd.to_datetime(df_new["date"], dayfirst=True, errors='coerce')
    
    rename_map = {"symbol": "ticker", "giatrirong": "net_value", "khoiluongrong": "net_volume", "nguoithuchien": "executor"}
    df_new = df_new.rename(columns=rename_map)

    if df_master.empty:
        df_final = df_new
    else:
        df_final = pd.concat([df_master, df_new], ignore_index=True)
        
    if "date" in df_final.columns and "ticker" in df_final.columns:
        df_final = df_final.drop_duplicates()
        
    if local_mode:
        df_final.to_parquet(local_master_path, index=False)
        print(f"💾 Saved locally to: {local_master_path}")
    else:
        ensure_folder_exists(bucket, os.path.dirname(master_key))
        df_final.to_parquet(local_master_path, index=False)
        upload_to_r2(local_master_path, bucket, master_key)
        print(f"☁️ Uploaded to R2: {master_key}")
        if os.path.exists(local_master_path): os.remove(local_master_path)

def update_tick_data(tickers, target_date_obj, local_mode=False):
    config = CONFIG["tick_data"]
    bucket = os.getenv("R2_BUCKET")
    
    ddmmyy = target_date_obj.strftime("%d%m%y")
    filename = config["file_pattern"].format(ddmmyy=ddmmyy)
    r2_key = f"{config['r2_folder']}{filename}"
    local_path = f"cafef_data/tick_data/{filename}" if local_mode else f"temp_{filename}"
    
    should_skip = False
    if local_mode:
        if os.path.exists(local_path): should_skip = True
    else:
        if list_r2_files(bucket, r2_key): should_skip = True

    if should_skip:
        print(f"✅ Tick data for {ddmmyy} already exists.")
        return

    print(f"⚡ Fetching TICK DATA for {target_date_obj.strftime('%Y-%m-%d')}...")
    all_ticks = []
    
    for i, ticker in enumerate(tickers):
        ticks = fetch_tick_data(config["url"], ticker, target_date_obj)
        if ticks: all_ticks.extend(ticks)
        if (i + 1) % 100 == 0: 
            print(f"   ⏱️ Processed {i+1}/{len(tickers)} ({len(all_ticks)} ticks)")
            time.sleep(0.5)

    if not all_ticks:
        print(f"🔸 No tick data found for {ddmmyy}.")
        return

    print(f"📦 Saving {len(all_ticks)} ticks...")
    df = pd.DataFrame(all_ticks)
    
    if local_mode:
        ensure_folder_exists_local(os.path.dirname(local_path))
        df.to_parquet(local_path, index=False)
        print(f"💾 Saved locally: {local_path}")
    else:
        ensure_folder_exists(bucket, config["r2_folder"])
        df.to_parquet(local_path, index=False)
        upload_to_r2(local_path, bucket, r2_key)
        os.remove(local_path)
        print(f"☁️ Uploaded: {r2_key}")

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
    
    args = parser.parse_args()

    # Update configs
    start_date_str = f"01/01/{args.start_year}"
    for key in ["proprietary", "insider", "order_stats"]:
        CONFIG[key]["start_date_default"] = start_date_str
    
    if args.local:
        print("🏠 RUNNING IN LOCAL MODE")
        print("   - Using local folder 'cafef_data/' for storage")
        print("   - Ignoring R2 upload/download")

    # 1. Get Tickers
    tickers = get_tickers_from_latest_stock_price(local_mode=args.local)
    if not tickers:
        print("⚠️ No tickers found (and fallback failed). Aborting.")
        return
    print(f"📋 Loaded {len(tickers)} tickers.")

    # 2. Update Range Data
    print("\n=== UPDATING RANGE DATASETS ===")
    update_range_dataset("proprietary", tickers, local_mode=args.local)
    update_range_dataset("insider", tickers, local_mode=args.local)
    update_range_dataset("order_stats", tickers, local_mode=args.local)

    # 3. Update Tick Data
    print("\n=== UPDATING TICK DATA ===")
    
    if args.tick_start_date:
        print(f"🚨 STARTING DEEP BACKFILL FROM {args.tick_start_date}")
        try:
            start_dt = datetime.strptime(args.tick_start_date, "%Y-%m-%d")
            end_dt = datetime.now()
            delta = end_dt - start_dt
            
            for i in range(delta.days + 1):
                target_date = start_dt + timedelta(days=i)
                if target_date.weekday() < 5:
                    print(f"\nProcessing Date: {target_date.strftime('%Y-%m-%d')}")
                    update_tick_data(tickers, target_date, local_mode=args.local)
                else:
                    print(f"Skipping weekend: {target_date.strftime('%Y-%m-%d')}")
        except ValueError:
            print("❌ Invalid date format.")
            
    else:
        # Today
        today = datetime.now()
        if today.weekday() < 5:
            update_tick_data(tickers, today, local_mode=args.local)
        
        # Short backfill
        if args.tick_backfill_days > 0:
            print(f"🔙 Checking backfill for {args.tick_backfill_days} days...")
            for i in range(1, args.tick_backfill_days + 1):
                past_date = today - timedelta(days=i)
                if past_date.weekday() < 5:
                    update_tick_data(tickers, past_date, local_mode=args.local)

if __name__ == "__main__":
    run()
