# =====================================================
# 0️⃣ UNIVERSAL ENV LOADER
# =====================================================
from pathlib import Path
import os
import sys

def load_env_safely():
    """Load .env for local runs, skip if env vars already exist."""
    if not os.getenv("R2_BUCKET"):
        try:
            from dotenv import load_dotenv
            env_path = Path(__file__).resolve().parent / ".env"
            if env_path.exists():
                load_dotenv(env_path)
                print("🔧 Loaded .env file (local environment).")
            else:
                print("⚠️ No .env found — expecting environment variables.")
        except ImportError:
            pass
    else:
        print("✅ Environment variables already available (CI/CD).")

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
from utils_r2 import upload_to_r2, download_from_r2, list_r2_files, ensure_folder_exists
import argparse

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
        "type": "range", # ranges per stock
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
        "type": "daily_consolidated", # ONE file for all stocks per day
    }
}

# =====================================================
# 3️⃣ HELPER FUNCTIONS
# =====================================================
def get_tickers_from_latest_stock_price():
    """Download the latest stock price parquet metadata/file to get list of tickers."""
    bucket = os.getenv("R2_BUCKET")
    files = list_r2_files(bucket, "cafef_data/cafef_stock_price_")
    files = [f for f in files if f.endswith(".parquet")]
    if not files:
        print("❌ No stock price data found to extract tickers.")
        return []
    
    def parse_date(f):
        m = re.search(r"(\d{6})\.parquet", f)
        if m: return datetime.strptime(m.group(1), "%d%m%y")
        return datetime.min
        
    latest_file = max(files, key=parse_date)
    print(f"📋 Using ticker list from: {latest_file}")
    
    local_path = "temp_tickers.parquet"
    if download_from_r2(bucket, latest_file, local_path):
        try:
            df = pd.read_parquet(local_path, columns=["ticker"])
            tickers = df["ticker"].unique().tolist()
            os.remove(local_path)
            return sorted(tickers)
        except Exception as e:
            print(f"⚠️ Error reading tickers: {e}")
            if os.path.exists(local_path): os.remove(local_path)
            return []
    return []

# -----------------------------------------------------
# GENERIC API FETCHERS
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
            
            # Order Stats wraps data in specific Logic
            data = r.json()
            if not data: break
            
            # Normal endpoints (Prop, Insider) have "Data" key
            # Order Stats has "Data" key too?
            rows = data.get("Data", [])
            if not rows: break

            all_data.extend(rows)
            
            if len(rows) < page_size: break
            page_index += 1
            time.sleep(0.05) 
            
        except Exception as e:
            print(f"⚠️ Error fetching {symbol} (Page {page_index}): {e}")
            break
            
    return all_data

def fetch_tick_data(url, symbol, date_obj):
    """Fetch tick data for a specific symbol and date."""
    try:
        # Date format YYYYMMDD
        date_str = date_obj.strftime("%Y%m%d")
        params = {"symbol": symbol, "date": date_str}
        r = requests.get(url, params=params, headers=HEADERS, timeout=5)
        
        if r.status_code != 200: return None
        data = r.json()
        
        # 'data' field contains the tick list
        ticks = data.get("data", [])
        if not ticks: return None
        
        # Enhance with metadata
        for t in ticks:
            t["ticker"] = symbol
            t["date"] = date_obj.strftime("%Y-%m-%d")
            
        return ticks
    except Exception as e:
        # print(f"⚠️ Error fetching ticks {symbol}: {e}")
        return None

# -----------------------------------------------------
# UPDATER LOGIC: RANGE DATA (Prop, Insider, OrderStats)
# -----------------------------------------------------
def update_range_dataset(data_type, tickers):
    config = CONFIG[data_type]
    bucket = os.getenv("R2_BUCKET")
    master_key = config["r2_path"]
    local_master = f"temp_{data_type}_master.parquet"
    
    # 1. Load Master
    df_master = pd.DataFrame()
    last_update_map = {}
    
    if list_r2_files(bucket, master_key):
        print(f"📥 Downloading master file: {master_key}")
        if download_from_r2(bucket, master_key, local_master):
            try:
                df_master = pd.read_parquet(local_master)
                df_master["date"] = pd.to_datetime(df_master["date"])
                last_update_map = df_master.groupby("ticker")["date"].max().to_dict()
                print(f"✅ Loaded {len(df_master)} rows.")
            except:
                print(f"⚠️ Master file corrupted. Starting fresh.")
                df_master = pd.DataFrame()
    else:
        print(f"✨ No master file found. Creating new.")

    # 2. Fetch New Data
    print(f"🔄 Updating {len(tickers)} tickers for {data_type}...")
    new_rows = []
    today = datetime.now()
    today_str = today.strftime("%d/%m/%Y")
    
    for i, ticker in enumerate(tickers):
        last_date = last_update_map.get(ticker)
        if last_date:
            start_dt = last_date + timedelta(days=1)
            if start_dt > today: continue
            start_str = start_dt.strftime("%d/%m/%Y")
        else:
            start_str = config["start_date_default"]
            
        if datetime.strptime(start_str, "%d/%m/%Y") <= today:
            raw_data = fetch_cafef_range_data(config["url"], ticker, start_str, today_str)
            if raw_data:
                for row in raw_data:
                    row["ticker"] = ticker
                    new_rows.append(row)
        
        if (i + 1) % 100 == 0: print(f"   ⏳ Processed {i + 1}/{len(tickers)}...")

    # 3. Merge & Save
    if not new_rows:
        print("✅ No new data found.")
        if os.path.exists(local_master): os.remove(local_master)
        return

    print(f"📊 Found {len(new_rows)} new records.")
    df_new = pd.DataFrame(new_rows)
    df_new.columns = [c.lower() for c in df_new.columns]
    
    # Standardize 'date'
    if "ngay" in df_new.columns:
        df_new = df_new.rename(columns={"ngay": "date"})
    if "date" in df_new.columns:
        df_new["date"] = pd.to_datetime(df_new["date"], dayfirst=True, errors='coerce')
        
    # Rename commonly used columns
    rename_map = {"symbol": "ticker", "giatrirong": "net_value", "khoiluongrong": "net_volume", "nguoithuchien": "executor"}
    df_new = df_new.rename(columns=rename_map)

    if df_master.empty:
        df_final = df_new
    else:
        # Align columns
        all_cols = list(set(df_master.columns) | set(df_new.columns))
        df_final = pd.concat([df_master, df_new], ignore_index=True)
        
    # Dedup
    if "date" in df_final.columns and "ticker" in df_final.columns:
        df_final = df_final.drop_duplicates()
        
    ensure_folder_exists(bucket, os.path.dirname(master_key))
    df_final.to_parquet(local_master, index=False)
    upload_to_r2(local_master, bucket, master_key)
    print(f"💾 Updated master file: {master_key} ({len(df_final)} rows)")
    if os.path.exists(local_master): os.remove(local_master)

# -----------------------------------------------------
# UPDATER LOGIC: TICK DATA (Daily Consolidated)
# -----------------------------------------------------
def update_tick_data(tickers, target_date_obj=None):
    """
    Fetch tick data for ALL tickers for a specific date (default: today).
    Save as ONE consolidated parquet file: cafef_tick_data_DDMMYY.parquet
    """
    config = CONFIG["tick_data"]
    bucket = os.getenv("R2_BUCKET")
    
    if target_date_obj is None:
        target_date_obj = datetime.now()
        
    ddmmyy = target_date_obj.strftime("%d%m%y")
    filename = config["file_pattern"].format(ddmmyy=ddmmyy)
    r2_key = f"{config['r2_folder']}{filename}"
    
    # Check if exists
    if list_r2_files(bucket, r2_key):
        print(f"✅ Tick data for {ddmmyy} already exists: {r2_key}")
        return

    print(f"⚡ Fetching TICK DATA for {target_date_obj.strftime('%Y-%m-%d')} ({len(tickers)} stocks)...")
    
    all_ticks = []
    
    for i, ticker in enumerate(tickers):
        ticks = fetch_tick_data(config["url"], ticker, target_date_obj)
        if ticks:
            all_ticks.extend(ticks)
        
        if (i + 1) % 100 == 0: 
            print(f"   ⏱️ Processed {i+1}/{len(tickers)} (Found {len(all_ticks)} ticks so far)")
            time.sleep(1) # Rate limit protection

    if not all_ticks:
        print(f"🔸 No tick data found for {ddmmyy}.")
        return

    print(f"📦 Saving {len(all_ticks)} ticks to {filename}...")
    df = pd.DataFrame(all_ticks)
    
    # Standardize
    # Columns usually: id, time, price, vol, color, etc.
    # Convert 'time' (Format HH:mm:ss) to full datetime? Or keep as is.
    # Keep as is to save space, user can combine with 'date' col.
    
    ensure_folder_exists(bucket, config["r2_folder"])
    local_path = f"temp_{filename}"
    df.to_parquet(local_path, index=False)
    upload_to_r2(local_path, bucket, r2_key)
    os.remove(local_path)
    print(f"🚀 Uploaded tick data: {r2_key}")

# =====================================================
# 4️⃣ RUNNER
# =====================================================
def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="daily", help="daily or backfill")
    parser.add_argument("--tick-backfill-days", type=int, default=0, help="Number of days back to check for missing tick data")
    args = parser.parse_args()

    # 1. Get Tickers
    tickers = get_tickers_from_latest_stock_price()
    if not tickers:
        print("⚠️ No tickers found. Aborting.")
        return

    # 2. Daily Updates (Range Data)
    # These always run incrementally "up to today"
    print("\n=== UPDATING RANGE DATASETS ===")
    update_range_dataset("proprietary", tickers)
    update_range_dataset("insider", tickers)
    update_range_dataset("order_stats", tickers)

    # 3. Tick Data
    print("\n=== UPDATING TICK DATA ===")
    
    # Always try Today
    today = datetime.now()
    # If today is weekend, maybe skip? API might return empty anyway.
    if today.weekday() < 5: # Mon-Fri
        update_tick_data(tickers, today)
    else:
        print("Today is weekend, skipping today's tick data fetch.")
    
    # Backfill Logic (Check previous N days)
    if args.tick_backfill_days > 0:
        print(f"🔙 Checking backfill for {args.tick_backfill_days} days...")
        for i in range(1, args.tick_backfill_days + 1):
            past_date = today - timedelta(days=i)
            if past_date.weekday() < 5:
                update_tick_data(tickers, past_date)

if __name__ == "__main__":
    run()
