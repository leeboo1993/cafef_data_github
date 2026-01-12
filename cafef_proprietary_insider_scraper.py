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
            print("⚠️ dotenv not installed, skipping .env loading.")
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

# =====================================================
# 2️⃣ CONFIGURATION
# =====================================================
CONFIG = {
    "proprietary": {
        "url": "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/GDTuDoanh.ashx",
        "r2_path": "cafef_data/proprietary_trading/all_proprietary_trading.parquet",
        "start_date_default": "01/01/2023", # Default if no master file exists
        "col_map": {
            "Symbol": "ticker",
            "Ngay": "date",
            "GiaTriRong": "net_value",
            "KhoiLuongRong": "net_volume",
            # Additional columns might be present, we'll confirm during runtime or assume standard
        }
    },
    "insider": {
        "url": "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/GDCoDong.ashx",
        "r2_path": "cafef_data/insider_trading/all_insider_trading.parquet",
        "start_date_default": "01/01/2023",
        "col_map": {
            "Symbol": "ticker",
            "Ngay": "date",
            "NguoiThucHien": "executor",
            "ChucVu": "position",
            "MaCK": "ticker", # Variable name might differ
        }
    }
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://cafef.vn/"
}

# =====================================================
# 3️⃣ HELPER FUNCTIONS
# =====================================================
def get_tickers_from_latest_stock_price():
    """Download the latest stock price parquet metadata/file to get list of tickers."""
    bucket = os.getenv("R2_BUCKET")
    # List files in cafef_data/
    files = list_r2_files(bucket, "cafef_data/cafef_stock_price_")
    # Filter for parquet
    files = [f for f in files if f.endswith(".parquet")]
    if not files:
        print("❌ No stock price data found to extract tickers.")
        return []
    
    # Sort by date in filename (cafef_stock_price_DDMMYY.parquet)
    # Extract YYMMDD for sorting
    def parse_date(f):
        m = re.search(r"(\d{6})\.parquet", f)
        if m:
            d = m.group(1)
            return datetime.strptime(d, "%d%m%y")
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

def fetch_cafef_data(url, symbol, start_date, end_date):
    """Fetch data from CafeF AJAX endpoint."""
    all_data = []
    page_index = 1
    page_size = 1000 # Try large page size to minimize requests
    
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
            if r.status_code != 200:
                break
            
            data = r.json()
            if not data or "Data" not in data or not data["Data"]:
                break
            
            rows = data["Data"]
            all_data.extend(rows)
            
            # Check pagination
            if len(rows) < page_size:
                break
            page_index += 1
            time.sleep(0.1) # Be nice
            
        except Exception as e:
            print(f"⚠️ Error fetching {symbol} (Page {page_index}): {e}")
            break
            
    return all_data

def update_dataset(data_type, config, tickers):
    bucket = os.getenv("R2_BUCKET")
    master_key = config["r2_path"]
    local_master = f"temp_{data_type}_master.parquet"
    
    # 1. Load Master File
    df_master = pd.DataFrame()
    last_update_map = {} # ticker -> last_date
    
    if list_r2_files(bucket, master_key):
        print(f"📥 Downloading master file: {master_key}")
        if download_from_r2(bucket, master_key, local_master):
            try:
                df_master = pd.read_parquet(local_master)
                # Ensure date is datetime
                df_master["date"] = pd.to_datetime(df_master["date"])
                # Calculate last update per ticker
                last_update_map = df_master.groupby("ticker")["date"].max().to_dict()
                print(f"✅ Loaded {len(df_master)} rows from master file.")
            except Exception as e:
                print(f"⚠️ Master file corrupted or unreadable: {e}. Starting fresh.")
                df_master = pd.DataFrame()
    else:
        print(f"✨ No master file found for {data_type}. Creating new dataset.")

    # 2. Iterate Tickers & Fetch New Data
    new_rows = []
    today_str = datetime.now().strftime("%d/%m/%Y")
    
    print(f"🔄 Updating {len(tickers)} tickers for {data_type}...")
    
    for i, ticker in enumerate(tickers):
        # Determine start date
        last_date = last_update_map.get(ticker)
        if last_date:
            # Start from next day
            start_dt = last_date + timedelta(days=1)
            # If start_dt is in future (e.g. run multiple times same day), skip
            if start_dt > datetime.now():
                continue
            start_str = start_dt.strftime("%d/%m/%Y")
        else:
            start_str = config["start_date_default"]
            
        # Fetch
        # Only fetch if start_date <= today
        if datetime.strptime(start_str, "%d/%m/%Y") <= datetime.now():
            # print(f"Processing {ticker}: {start_str} -> {today_str}")
            raw_data = fetch_cafef_data(config["url"], ticker, start_str, today_str)
            
            if raw_data:
                for row in raw_data:
                    # Enrich with ticker if missing (API usually returns it but safe to add)
                    row["ticker"] = ticker
                    new_rows.append(row)
        
        # Progress log every 50 tickers
        if (i + 1) % 50 == 0:
            print(f"   ⏳ Processed {i + 1}/{len(tickers)} tickers...")

    # 3. Process & Merge
    if not new_rows:
        print("✅ No new data found.")
        # Clean up local master if it exists
        if os.path.exists(local_master): os.remove(local_master)
        return

    print(f"📊 Found {len(new_rows)} new records.")
    df_new = pd.DataFrame(new_rows)
    
    # Rename columns (basic standardizing)
    # The API returns PascalCase usually: Ngay, GiaTriMua, etc.
    # We will simply lower-case all columns for consistency first
    df_new.columns = [c.lower() for c in df_new.columns]
    
    # Rename specific columns if needed
    if "symbol" in df_new.columns:
        df_new = df_new.rename(columns={"symbol": "ticker"})
    if "ngay" in df_new.columns:
        df_new = df_new.rename(columns={"ngay": "date"})
        df_new["date"] = pd.to_datetime(df_new["date"], format="%d/%m/%Y %H:%M:%S", errors='coerce')
        # If coerce failed, try DD/MM/YYYY
        mask = df_new["date"].isna()
        if mask.any():
             df_new.loc[mask, "date"] = pd.to_datetime(df_new.loc[mask, "date_raw"], format="%d/%m/%Y", errors='coerce') 

    # Combine
    if df_master.empty:
        df_final = df_new
    else:
        # Align columns
        # Add missing cols to df_master
        for c in df_new.columns:
            if c not in df_master.columns:
                df_master[c] = None
        # Add missing cols to df_new
        for c in df_master.columns:
            if c not in df_new.columns:
                df_new[c] = None
                
        df_final = pd.concat([df_master, df_new], ignore_index=True)
        
    # Deduplicate just in case
    if "date" in df_final.columns and "ticker" in df_final.columns:
        df_final = df_final.drop_duplicates(subset=["date", "ticker", "khoiluongkhoplenh", "khoiluongthoa thuan"], keep="last") # heuristic dedup keys ? 
        # Actually APIs might return detailed transaction logs or daily summaries?
        # Proprietary endpoint usually returns Daily Summary per ticker.
        # Insider endpoint returns transaction events.
        # Deduping by all columns is safest.
        df_final = df_final.drop_duplicates()

    # 4. Save & Upload
    ensure_folder_exists(bucket, os.path.dirname(master_key))
    df_final.to_parquet(local_master, index=False)
    upload_to_r2(local_master, bucket, master_key)
    
    print(f"💾 Updated master file uploaded: {master_key} ({len(df_final)} total rows)")
    
    # Clean up
    if os.path.exists(local_master): os.remove(local_master)

# =====================================================
# 4️⃣ RUNNER
# =====================================================
def run_scraper():
    print("🚀 Starting Proprietary & Insider Trading Scraper...")
    
    tickers = get_tickers_from_latest_stock_price()
    if not tickers:
        print("⚠️ Could not load tickers. Using fallback list or aborting.")
        # Fallback to key tickers if critical?
        tickers = ["HPG", "VNM", "SSI", "VND", "VIC", "VHM", "FPT"] # Debug fallback
    else:
        print(f"📋 Loaded {len(tickers)} tickers from stock price data.")

    # 1. Proprietary Trading
    print("\n-----------------------------------")
    print("💼 Processing PROPRIETARY TRADING")
    print("-----------------------------------")
    update_dataset("proprietary", CONFIG["proprietary"], tickers)
    
    # 2. Insider Trading
    print("\n-----------------------------------")
    print("🕵️ Processing INSIDER TRADING")
    print("-----------------------------------")
    update_dataset("insider", CONFIG["insider"], tickers)

    print("\n🎉 Scraper finished.")

if __name__ == "__main__":
    run_scraper()
