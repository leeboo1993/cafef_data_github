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
import glob
import time
import shutil
import requests
import pandas as pd
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime, timedelta
from utils_r2 import upload_to_r2, ensure_folder_exists, clean_old_backups_r2, list_r2_files

# =====================================================
# 2️⃣ CONSTANTS & CONFIG
# =====================================================
DATA_CONFIG = {
    "stock_price": {
        "url_pattern": "https://cafef1.mediacdn.vn/data/ami_data/{yyyymmdd}/CafeF.SolieuGD.Upto{ddmmyyyy}.zip",
        "file_pattern": "cafef_stock_price_",
        "r2_folder": "cafef_data/",
        "col_map": {
            "<Ticker>": "ticker", "<DTYYYYMMDD>": "date",
            "<Open>": "open", "<High>": "high", "<Low>": "low",
            "<Close>": "close", "<Volume>": "volume"
        },
        "required_cols": ["<Ticker>", "<DTYYYYMMDD>", "<Open>", "<High>", "<Low>", "<Close>", "<Volume>"]
    },
    "vn_index": {
        "url_pattern": "https://cafef1.mediacdn.vn/data/ami_data/{yyyymmdd}/CafeF.Index.Upto{ddmmyyyy}.zip",
        "file_pattern": "cafef_vnindex_data_",
        "r2_folder": "cafef_data/vn_index/",
        "col_map": {
            "<Ticker>": "ticker", "<DTYYYYMMDD>": "date",
            "<Open>": "open", "<High>": "high", "<Low>": "low",
            "<Close>": "close", "<Volume>": "volume"
        },
        "required_cols": ["<Ticker>", "<DTYYYYMMDD>", "<Open>", "<High>", "<Low>", "<Close>", "<Volume>"]
    },
    "stock_supply_demand": {
        "url_pattern": "https://cafef1.mediacdn.vn/data/ami_data/{yyyymmdd}/CafeF.CCNN.Upto{ddmmyyyy}.zip",
        "file_pattern": "cafef_stock_supply_demand_",
        "r2_folder": "cafef_data/stock_supply_demand/",
        "col_map": {
            "<Ticker>": "ticker", "<DTYYYYMMDD>": "date",
            "<Open>": "open", "<High>": "high", "<Low>": "low",
            "<Close>": "close", "<Volume>": "volume", "<OI>": "oi"
        },
        "required_cols": ["<Ticker>", "<DTYYYYMMDD>", "<Open>", "<High>", "<Low>", "<Close>", "<Volume>", "<OI>"]
    },
    "index_supply_demand": {
        "url_pattern": "https://cafef1.mediacdn.vn/data/ami_data/{yyyymmdd}/CafeF.CCNN.Index.Upto{ddmmyyyy}.zip",
        "file_pattern": "cafef_index_supply_demand_",
        "r2_folder": "cafef_data/index_supply_demand/",
        "col_map": {
            "<Ticker>": "ticker", "<DTYYYYMMDD>": "date",
            "<Open>": "open", "<High>": "high", "<Low>": "low",
            "<Close>": "close", "<Volume>": "volume", "<OI>": "oi"
        },
        "required_cols": ["<Ticker>", "<DTYYYYMMDD>", "<Open>", "<High>", "<Low>", "<Close>", "<Volume>", "<OI>"]
    }
}

# =====================================================
# 3️⃣ HELPER FUNCTIONS
# =====================================================
def build_cafef_url(pattern, date_obj):
    ddmmyyyy = date_obj.strftime("%d%m%Y")
    yyyymmdd = date_obj.strftime("%Y%m%d")
    return pattern.format(yyyymmdd=yyyymmdd, ddmmyyyy=ddmmyyyy), ddmmyyyy

def download_and_extract(url, output_dir):
    print(f"⬇️ Downloading: {url}")
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            print(f"❌ Failed to download (Status: {r.status_code})")
            return None
        
        with ZipFile(BytesIO(r.content)) as z:
            z.extractall(output_dir)
            files = [os.path.join(output_dir, f) for f in z.namelist()]
            print(f"📦 Extracted {len(files)} files.")
            return files
    except Exception as e:
        print(f"❌ Error downloading/extracting: {e}")
        return None

def validate_and_process_files(file_paths, config):
    req_cols = config["required_cols"]
    col_map = config["col_map"]
    all_dfs = []

    for path in file_paths:
        # Skip if not a text/csv file (sometimes zips contain garbage)
        if not (path.lower().endswith(".csv") or path.lower().endswith(".txt") or ".txt" in path.lower()):
            continue
            
        try:
            # Try utf-8-sig first, then latin1
            try:
                df = pd.read_csv(path, encoding="utf-8-sig")
            except:
                df = pd.read_csv(path, encoding="latin1")
            
            # Check for required columns
            current_cols = list(df.columns)
            # Basic check: verify if the first N columns match the required ones
            # (Sometimes there are extra columns, checking intersection or prefix is safer)
            if not all(col in current_cols for col in req_cols):
                 # Try strict check only on the number of required columns
                if list(df.columns[:len(req_cols)]) != req_cols:
                    print(f"⚠️ Skipping {os.path.basename(path)}: Columns mismatch.")
                    continue

            # Rename columns
            df = df.rename(columns=col_map)
            
            # Identify Exchange (Heuristic based on filename)
            fname = os.path.basename(path).upper()
            exch = "HSX" if "HSX" in fname or "HOSE" in fname else \
                   "HNX" if "HNX" in fname else \
                   "UPCOM" if "UPCOM" in fname else "UNKNOWN"
            
            # If exchange not in file, maybe rely on ticker prefix? For now default to UNKNOWN or derive later.
            # Actually for VNINDEX, the ticker is VNINDEX/HNX-INDEX, so exchange column might be redundant but harmless.
            if "exchange" not in df.columns:
                 df["exchange"] = exch

            # Standardize Date
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
            
            all_dfs.append(df)
            
        except Exception as e:
            print(f"⚠️ Error processing {os.path.basename(path)}: {e}")

    if not all_dfs:
        return None
    
    return pd.concat(all_dfs, ignore_index=True)

# =====================================================
# 4️⃣ MAIN RUNNER
# =====================================================
def run_cafef_downloader(max_days_back=5):
    bucket = os.getenv("R2_BUCKET")
    if not bucket:
        print("❌ Error: R2_BUCKET env var not set.")
        return

    work_dir = Path.cwd() / "cafef_temp"
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir()

    for data_type, config in DATA_CONFIG.items():
        print(f"\n🚀 Processing: {data_type.upper()}")
        
        # Ensure R2 folder exists
        ensure_folder_exists(bucket, config["r2_folder"])
        
        # Check if today's data already exists to avoid re-work
        # (Optimistic check: if we have a file with today's date)
        # However, CafeF uploads data later in the day, so we might technically run this at night.
        # Let's perform the check logic:
        
        # For this script, we iterate backwards and find the FIRST available data
        # If we successfully process a date, we stop (assuming daily run).
        # OR we can force check 'today' specifically.
        
        found_data = False
        for i in range(max_days_back):
            date_obj = datetime.now() - timedelta(days=i)
            file_date_str = date_obj.strftime("%d%m%y")
            
            # Check if this date's file already exists in R2
            expected_r2_key = f"{config['r2_folder']}{config['file_pattern']}{file_date_str}.parquet"
            # Note: utils_r2 doesn't have a simple exists check, but we can list files.
            # To save API calls, we could fetch list once. But explicit check is fine.
            existing = list_r2_files(bucket, expected_r2_key)
            if existing:
                print(f"✅ Data for {file_date_str} already exists in R2: {expected_r2_key}")
                found_data = True
                break # Move to next data_type if we found the latest data
            
            # Try to download
            url, _ = build_cafef_url(config["url_pattern"], date_obj)
            extracted_files = download_and_extract(url, str(work_dir))
            
            if not extracted_files:
                print(f"🔸 No data found for {date_obj.strftime('%Y-%m-%d')}...")
                continue
            
            # Process
            df = validate_and_process_files(extracted_files, config)
            if df is None or df.empty:
                print(f"🔸 Extracted files were invalid or empty for {date_obj.strftime('%Y-%m-%d')}.")
                # Cleanup extracted files for next iteration
                for f in extracted_files:
                    try: os.remove(f) 
                    except: pass
                continue
            
            # Convert to Parquet
            local_parquet = work_dir / f"{config['file_pattern']}{file_date_str}.parquet"
            df.to_parquet(local_parquet, index=False)
            print(f"💾 Saved local parquet: {local_parquet.name} ({len(df)} rows)")
            
            # Upload
            upload_to_r2(str(local_parquet), bucket, expected_r2_key)
            found_data = True
            
            # Clean up local file
            os.remove(local_parquet)
            break # Stop looking back once we found the latest valid data
            
        if not found_data:
            print(f"❌ Could not find valid data for {data_type} in last {max_days_back} days.")

    # Cleanup temp dir
    if work_dir.exists():
        shutil.rmtree(work_dir)
    print("\n🎉 All tasks completed.")

if __name__ == "__main__":
    run_cafef_downloader()
