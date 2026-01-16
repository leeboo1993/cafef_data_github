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
    from utils_r2 import upload_to_r2, download_from_r2, list_r2_files, ensure_folder_exists, clean_old_backups_r2
except ImportError:
    # Mocks for when utils_r2 is missing entirely (unlikely but safe)
    def upload_to_r2(*args): pass
    def download_from_r2(*args): return False
    def list_r2_files(*args): return []
    def ensure_folder_exists(*args): pass
    def clean_old_backups_r2(*args): pass

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
                tickers = df["ticker"].dropna().astype(str).unique().tolist()
                print(f"✅ Found {len(tickers)} unique tickers.")
                return sorted(tickers)
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
    local_path = "temp_tickers.parquet"
    if download_from_r2(bucket, latest_file, local_path):
        try:
            df = pd.read_parquet(local_path, columns=["ticker"])
            tickers = df["ticker"].dropna().astype(str).unique().tolist()
            os.remove(local_path)
            return sorted(tickers)
        except Exception as e:
            if os.path.exists(local_path): os.remove(local_path)
            return []
    return []

def find_latest_master_file(folder, prefix, local_mode=False, bucket=None):
    """Find the latest master file pattern: prefix_DDMMYY.parquet"""
    candidates = []
    
    if local_mode:
        if os.path.exists(folder):
            for f in os.listdir(folder):
                if f.startswith(prefix) and f.endswith(".parquet"):
                    candidates.append(os.path.join(folder, f))
    else:
        files = list_r2_files(bucket, folder)
        for f in files:
            fname = os.path.basename(f)
            if fname.startswith(prefix) and fname.endswith(".parquet"):
                candidates.append(f)
                
    if not candidates:
        # Fallback for Legacy Files (e.g. all_proprietary_trading.parquet without date)
        if local_mode:
            legacy_file = os.path.join(folder, f"{prefix}.parquet")
            if os.path.exists(legacy_file):
                return legacy_file
                
        return None
        
    def parse_filename_date(f):
        # Expecting prefix_DDMMYY.parquet
        m = re.search(r"_(\d{6})\.parquet$", f)
        if m:
            try:
                return datetime.strptime(m.group(1), "%d%m%y")
            except: pass
        return datetime.min

    latest = max(candidates, key=parse_filename_date)
    return latest

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
    date_keywords = ["date", "ngay", "time", "day"]
    for col in df.columns:
        lower_col = col.lower()
        if any(k in lower_col for k in date_keywords):
            if df[col].dtype == object:
                # check first non-null
                first_valid = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if isinstance(first_valid, str) and "/Date(" in first_valid:
                    print(f"   🔧 Parsing ASP.NET dates in column: {col}")
                    df[col] = df[col].apply(parse_asp_date)
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                elif "date" in lower_col or "ngay" in lower_col:
                     df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
    return df

def process_ticker_range_data(ticker, config, last_update_map):
    """Fetch range data for a single ticker."""
    url = config["url"]
    start_date = config["start_date_default"]
    
    # Check last update
    if ticker in last_update_map:
        last_date = last_update_map[ticker]
        start_date = (last_date + timedelta(days=1)).strftime("%d/%m/%Y")
        
    params = {
        "Symbol": ticker,
        "StartDate": start_date,
        "EndDate": datetime.now().strftime("%d/%m/%Y"),
        "PageIndex": 1,
        "PageSize": 10000  # Fetch large batch
    }
    
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=30)
        if r.status_code != 200: return []
        
        data = r.json()
        
        # Traverse key chain
        cursor = data
        for k in config["data_key_chain"]:
            if isinstance(cursor, dict) and k in cursor:
                cursor = cursor[k]
            elif isinstance(cursor, list): 
                pass
            else:
                return []
                
        if isinstance(cursor, list):
            for row in cursor:
                row["symbol"] = ticker
            return cursor
        return []
    except:
        return []

def update_range_dataset(data_type, tickers, local_mode=False, max_workers=10):
    config = CONFIG[data_type]
    bucket = os.getenv("R2_BUCKET")
    folder = config["r2_folder"]
    prefix = config["file_prefix"]
    
    # Temp folder for chunks
    temp_chunk_dir = f"cafef_data/temp_chunks/{data_type}"
    ensure_folder_exists_local(temp_chunk_dir)
    
    if local_mode:
        ensure_folder_exists_local(folder)
        
    # 1. Find and Load Master
    try:
        df_master = pd.DataFrame()
        last_update_map = {}
        
        latest_file_path = find_latest_master_file(folder, prefix, local_mode, bucket)
        
        if latest_file_path:
            print(f"📥 Loading master: {latest_file_path}")
            local_load_path = latest_file_path
            
            if not local_mode:
                local_load_path = f"temp_master_{data_type}.parquet"
                download_from_r2(bucket, latest_file_path, local_load_path)
                
            try:
                df_master = pd.read_parquet(local_load_path)
                if "ngay" in df_master.columns: df_master = df_master.rename(columns={"ngay": "date"})
                if "date" in df_master.columns:
                    df_master["date"] = pd.to_datetime(df_master["date"])
                    last_update_map = df_master.groupby("ticker")["date"].max().to_dict()
                    print(f"✅ Loaded history: {len(df_master)} rows (Latest: {df_master['date'].max().strftime('%Y-%m-%d')})")
                else:
                    df_master = pd.DataFrame()
            except:
                df_master = pd.DataFrame()
                
            if not local_mode and os.path.exists(local_load_path):
                os.remove(local_load_path)
        else:
            print(f"✨ No history found. Starting fresh.")

        # 2. Check for Existing Chunks (Resume Capability)
        processed_tickers = set()
        existing_chunks = [os.path.join(temp_chunk_dir, f) for f in os.listdir(temp_chunk_dir) if f.endswith(".parquet")]
        
        if existing_chunks:
            print(f"🔄 Found {len(existing_chunks)} partial chunks. Resuming...")
            for chunk in existing_chunks:
                try:
                    # We interpret "processed" as tickers present in these chunks
                    # Optimally we would just read the 'ticker' column unique values
                    df_chunk = pd.read_parquet(chunk, columns=["ticker"])
                    chunk_tickers = df_chunk["ticker"].unique()
                    processed_tickers.update(chunk_tickers)
                except:
                    pass
            print(f"⏩ Skipping {len(processed_tickers)} already processed tickers.")

        # Filter out processed tickers
        tickers_to_process = [t for t in tickers if t not in processed_tickers]
        
        if not tickers_to_process:
            print("✅ All tickers already processed in chunks. Proceeding to merge.")
        else:
            # 3. Batch Processing
            batch_size = 500
            total_batches = (len(tickers_to_process) // batch_size) + 1
            
            print(f"🔄 Updating {len(tickers_to_process)} tickers for {data_type} (Workers: {max_workers})...")
            print(f"📦 Batch Processing: {total_batches} batches of ~{batch_size} tickers.")

            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = start_idx + batch_size
                batch_tickers = tickers_to_process[start_idx:end_idx]
                
                if not batch_tickers: continue
                
                print(f"   [{data_type}] 🚀 Batch {batch_idx + 1}/{total_batches} ({len(batch_tickers)} tickers)...")
                
                new_rows = []
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {executor.submit(process_ticker_range_data, t, config, last_update_map): t for t in batch_tickers}
                    
                    for future in as_completed(futures):
                        res = future.result()
                        if res:
                            new_rows.extend(res)

                # Save Batch
                if new_rows:
                    df_batch = pd.DataFrame(new_rows)
                    df_batch.columns = [c.lower() for c in df_batch.columns]
                    
                    # Deduplicate columns (e.g. Symbol vs symbol)
                    df_batch = df_batch.loc[:, ~df_batch.columns.duplicated()]
                    
                    # Pre-clean batch
                    if "ngay" in df_batch.columns: df_batch = df_batch.rename(columns={"ngay": "date"})
                    if "date" in df_batch.columns: df_batch["date"] = pd.to_datetime(df_batch["date"], dayfirst=True, errors='coerce')
                    df_batch = clean_dataframe_dates(df_batch)
                    
                    rename_map = {"symbol": "ticker", "giatrirong": "net_value", "khoiluongrong": "net_volume", "nguoithuchien": "executor"}
                    df_batch = df_batch.rename(columns=rename_map)

                    batch_file = os.path.join(temp_chunk_dir, f"batch_{batch_idx}_{int(time.time())}.parquet")
                    df_batch.to_parquet(batch_file, index=False)
                    print(f"      💾 Saved batch: {len(df_batch)} records")
                else:
                    print("      ⚠️ No data in this batch.")

        # 4. Final Merge (Master + All Chunks)
        print(f"🔗 Merging master and chunks for {data_type}...")
        all_chunks = [os.path.join(temp_chunk_dir, f) for f in os.listdir(temp_chunk_dir) if f.endswith(".parquet")]
        
        if not all_chunks and df_master.empty:
            print("⚠️ No data available to save.")
            return

        dfs_to_concat = [df_master] if not df_master.empty else []
        
        for chunk in all_chunks:
            try:
                dfs_to_concat.append(pd.read_parquet(chunk))
            except Exception as e:
                print(f"⚠️ Failed to read chunk {chunk}: {e}")
                
        if not dfs_to_concat:
            print("⚠️ No valid data frames to merge.")
            return
            
        df_final = pd.concat(dfs_to_concat, ignore_index=True)
        
        # Ensure final date column is datetime
        if "date" in df_final.columns:
            df_final["date"] = pd.to_datetime(df_final["date"], errors='coerce')
        
        # 5. Final Cleanup
        if "date" in df_final.columns and "ticker" in df_final.columns:
            df_final = df_final.drop_duplicates()
        
        # Determine new filename
        max_date = datetime.now()
        if "date" in df_final.columns and not df_final.dropna(subset=["date"]).empty:
            max_date = df_final["date"].max()
            
        new_filename = f"{prefix}_{max_date.strftime('%d%m%y')}.parquet"
        
        if local_mode:
            new_path = os.path.join(folder, new_filename)
            df_final.to_parquet(new_path, index=False)
            print(f"💾 Updated master saved: {new_path}")
            
            # Cleanup old local file
            if latest_file_path and latest_file_path != new_path:
                try:
                    os.remove(latest_file_path)
                    print(f"🗑️ Removed old: {os.path.basename(latest_file_path)}")
                except: pass
        else:
            temp_path = f"temp_{new_filename}"
            r2_key = f"{folder}{new_filename}"
            df_final.to_parquet(temp_path, index=False)
            upload_to_r2(temp_path, bucket, r2_key)
            print(f"☁️ Uploaded: {r2_key}")
            os.remove(temp_path)
            
            # Cleanup old backups
            # DISABLED: This was deleting valid data files because they weren't from today
            # TODO: Re-enable after scraper successfully updates ALL files to current date
            # print(f"🧹 Cleaning old backups for {data_type} in R2...")
            # clean_old_backups_r2(bucket, folder, keep=2)
            
    finally:
        # 6. Cleanup Chunks
        if os.path.exists(temp_chunk_dir):
            print("🧹 Cleaning up temporary chunks...")
            import shutil
            shutil.rmtree(temp_chunk_dir)
        print("✨ Cleanup done.")

# =====================================================
# 4️⃣ RUNNER
# =====================================================
def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="daily", help="daily or backfill")
    parser.add_argument("--start-year", type=int, default=2023, help="Start year for Range Data")
    parser.add_argument("--local", action="store_true", help="Run local only (Save to disk, skip R2)")
    parser.add_argument("--workers", type=int, default=10, help="Number of parallel workers per task")
    parser.add_argument("--lookback-days", type=int, default=0, help="Number of business days to look back (overrides start-year)")
    
    args = parser.parse_args()

    # Update configs
    if args.lookback_days > 0:
        # Calculate start date: T - N business days
        date = datetime.now()
        count = 0
        while count < args.lookback_days:
            date -= timedelta(days=1)
            if date.weekday() < 5: # Mon-Fri are 0-4
                count += 1
        start_date_str = date.strftime("%d/%m/%Y")
        print(f"📅 Scrape Window: T-{args.lookback_days} Business Days -> Start Date: {start_date_str}")
    else:
        start_date_str = f"01/01/{args.start_year}"

    for key in ["proprietary", "insider", "order_stats"]:
        CONFIG[key]["start_date_default"] = start_date_str
    
    if args.local:
        print(f"🏠 RUNNING IN LOCAL MODE (Workers/Task: {args.workers})")
        print("📊 MODE: RANGE DATA ONLY (Insider, Proprietary, OrderStats)")

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
            pass

    print("\n✅ All tasks completed.")

if __name__ == "__main__":
    run()
