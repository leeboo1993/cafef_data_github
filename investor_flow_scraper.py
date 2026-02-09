#!/usr/bin/env python3
"""
Investor Flow Scraper - Consolidates Foreign and Proprietary market flows and uploads to R2.
"""

import os
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Import R2 utils
try:
    from utils_r2 import upload_to_r2, backup_and_cleanup_r2, ensure_folder_exists, download_from_r2, list_r2_files
except ImportError:
    print("⚠️ utils_r2.py not found. R2 upload will be disabled.")
    def upload_to_r2(*args, **kwargs): pass
    def backup_and_cleanup_r2(*args, **kwargs): pass
    def ensure_folder_exists(*args, **kwargs): pass
    def download_from_r2(*args, **kwargs): return False
    def list_r2_files(*args, **kwargs): return []

R2_FOLDER = "cafef_data/investor_flows/"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://cafef.vn/'
}

def load_env_safely():
    """Load .env for local runs, skip if env vars already exist."""
    if not os.getenv("R2_BUCKET"):
        env_path = Path(__file__).resolve().parent / ".env"
        if env_path.exists():
            load_dotenv(env_path)

from concurrent.futures import ThreadPoolExecutor, as_completed

def download_foreign_data_single(date_obj):
    """Worker function to download data for a single date"""
    if date_obj.weekday() >= 5: return None
    date_str = date_obj.strftime('%d/%m/%Y')
    base_url = "https://cafef.vn/du-lieu/Ajax/PageNew/DataGDNN/GDNuocNgoai.ashx"
    params = {'TradeCenter': 'HOSE', 'Date': date_str}
    
    try:
        response = requests.get(base_url, params=params, headers=HEADERS, timeout=15)
        if response.status_code == 200:
            data = response.json()
            if data and 'Data' in data and 'ListDataNN' in data['Data']:
                stock_list = data['Data']['ListDataNN']
                if stock_list:
                    df = pd.DataFrame(stock_list)
                    df['Date'] = date_obj
                    return df
    except Exception as e:
        print(f"    ✗ Error on {date_str}: {e}")
    return None

def download_foreign_data(start_date, end_date, workers=20):
    """Download foreign investor data (HSX) in parallel"""
    print(f"\n📊 Downloading Foreign Trading Data (HOSE) from {start_date} to {end_date} using {workers} workers...")
    
    dates = []
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() < 5:
            dates.append(current_date)
        current_date += timedelta(days=1)
        
    all_dfs = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_date = {executor.submit(download_foreign_data_single, d): d for d in dates}
        
        count = 0
        for future in as_completed(future_to_date):
            res = future.result()
            if res is not None:
                all_dfs.append(res)
            
            count += 1
            if count % 100 == 0:
                print(f"    progress: {count}/{len(dates)} dates processed...")

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        numeric_cols = ['BuyVolume', 'SellVolume', 'BuyValue', 'SellValue', 'Room', 'Percent']
        for col in numeric_cols:
            if col in combined.columns:
                combined[col] = pd.to_numeric(combined[col], errors='coerce')
        return combined
    return None

# ----------------------------------------------------------------------------
# PROPRIETARY DATA (Market-wide VNINDEX)
# ----------------------------------------------------------------------------

def download_proprietary_data(start_date, end_date):
    """Download market aggregate proprietary trading data."""
    print(f"\n📊 Downloading Proprietary Data (VNINDEX) from {start_date} to {end_date}...")
    url = "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/GDTuDoanh.ashx"
    
    params = {
        "Symbol": "VNINDEX",
        "StartDate": start_date.strftime("%d/%m/%Y"),
        "EndDate": end_date.strftime("%d/%m/%Y"),
        "PageIndex": 1,
        "PageSize": 10000
    }
    
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=30)
        data_json = r.json()
        if data_json.get('Success') and data_json.get('Data'):
            items = data_json['Data']['Data']['ListDataTudoanh']
            if items:
                df = pd.DataFrame(items)
                df = df.rename(columns={
                    'Date': 'date',
                    'Symbol': 'ticker',
                    'KLcpMua': 'prop_buy_vol',
                    'KlcpBan': 'prop_sell_vol',
                    'GtMua': 'prop_buy_val',
                    'GtBan': 'prop_sell_val'
                })
                df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
                cols = ['prop_buy_vol', 'prop_sell_vol', 'prop_buy_val', 'prop_sell_val']
                for col in cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                df['prop_net_val'] = df['prop_buy_val'] - df['prop_sell_val']
                print(f"    ✓ Fetched {len(df)} proprietary records.")
                return df
    except Exception as e:
        print(f"    ✗ Error fetching proprietary: {e}")
    return None

# ----------------------------------------------------------------------------
# MASTER SYNC
# ----------------------------------------------------------------------------

def sync_investor_flows(lookback_days=365, workers=20):
    print("="*60)
    print(f"💼 INVESTOR FLOW SCRAPER - FOREIGN & PROPRIETARY (Lookback: {lookback_days} days, Workers: {workers})")
    print("="*60)
    
    bucket = os.getenv("R2_BUCKET")
    end_date = datetime.now()
    
    # 1. Foreign Data Sync (Historical merge)
    foreign_file = "all_foreign_flows.parquet"
    foreign_master = None
    
    # Try to load existing foreign data from R2 to get start date
    if bucket:
        r2_files = list_r2_files(bucket, f"{R2_FOLDER}all_foreign_flows")
        if r2_files:
            latest_r2 = sorted(r2_files)[-1]
            if download_from_r2(bucket, latest_r2, "temp_foreign.parquet"):
                foreign_master = pd.read_parquet("temp_foreign.parquet")
                foreign_master['Date'] = pd.to_datetime(foreign_master['Date'])
                # If we are NOT backfilling, we only need the last few days
                if lookback_days <= 365:
                    start_date = foreign_master['Date'].max() - timedelta(days=5)
                else:
                    start_date = end_date - timedelta(days=lookback_days)
                os.remove("temp_foreign.parquet")
            else:
                start_date = end_date - timedelta(days=lookback_days)
        else:
            start_date = end_date - timedelta(days=lookback_days)
    else:
        start_date = end_date - timedelta(days=lookback_days)

    new_foreign = download_foreign_data(start_date, end_date, workers=workers)
    if new_foreign is not None:
        if foreign_master is not None:
            # Merge and deduplicate
            new_foreign['Date'] = pd.to_datetime(new_foreign['Date'])
            cutoff = new_foreign['Date'].min()
            foreign_master = foreign_master[foreign_master['Date'] < cutoff]
            foreign_master = pd.concat([foreign_master, new_foreign], ignore_index=True)
        else:
            foreign_master = new_foreign
            
        today_str = datetime.now().strftime('%d%m%y')
        foreign_filename = f"all_foreign_flows_{today_str}.parquet"
        foreign_master.to_parquet(foreign_filename, index=False)
        print(f"✅ Saved Foreign Flows: {foreign_filename}")
        
    # 2. Proprietary Data Sync
    prop_start_date = end_date - timedelta(days=lookback_days)
    prop_df = download_proprietary_data(prop_start_date, end_date)
    if prop_df is not None:
        today_str = datetime.now().strftime('%d%m%y')
        prop_filename = f"proprietary_market_flows_{today_str}.parquet"
        prop_df.to_parquet(prop_filename, index=False)
        print(f"✅ Saved Proprietary Flows: {prop_filename}")
        
    saved_files = []
    if 'foreign_filename' in locals() and os.path.exists(foreign_filename):
        saved_files.append(foreign_filename)
    if 'prop_filename' in locals() and os.path.exists(prop_filename):
        saved_files.append(prop_filename)
        
    return saved_files

def upload_and_cleanup(files):
    bucket = os.getenv("R2_BUCKET")
    if not bucket or not files: return
    
    print("\n" + "=" * 80)
    print("☁️ UPLOADING TO CLOUDFLARE R2")
    print("=" * 80)
    
    ensure_folder_exists(bucket, R2_FOLDER)
    for f in files:
        upload_to_r2(f, bucket, f"{R2_FOLDER}{f}")
        
    print(f"\n🧹 Cleaning up old files in R2 (keeping last 2)...")
    backup_and_cleanup_r2(bucket, R2_FOLDER, file_pattern="all_foreign_flows_", keep=2)
    backup_and_cleanup_r2(bucket, R2_FOLDER, file_pattern="proprietary_market_flows_", keep=2)
    
    for f in files:
        if os.path.exists(f): os.remove(f)
    print(f"\n✨ Cleanup complete.")

import argparse

def main():
    parser = argparse.ArgumentParser(description='Sync Investor Flows')
    parser.add_argument('--backfill-days', type=int, default=365, help='Number of days to look back')
    parser.add_argument('--workers', type=int, default=20, help='Number of parallel workers')
    args = parser.parse_args()
    
    load_env_safely()
    files = sync_investor_flows(lookback_days=args.backfill_days, workers=args.workers)
    if files:
        upload_and_cleanup(files)

if __name__ == "__main__":
    main()
