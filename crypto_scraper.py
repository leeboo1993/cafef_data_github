#!/usr/bin/env python3
"""
Crypto Scraper - Fetches crypto prices and uploads to Cloudflare R2.
"""

import os
import pandas as pd
import yfinance as yf
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Import R2 utils
try:
    from utils_r2 import upload_to_r2, backup_and_cleanup_r2, ensure_folder_exists
except ImportError:
    print("⚠️ utils_r2.py not found. R2 upload will be disabled.")
    def upload_to_r2(*args, **kwargs): pass
    def backup_and_cleanup_r2(*args, **kwargs): pass
    def ensure_folder_exists(*args, **kwargs): pass

R2_FOLDER = "cafef_data/alternative_assets/"
TICKERS = ["BTC-USD", "ETH-USD"]

def load_env_safely():
    """Load .env for local runs, skip if env vars already exist."""
    if not os.getenv("R2_BUCKET"):
        env_path = Path(__file__).resolve().parent / ".env"
        if env_path.exists():
            load_dotenv(env_path)

import argparse

def scrape_crypto(days=7):
    print("="*60)
    print(f"₿ CRYPTO PRICE SCRAPER (Lookback: {days} days)")
    print("="*60)
    
    if days == 'max':
        start_date_obj = datetime(2000, 1, 1)
    else:
        from datetime import timedelta
        start_date_obj = datetime.now() - timedelta(days=int(days))
        
    start_date = start_date_obj.strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    data_list = []
    for ticker in TICKERS:
        print(f"  📊 Fetching {ticker}...")
        try:
            df = yf.download(ticker, start=start_date, end=end_date, auto_adjust=True, progress=False)
            if not df.empty:
                df = df.reset_index()
                # Ensure columns are flat strings
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [col[0] for col in df.columns]
                
                if 'Close' in df.columns and 'Date' in df.columns:
                    df = df[['Date', 'Close']]
                    df['Ticker'] = ticker
                    data_list.append(df)
                else:
                    print(f"    ✗ Missing columns for {ticker}: {df.columns}")
            else:
                print(f"    ✗ No data for {ticker}")
        except Exception as e:
            print(f"    ✗ Error fetching {ticker}: {e}")
            
    if not data_list:
        print("❌ No crypto data fetched.")
        return None
        
    combined_df = pd.concat(data_list, ignore_index=True)
    combined_df['Date'] = pd.to_datetime(combined_df['Date'])
    
    today_str = datetime.now().strftime('%d%m%y')
    filename = f"crypto_prices_{today_str}.parquet"
    combined_df.to_parquet(filename, index=False)
    
    print(f"\n✅ Created: {filename} ({len(combined_df)} records)")
    return filename

def upload_and_cleanup(filename):
    bucket = os.getenv("R2_BUCKET")
    if not bucket: return
    
    print("\n" + "=" * 80)
    print("☁️ UPLOADING TO CLOUDFLARE R2")
    print("=" * 80)
    
    ensure_folder_exists(bucket, R2_FOLDER)
    upload_to_r2(filename, bucket, f"{R2_FOLDER}{filename}")
    
    print(f"\n🧹 Cleaning up old crypto files in R2 (keeping last 2)...")
    backup_and_cleanup_r2(bucket, R2_FOLDER, file_pattern="crypto_prices_", keep=2)
    
    if os.path.exists(filename): os.remove(filename)
    print(f"\n✨ Cleanup complete.")

def main():
    parser = argparse.ArgumentParser(description='Sync Crypto Prices')
    parser.add_argument('--days', type=str, default='7', help='Number of days to sync or "max"')
    args = parser.parse_args()
    
    load_env_safely()
    filename = scrape_crypto(days=args.days)
    if filename:
        upload_and_cleanup(filename)

if __name__ == "__main__":
    main()
