#!/usr/bin/env python3
"""
Global Market Scraper - Fetches data from Yahoo Finance and uploads to Cloudflare R2.
Tracks major indices, commodities, and currencies.
"""

import os
import pandas as pd
import yfinance as yf
from datetime import datetime
from pathlib import Path
import argparse
import warnings
from dotenv import load_dotenv

# Import R2 utils
try:
    from utils_r2 import upload_to_r2, backup_and_cleanup_r2, ensure_folder_exists
except ImportError:
    print("⚠️ utils_r2.py not found. R2 upload will be disabled.")
    def upload_to_r2(*args, **kwargs): pass
    def backup_and_cleanup_r2(*args, **kwargs): pass
    def ensure_folder_exists(*args, **kwargs): pass

warnings.filterwarnings('ignore')

# Configuration
TICKERS = {
    # US Indices
    '^GSPC': 'SP500',          # S&P 500
    '^DJI': 'DJI',             # Dow Jones
    '^IXIC': 'NASDAQ',         # NASDAQ
    
    # Asian Indices
    '000001.SS': 'SSE',        # Shanghai Composite
    '^HSI': 'HSI',             # Hang Seng
    '^N225': 'Nikkei',         # Nikkei 225
    
    # Commodities
    'CL=F': 'WTI_Oil',         # WTI Crude Oil
    'GC=F': 'Gold',            # Gold
    
    # Currencies
    'USDVND=X': 'USD_VND',     # USD/VND
    
    # Volatility
    '^VIX': 'VIX'              # CBOE Volatility Index
}

R2_FOLDER = "cafef_data/global_markets/"

def load_env_safely():
    """Load .env for local runs, skip if env vars already exist."""
    if not os.getenv("R2_BUCKET"):
        env_path = Path(__file__).resolve().parent / ".env"
        if env_path.exists():
            load_dotenv(env_path)

def scrape_global_markets(start_date='7d'):
    """Fetch global market data from Yahoo Finance."""
    if start_date == '7d':
        from datetime import timedelta
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
    print("=" * 80)
    print(f"🌍 SCRAPING GLOBAL MARKET DATA (Start: {start_date})")
    print("=" * 80)
    
    temp_dir = Path("temp_global_markets")
    temp_dir.mkdir(exist_ok=True)
    
    end_date = datetime.now()
    all_data = {}
    
    for ticker, name in TICKERS.items():
        print(f"\n📊 Downloading {name} ({ticker})...")
        try:
            df = yf.download(ticker, start=start_date, end=end_date.strftime('%Y-%m-%d'), progress=False)
            
            if not df.empty:
                # Ensure columns are flat if MultiIndex (sometimes yfinance does this)
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [col[0] for col in df.columns]
                
                # Keep only Close and Volume
                keep_cols = []
                if 'Close' in df.columns: keep_cols.append('Close')
                if 'Volume' in df.columns: keep_cols.append('Volume')
                
                df = df[keep_cols].copy()
                df.columns = [f'{name}_{col}' for col in keep_cols]
                df['Date'] = df.index
                df = df.reset_index(drop=True)
                
                all_data[name] = df
                print(f"  ✓ Downloaded {len(df):,} days")
                
                # Save individual file locally
                local_file = temp_dir / f"{name.lower()}.parquet"
                df.to_parquet(local_file, index=False)
            else:
                print(f"  ✗ No data available for {ticker}")
                
        except Exception as e:
            print(f"  ✗ Error downloading {ticker}: {str(e)}")
            
    if not all_data:
        print("\n❌ No data downloaded for any ticker.")
        return None
        
    # Create Master File
    print("\n" + "=" * 80)
    print("📝 CREATING GLOBAL MARKETS MASTER FILE")
    print("=" * 80)
    
    # Using SP500 as base if available, otherwise any available
    base_name = 'SP500' if 'SP500' in all_data else list(all_data.keys())[0]
    master_df = all_data[base_name][['Date']].copy()
    
    for name, df in all_data.items():
        master_df = master_df.merge(df, on='Date', how='outer')
        
    master_df = master_df.sort_values('Date').reset_index(drop=True)
    
    # Calculate returns
    for name in all_data.keys():
        close_col = f'{name}_Close'
        if close_col in master_df.columns:
            master_df[f'{name}_Return'] = master_df[close_col].pct_change()
            
    today_str = datetime.now().strftime('%d%m%y')
    master_filename = f"global_markets_master_{today_str}.parquet"
    master_path = temp_dir / master_filename
    
    master_df.to_parquet(master_path, index=False)
    print(f"✓ Created master file: {master_filename} ({len(master_df):,} records)")
    
    return master_path, temp_dir

def upload_to_r2_and_cleanup(master_path, temp_dir):
    """Upload data to R2 and clean up temporary directory."""
    bucket = os.getenv("R2_BUCKET")
    if not bucket:
        print("\n⚠️ R2_BUCKET not set. Skipping upload.")
        return
        
    print("\n" + "=" * 80)
    print("☁️ UPLOADING TO CLOUDFLARE R2")
    print("=" * 80)
    
    ensure_folder_exists(bucket, R2_FOLDER)
    
    # 1. Upload Master File
    master_key = f"{R2_FOLDER}{master_path.name}"
    upload_to_r2(str(master_path), bucket, master_key)
    
    # 2. Upload individual ticker files (Overwriting LATEST)
    for ticker_file in temp_dir.glob("*.parquet"):
        if "master" in ticker_file.name:
            continue
        ticker_key = f"{R2_FOLDER}{ticker_file.name}"
        upload_to_r2(str(ticker_file), bucket, ticker_key)
        
    # 3. Cleanup R2
    print(f"\n🧹 Cleaning up old master files in R2 (keeping last 2)...")
    backup_and_cleanup_r2(bucket, R2_FOLDER, file_pattern="global_markets_master_", keep=2)
    
    # 4. Local Cleanup
    import shutil
    shutil.rmtree(temp_dir)
    print(f"\n✨ Cleanup complete. Local temp files removed.")

def main():
    parser = argparse.ArgumentParser(description='Scrape Global Market Data and upload to R2')
    parser.add_argument('--start', type=str, default='7d', help='Start date (YYYY-MM-DD) or "7d" for last week')
    args = parser.parse_args()
    
    load_env_safely()
    
    result = scrape_global_markets(args.start)
    if result:
        master_path, temp_dir = result
        upload_to_r2_and_cleanup(master_path, temp_dir)
        print("\n✅ Global Market Data automation complete.")

if __name__ == "__main__":
    main()
