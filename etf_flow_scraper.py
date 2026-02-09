#!/usr/bin/env python3
"""
ETF Flow Scraper - Tracks EM/FM Fund Flows (Proxy) and uploads to Cloudflare R2.
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

# ETFs to track
ETFS = {
    'EEM': 'iShares MSCI Emerging Markets ETF',
    'VWO': 'Vanguard FTSE Emerging Markets ETF',
    'VNM': 'VanEck Vietnam ETF',
    'FM': 'iShares MSCI Frontier 100 ETF',
}

R2_FOLDER = "cafef_data/etf_flows/"

def load_env_safely():
    """Load .env for local runs, skip if env vars already exist."""
    if not os.getenv("R2_BUCKET"):
        env_path = Path(__file__).resolve().parent / ".env"
        if env_path.exists():
            load_dotenv(env_path)

def fetch_etf_data(ticker: str, period: str = '2y') -> pd.DataFrame:
    """Fetch ETF price and volume data from Yahoo Finance"""
    print(f"  📊 Fetching {ticker}...")
    try:
        etf = yf.Ticker(ticker)
        df = etf.history(period=period)
        if df.empty: return pd.DataFrame()
        
        df = df.reset_index()
        # Handle MultiIndex if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]
            
        df['Ticker'] = ticker
        
        # Calculate flows proxy: Volume * Price = Dollar Volume
        df['Dollar_Volume'] = df['Close'] * df['Volume']
        
        # Calculate price change as sentiment indicator
        df['Return'] = df['Close'].pct_change()
        df['Return_5D'] = df['Close'].pct_change(5)
        
        # Inflow proxy: If price up and volume high = inflows
        rolling_mean = df['Volume'].rolling(20).mean()
        rolling_std = df['Volume'].rolling(20).std()
        df['Volume_Z'] = (df['Volume'] - rolling_mean) / rolling_std
        df['Flow_Signal'] = df['Return'] * df['Volume_Z']  # Positive = inflows
        
        return df
    except Exception as e:
        print(f"    ✗ Error fetching {ticker}: {e}")
        return pd.DataFrame()

def calculate_em_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate overall EM sentiment from ETF data"""
    if df.empty: return pd.DataFrame()
    
    # Pivot to get daily data for each ETF
    daily = df.pivot_table(
        index='Date',
        columns='Ticker',
        values=['Return', 'Flow_Signal', 'Volume_Z'],
        aggfunc='first'
    )
    
    # Flatten column names
    daily.columns = ['_'.join(col).strip() for col in daily.columns.values]
    daily = daily.reset_index()
    
    # Create aggregate EM sentiment
    em_etfs = ['EEM', 'VWO']
    fm_etfs = ['FM', 'VNM']
    
    # EM Sentiment (average of EEM and VWO)
    em_cols = [c for c in daily.columns if any(e in c for e in em_etfs) and 'Flow_Signal' in c]
    if em_cols:
        daily['EM_Flow_Sentiment'] = daily[em_cols].mean(axis=1)
    
    # FM Sentiment
    fm_cols = [c for c in daily.columns if any(e in c for e in fm_etfs) and 'Flow_Signal' in c]
    if fm_cols:
        daily['FM_Flow_Sentiment'] = daily[fm_cols].mean(axis=1)
    
    # VNM-specific signal
    if 'Flow_Signal_VNM' in daily.columns:
        daily['VNM_Flow_Signal'] = daily['Flow_Signal_VNM']
    
    return daily

import argparse

def scrape_etfs(period='2y'):
    print("="*60)
    print(f"🌍 ETF FLOW SCRAPER - EM/FM Sentiment Proxy (Period: {period})")
    print("="*60)
    
    all_dfs = []
    for ticker in ETFS.keys():
        df = fetch_etf_data(ticker, period=period)
        if not df.empty:
            all_dfs.append(df)
            
    if not all_dfs:
        print("❌ No data fetched for any ETF.")
        return None
        
    combined_df = pd.concat(all_dfs, ignore_index=True)
    sentiment_df = calculate_em_sentiment(combined_df)
    
    today_str = datetime.now().strftime('%d%m%y')
    master_file = f"etf_master_{today_str}.parquet"
    sentiment_file = f"em_fm_sentiment_{today_str}.parquet"
    
    combined_df.to_parquet(master_file, index=False)
    sentiment_df.to_parquet(sentiment_file, index=False)
    
    print(f"\n✅ Created: {master_file} and {sentiment_file}")
    return master_file, sentiment_file

def upload_and_cleanup(master_file, sentiment_file):
    bucket = os.getenv("R2_BUCKET")
    if not bucket:
        print("\n⚠️ R2_BUCKET not set. Skipping upload.")
        return
        
    print("\n" + "=" * 80)
    print("☁️ UPLOADING TO CLOUDFLARE R2")
    print("=" * 80)
    
    ensure_folder_exists(bucket, R2_FOLDER)
    
    upload_to_r2(master_file, bucket, f"{R2_FOLDER}{master_file}")
    upload_to_r2(sentiment_file, bucket, f"{R2_FOLDER}{sentiment_file}")
    
    print(f"\n🧹 Cleaning up old ETF files in R2 (keeping last 2)...")
    backup_and_cleanup_r2(bucket, R2_FOLDER, file_pattern="etf_master_", keep=2)
    backup_and_cleanup_r2(bucket, R2_FOLDER, file_pattern="em_fm_sentiment_", keep=2)
    
    if os.path.exists(master_file): os.remove(master_file)
    if os.path.exists(sentiment_file): os.remove(sentiment_file)
    print(f"\n✨ Cleanup complete.")

def main():
    parser = argparse.ArgumentParser(description='Sync ETF Flows')
    parser.add_argument('--period', type=str, default='2y', help='Yahoo Finance period (e.g. 2y, 5y, max)')
    args = parser.parse_args()
    
    load_env_safely()
    files = scrape_etfs(period=args.period)
    if files:
        upload_and_cleanup(*files)

if __name__ == "__main__":
    main()
