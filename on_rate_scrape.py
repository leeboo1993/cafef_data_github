#!/usr/bin/env python3
"""
Incremental Interbank Rate Collection with Date-based Filename
Filename format: interbank_rate_DDMMYY.csv (DDMMYY = latest date in file)

Usage:
    python on_rate_scrape.py           # Daily update
    python on_rate_scrape.py --check   # Check status
"""
import argparse
import os
import pandas as pd
from datetime import date, datetime
from typing import Dict, Optional
import glob
import shutil
import httpx
from bs4 import BeautifulSoup
import re
from pathlib import Path

# Try dotenv for local runs; safe no-op in GitHub Actions
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# Import R2 utilities
from utils_r2 import (
    upload_to_r2,
    download_from_r2,
    list_r2_files,
    backup_and_cleanup_r2,
)

# R2 Configuration
BUCKET = os.getenv("R2_BUCKET")
PREFIX_MAIN = "cafef_data/"
SAVE_DIR = Path.cwd() / "interbank_rate"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

# SBV Configuration
SBV_INTERBANK_URL = "https://www.sbv.gov.vn/lãi-suất1"

# Tenor mappings
VIETNAMESE_TENORS = {
    'qua đêm': 'ON',
    '1 tuần': '1W',
    '2 tuần': '2W',
    '1 tháng': '1M',
    '3 tháng': '3M',
    '6 tháng': '6M',
    '9 tháng': '9M',
    '12 tháng': '12M',
}


def parse_vietnamese_float(value: str) -> Optional[float]:
    """Parse Vietnamese number format"""
    if not value or value.strip() in ['', '-', 'N/A', 'NA']:
        return None
    
    try:
        cleaned = value.strip().replace('%', '').strip()
        if ',' in cleaned:
            cleaned = cleaned.replace('.', '')
            cleaned = cleaned.replace(',', '.')
            return float(cleaned)
        if re.fullmatch(r'\d{1,3}(?:\.\d{3})+', cleaned):
            cleaned = cleaned.replace('.', '')
        return float(cleaned)
    except (ValueError, AttributeError):
        return None


def normalize_tenor(text: str) -> str:
    """Normalize Vietnamese tenor labels"""
    t = (text or "").strip().lower()
    for vn, label in VIETNAMESE_TENORS.items():
        if vn in t:
            return label
    m = re.search(r"(\d+)\s*(tuần|tháng)", t)
    if m:
        value = int(m.group(1))
        unit = m.group(2)
        if unit == "tuần":
            return f"{value}W"
        if unit == "tháng":
            return f"{value}M"
    if "qua đêm" in t or "o/n" in t:
        return "ON"
    return ""


def fetch_latest_interbank_rates() -> Dict[str, Dict]:
    """Fetch latest interbank rates and volumes from SBV"""
    print(f"  Fetching from SBV...")
    
    client = httpx.Client(
        timeout=30.0,
        follow_redirects=True,
        verify=False,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    )
    
    try:
        response = client.get(SBV_INTERBANK_URL)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        heading = soup.find(lambda tag: 
            tag.name in ("h2", "h3") and 
            "liên ngân hàng" in tag.get_text(" ", strip=True).lower()
        )
        
        if not heading:
            return {}
        
        container = heading.find_parent()
        if not container:
            return {}
        
        # Find applied date
        applied_date = None
        subnote = container.find(lambda tag: 
            tag.name in ("div", "p") and 
            "ngày áp dụng" in tag.get_text(" ", strip=True).lower()
        )
        if subnote:
            strong = subnote.find("strong")
            if strong:
                date_text = strong.get_text(strip=True)
                try:
                    applied_date = datetime.strptime(date_text, "%d/%m/%Y").date()
                except:
                    pass
        
        if not applied_date:
            applied_date = date.today()
        
        # Find table
        table = container.find("table")
        if not table:
            return {}
        
        rates = {}
        for row in table.find_all("tr"):
            tds = row.find_all("td")
            if len(tds) < 2:
                continue
            
            tenor_text = tds[0].get_text(" ", strip=True)
            rate_text = tds[1].get_text(" ", strip=True)
            volume_text = tds[2].get_text(" ", strip=True) if len(tds) > 2 else None
            
            tenor_label = normalize_tenor(tenor_text)
            rate = parse_vietnamese_float(rate_text.replace('*', ''))
            volume = parse_vietnamese_float(volume_text) if volume_text else None
            
            if tenor_label and rate is not None:
                rates[tenor_label] = {
                    'date': applied_date,
                    'rate': rate,
                    'volume': volume
                }
        
        return rates
        
    except Exception as e:
        print(f"  Error: {e}")
        return {}
    finally:
        client.close()


def find_latest_file() -> Optional[str]:
    """Find the most recent interbank CSV file"""
    pattern = "interbank_rate_*.csv"
    files = glob.glob(pattern)
    
    if not files:
        return None
    
    # Sort by date in filename (DDMMYY)
    def extract_date(filename):
        try:
            date_str = filename.replace('interbank_rate_', '').replace('.csv', '')
            return datetime.strptime(date_str, '%d%m%y')
        except:
            return datetime.min
    
    files.sort(key=extract_date, reverse=True)
    return files[0]


def load_existing_from_r2(bucket, prefix):
    """Load existing interbank rate CSV from R2."""
    files = list_r2_files(bucket, prefix)
    csv_files = [f for f in files if re.search(r"interbank_rate_\d{6}\.csv$", f)]
    
    if not csv_files:
        print("ℹ️ No existing interbank rate CSV found on R2.")
        return None
    
    # Sort to get the latest one (DDMMYY format)
    csv_files.sort(reverse=True)
    csv_key = csv_files[0]
    local_csv = SAVE_DIR / "temp_interbank_rate.csv"
    
    if download_from_r2(bucket, csv_key, str(local_csv)):
        try:
            df = pd.read_csv(local_csv)
            print(f"✅ Loaded existing interbank rate data from R2: {len(df)} rows")
            
            # Get latest date
            latest_date = get_file_latest_date(str(local_csv))
            
            # Copy to working file
            working_file = f"interbank_rate_{latest_date.strftime('%d%m%y')}.csv" if latest_date else "interbank_rate.csv"
            shutil.copy(local_csv, working_file)
            
            # Clean up temp file
            os.remove(local_csv)
            return working_file
        except Exception as e:
            print(f"⚠️ Error loading CSV from R2: {e}")
            if os.path.exists(local_csv):
                os.remove(local_csv)
            return None
    else:
        return None



def get_file_latest_date(csv_file: str) -> Optional[date]:
    """Get the latest date from CSV file"""
    try:
        df = pd.read_csv(csv_file)
        if df.empty or 'date' not in df.columns:
            return None
        
        # Get latest date - handle DD/MM/YYYY format
        latest = pd.to_datetime(df['date'], format='mixed', dayfirst=True).max()
        return latest.date()
    except Exception as e:
        print(f"  ⚠️  Error reading file: {e}")
        return None


def create_initial_file(initial_data_file: str = 'interbank_rates_complete.csv') -> str:
    """Create initial file from existing data"""
    if not os.path.exists(initial_data_file):
        print(f"  ❌ Initial data file not found: {initial_data_file}")
        print(f"  Create {initial_data_file} first using convert_fiinprox.py")
        sys.exit(1)
    
    # Load data
    df = pd.read_csv(initial_data_file)
    latest = pd.to_datetime(df['date'], format='mixed', dayfirst=True).max().date()
    
    # Create filename with latest date
    filename = f"interbank_rate_{latest.strftime('%d%m%y')}.csv"
    shutil.copy(initial_data_file, filename)
    
    print(f"  ✅ Created initial file: {filename}")
    print(f"  Latest date in file: {latest}")
    return filename


def update_file_with_new_data(csv_file: str, new_rates: Dict, new_date: date) -> str:
    """
    Update file with new data and rename with new date
    
    Logic:
    1. If new_date has blank row -> Fill it (backfill)
    2. If new_date is missing -> Add it with data
    3. If data not available -> Add blank row for continuity
    
    Returns: new filename
    """
    # Load existing data
    df = pd.read_csv(csv_file)
    df['date'] = pd.to_datetime(df['date'], format='mixed', dayfirst=True)
    
    # Check if date already exists
    existing_row = df[df['date'] == pd.to_datetime(new_date)]
    
    if not existing_row.empty:
        # Date exists - check if it's blank
        row_idx = existing_row.index[0]
        is_blank = pd.isna(existing_row['ON'].iloc[0]) or existing_row['ON'].iloc[0] == ''
        
        if is_blank and new_rates:
            # Backfill blank row with data
            print(f"  📝 Backfilling data for existing blank date: {new_date}")
            for tenor, data in new_rates.items():
                df.at[row_idx, tenor] = data['rate']
                df.at[row_idx, f"{tenor}_vol_bn"] = data.get('volume', '')
                vol_str = f" (vol: {data.get('volume', 'N/A')} bn)" if data.get('volume') else ""
                print(f"    ✏️  {tenor} = {data['rate']}%{vol_str}")
        else:
            # Date exists and has data - no update needed
            print(f"  ⏭️  Date {new_date} already has data - skipping")
            return csv_file
    else:
        # Date doesn't exist - add new row
        if new_rates:
            print(f"  ➕ Adding new date with data: {new_date}")
            new_row = {'date': pd.Timestamp(new_date)}
            for tenor, data in new_rates.items():
                new_row[tenor] = data['rate']
                new_row[f"{tenor}_vol_bn"] = data.get('volume', '')
                vol_str = f" (vol: {data.get('volume', 'N/A')} bn)" if data.get('volume') else ""
                print(f"    ➕ {tenor} = {data['rate']}%{vol_str}")
        else:
            # No data available - add blank placeholder
            print(f"  📝 Adding blank placeholder for: {new_date}")
            new_row = {'date': pd.Timestamp(new_date)}
            # All tenor columns will be empty
        
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    
    # Sort by date
    df = df.sort_values('date')
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    
    # Determine filename based on latest date in file
    latest_in_file = pd.to_datetime(df['date']).max().date()
    new_filename = f"interbank_rate_{latest_in_file.strftime('%d%m%y')}.csv"
    
    # Save to new file
    df.to_csv(new_filename, index=False)
    
    # Remove old file if different
    if new_filename != csv_file and os.path.exists(csv_file):
        os.remove(csv_file)
        print(f"  🗑️  Removed old file: {csv_file}")
    
    print(f"  ✅ Saved to: {new_filename}")
    return new_filename


def ensure_continuous_timeline(csv_file: str) -> str:
    """
    Ensure timeline is continuous by adding blank rows for missing dates
    (Weekends, holidays, etc.)
    """
    df = pd.read_csv(csv_file)
    df['date'] = pd.to_datetime(df['date'])
    
    # Get date range
    min_date = df['date'].min()
    max_date = df['date'].max()
    
    # Create complete date range
    all_dates = pd.date_range(start=min_date, end=max_date, freq='D')
    
    # Find missing dates
    existing_dates = set(df['date'])
    missing_dates = [d for d in all_dates if d not in existing_dates]
    
    if missing_dates:
        print(f"  📅 Found {len(missing_dates)} missing dates - adding placeholders")
        # Add blank rows for missing dates
        for missing_date in missing_dates:
            new_row = {'date': missing_date}
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        
        # Sort and save
        df = df.sort_values('date')
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        df.to_csv(csv_file, index=False)
        print(f"  ✅ Added {len(missing_dates)} blank placeholder rows")
    
    return csv_file


def check_status(csv_file: str):
    """Check current status of data"""
    print(f"\n📊 Current Status:")
    print(f"  File: {csv_file}")
    
    df = pd.read_csv(csv_file)
    latest = pd.to_datetime(df['date'], format='mixed', dayfirst=True).max().date()
    earliest = pd.to_datetime(df['date'], format='mixed', dayfirst=True).min().date()
    
    print(f"  Total dates: {len(df)}")
    print(f"  Date range: {earliest} to {latest}")
    print(f"  Latest date: {latest}")
    print(f"  Days old: {(date.today() - latest).days}")
    
    # Check coverage
    tenors = [col for col in df.columns if col != 'date' and not col.endswith('_vol_bn')]
    print(f"\n  Tenor coverage:")
    for tenor in tenors:
        count = df[tenor].notna().sum()
        pct = (count / len(df)) * 100
        print(f"    {tenor}: {count}/{len(df)} ({pct:.1f}%)")


def main():
    parser = argparse.ArgumentParser(description='Incremental Interbank Rate Collection')
    parser.add_argument('--check', action='store_true',
                       help='Check current status and exit')
    parser.add_argument('--init', type=str,
                       help='Initialize from existing file')
    parser.add_argument('--local-only', action='store_true',
                       help='Run in local-only mode (no R2 operations)')
    
    args = parser.parse_args()
    
    print(f"Interbank Rate Incremental Collection")
    print(f"{'='*60}\n")
    
    # Determine if running in local-only mode
    local_only = args.local_only or not BUCKET
    
    if local_only:
        print("📍 Running in LOCAL-ONLY mode (no R2 operations)")
    
    # Find or download existing file
    csv_file = None
    
    if not local_only and BUCKET:
        # Try to load from R2 first
        print("☁️ Checking R2 for existing data...")
        csv_file = load_existing_from_r2(BUCKET, f"{PREFIX_MAIN}interbank_rate/")
    
    if not csv_file:
        # Try to find local file
        csv_file = find_latest_file()
    
    if not csv_file:
        if args.init:
            csv_file = create_initial_file(args.init)
        else:
            print("  ❌ No existing interbank file found")
            print("  Run with --init to create from existing data:")
            print("  python on_rate_scrape.py --init interbank_rates_complete.csv")
            return
    
    print(f"📁 Found file: {csv_file}")
    
    # Check mode
    if args.check:
        check_status(csv_file)
        return
    
    # Get latest date in file
    latest_date = get_file_latest_date(csv_file)
    if not latest_date:
        print("  ❌ Could not determine latest date in file")
        return
    
    print(f"  Latest date in file: {latest_date}")
    
    # Fetch latest rates from SBV
    print(f"\n🌐 Fetching latest rates from SBV...")
    new_rates = fetch_latest_interbank_rates()
    
    if not new_rates:
        print("  ⚠️ No new rates fetched from SBV")
        # If running in automation, we still want to upload existing data
        if not local_only and BUCKET:
            print("  📤 Uploading existing data to R2...")
            upload_to_r2_with_backup(csv_file, local_only)
        return
    
    # Get date from fetched rates (or use today if no data)
    if new_rates:
        first_tenor = list(new_rates.keys())[0]
        new_date = new_rates[first_tenor]['date']
    else:
        new_date = date.today()
    
    print(f"  Target date: {new_date}")
    
    # Always update (might be backfilling blank row or adding new)
    print(f"\n📝 Processing data for {new_date}...")
    
    new_filename = update_file_with_new_data(csv_file, new_rates, new_date)
    
    print(f"\n{'='*60}")
    print(f"✅ Successfully processed!")
    print(f"  File: {new_filename}")
    print(f"  Date: {new_date}")
    if new_rates:
        print(f"  Status: Data added/updated")
    else:
        print(f"  Status: Blank placeholder added")
    print(f"{'='*60}")
    
    # Show status
    check_status(new_filename)
    
    # Upload to R2 if enabled
    if not local_only and BUCKET:
        upload_to_r2_with_backup(new_filename, local_only)
    else:
        print(f"\n📁 File saved locally: {new_filename}")
        print(f"💡 Run without --local-only to upload to R2")


def upload_to_r2_with_backup(csv_file, local_only=False):
    """Upload interbank rate file to R2 with backup management."""
    if local_only or not BUCKET:
        return
    
    try:
        # Extract date from filename
        filename = os.path.basename(csv_file)
        
        # Upload to R2
        r2_key = f"{PREFIX_MAIN}interbank_rate/{filename}"
        upload_to_r2(csv_file, BUCKET, r2_key)
        print(f"☁️ Uploaded to R2: {r2_key}")
        
        # Clean old backups (keep only 1 backup)
        print("🧹 Cleaning old backups for interbank_rate in R2...")
        backup_and_cleanup_r2(BUCKET, f"{PREFIX_MAIN}interbank_rate/", keep=1)
        
        print("✅ R2 sync complete")
    except Exception as e:
        print(f"⚠️ Error uploading to R2: {e}")
    finally:
        # Clean up local file
        try:
            if os.path.exists(csv_file):
                os.remove(csv_file)
                print(f"🧹 Deleted local file: {csv_file}")
            if SAVE_DIR.exists() and not any(SAVE_DIR.iterdir()):
                SAVE_DIR.rmdir()
                print(f"🗑️ Removed empty folder: {SAVE_DIR}")
        except Exception as e:
            print(f"⚠️ Cleanup error: {e}")



if __name__ == '__main__':
    main()
