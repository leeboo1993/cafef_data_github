#!/usr/bin/env python3
"""
Smart R2 Sync - Downloads only the latest dated files and cleans up old local files.
This guarantees we always have the latest data without accumulating old files.
"""

import boto3
import os
import re
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
from collections import defaultdict

# Load environment variables
load_dotenv()

# R2 Configuration
R2_ENDPOINT = os.getenv('R2_ENDPOINT')
R2_ACCESS_KEY_ID = os.getenv('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.getenv('R2_SECRET_ACCESS_KEY')
R2_BUCKET = os.getenv('R2_BUCKET')

# Local data directory
DATA_DIR = Path(__file__).resolve().parent.parent.parent / 'data'
DATA_DIR.mkdir(exist_ok=True)

def parse_date_from_filename(filename):
    """
    Extract date from filename with pattern: *_DDMMYY.* or *_YYMMDD.*
    Format detection based on filename:
    - gold_price_*, deposit_rate_*: YYMMDD (e.g., 260119 = 2026-01-19)
    - All others: DDMMYY (e.g., 190126 = 2026-01-19)
    Returns datetime object or None if no date found.
    """
    # Match 6-digit date pattern
    pattern = r'_(\d{6})(?:_|\.|$)'
    match = re.search(pattern, filename)
    
    if not match:
        return None
    
    date_str = match.group(1)
    
    # Detect format based on filename
    uses_yymmdd = any(x in filename.lower() for x in ['gold_price', 'deposit_rate', 'vcb_fx', 'usd_black_market', 'usd_market'])
    
    # New sources use DDMMYY
    # global_markets_master_DDMMYY.parquet
    # etf_master_DDMMYY.parquet
    # em_fm_sentiment_DDMMYY.parquet
    # all_foreign_flows_DDMMYY.parquet
    # proprietary_market_flows_DDMMYY.parquet
    # crypto_prices_DDMMYY.parquet
    # bond_stress_index_DDMMYY.parquet
    
    try:
        if uses_yymmdd:
            # YYMMDD format: 260119 = 2026-01-19
            year = 2000 + int(date_str[0:2])
            month = int(date_str[2:4])
            day = int(date_str[4:6])
        else:
            # DDMMYY format: 190126 = 2026-01-19
            day = int(date_str[0:2])
            month = int(date_str[2:4])
            year = 2000 + int(date_str[4:6])
        
        return datetime(year, month, day)
    except ValueError:
        return None

def get_latest_files_from_r2(s3_client):
    """
    Get list of only the latest dated files from R2.
    Returns dict: {pattern: (key, size, date)}
    """
    print("\n🔍 Analyzing R2 bucket to find latest files...")
    
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=R2_BUCKET)
    
    # Group files by pattern
    file_groups = defaultdict(list)
    
    for page in page_iterator:
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            key = obj['Key']
            size = obj['Size']
            
            if key.endswith('/'):
                continue
            
            date = parse_date_from_filename(key)
            
            if date:
                # Create pattern by removing date
                pattern = re.sub(r'_\d{6}(?:_|\.|)', '_LATEST.', key)
                file_groups[pattern].append({
                    'key': key,
                    'size': size,
                    'date': date
                })
            else:
                # Non-dated files - always include
                file_groups[key].append({
                    'key': key,
                    'size': size,
                    'date': None
                })
    
    # Select latest file from each group
    latest_files = {}
    
    for pattern, files in file_groups.items():
        if len(files) == 1:
            # Single file or non-dated
            f = files[0]
            latest_files[pattern] = (f['key'], f['size'], f['date'])
        else:
            # Multiple dated files - pick latest
            latest = max(files, key=lambda x: x['date'] if x['date'] else datetime.min)
            latest_files[pattern] = (latest['key'], latest['size'], latest['date'])
            print(f"   📂 {pattern}: selecting {latest['key']} from {len(files)} versions")
    
    return latest_files

def cleanup_old_local_files(data_dir, keep_patterns):
    """Remove old local dated files that don't match the keep patterns."""
    print("\n🧹 Cleaning up old local files...")
    
    removed_count = 0
    freed_space = 0
    
    # Build set of files to keep (exact filenames from patterns)
    keep_files = set()
    for key, _, _ in keep_patterns.values():
        local_path = data_dir / key
        keep_files.add(local_path)
    
    # Scan local directory for dated files
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.startswith('.'):
                continue
            
            local_path = Path(root) / file
            
            # If it has a date pattern and is NOT in our keep list
            if parse_date_from_filename(file) and local_path not in keep_files:
                size = local_path.stat().st_size
                try:
                    local_path.unlink()
                    removed_count += 1
                    freed_space += size
                    print(f"   ❌ Removed: {local_path.relative_to(data_dir)} ({size / 1024 / 1024:.1f} MB)")
                except Exception as e:
                    print(f"   ⚠️  Failed to remove {file}: {e}")
    
    if removed_count > 0:
        print(f"\n✅ Cleaned up {removed_count} old files, freed {freed_space / 1024 / 1024:.1f} MB")
    else:
        print("   ✓ No old files to clean")

def smart_sync_from_r2():
    """Smart sync: download only latest files and cleanup old ones."""
    
    print("="*60)
    print("🚀 SMART R2 DATA SYNC")
    print("="*60)
    print(f"Bucket: {R2_BUCKET}")
    print(f"Endpoint: {R2_ENDPOINT}")
    
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name='auto'
    )
    
    try:
        # Step 1: Find latest files in R2
        latest_files = get_latest_files_from_r2(s3_client)
        
        print(f"\n📊 Found {len(latest_files)} file patterns to sync")
        
        # Step 2: Clean up old local files
        cleanup_old_local_files(DATA_DIR, latest_files)
        
        # Step 3: Download latest files
        print("\n⬇️  Downloading latest files...")
        
        downloaded = 0
        skipped = 0
        total_size = 0
        
        for pattern, (key, size, date) in latest_files.items():
            # Custom local path mapping based on R2 key
            if 'global_markets' in key:
                local_path = DATA_DIR / 'global_markets' / Path(key).name
            elif 'etf_flows' in key:
                local_path = DATA_DIR / 'etf_flows' / Path(key).name
            elif 'investor_flows' in key:
                local_path = DATA_DIR / 'investor_flows' / Path(key).name
            elif 'alternative_assets' in key:
                local_path = DATA_DIR / 'alternative_assets' / Path(key).name
            elif 'bond_market' in key:
                local_path = DATA_DIR / 'bond_market' / Path(key).name
            else:
                local_path = DATA_DIR / key
                
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            total_size += size
            
            # Skip if already exists with same size
            if local_path.exists() and local_path.stat().st_size == size:
                skipped += 1
                print(f"   ✓ Skip: {key} (already up-to-date)")
                continue
            
            # Download
            date_str = f" [{date.strftime('%Y-%m-%d')}]" if date else ""
            print(f"   ⬇️  Download: {key}{date_str} ({size / 1024 / 1024:.1f} MB)")
            s3_client.download_file(R2_BUCKET, key, str(local_path))
            downloaded += 1
        
        print("\n" + "="*60)
        print("✅ SMART SYNC COMPLETE")
        print("="*60)
        print(f"Files downloaded: {downloaded}")
        print(f"Files skipped:    {skipped}")
        print(f"Total data size:  {total_size / 1024 / 1024:.1f} MB")
        print(f"Saved to:         {DATA_DIR.absolute()}")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise

if __name__ == '__main__':
    smart_sync_from_r2()
