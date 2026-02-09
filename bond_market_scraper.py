#!/usr/bin/env python3
"""
Bond Market Scraper - Exports bond market stress index and data to R2.
"""

import os
import pandas as pd
import json
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

R2_FOLDER = "cafef_data/bond_market/"

# Manual data entry based on VBMA reports and research (from new_model/data/track_bond_market.py)
BOND_MARKET_DATA = {
    'yearly_issuance': {
        2019: {'total': 280, 'bank': 140, 'real_estate': 80, 'other': 60},
        2020: {'total': 430, 'bank': 200, 'real_estate': 130, 'other': 100},
        2021: {'total': 658, 'bank': 280, 'real_estate': 220, 'other': 158},
        2022: {'total': 255, 'bank': 136, 'real_estate': 52, 'other': 67},
        2023: {'total': 311, 'bank': 175, 'real_estate': 40, 'other': 96},
        2024: {'total': 380, 'bank': 210, 'real_estate': 55, 'other': 115},
        2025: {'total': 420, 'bank': 230, 'real_estate': 70, 'other': 120},
    },
    'defaults': {
        2022: {'value': 35000, 'count': 24, 're_pct': 0.70},
        2023: {'value': 94400, 'count': 69, 're_pct': 0.84},
        2024: {'value': 45000, 'count': 35, 're_pct': 0.75},
        2025: {'value': 25000, 'count': 20, 're_pct': 0.65},
    },
    'events': {
        '2022-03': 'Tan Hoang Minh bonds cancelled',
        '2022-10': 'Van Thinh Phat scandal breaks',
        '2022-10': 'Decree 65 tightens bond issuance',
        '2022-11': 'SCB bank crisis',
        '2023-03': 'Default rate peaks at 8.15%',
        '2023-06': 'Novaland debt restructuring',
        '2024-01': 'Decree 08 bond rollover support',
        '2024-06': 'Market stabilization begins',
        '2025-09': 'FTSE upgrade expected',
    }
}

def load_env_safely():
    """Load .env for local runs, skip if env vars already exist."""
    if not os.getenv("R2_BUCKET"):
        env_path = Path(__file__).resolve().parent / ".env"
        if env_path.exists():
            load_dotenv(env_path)

def calculate_bond_stress_index():
    """Calculate a bond market stress index."""
    results = []
    for year in range(2019, 2026):
        issuance = BOND_MARKET_DATA['yearly_issuance'].get(year, {})
        defaults = BOND_MARKET_DATA['defaults'].get(year, {'value': 0, 'count': 0, 're_pct': 0})
        total_issuance = issuance.get('total', 0)
        re_issuance = issuance.get('real_estate', 0)
        default_value = defaults['value'] / 1000
        
        if total_issuance > 0:
            re_concentration = re_issuance / total_issuance
            default_ratio = default_value / total_issuance if total_issuance > 0 else 0
            stress_index = default_ratio * 100 + re_concentration * 10
        else:
            stress_index, re_concentration, default_ratio = 0, 0, 0
            
        results.append({
            'year': year,
            'total_issuance_T': total_issuance,
            're_issuance_T': re_issuance,
            'default_value_T': default_value,
            'default_count': defaults['count'],
            're_concentration': re_concentration,
            'stress_index': stress_index
        })
    return pd.DataFrame(results)

def scrape_bond_market():
    print("="*60)
    print("📉 BOND MARKET TRACKER")
    print("="*60)
    
    df = calculate_bond_stress_index()
    
    today_str = datetime.now().strftime('%d%m%y')
    parquet_file = f"bond_stress_index_{today_str}.parquet"
    json_file = f"bond_market_data_{today_str}.json"
    
    df.to_parquet(parquet_file, index=False)
    with open(json_file, 'w') as f:
        json.dump(BOND_MARKET_DATA, f, indent=2)
        
    print(f"✅ Created: {parquet_file} and {json_file}")
    return parquet_file, json_file

def upload_and_cleanup(parquet_file, json_file):
    bucket = os.getenv("R2_BUCKET")
    if not bucket: return
    
    print("\n" + "=" * 80)
    print("☁️ UPLOADING TO CLOUDFLARE R2")
    print("=" * 80)
    
    ensure_folder_exists(bucket, R2_FOLDER)
    upload_to_r2(parquet_file, bucket, f"{R2_FOLDER}{parquet_file}")
    upload_to_r2(json_file, bucket, f"{R2_FOLDER}{json_file}")
    
    print(f"\n🧹 Cleaning up old bond files in R2 (keeping last 2)...")
    backup_and_cleanup_r2(bucket, R2_FOLDER, file_pattern="bond_stress_index_", keep=2)
    backup_and_cleanup_r2(bucket, R2_FOLDER, file_pattern="bond_market_data_", keep=2)
    
    if os.path.exists(parquet_file): os.remove(parquet_file)
    if os.path.exists(json_file): os.remove(json_file)
    print(f"\n✨ Cleanup complete.")

def main():
    load_env_safely()
    files = scrape_bond_market()
    if files:
        upload_and_cleanup(*files)

if __name__ == "__main__":
    main()
