# ======================================================
# gold_price_scraper.py  —  JSON version (local + GitHub Actions)
# ======================================================

import os, re, json
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

# Try dotenv for local runs; safe no-op in GitHub Actions
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from utils_r2 import (
    upload_to_r2,
    download_from_r2,
    list_r2_files,
    r2_client,
)

# ======================================================
# CONFIGURATION
# ======================================================
HEADERS = {"User-Agent": "Mozilla/5.0"}
SAVE_DIR = Path.cwd() / "gold_price"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

BUCKET = os.getenv("R2_BUCKET")

# Folder structure on R2
PREFIX_MAIN = "cafef_data/"

print("🪙 Fetching SJC gold data (JSON format)...")

# ======================================================
# 1️⃣ FETCH GOLD DATA FROM CAFEF
# ======================================================
def fetch_gold_data():
    urls = {
        "bar": "https://cafef.vn/du-lieu/Ajax/ajaxgoldpricehistory.ashx?index=all",
        "ring": "https://cafef.vn/du-lieu/Ajax/AjaxGoldPriceRing.ashx?time=all&zone=11"
    }

    def fetch(url):
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r.json().get("Data", {}).get("goldPriceWorldHistories", [])

    # --- Download JSON ---
    df_bar = pd.DataFrame(fetch(urls["bar"]))
    df_ring = pd.DataFrame(fetch(urls["ring"]))

    # --- Identify correct timestamp field ---
    date_col_bar = "createdAt" if "createdAt" in df_bar.columns else "lastUpdated"
    date_col_ring = "createdAt" if "createdAt" in df_ring.columns else "lastUpdated"

    # --- Parse to datetime robustly ---
    df_bar["date"] = pd.to_datetime(df_bar[date_col_bar], errors="coerce", format="mixed", utc=True)
    df_ring["date"] = pd.to_datetime(df_ring[date_col_ring], errors="coerce", format="mixed", utc=True)

    # --- Drop invalid dates ---
    df_bar = df_bar.dropna(subset=["date"])
    df_ring = df_ring.dropna(subset=["date"])

    # --- Rename columns for clarity ---
    df_bar = df_bar.rename(columns={"buyPrice": "bar_buy", "sellPrice": "bar_sell"})
    df_ring = df_ring.rename(columns={"buyPrice": "ring_buy", "sellPrice": "ring_sell"})
    
    # --- Normalize units: Convert ring prices from thousand VND to million VND ---
    df_ring["ring_buy"] = df_ring["ring_buy"] / 1000
    df_ring["ring_sell"] = df_ring["ring_sell"] / 1000
    
    # --- Convert to date only (drop time) to avoid duplicates ---
    df_bar["date"] = df_bar["date"].dt.date
    df_ring["date"] = df_ring["date"].dt.date
    
    # --- Aggregate by date (take the latest/last values for each date) ---
    df_bar = df_bar.groupby("date")[["bar_buy", "bar_sell"]].last().reset_index()
    df_ring = df_ring.groupby("date")[["ring_buy", "ring_sell"]].last().reset_index()

    # --- Merge ---
    df = pd.merge(
        df_bar[["date", "bar_buy", "bar_sell"]],
        df_ring[["date", "ring_buy", "ring_sell"]],
        on="date",
        how="outer"
    )

    # --- Convert date back to datetime for consistency ---
    df["date"] = pd.to_datetime(df["date"])
    
    # --- Sort by date ---
    df = df.sort_values("date").reset_index(drop=True)

    # --- Drop rows with no valid prices ---
    df = df.dropna(subset=["bar_buy", "bar_sell", "ring_buy", "ring_sell"], how="all")

    print(f"✅ Retrieved {len(df)} gold records (unique dates).")
    return df


# ======================================================
# 2️⃣ LOAD EXISTING JSON DATA FROM R2
# ======================================================
def load_existing_json_data(bucket, prefix):
    """Load existing gold price JSON from R2."""
    files = list_r2_files(bucket, prefix)
    json_files = [f for f in files if f.endswith("gold_price.json")]
    
    if not json_files:
        print("ℹ️ No existing JSON file found on R2.")
        return pd.DataFrame()
    
    # Download the latest JSON file
    json_key = json_files[0]  # Should only be one gold_price.json
    local_json = SAVE_DIR / "temp_gold_price.json"
    
    if download_from_r2(bucket, json_key, str(local_json)):
        try:
            with open(local_json, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Convert JSON data back to DataFrame
            records = []
            for date_str, prices in data.get("data", {}).items():
                record = {"date": pd.to_datetime(date_str)}
                record.update(prices)
                records.append(record)
            
            df = pd.DataFrame(records)
            print(f"✅ Loaded {len(df)} existing records from R2.")
            
            # Clean up temp file
            os.remove(local_json)
            return df
        except Exception as e:
            print(f"⚠️ Error loading JSON: {e}")
            return pd.DataFrame()
    else:
        return pd.DataFrame()


# ======================================================
# 3️⃣ MERGE AND GENERATE JSON AND CSV
# ======================================================
def generate_gold_json(df, output_path):
    """Convert DataFrame to JSON format indexed by date."""
    # Sort by date
    df = df.sort_values("date").reset_index(drop=True)
    
    # Convert to date-indexed dictionary
    data_dict = {}
    for _, row in df.iterrows():
        date_str = row["date"].strftime("%Y-%m-%d")
        data_dict[date_str] = {
            "bar_buy": float(row["bar_buy"]) if pd.notna(row["bar_buy"]) else None,
            "bar_sell": float(row["bar_sell"]) if pd.notna(row["bar_sell"]) else None,
            "ring_buy": float(row["ring_buy"]) if pd.notna(row["ring_buy"]) else None,
            "ring_sell": float(row["ring_sell"]) if pd.notna(row["ring_sell"]) else None,
        }
    
    # Create JSON structure
    json_data = {
        "last_updated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "total_records": len(data_dict),
        "units": "million_vnd",
        "note": "All prices are in million VND (triệu VNĐ). Ring prices converted from thousand VND.",
        "data": data_dict
    }
    
    # Save to file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    
    print(f"💾 Saved JSON → {output_path} ({len(data_dict)} records)")
    return output_path


def generate_gold_csv(df, output_path):
    """Generate CSV file with dates as rows and prices as columns."""
    # Sort by date
    df = df.sort_values("date").reset_index(drop=True)
    
    # Format date column
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    
    # Save to CSV
    df.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"💾 Saved CSV → {output_path} ({len(df)} records)")
    return output_path


# ======================================================
# 4️⃣ CHECK EXISTING JSON DATES
# ======================================================
def check_existing_json_dates(df):
    """Print summary of dates available in the JSON data."""
    if df.empty:
        print("ℹ️ No existing data.")
        return
    
    df = df.sort_values("date")
    min_date = df["date"].min().strftime("%Y-%m-%d")
    max_date = df["date"].max().strftime("%Y-%m-%d")
    total = len(df)
    
    print(f"\n📊 Data Summary:")
    print(f"   Total records: {total}")
    print(f"   Date range: {min_date} to {max_date}")
    print(f"   Latest 5 dates:")
    for date in df["date"].tail(5):
        print(f"     - {date.strftime('%Y-%m-%d')}")


# ======================================================
# 5️⃣ CHECK IF UPDATE NEEDED
# ======================================================
def latest_json_date(bucket, prefix):
    """Check if JSON file exists and get last_updated timestamp."""
    files = list_r2_files(bucket, prefix)
    json_files = [f for f in files if f.endswith("gold_price.json")]
    
    if not json_files:
        return None
    
    # Download and check last_updated
    json_key = json_files[0]
    local_json = SAVE_DIR / "temp_check.json"
    
    if download_from_r2(bucket, json_key, str(local_json)):
        try:
            with open(local_json, 'r', encoding='utf-8') as f:
                data = json.load(f)
            last_updated = data.get("last_updated", None)
            os.remove(local_json)
            if last_updated:
                return datetime.fromisoformat(last_updated)
        except Exception:
            pass
    
    return None


# ======================================================
# 6️⃣ MAIN SCRIPT
# ======================================================
def update_gold_prices(local_only=False):
    """Update gold prices. If local_only=True, skip R2 operations."""
    today = datetime.now()

    # --- Check if running in local-only mode ---
    if local_only or not BUCKET:
        print("📍 Running in LOCAL-ONLY mode (no R2 operations)")
        existing_df = pd.DataFrame()
    else:
        # --- Skip if already up-to-date on R2 ---
        latest_remote = latest_json_date(BUCKET, PREFIX_MAIN)
        if latest_remote and latest_remote.date() >= today.date():
            print(f"✅ Already up-to-date ({latest_remote.strftime('%Y-%m-%d')}) → skip download.")
            return

        # --- Load existing data from R2 ---
        existing_df = load_existing_json_data(BUCKET, PREFIX_MAIN)

    # --- Fetch new data from API ---
    new_df = fetch_gold_data()

    # --- Merge with existing data ---
    if not existing_df.empty:
        before = len(existing_df)
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
        after = len(combined)
        print(f"📈 Merged data: {after - before} new records added.")
    else:
        combined = new_df
        print(f"🆕 First-time import: {len(new_df)} records.")

    # --- Generate JSON file ---
    # Get latest date from data for filename
    latest_date = combined["date"].max()
    date_suffix = latest_date.strftime("%y%m%d")
    
    json_path = SAVE_DIR / f"gold_price_{date_suffix}.json"
    generate_gold_json(combined, json_path)

    # --- Generate CSV file ---
    csv_path = SAVE_DIR / f"gold_price_{date_suffix}.csv"
    generate_gold_csv(combined.copy(), csv_path)

    # --- Show data summary ---
    check_existing_json_dates(combined)

    # --- Upload to R2 (skip if local_only) ---
    if not local_only and BUCKET:
        # Before uploading new files, move current files to backup
        from utils_r2 import list_r2_files
        
        # List current files
        current_files = list_r2_files(BUCKET, PREFIX_MAIN)
        gold_files = [f for f in current_files if 'gold_price_' in f and (f.endswith('.json') or f.endswith('.csv'))]
        
        # Move current files to backup (only if they're different from what we're uploading)
        backup_prefix = f"{PREFIX_MAIN}gold_price/gold_price_backup/"
        s3 = r2_client()
        
        for old_file in gold_files:
            # Skip if it's the same file we're about to upload
            if date_suffix in old_file:
                continue
            
            # Extract just the filename from the old file path
            filename = old_file.split('/')[-1]
            backup_key = f"{backup_prefix}{filename}"
            s3.copy_object(
                Bucket=BUCKET,
                CopySource={'Bucket': BUCKET, 'Key': old_file},
                Key=backup_key
            )
            print(f"📦 Backed up {old_file} → {backup_key}")
        
        # Clean old backups (keep only 1)
        backup_files = list_r2_files(BUCKET, backup_prefix)
        backup_gold = sorted([f for f in backup_files if 'gold_price_' in f and (f.endswith('.json') or f.endswith('.csv'))])
        
        # Group by date and keep only the most recent
        from collections import defaultdict
        by_date = defaultdict(list)
        for f in backup_gold:
            match = re.search(r'gold_price_(\d{6})', f)
            if match:
                by_date[match.group(1)].append(f)
        
        # Keep only the most recent date
        if len(by_date) > 1:
            dates_sorted = sorted(by_date.keys(), reverse=True)
            for old_date in dates_sorted[1:]:  # Skip the most recent
                for f in by_date[old_date]:
                    s3.delete_object(Bucket=BUCKET, Key=f)
                    print(f"🗑️ Deleted old backup: {f}")
        
        # Upload new files
        upload_to_r2(str(json_path), BUCKET, f"{PREFIX_MAIN}gold_price/gold_price_{date_suffix}.json")
        upload_to_r2(str(csv_path), BUCKET, f"{PREFIX_MAIN}gold_price/gold_price_{date_suffix}.csv")
        print(f"☁️ Uploaded new gold data (JSON + CSV) to R2 with date suffix: {date_suffix}")
        
        # Clean up local files
        try:
            for f in SAVE_DIR.glob("gold_price_*"):
                os.remove(f)
                print(f"🧹 Deleted local file: {f}")
            if not any(SAVE_DIR.iterdir()):
                SAVE_DIR.rmdir()
                print(f"🗑️ Removed empty folder: {SAVE_DIR}")
        except Exception as e:
            print(f"⚠️ Cleanup error: {e}")
    else:
        print(f"\n📁 Files saved locally:")
        print(f"   - {json_path}")
        print(f"   - {csv_path}")
        print(f"\n💡 Review the files, then run with R2 upload enabled.")


# ======================================================
# 7️⃣ ENTRY POINT
# ======================================================
if __name__ == "__main__":
    # Run in local-only mode if BUCKET is not set
    local_mode = not bool(BUCKET)
    update_gold_prices(local_only=local_mode)
    print("✅ Gold price update completed successfully.")