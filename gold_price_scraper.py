# ======================================================
# gold_price_scraper.py  —  Production version (local + GitHub Actions)
# ======================================================

import os, re
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
    clean_old_backups_r2,
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
PREFIX_BACKUP = "cafef_data/cafef_data_backup/gold_price/"

print("🪙 Fetching SJC gold data (incremental update, no spreads)...")

# ======================================================
# 1️⃣ FETCH GOLD DATA FROM CAFEF (robust mixed timestamp parsing)
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

    # --- Merge ---
    df = pd.merge(
        df_bar[["date", "bar_buy", "bar_sell"]],
        df_ring[["date", "ring_buy", "ring_sell"]],
        on="date",
        how="outer"
    )

    # --- Normalize time zone and sort ---
    df["date"] = df["date"].dt.tz_localize(None)
    df = df.sort_values("date").reset_index(drop=True)

    # --- Drop rows with no valid prices ---
    df = df.dropna(subset=["bar_buy", "bar_sell", "ring_buy", "ring_sell"], how="all")

    print(f"✅ Retrieved {len(df)} gold records.")
    return df


# ======================================================
# 2️⃣ CLEAN R2 CACHE
# ======================================================
def clean_r2_cache(bucket, prefix):
    """Delete old gold_price_*.parquet files from R2 before uploading new."""
    s3 = r2_client()
    existing_files = list_r2_files(bucket, prefix)
    old_files = [f for f in existing_files if "gold_price_" in f and f.endswith(".parquet")]

    if not old_files:
        print("🧭 No old cache files found on R2.")
        return

    for key in old_files:
        s3.delete_object(Bucket=bucket, Key=key)
        print(f"🗑️ Deleted old cache file on R2: {key}")


# ======================================================
# 3️⃣ INCREMENTAL UPDATE LOGIC
# ======================================================
def incremental_update(new_df, local_path):
    """Merge new data with existing local parquet (if present)."""
    if os.path.exists(local_path):
        old_df = pd.read_parquet(local_path)
        before = len(old_df)
        combined = pd.concat([old_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
        after = len(combined)
        print(f"📈 Incremental update: {after - before} new rows added.")
    else:
        combined = new_df
        print(f"🆕 First-time load: {len(new_df)} rows saved.")
    return combined


# ======================================================
# 4️⃣ CHECK IF UPDATE NEEDED (skip logic)
# ======================================================
def latest_r2_date(bucket, prefix):
    """Return latest date from file name on R2 (gold_price_DDMMYY.parquet)."""
    files = list_r2_files(bucket, prefix)
    dates = []
    for f in files:
        m = re.search(r"gold_price_(\d{6})\.parquet$", f)
        if m:
            try:
                d = datetime.strptime(m.group(1), "%d%m%y")
                dates.append(d)
            except:
                pass
    return max(dates) if dates else None


# ======================================================
# 5️⃣ MAIN SCRIPT
# ======================================================
def update_gold_prices():
    today = datetime.now()
    today_str = today.strftime("%d%m%y")
    parquet_path = SAVE_DIR / f"gold_price_{today_str}.parquet"

    # --- Skip if already up-to-date on R2 ---
    latest_remote = latest_r2_date(BUCKET, PREFIX_MAIN)
    if latest_remote and latest_remote.date() >= today.date():
        print(f"✅ Already up-to-date ({latest_remote.strftime('%d/%m/%Y')}) → skip download.")
        return

    # --- 🔥 NEW SECTION: Clean any local or GitHub cache first ---
    local_cache_dirs = [Path.cwd(), SAVE_DIR, Path.cwd() / ".cache", Path.cwd() / "__pycache__"]
    for d in local_cache_dirs:
        if d.exists():
            for f in d.glob("gold_price_*.parquet"):
                try:
                    os.remove(f)
                    print(f"🧹 Deleted cached file: {f}")
                except Exception as e:
                    print(f"⚠️ Could not delete {f}: {e}")

    # --- Fetch new data ---
    df = fetch_gold_data()

    # --- Merge with local if exists ---
    combined = incremental_update(df, parquet_path)

    # --- Save local parquet ---
    combined.to_parquet(parquet_path, index=False, compression="gzip")
    print(f"💾 Saved Parquet → {parquet_path} ({len(combined)} rows)")

    # --- Clean old cache on R2 ---
    clean_r2_cache(BUCKET, PREFIX_MAIN)

    # --- Upload new file ---
    upload_to_r2(parquet_path, BUCKET, f"{PREFIX_MAIN}{parquet_path.name}")

    # --- Clean old backups ---
    clean_old_backups_r2(BUCKET, PREFIX_BACKUP, keep=2)

    print("☁️ Uploaded new gold data and cleaned old backups.")

    # --- Local cleanup ---
    try:
        for f in SAVE_DIR.glob("*.parquet"):
            os.remove(f)
            print(f"🧹 Deleted local file: {f}")
        if not any(SAVE_DIR.iterdir()):
            SAVE_DIR.rmdir()
            print(f"🗑️ Removed empty folder: {SAVE_DIR}")
    except Exception as e:
        print(f"⚠️ Cleanup error: {e}")


# ======================================================
# 6️⃣ ENTRY POINT
# ======================================================
if __name__ == "__main__":
    update_gold_prices()
    print("✅ Gold price update completed successfully.")