import os
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from utils_r2 import upload_to_r2, clean_old_backups_r2, list_r2_files

# ======================================================
# CONFIGURATION
# ======================================================
HEADERS = {"User-Agent": "Mozilla/5.0"}
SAVE_DIR = Path.cwd() / "gold_price"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

BUCKET = os.getenv("R2_BUCKET")

# 🔧 FIXED: separate folder for gold data
PREFIX_MAIN = "cafef_data/"
PREFIX_BACKUP = "cafef_data/cafef_data_backup/gold_backup/"

print("🪙 Fetching SJC gold data (incremental update, no spreads)...")

# ======================================================
# 1️⃣ FETCH GOLD DATA FROM CAFEF
# ======================================================
# ======================================================
# 1️⃣ FETCH GOLD DATA FROM CAFEF (fixed timezone issue)
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

    bar_hist = fetch(urls["bar"])
    ring_hist = fetch(urls["ring"])

    bar_records = [
        {
            "date": pd.to_datetime(item.get("createdAt") or item.get("lastUpdated"), errors="coerce"),
            "bar_buy": item.get("buyPrice"),
            "bar_sell": item.get("sellPrice"),
        }
        for item in bar_hist
    ]

    ring_records = [
        {
            "date": pd.to_datetime(item.get("createdAt") or item.get("lastUpdated"), errors="coerce"),
            "ring_buy": item.get("buyPrice"),
            "ring_sell": item.get("sellPrice"),
        }
        for item in ring_hist
    ]

    df_bar = pd.DataFrame(bar_records).dropna(subset=["date"]).sort_values("date")
    df_ring = pd.DataFrame(ring_records).dropna(subset=["date"]).sort_values("date")

    # 🧭 FIX: unify timezone (convert both to naive UTC-free)
    df_bar["date"] = pd.to_datetime(df_bar["date"]).dt.tz_localize(None)
    df_ring["date"] = pd.to_datetime(df_ring["date"]).dt.tz_localize(None)

    # Merge
    df = pd.merge(df_bar, df_ring, on="date", how="outer").sort_values("date")

    # Clean
    df = df.dropna(subset=["bar_buy", "bar_sell", "ring_buy", "ring_sell"], how="all")
    df = df.reset_index(drop=True)
    print(f"✅ Retrieved {len(df)} rows of gold data.")
    return df


# ======================================================
# 2️⃣ INCREMENTAL UPDATE LOGIC
# ======================================================
def incremental_update(new_df, local_path):
    """
    Merge new CaféF data with the latest existing file (if exists),
    keeping only unique date entries.
    """
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
# 3️⃣ MAIN SCRIPT
# ======================================================
def update_gold_prices():
    today = datetime.now().strftime("%d%m%y")
    parquet_path = SAVE_DIR / f"gold_price_{today}.parquet"

    # --- Fetch new data
    new_df = fetch_gold_data()

    # --- Merge incrementally
    combined_df = incremental_update(new_df, parquet_path)

    # --- Save
    combined_df.to_parquet(parquet_path, index=False, compression="gzip")
    print(f"💾 Saved Parquet → {parquet_path} ({len(combined_df)} rows)")

    # 🔧 FIXED: upload to gold-specific folder
    upload_to_r2(parquet_path, BUCKET, f"{PREFIX_MAIN}{parquet_path.name}")

    # 🔧 FIXED: clean backups in gold-specific folder
    clean_old_backups_r2(BUCKET, PREFIX_BACKUP, keep=2)
    print("☁️ Uploaded new gold data and cleaned old backups.")


# ======================================================
# 4️⃣ ENTRY POINT
# ======================================================
if __name__ == "__main__":
    update_gold_prices()
    print("✅ Gold price update completed successfully.")