from utils_r2 import r2_client, upload_to_r2, clean_old_backups_r2
from datetime import datetime
import os
import pandas as pd
import requests
from pathlib import Path

# ============================================================
# CONFIG
# ============================================================
HEADERS = {"User-Agent": "Mozilla/5.0"}
SAVE_DIR = Path.cwd() / "gold_price"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

print("🟡 Fetching gold data ...")

# ============================================================
# 1️⃣ FETCH GOLD DATA
# ============================================================
def fetch_gold():
    urls = {
        "bar": "https://cafef.vn/du-lieu/Ajax/ajaxgoldpricehistory.ashx?index=all",
        "ring": "https://cafef.vn/du-lieu/Ajax/AjaxGoldPriceRing.ashx?time=all&zone=11"
    }

    def fetch(url):
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r.json().get("Data", {}).get("goldPriceWorldHistories", [])

    # --- Load raw JSONs ---
    bar = pd.DataFrame(fetch(urls["bar"]))
    ring = pd.DataFrame(fetch(urls["ring"]))

    if bar.empty or ring.empty:
        raise ValueError("⚠️ No gold data retrieved from CaféF.")

    # --- Merge createdAt / lastUpdated columns safely ---
    bar["date"] = pd.to_datetime(
        bar["createdAt"].fillna(bar["lastUpdated"]), errors="coerce"
    )
    ring["date"] = pd.to_datetime(
        ring["createdAt"].fillna(ring["lastUpdated"]), errors="coerce"
    )

    # --- Rename columns ---
    bar = bar.rename(columns={"buyPrice": "bar_buy", "sellPrice": "bar_sell"})
    ring = ring.rename(columns={"buyPrice": "ring_buy", "sellPrice": "ring_sell"})

    # --- Combine two DataFrames ---
    df = pd.merge(
        bar[["date", "bar_buy", "bar_sell"]],
        ring[["date", "ring_buy", "ring_sell"]],
        on="date",
        how="outer"
    ).sort_values("date")

    # --- Drop rows without any valid prices ---
    df = df.dropna(subset=["bar_buy", "bar_sell", "ring_buy", "ring_sell"], how="all")

    # --- Calculate spreads and gaps ---
    df["bar_spread"] = df["bar_sell"] - df["bar_buy"]
    df["ring_spread"] = df["ring_sell"] - df["ring_buy"]
    df["bar_ring_gap"] = df["bar_sell"] - df["ring_sell"]

    # --- Final clean ---
    df = df.reset_index(drop=True)
    print(f"✅ Cleaned {len(df)} gold price rows.")
    return df


# ============================================================
# 2️⃣ SAVE + UPLOAD TO R2
# ============================================================
if __name__ == "__main__":
    df = fetch_gold()

    today = datetime.now().strftime("%d%m%y")
    parquet_path = SAVE_DIR / f"gold_price_{today}.parquet"
    df.to_parquet(parquet_path, index=False, compression="gzip")
    print(f"💾 Saved Parquet locally → {parquet_path}")

    # --- Upload to R2 ---
    bucket = os.getenv("R2_BUCKET")
    prefix_main = "cafef_data/"
    prefix_backup = "cafef_data/cafef_data_backup/"

    upload_to_r2(parquet_path, bucket, f"{prefix_main}{parquet_path.name}")
    clean_old_backups_r2(bucket, prefix_backup, keep=2)

    print(f"☁️ Uploaded and cleaned backups on R2 → {parquet_path.name}")
    print("🎯 Gold price update completed successfully.")