from utils_r2 import r2_client, upload_to_r2, clean_old_backups_r2
from datetime import datetime
import os, pandas as pd, requests
from pathlib import Path

HEADERS = {"User-Agent": "Mozilla/5.0"}
SAVE_DIR = Path.cwd() / "gold_price"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

print("🟡 Fetching gold data ...")

def fetch_gold():
    urls = {
        "bar": "https://cafef.vn/du-lieu/Ajax/ajaxgoldpricehistory.ashx?index=all",
        "ring": "https://cafef.vn/du-lieu/Ajax/AjaxGoldPriceRing.ashx?time=all&zone=11"
    }

    def fetch(url):
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r.json().get("Data", {}).get("goldPriceWorldHistories", [])

    bar = pd.DataFrame(fetch(urls["bar"]))
    ring = pd.DataFrame(fetch(urls["ring"]))

    bar["date"] = pd.to_datetime(bar["createdAt"] or bar["lastUpdated"], errors="coerce")
    ring["date"] = pd.to_datetime(ring["createdAt"] or ring["lastUpdated"], errors="coerce")
    bar = bar.rename(columns={"buyPrice": "bar_buy", "sellPrice": "bar_sell"})
    ring = ring.rename(columns={"buyPrice": "ring_buy", "sellPrice": "ring_sell"})

    df = pd.merge(bar[["date", "bar_buy", "bar_sell"]], ring[["date", "ring_buy", "ring_sell"]], on="date", how="outer")
    df = df.sort_values("date").dropna(subset=["bar_buy", "bar_sell", "ring_buy", "ring_sell"], how="all")

    df["bar_spread"] = df["bar_sell"] - df["bar_buy"]
    df["ring_spread"] = df["ring_sell"] - df["ring_buy"]
    df["bar_ring_gap"] = df["bar_sell"] - df["ring_sell"]
    return df

if __name__ == "__main__":
    df = fetch_gold()
    today = datetime.now().strftime("%d%m%y")
    parquet_path = SAVE_DIR / f"gold_price_{today}.parquet"
    df.to_parquet(parquet_path, index=False, compression="gzip")

    bucket = os.getenv("R2_BUCKET")
    prefix_main = "cafef_data/"
    prefix_backup = "cafef_data/cafef_data_backup/"

    upload_to_r2(parquet_path, bucket, f"{prefix_main}{parquet_path.name}")
    clean_old_backups_r2(bucket, prefix_backup, keep=2)
    print(f"✅ Uploaded gold data {parquet_path.name}")