import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta
import os, glob

# =====================================================
# 1️⃣ Build CaféF ZIP URL
# =====================================================
def build_cafef_url(date_obj):
    ddmmyyyy = date_obj.strftime("%d%m%Y")
    yyyymmdd = date_obj.strftime("%Y%m%d")
    url = f"https://cafef1.mediacdn.vn/data/ami_data/{yyyymmdd}/CafeF.SolieuGD.Upto{ddmmyyyy}.zip"
    return url, ddmmyyyy


# =====================================================
# 2️⃣ Download and extract ZIP
# =====================================================
def download_and_extract(url, date_str, output_dir):
    print(f"⬇️ Trying {url}")
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        print(f"⚠️ No data for {date_str} (HTTP {r.status_code})")
        return None
    try:
        with ZipFile(BytesIO(r.content)) as z:
            z.extractall(output_dir)
            files = [os.path.join(output_dir, f) for f in z.namelist()]
            print(f"📦 Extracted: {z.namelist()}")
            return files
    except Exception as e:
        print(f"❌ Extraction error for {date_str}: {e}")
        return None


# =====================================================
# 3️⃣ Validate CaféF file structure
# =====================================================
def validate_cafef_data(file_paths):
    required_cols = ["<Ticker>", "<DTYYYYMMDD>", "<Open>", "<High>", "<Low>", "<Close>", "<Volume>"]
    valid = []
    for path in file_paths:
        try:
            df = pd.read_csv(path, encoding="utf-8-sig")
        except UnicodeDecodeError:
            df = pd.read_csv(path, encoding="latin1")
        if list(df.columns[:7]) == required_cols:
            valid.append((df, path))
    return valid


# =====================================================
# 4️⃣ Combine HSX / HNX / UPCOM data
# =====================================================
def combine_trading_data(valid_files):
    combined = []
    for df, path in valid_files:
        if "HSX" in path or "HOSE" in path.upper():
            exch = "HSX"
        elif "HNX" in path.upper():
            exch = "HNX"
        elif "UPCOM" in path.upper():
            exch = "UPCOM"
        else:
            exch = "UNKNOWN"

        df = df.rename(columns={
            "<Ticker>": "ticker",
            "<DTYYYYMMDD>": "date",
            "<Open>": "open",
            "<High>": "high",
            "<Low>": "low",
            "<Close>": "close",
            "<Volume>": "volume"
        })
        df["exchange"] = exch
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
        combined.append(df[["ticker", "exchange", "date", "open", "high", "low", "close", "volume"]])
    return pd.concat(combined, ignore_index=True)


# =====================================================
# 5️⃣ Clean old backups (keep 2 most recent by date)
# =====================================================
def clean_old_backups(backup_dir, keep=2):
    files = glob.glob(os.path.join(backup_dir, "cafef_stock_price_*.parquet"))
    if len(files) <= keep:
        return

    def extract_date(f):
        try:
            basename = os.path.basename(f)
            date_part = basename.replace("cafef_stock_price_", "").replace(".parquet", "")
            return datetime.strptime(date_part, "%d%m%y")
        except Exception:
            return datetime.min

    files_sorted = sorted(files, key=extract_date, reverse=True)
    to_delete = files_sorted[keep:]
    for f in to_delete:
        try:
            os.remove(f)
            print(f"🗑️ Deleted old backup: {os.path.basename(f)}")
        except Exception as e:
            print(f"⚠️ Could not delete {f}: {e}")


# =====================================================
# 6️⃣ Main updater
# =====================================================
def update_vn_trading_data(max_days_back=7):
    base_dir = os.getcwd()
    output_dir = os.path.join(base_dir, "cafef_price")
    os.makedirs(output_dir, exist_ok=True)

    backup_dir = os.path.join(output_dir, "backup")
    os.makedirs(backup_dir, exist_ok=True)

    today = datetime.now()
    today_str = today.strftime("%d%m%y")
    yesterday_str = (today - timedelta(days=1)).strftime("%d%m%y")

    parquet_today = os.path.join(output_dir, f"cafef_stock_price_{today_str}.parquet")
    parquet_yesterday = os.path.join(output_dir, f"cafef_stock_price_{yesterday_str}.parquet")

    # 🕐 Move yesterday’s file to backup
    if os.path.exists(parquet_yesterday):
        os.rename(parquet_yesterday, os.path.join(backup_dir, os.path.basename(parquet_yesterday)))
        print(f"🕐 Backed up yesterday’s file → {backup_dir}")

    # 🔄 Try to find the newest valid CaféF ZIP
    for i in range(max_days_back):
        date_obj = today - timedelta(days=i)
        url, date_str = build_cafef_url(date_obj)
        files = download_and_extract(url, date_str, output_dir)
        if not files:
            continue

        valid = validate_cafef_data(files)
        if not valid:
            continue

        new_df = combine_trading_data(valid)
        new_df.to_parquet(parquet_today, index=False, compression="snappy")
        print(f"💾 Saved Parquet → {parquet_today} ({len(new_df)} rows)")

        clean_old_backups(backup_dir, keep=2)
        return parquet_today, date_str

    raise ValueError("❌ No valid CaféF data found in recent days.")


# =====================================================
# 7️⃣ Entry point
# =====================================================
if __name__ == "__main__":
    parquet_path, used_date = update_vn_trading_data()
    print(f"\n✅ Done → Saved: {parquet_path}")