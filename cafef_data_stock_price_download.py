import os
import re
import glob
import boto3
import requests
import pandas as pd
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime, timedelta

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
# 5️⃣ Utilities
# =====================================================
def extract_date_from_name(name):
    m = re.search(r"(\d{6})\.parquet$", name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%d%m%y")
    except Exception:
        return None


def r2_client():
    session = boto3.session.Session()
    return session.client(
        service_name="s3",
        endpoint_url=os.getenv("R2_ENDPOINT"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
    )


def list_r2_files(bucket, prefix):
    s3 = r2_client()
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in resp:
        return []
    return [obj["Key"] for obj in resp["Contents"]]


def ensure_folder_exists(bucket, folder):
    s3 = r2_client()
    if not list_r2_files(bucket, folder):
        s3.put_object(Bucket=bucket, Key=f"{folder.rstrip('/')}/")
        print(f"📁 Created folder on R2: {folder}")


def upload_to_r2(local_path, bucket, key):
    s3 = r2_client()
    s3.upload_file(local_path, bucket, key)
    print(f"☁️ Uploaded to R2 → s3://{bucket}/{key}")


def move_to_backup_r2(bucket, prefix_main, prefix_backup, keep=2):
    s3 = r2_client()
    main_files = list_r2_files(bucket, prefix_main)
    parquet_files = [f for f in main_files if f.endswith(".parquet")]
    if not parquet_files:
        print("⚙️ No existing file to backup.")
        return
    # Sort by date
    parquet_files = sorted(parquet_files, key=lambda x: extract_date_from_name(x) or datetime.min, reverse=True)
    latest = parquet_files[0]
    for old_file in parquet_files[1:]:
        new_key = old_file.replace(prefix_main, prefix_backup)
        s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": old_file}, Key=new_key)
        s3.delete_object(Bucket=bucket, Key=old_file)
        print(f"📦 Moved {old_file} → {new_key}")
    # Clean up old backups (keep 2)
    backups = list_r2_files(bucket, prefix_backup)
    backups = sorted(backups, key=lambda x: extract_date_from_name(x) or datetime.min, reverse=True)
    for b in backups[keep:]:
        s3.delete_object(Bucket=bucket, Key=b)
        print(f"🗑️ Deleted old backup: {b}")


# =====================================================
# 6️⃣ Main updater with skip logic + R2 integration
# =====================================================
def update_vn_trading_data(max_days_back=7):
    base_dir = os.getcwd()
    output_dir = os.path.join(base_dir, "cafef_price")
    os.makedirs(output_dir, exist_ok=True)

    bucket = os.getenv("R2_BUCKET")
    prefix_main = "cafef_data/"
    prefix_backup = "cafef_data/cafef_data_backup/"

    # Ensure folders exist
    ensure_folder_exists(bucket, prefix_main)
    ensure_folder_exists(bucket, prefix_backup)

    # Check existing latest file on R2
    existing_files = list_r2_files(bucket, prefix_main)
    existing_dates = [extract_date_from_name(f) for f in existing_files if f.endswith(".parquet")]
    latest_existing = max([d for d in existing_dates if d is not None], default=None)

    today = datetime.now()
    today_str = today.strftime("%d%m%y")

    if latest_existing and latest_existing.date() >= today.date():
        print(f"✅ Already up-to-date ({latest_existing.strftime('%d/%m/%Y')}) → skip download.")
        return None

    # Try download
    for i in range(max_days_back):
        date_obj = today - timedelta(days=i)
        url, date_str = build_cafef_url(date_obj)
        files = download_and_extract(url, date_str, output_dir)
        if not files:
            continue
        valid = validate_cafef_data(files)
        if not valid:
            continue

        parquet_path = os.path.join(output_dir, f"cafef_stock_price_{date_obj.strftime('%d%m%y')}.parquet")
        new_df = combine_trading_data(valid)
        cutoff = datetime.now() - timedelta(days=730)
        new_df = new_df[new_df["date"] >= cutoff]
        new_df.to_parquet(parquet_path, index=False, compression="gzip")

        print(f"💾 Saved Parquet → {parquet_path} ({len(new_df)} rows)")

        # Backup old + upload new
        move_to_backup_r2(bucket, prefix_main, prefix_backup, keep=2)
        upload_to_r2(parquet_path, bucket, f"{prefix_main}cafef_stock_price_{date_obj.strftime('%d%m%y')}.parquet")

        print(f"✅ Uploaded new file to R2: {prefix_main}")
        return parquet_path

    raise ValueError("❌ No valid CaféF data found in recent days.")


# =====================================================
# 7️⃣ Entry point
# =====================================================
if __name__ == "__main__":
    parquet_path = update_vn_trading_data()
    if parquet_path:
        print(f"\n✅ Done → Uploaded new CaféF data: {os.path.basename(parquet_path)}")
    else:
        print("\n🟢 No new data to upload (already up-to-date).")