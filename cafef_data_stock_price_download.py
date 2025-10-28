# =====================================================
# 0️⃣ UNIVERSAL ENV LOADER
# =====================================================
from pathlib import Path
import os

def load_env_safely():
    """Load .env for local runs, skip if env vars already exist."""
    if not os.getenv("R2_BUCKET"):
        from dotenv import load_dotenv
        env_path = Path(__file__).resolve().parent / ".env"
        if env_path.exists():
            load_dotenv(env_path)
            print("🔧 Loaded .env file (local environment).")
        else:
            print("⚠️ No .env found — expecting environment variables.")
    else:
        print("✅ Environment variables already available (CI/CD).")

load_env_safely()


# =====================================================
# 1️⃣ IMPORTS
# =====================================================
import re, glob, boto3, requests, pandas as pd
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime, timedelta

# =====================================================
# 2️⃣ R2 UTILITIES
# =====================================================
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
    return [obj["Key"] for obj in resp.get("Contents", [])]

def upload_to_r2(local_path, bucket, key):
    s3 = r2_client()
    s3.upload_file(local_path, bucket, key)
    print(f"☁️ Uploaded → {key}")

def ensure_folder_exists(bucket, prefix):
    s3 = r2_client()
    if not list_r2_files(bucket, prefix):
        s3.put_object(Bucket=bucket, Key=f"{prefix.rstrip('/')}/")
        print(f"📁 Created folder on R2: {prefix}")

def extract_date_from_name(name):
    m = re.search(r"(\d{6})\.parquet$", name)
    return datetime.strptime(m.group(1), "%d%m%y") if m else None

def clean_r2_cache(bucket, prefix, pattern="cafef_stock_price_"):
    """Delete old cache files before upload."""
    s3 = r2_client()
    files = list_r2_files(bucket, prefix)
    targets = [f for f in files if pattern in f and f.endswith(".parquet")]
    for key in targets:
        s3.delete_object(Bucket=bucket, Key=key)
        print(f"🗑️ Deleted old cache file: {key}")
    if not targets:
        print("🧭 No old cache files found.")

def is_file_valid(bucket, key):
    s3 = r2_client()
    try:
        meta = s3.head_object(Bucket=bucket, Key=key)
        return meta["ContentLength"] > 0
    except Exception:
        return False

def get_latest_valid_file(bucket, prefix, pattern):
    files = list_r2_files(bucket, prefix)
    valid = [(f, extract_date_from_name(f)) for f in files if pattern in f and f.endswith(".parquet")]
    valid = [(k, d) for k, d in valid if d]
    if not valid:
        return None, None
    latest_key, latest_dt = max(valid, key=lambda x: x[1])
    if is_file_valid(bucket, latest_key):
        return latest_key, latest_dt
    print(f"⚠️ Found broken file: {latest_key}")
    return None, None


# =====================================================
# 3️⃣ CORE
# =====================================================
def build_cafef_url(date_obj):
    ddmmyyyy = date_obj.strftime("%d%m%Y")
    yyyymmdd = date_obj.strftime("%Y%m%d")
    return f"https://cafef1.mediacdn.vn/data/ami_data/{yyyymmdd}/CafeF.SolieuGD.Upto{ddmmyyyy}.zip", ddmmyyyy

def download_and_extract(url, date_str, output_dir):
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        return None
    try:
        with ZipFile(BytesIO(r.content)) as z:
            z.extractall(output_dir)
            return [os.path.join(output_dir, f) for f in z.namelist()]
    except Exception:
        return None

def validate_cafef_data(file_paths):
    req = ["<Ticker>", "<DTYYYYMMDD>", "<Open>", "<High>", "<Low>", "<Close>", "<Volume>"]
    valid = []
    for path in file_paths:
        try:
            df = pd.read_csv(path, encoding="utf-8-sig")
        except:
            df = pd.read_csv(path, encoding="latin1")
        if list(df.columns[:7]) == req:
            valid.append((df, path))
    return valid

def combine_trading_data(valid_files):
    all_df = []
    for df, path in valid_files:
        exch = "HSX" if "HSX" in path or "HOSE" in path.upper() else \
               "HNX" if "HNX" in path.upper() else \
               "UPCOM" if "UPCOM" in path.upper() else "UNKNOWN"
        df = df.rename(columns={
            "<Ticker>": "ticker", "<DTYYYYMMDD>": "date",
            "<Open>": "open", "<High>": "high", "<Low>": "low",
            "<Close>": "close", "<Volume>": "volume"
        })
        df["exchange"] = exch
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
        all_df.append(df)
    return pd.concat(all_df, ignore_index=True)


# =====================================================
# 4️⃣ RUNNER
# =====================================================
def update_vn_trading_data(max_days_back=7):
    out_dir = Path.cwd() / "cafef_price"
    out_dir.mkdir(exist_ok=True)
    bucket = os.getenv("R2_BUCKET")
    prefix_main = "cafef_data/"
    ensure_folder_exists(bucket, prefix_main)

    clean_r2_cache(bucket, prefix_main)
    latest_key, latest_dt = get_latest_valid_file(bucket, prefix_main, "cafef_stock_price_")

    if latest_dt and latest_dt.date() >= datetime.now().date():
        print(f"✅ Already up-to-date ({latest_dt.strftime('%d/%m/%Y')})")
        return None

    for i in range(max_days_back):
        date_obj = datetime.now() - timedelta(days=i)
        url, date_str = build_cafef_url(date_obj)
        files = download_and_extract(url, date_str, out_dir)
        if not files:
            continue
        valid = validate_cafef_data(files)
        if not valid:
            continue

        df = combine_trading_data(valid)
        pfile = out_dir / f"cafef_stock_price_{date_obj.strftime('%d%m%y')}.parquet"
        df.to_parquet(pfile, index=False)
        upload_to_r2(str(pfile), bucket, f"{prefix_main}{pfile.name}")
        print(f"✅ Uploaded new data {pfile.name}")
        for f in glob.glob(str(out_dir / "*.csv")): os.remove(f)
        os.remove(pfile)
        return pfile

    print("❌ No valid data found.")
    return None


if __name__ == "__main__":
    update_vn_trading_data()