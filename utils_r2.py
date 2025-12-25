import os
import re
from datetime import datetime
import boto3

def r2_client():
    session = boto3.session.Session()
    return session.client(
        service_name="s3",
        endpoint_url=os.getenv("R2_ENDPOINT"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
    )

def extract_date_from_name(name):
    m = re.search(r"(\d{6})\.parquet$", name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%d%m%y")
    except Exception:
        return None

def list_r2_files(bucket, prefix):
    s3 = r2_client()
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in resp:
        return []
    return [obj["Key"] for obj in resp["Contents"]]

def upload_to_r2(local_path, bucket, key):
    s3 = r2_client()
    s3.upload_file(local_path, bucket, key)
    print(f"☁️ Uploaded → s3://{bucket}/{key}")

def download_from_r2(bucket, key, local_path):
    """Download file from R2 to local path."""
    s3 = r2_client()
    try:
        s3.download_file(bucket, key, local_path)
        print(f"⬇️ Downloaded {key} → {local_path}")
        return True
    except Exception as e:
        print(f"⚠️ Could not download {key}: {e}")
        return False


def ensure_folder_exists(bucket, folder):
    s3 = r2_client()
    if not list_r2_files(bucket, folder):
        s3.put_object(Bucket=bucket, Key=f"{folder.rstrip('/')}/")
        print(f"📁 Created folder: {folder}")

def clean_old_backups_r2(bucket, prefix, keep=2):
    s3 = r2_client()
    files = list_r2_files(bucket, prefix)
    dated = sorted(
        [f for f in files if extract_date_from_name(f)],
        key=lambda x: extract_date_from_name(x),
        reverse=True
    )
    for old in dated[keep:]:
        s3.delete_object(Bucket=bucket, Key=old)
        print(f"🗑️ Deleted old backup: {old}")