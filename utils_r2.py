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

def cleanup_incomplete_uploads(bucket, age_days=1):
    """Aborts incomplete multipart uploads older than specified days."""
    s3 = r2_client()
    try:
        response = s3.list_multipart_uploads(Bucket=bucket)
        if "Uploads" not in response:
            print("✅ No stuck multipart uploads found.")
            return

        print(f"🧹 Checking for stuck uploads in {bucket}...")
        for upload in response["Uploads"]:
            key = upload["Key"]
            upload_id = upload["UploadId"]
            initiated = upload["Initiated"]
            
            # Check age
            if (datetime.now(initiated.tzinfo) - initiated).days >= age_days:
                print(f"⚠️ Aborting stuck upload: {key} (Initiated: {initiated})")
                s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
                print(f"   -> Aborted.")
    except Exception as e:
        print(f"⚠️ Error cleaning uploads: {e}")

def backup_and_cleanup_r2(bucket, folder, file_pattern="", keep=2):
    """
    Keeps the latest 'keep' files in the main folder, moves older files to {folder}_backup/ subfolder.
    
    Args:
        bucket: R2 bucket name
        folder: Main folder path (e.g., "cafef_data/deposit_rate/")
        file_pattern: Optional pattern to match files (e.g., "deposit_rate_")
        keep: Number of latest files to keep in main folder (default 2)
    
    Structure created:
        cafef_data/deposit_rate/
          ├── deposit_rate_260116.csv (latest)
          ├── deposit_rate_260116.json (latest)
          └── deposit_rate_backup/
              ├── deposit_rate_260115.csv (old)
              └── deposit_rate_260115.json (old)
    """
    s3 = r2_client()
    
    # Ensure folder ends with /
    if not folder.endswith('/'):
        folder += '/'
    
    # List all files in main folder (excluding backup subfolder)
    all_files = list_r2_files(bucket, folder)
    backup_folder = f"{folder}{folder.rstrip('/').split('/')[-1]}_backup/"
    
    # Filter out backup folder contents
    main_files = [f for f in all_files if not f.startswith(backup_folder)]
    
    if not main_files:
        return
    
    # Group by extension
    by_ext = {}
    for f in main_files:
        ext = f.split('.')[-1] if '.' in f else 'no_ext'
        if ext not in by_ext:
            by_ext[ext] = []
        by_ext[ext].append(f)
    
    # Process each extension group
    for ext, files in by_ext.items():
        # Sort by date suffix (YYMMDD) if available
        dated_files = []
        for f in files:
            date_obj = extract_date_from_name(f)
            if date_obj:
                dated_files.append((f, date_obj))
        
        if not dated_files:
            continue
        
        # Sort by date descending
        dated_files.sort(key=lambda x: x[1], reverse=True)
        
        # Keep latest N files in main folder
        files_to_backup = [f[0] for f in dated_files[keep:]]
        
        # Move old files to backup
        for old_file in files_to_backup:
            new_key = old_file.replace(folder, backup_folder, 1)
            
            try:
                # Copy to backup location
                s3.copy_object(
                    Bucket=bucket,
                    Copy Source={'Bucket': bucket, 'Key': old_file},
                    Key=new_key
                )
                # Delete from main folder
                s3.delete_object(Bucket=bucket, Key=old_file)
                print(f"📦 Moved to backup: {os.path.basename(old_file)}")
            except Exception as e:
                print(f"⚠️ Error backing up {old_file}: {e}")