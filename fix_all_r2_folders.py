import os
import re
from dotenv import load_dotenv
from utils_r2 import list_r2_files, r2_client, backup_and_cleanup_r2

load_dotenv()
bucket = os.getenv("R2_BUCKET")
s3 = r2_client()

print("🔍 Listing all files in cafef_data/ ...")
all_files = list_r2_files(bucket, "cafef_data/")

# Identify all backup directories
backup_files = [f for f in all_files if "_backup/" in f]
parent_dirs = set()

# 1) Restore everything from backup folders into main folders
for f in backup_files:
    # Example f: cafef_data/gold_price/gold_price_backup/gold_price_260210.csv
    # We want to move it to cafef_data/gold_price/gold_price_260210.csv
    
    parts = f.split('/')
    # Find the part that ends with "_backup"
    backup_part_idx = next(i for i, part in enumerate(parts) if part.endswith("_backup"))
    
    # The parent directory is everything up to the backup part
    parent_prefix = "/".join(parts[:backup_part_idx]) + "/"
    parent_dirs.add(parent_prefix)
    
    # The new key is the parent prefix + the file name
    file_name = parts[-1]
    new_key = parent_prefix + file_name
    
    print(f"📦 Moving {f} -> {new_key}")
    try:
        s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': f}, Key=new_key)
        s3.delete_object(Bucket=bucket, Key=f)
    except Exception as e:
        print(f"⚠️ Error moving {f}: {e}")

# 2) Run backup_and_cleanup_r2 on all identified parent directories
for pdir in parent_dirs:
    print(f"\n🔄 Running backup_and_cleanup_r2 for: {pdir}")
    try:
        backup_and_cleanup_r2(bucket, pdir, keep=1)
    except Exception as e:
        print(f"⚠️ Error cleaning up {pdir}: {e}")

print("\n✅ All R2 prefixes have been fixed and cleaned up!")
