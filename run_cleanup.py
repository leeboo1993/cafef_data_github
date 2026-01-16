
import os
from utils_r2 import cleanup_incomplete_uploads, list_r2_files, r2_client

BUCKET = os.getenv("R2_BUCKET")
if not BUCKET:
    print("❌ Error: R2_BUCKET env var not set.")
    exit(1)

def delete_legacy_folder(bucket, folder_prefix):
    """Recursively deletes a folder (prefix) in R2."""
    s3 = r2_client()
    files = list_r2_files(bucket, folder_prefix)
    
    if not files:
        # print(f"   (Clean: {folder_prefix})") # Verified clean
        return
    
    print(f"🗑️ Found legacy folder: {folder_prefix} ({len(files)} files). Deleting...")
    
    # Batch delete in chunks of 1000
    chunk_size = 1000
    for i in range(0, len(files), chunk_size):
        batch_keys = [{'Key': k} for k in files[i:i+chunk_size]]
        try:
            s3.delete_objects(Bucket=bucket, Delete={'Objects': batch_keys})
            print(f"   🔥 Deleted batch of {len(batch_keys)} files.")
        except Exception as e:
            print(f"   ⚠️ Error deleting batch: {e}")

    print(f"✨ Deleted legacy folder: {folder_prefix}")

print(f"🧹 Running daily cleanup for stuck multipart uploads in {BUCKET}...")
cleanup_incomplete_uploads(BUCKET, age_days=1)

print("\n🧹 Checking for legacy '_backup' folders to remove...")
LEGACY_FOLDERS = [
    "cafef_data/deposit_rate_backup/",
    "cafef_data/gold_price_backup/",
    "cafef_data/cafef_data_backup/",
    "cafef_data/vietnamnet_interest_rate_backup/",
    "cafef_data/stock_trading_account_backup/",
    "cafef_data/usd_black_market_backup/",
    "cafef_data/vcb_fx_data_backup/"
]

for folder in LEGACY_FOLDERS:
    delete_legacy_folder(BUCKET, folder)

print("🎉 Cleanup check complete.")
