import os
from dotenv import load_dotenv
from utils_r2 import list_r2_files, r2_client, backup_and_cleanup_r2

load_dotenv()
bucket = os.getenv("R2_BUCKET")
s3 = r2_client()

print("Listing all files in cafef_data/deposit_rate/")
files = list_r2_files(bucket, "cafef_data/deposit_rate/")

# Move everything from backup back to main folder
for f in files:
    if "deposit_rate_backup/" in f:
        new_key = f.replace("deposit_rate_backup/", "")
        print(f"Moving {f} to {new_key}")
        s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': f}, Key=new_key)
        s3.delete_object(Bucket=bucket, Key=f)

print("Now running backup_and_cleanup_r2 to fix it properly...")
backup_and_cleanup_r2(bucket, "cafef_data/deposit_rate/", keep=1)
print("Done!")
