
import os
from dotenv import load_dotenv
from utils_r2 import list_r2_files

load_dotenv()

bucket = os.getenv("R2_BUCKET")
if not bucket:
    print("❌ R2_BUCKET not found in env")
else:
    print(f"Listing files in {bucket}/cafef_data/usd_black_market/ ...")
    files = list_r2_files(bucket, "cafef_data/usd_black_market/")
    print(f"Found {len(files)} files:")
    for f in sorted(files):
        print(f" - {f}")

    # Check sort logic
    print("\nSort Logic Check:")
    prefix_files = [f for f in files if "usd_market_data_" in f]
    if prefix_files:
        latest = sorted(prefix_files)[-1]
        print(f"Latest (by lexicographical sort): {latest}")
    else:
        print("No files with prefix 'usd_market_data_' found.")
