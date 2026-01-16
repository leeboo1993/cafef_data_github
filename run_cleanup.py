
import os
from utils_r2 import cleanup_incomplete_uploads

BUCKET = os.getenv("R2_BUCKET")
if not BUCKET:
    print("❌ Error: R2_BUCKET env var not set.")
    exit(1)

print(f"🧹 Running daily cleanup for stuck multipart uploads in {BUCKET}...")
cleanup_incomplete_uploads(BUCKET, age_days=1)
print("🎉 Cleanup check complete.")
