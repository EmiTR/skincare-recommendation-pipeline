"""
upload_to_s3.py
===============
Uploads scraped Flaconi and DM CSV files to the Bronze layer of the S3 pipeline bucket.
Before we can run this script, the S3 bucket needs to exist — which means we need to write the Terraform S3 module first.

Usage:
    python upload_to_s3.py

Requirements:
    pip install boto3

Configuration:
    Set BUCKET_NAME to your actual S3 bucket name before running.
    AWS credentials must be configured via: aws configure
"""

import boto3
import os
import sys
import logging
from datetime import datetime
from botocore.exceptions import NoCredentialsError, ClientError

# ---------------------------------------------------------------------------
# Configuration — update BUCKET_NAME before running
# ---------------------------------------------------------------------------
BUCKET_NAME = "your-pipeline-bucket-name"   # ← change this to your actual bucket name
AWS_REGION  = "eu-central-1"                # ← change if you use a different region

# Local file paths → S3 bronze layer destination keys
FILES = {
    "flaconi_gesichtscreme.csv":  "raw/flaconi/flaconi_gesichtscreme.csv",
    "flaconi_ingredients.csv":    "raw/flaconi/flaconi_ingredients.csv",
    "dm_final.csv":               "raw/dm/dm_final.csv",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Validation — check files exist locally before uploading
# ---------------------------------------------------------------------------
def validate_local_files(file_map: dict) -> bool:
    """Check that all local CSV files exist before attempting upload."""
    logger.info("🔍 Validating local files...")
    all_ok = True
    for local_file in file_map.keys():
        if os.path.exists(local_file):
            size_kb = os.path.getsize(local_file) / 1024
            logger.info(f"  ✅ Found: {local_file} ({size_kb:.1f} KB)")
        else:
            logger.error(f"  ❌ Missing: {local_file} — file not found in current directory")
            all_ok = False
    return all_ok


def validate_file_not_empty(local_file: str) -> bool:
    """Check that a file is not empty."""
    if os.path.getsize(local_file) == 0:
        logger.error(f"  ❌ File is empty: {local_file}")
        return False
    return True


def validate_csv_has_rows(local_file: str, min_rows: int = 10) -> bool:
    """Check that a CSV file has at least min_rows of data."""
    with open(local_file, "r", encoding="utf-8-sig") as f:
        row_count = sum(1 for _ in f) - 1  # subtract header
    if row_count < min_rows:
        logger.warning(f"  ⚠️  {local_file} has only {row_count} rows — expected at least {min_rows}")
        return False
    logger.info(f"  ✅ Row count OK: {local_file} has {row_count} data rows")
    return True


# ---------------------------------------------------------------------------
# S3 validation — check bucket exists and is accessible
# ---------------------------------------------------------------------------
def validate_s3_connection(s3_client, bucket_name: str) -> bool:
    """Check that the S3 bucket exists and we have access."""
    logger.info(f"🔍 Validating S3 connection to bucket: {bucket_name}")
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"  ✅ Bucket accessible: {bucket_name}")
        return True
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            logger.error(f"  ❌ Bucket does not exist: {bucket_name}")
        elif error_code == "403":
            logger.error(f"  ❌ Access denied to bucket: {bucket_name} — check IAM permissions")
        else:
            logger.error(f"  ❌ S3 error: {e}")
        return False


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------
def upload_file(s3_client, local_file: str, bucket: str, s3_key: str) -> bool:
    """Upload a single file to S3 and verify it landed correctly."""
    try:
        logger.info(f"  ⬆️  Uploading {local_file} → s3://{bucket}/{s3_key}")
        s3_client.upload_file(local_file, bucket, s3_key)

        # Verify the upload by checking the object exists in S3
        response = s3_client.head_object(Bucket=bucket, Key=s3_key)
        uploaded_size = response["ContentLength"]
        local_size    = os.path.getsize(local_file)

        if uploaded_size == local_size:
            logger.info(f"  ✅ Verified: {s3_key} ({uploaded_size / 1024:.1f} KB)")
            return True
        else:
            logger.error(f"  ❌ Size mismatch! Local: {local_size}B, S3: {uploaded_size}B")
            return False

    except FileNotFoundError:
        logger.error(f"  ❌ Local file not found: {local_file}")
        return False
    except ClientError as e:
        logger.error(f"  ❌ Upload failed for {local_file}: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("🚀 Starting S3 upload pipeline")
    logger.info(f"   Bucket : {BUCKET_NAME}")
    logger.info(f"   Region : {AWS_REGION}")
    logger.info(f"   Files  : {len(FILES)}")
    logger.info("=" * 60)

    # --- Step 1: Validate local files ---
    if not validate_local_files(FILES):
        logger.error("❌ One or more local files are missing. Aborting.")
        sys.exit(1)

    # --- Step 2: Validate CSV quality ---
    logger.info("\n🔍 Validating CSV content...")
    min_rows_per_file = {
        "flaconi_gesichtscreme.csv": 100,
        "flaconi_ingredients.csv":   100,
        "dm_final.csv":              100,
    }
    for local_file, min_rows in min_rows_per_file.items():
        validate_file_not_empty(local_file)
        validate_csv_has_rows(local_file, min_rows)

    # --- Step 3: Connect to S3 ---
    logger.info("\n🔗 Connecting to AWS S3...")
    try:
        s3_client = boto3.client("s3", region_name=AWS_REGION)
        # Quick credential check
        boto3.client("sts").get_caller_identity()
        logger.info("  ✅ AWS credentials valid")
    except NoCredentialsError:
        logger.error("  ❌ No AWS credentials found. Run: aws configure")
        sys.exit(1)
    except Exception as e:
        logger.error(f"  ❌ AWS connection error: {e}")
        sys.exit(1)

    # --- Step 4: Validate bucket access ---
    if not validate_s3_connection(s3_client, BUCKET_NAME):
        logger.error("❌ Cannot access S3 bucket. Aborting.")
        sys.exit(1)

    # --- Step 5: Upload files ---
    logger.info("\n📤 Uploading files to S3 bronze layer...")
    results = {}
    for local_file, s3_key in FILES.items():
        results[local_file] = upload_file(s3_client, local_file, BUCKET_NAME, s3_key)

    # --- Step 6: Summary ---
    logger.info("\n" + "=" * 60)
    logger.info("📊 Upload Summary")
    logger.info("=" * 60)
    success = [f for f, ok in results.items() if ok]
    failed  = [f for f, ok in results.items() if not ok]

    for f in success:
        logger.info(f"  ✅ {f}")
    for f in failed:
        logger.error(f"  ❌ {f}")

    duration = (datetime.now() - start_time).seconds
    logger.info(f"\n  Total  : {len(FILES)} files")
    logger.info(f"  Success: {len(success)}")
    logger.info(f"  Failed : {len(failed)}")
    logger.info(f"  Time   : {duration}s")

    if failed:
        logger.error("\n❌ Some uploads failed. Check errors above.")
        sys.exit(1)
    else:
        logger.info("\n✅ All files uploaded successfully to S3 bronze layer!")
        logger.info(f"   → s3://{BUCKET_NAME}/raw/flaconi/")
        logger.info(f"   → s3://{BUCKET_NAME}/raw/dm/")


if __name__ == "__main__":
    main()
