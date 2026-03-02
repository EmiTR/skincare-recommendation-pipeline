# scraper/upload_to_s3.py - Updated for you
BUCKET_NAME = "skincare-recommendation-pipeline-2026"  # Make it unique!
AWS_REGION = "us-east-1"

def upload_to_s3():
    s3 = boto3.client('s3', region_name=AWS_REGION)
    
    csv_files = list(Path("data/").glob("*.csv"))
    print(f"📁 Found {len(csv_files)} files:")
    for f in csv_files: print(f"  - {f.name}")
    
    for csv_file in csv_files:
        s3_key = f"bronze/{csv_file.name}"
        print(f"⏳ Uploading {csv_file.name}...")
        s3.upload_file(str(csv_file), BUCKET_NAME, s3_key)
        print(f"✅ {csv_file.name} → s3://{BUCKET_NAME}/{s3_key}")

if __name__ == "__main__":
    upload_to_s3()
