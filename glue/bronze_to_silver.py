"""
bronze_to_silver.py
===================
AWS Glue ETL job — Bronze to Silver layer transformation

What this script does:
1. Reads raw CSVs from S3 bronze layer (raw/)
2. Cleans and normalizes Flaconi product + ingredients data
3. Cleans and normalizes DM product + ingredients data
4. Joins Flaconi product info with ingredients on 'url' (inner join)
5. Validates data quality — stops pipeline via SNS if missing ingredients found
6. Writes cleaned data to S3 silver layer (cleaned/)

Triggered by: AWS Step Functions (called from state machine)
Output: cleaned/flaconi/flaconi_cleaned.csv, cleaned/dm/dm_cleaned.csv
"""

import sys
import re
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType

# ---------------------------------------------------------------------------
# Initialize Glue context
# ---------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "SOURCE_BUCKET",
    "SOURCE_FLACONI_PRODUCTS",
    "SOURCE_FLACONI_INGREDIENTS",
    "SOURCE_DM",
    "TARGET_FLACONI",
    "TARGET_DM",
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BUCKET                    = args["SOURCE_BUCKET"]
SOURCE_FLACONI_PRODUCTS   = f"s3://{BUCKET}/{args['SOURCE_FLACONI_PRODUCTS']}"
SOURCE_FLACONI_INGREDIENTS = f"s3://{BUCKET}/{args['SOURCE_FLACONI_INGREDIENTS']}"
SOURCE_DM                 = f"s3://{BUCKET}/{args['SOURCE_DM']}"
TARGET_FLACONI            = f"s3://{BUCKET}/{args['TARGET_FLACONI']}"
TARGET_DM                 = f"s3://{BUCKET}/{args['TARGET_DM']}"

# SNS topic for pipeline failure alerts
SNS_TOPIC_ARN = f"arn:aws:sns:eu-central-1:{boto3.client('sts').get_caller_identity()['Account']}:beauty-boba-dev-pipeline-alerts"

print(f"✅ Glue job initialized: {args['JOB_NAME']}")
print(f"   Source bucket : {BUCKET}")
print(f"   Flaconi prods : {SOURCE_FLACONI_PRODUCTS}")
print(f"   Flaconi ings  : {SOURCE_FLACONI_INGREDIENTS}")
print(f"   DM data       : {SOURCE_DM}")


# ---------------------------------------------------------------------------
# Helper: Send SNS alert and stop pipeline
# ---------------------------------------------------------------------------
def alert_and_fail(message: str):
    """Publish failure message to SNS and raise exception to stop the job."""
    print(f"❌ PIPELINE FAILURE: {message}")
    try:
        sns = boto3.client("sns", region_name="eu-central-1")
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="❌ Beauty Boba Pipeline Failure — Glue ETL",
            Message=f"The Glue ETL job failed with the following error:\n\n{message}\n\nJob: {args['JOB_NAME']}"
        )
        print("📧 SNS alert sent successfully.")
    except Exception as sns_error:
        print(f"⚠️  Could not send SNS alert: {sns_error}")
    raise Exception(f"Pipeline stopped: {message}")


# ---------------------------------------------------------------------------
# Helper: Clean price string → float
# Handles: "18,90 €", "ab 16,78 €", "UVP 29,95 €", None
# ---------------------------------------------------------------------------
def clean_price(price_str: str):
    if price_str is None:
        return None
    # Remove "ab", "UVP", "€", spaces — keep digits, commas, dots
    cleaned = re.sub(r"[^\d,\.]", " ", price_str).strip()
    # Take the first number found (handles "ab 16,78" → "16,78")
    match = re.search(r"[\d]+[,\.][\d]+", cleaned)
    if match:
        number = match.group().replace(",", ".")
        try:
            return float(number)
        except ValueError:
            return None
    return None

clean_price_udf = F.udf(clean_price, FloatType())


# ---------------------------------------------------------------------------
# Helper: Normalize ingredients string
# Steps:
#   1. Strip "Ingredients:" prefix (DM data)
#   2. Lowercase everything
#   3. Standardize water/aqua variants → "aqua"
#   4. Split on comma → list
#   5. Strip whitespace from each ingredient
#   6. Remove special characters (dots, asterisks)
#   7. Remove empty strings and duplicates
#   8. Rejoin as clean comma-separated string
# ---------------------------------------------------------------------------
def normalize_ingredients(raw: str):
    if raw is None or str(raw).strip() == "":
        return None

    text = str(raw).strip()

    # Step 1: Strip "Ingredients:" prefix
    text = re.sub(r"^ingredients\s*:\s*", "", text, flags=re.IGNORECASE)

    # Step 2: Lowercase
    text = text.lower()

    # Step 3: Standardize water/aqua variants
    water_variants = [
        r"water\\aqua\\eau",
        r"aqua\s*/\s*water",
        r"water\s*\(aqua\/eau\)",
        r"water\(aqua\/eau\)",
        r"water\s*\(aqua\)",
        r"aqua\s*/\s*water\s*/\s*eau",
        r"water\/aqua\/eau",
        r"water\\\\aqua\\\\eau",
        r"\bwater\b",
        r"\beau\b",
    ]
    for pattern in water_variants:
        text = re.sub(pattern, "aqua", text, flags=re.IGNORECASE)

    # Step 4: Split on comma
    parts = text.split(",")

    # Step 5-6: Strip whitespace and special characters from each ingredient
    cleaned_parts = []
    for part in parts:
        part = part.strip()
        part = re.sub(r"[.\*\[\]\(\)]", "", part).strip()
        if part:
            cleaned_parts.append(part)

    # Step 7: Remove duplicates while preserving order
    seen = set()
    unique_parts = []
    for part in cleaned_parts:
        if part not in seen:
            seen.add(part)
            unique_parts.append(part)

    if not unique_parts:
        return None

    # Step 8: Rejoin as clean string
    return ", ".join(unique_parts)

normalize_ingredients_udf = F.udf(normalize_ingredients, StringType())


# ---------------------------------------------------------------------------
# SECTION 1: Process Flaconi data
# ---------------------------------------------------------------------------
print("\n📂 Reading Flaconi data from S3 bronze layer...")

# Read products file (semicolon-separated)
df_products = spark.read.csv(
    SOURCE_FLACONI_PRODUCTS,
    header=True,
    sep=";",
    encoding="UTF-8"
)

# Read ingredients file (comma-separated)
df_ingredients = spark.read.csv(
    SOURCE_FLACONI_INGREDIENTS,
    header=True,
    sep=",",
    encoding="UTF-8"
)

print(f"   Flaconi products   : {df_products.count()} rows")
print(f"   Flaconi ingredients: {df_ingredients.count()} rows")

# --- Clean Flaconi products ---
print("\n🧹 Cleaning Flaconi product data...")
df_products_clean = df_products.select(
    F.trim(F.col("brand")).alias("brand"),
    F.trim(F.col("series")).alias("product_name"),
    F.trim(F.col("product_type")).alias("product_type"),
    F.trim(F.col("url")).alias("url"),
    clean_price_udf(F.col("price")).alias("price_eur"),
    clean_price_udf(F.col("uvp_price")).alias("uvp_price_eur"),
)

# Drop rows with null URL (needed for join)
df_products_clean = df_products_clean.filter(F.col("url").isNotNull())

# --- Clean Flaconi ingredients ---
print("🧹 Cleaning Flaconi ingredients data...")
df_ingredients_clean = df_ingredients.select(
    F.trim(F.col("url")).alias("url"),
    normalize_ingredients_udf(F.col("ingredients")).alias("ingredients_clean"),
)

# --- Inner join on URL ---
print("🔗 Joining Flaconi products with ingredients on URL (inner join)...")
df_flaconi = df_products_clean.join(df_ingredients_clean, on="url", how="inner")

print(f"   After join: {df_flaconi.count()} rows")

# --- Data quality check: missing ingredients ---
missing_ingredients = df_flaconi.filter(
    F.col("ingredients_clean").isNull() | (F.col("ingredients_clean") == "")
).count()

print(f"   Products with missing ingredients: {missing_ingredients}")

if missing_ingredients > 0:
    alert_and_fail(
        f"Data quality check failed: {missing_ingredients} Flaconi products have missing or empty "
        f"ingredients after cleaning. Please check the source data in s3://{BUCKET}/raw/flaconi/ "
        f"and re-run the pipeline."
    )

# Add source column for traceability
df_flaconi = df_flaconi.withColumn("source", F.lit("flaconi"))

print(f"✅ Flaconi silver layer ready: {df_flaconi.count()} products")
df_flaconi.printSchema()


# ---------------------------------------------------------------------------
# SECTION 2: Process DM data
# ---------------------------------------------------------------------------
print("\n📂 Reading DM data from S3 bronze layer...")

df_dm_raw = spark.read.csv(
    SOURCE_DM,
    header=True,
    sep=",",
    encoding="UTF-8"
)

print(f"   DM raw rows: {df_dm_raw.count()}")

# --- Select and rename relevant columns only ---
print("🧹 Cleaning DM product data...")
df_dm = df_dm_raw.select(
    F.trim(F.col("product_url")).alias("url"),
    F.trim(F.col("brand")).alias("brand"),
    F.trim(F.col("product_name")).alias("product_name"),
    clean_price_udf(F.col("price")).alias("price_eur"),
    F.trim(F.col("subcategory")).alias("product_type"),
    normalize_ingredients_udf(F.col("ingredients")).alias("ingredients_clean"),
    # Keep rating for potential future use in recommendations
    F.col("rating_search").cast(FloatType()).alias("rating"),
    F.col("review_count_search").alias("review_count"),
)

# Drop rows with null URL
df_dm = df_dm.filter(F.col("url").isNotNull())

# --- Data quality check: missing ingredients ---
missing_dm = df_dm.filter(
    F.col("ingredients_clean").isNull() | (F.col("ingredients_clean") == "")
).count()

print(f"   DM products with missing ingredients: {missing_dm}")

if missing_dm > 0:
    alert_and_fail(
        f"Data quality check failed: {missing_dm} DM products have missing or empty ingredients "
        f"after cleaning. Please check the source data in s3://{BUCKET}/raw/dm/ "
        f"and re-run the pipeline."
    )

# Add source column
df_dm = df_dm.withColumn("source", F.lit("dm"))

print(f"✅ DM silver layer ready: {df_dm.count()} products")
df_dm.printSchema()


# ---------------------------------------------------------------------------
# SECTION 3: Final validation summary
# ---------------------------------------------------------------------------
print("\n📊 Silver Layer Summary")
print("=" * 50)
print(f"  Flaconi products : {df_flaconi.count()}")
print(f"  DM products      : {df_dm.count()}")
print(f"  Flaconi columns  : {df_flaconi.columns}")
print(f"  DM columns       : {df_dm.columns}")

# Sample output for verification
print("\n🔍 Flaconi sample (first 3 rows):")
df_flaconi.select("brand", "product_name", "price_eur", "ingredients_clean").show(3, truncate=80)

print("\n🔍 DM sample (first 3 rows):")
df_dm.select("brand", "product_name", "price_eur", "ingredients_clean").show(3, truncate=80)


# ---------------------------------------------------------------------------
# SECTION 4: Write silver layer to S3
# ---------------------------------------------------------------------------
print(f"\n💾 Writing Flaconi silver layer to {TARGET_FLACONI}...")
df_flaconi.coalesce(1).write.mode("overwrite").option("header", "true").csv(TARGET_FLACONI)

print(f"💾 Writing DM silver layer to {TARGET_DM}...")
df_dm.coalesce(1).write.mode("overwrite").option("header", "true").csv(TARGET_DM)

print("\n✅ Bronze to Silver ETL complete!")
print(f"   → {TARGET_FLACONI}")
print(f"   → {TARGET_DM}")

job.commit()
