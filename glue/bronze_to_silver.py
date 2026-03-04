"""
bronze_to_silver.py
===================
AWS Glue ETL job — Bronze to Silver layer transformation

Output schema (both tables):
  product_id, source, brand, product_name, price_eur,
  url, ingredient, position
"""

import sys
import re
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, FloatType, IntegerType,
    ArrayType, StructType, StructField
)

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

BUCKET                     = args["SOURCE_BUCKET"]
SOURCE_FLACONI_PRODUCTS    = "s3://{}/{}".format(BUCKET, args["SOURCE_FLACONI_PRODUCTS"])
SOURCE_FLACONI_INGREDIENTS = "s3://{}/{}".format(BUCKET, args["SOURCE_FLACONI_INGREDIENTS"])
SOURCE_DM                  = "s3://{}/{}".format(BUCKET, args["SOURCE_DM"])
TARGET_FLACONI             = "s3://{}/{}".format(BUCKET, args["TARGET_FLACONI"])
TARGET_DM                  = "s3://{}/{}".format(BUCKET, args["TARGET_DM"])

ACCOUNT_ID    = boto3.client("sts").get_caller_identity()["Account"]
SNS_TOPIC_ARN = "arn:aws:sns:eu-central-1:{}:beauty-boba-dev-pipeline-alerts".format(ACCOUNT_ID)

print("Glue job initialized: {}".format(args["JOB_NAME"]))

# ---------------------------------------------------------------------------
# Synonyms and footnote patterns
# ---------------------------------------------------------------------------
SYNONYMS = {
    "WATER":                    "AQUA",
    "EAU":                      "AQUA",
    "AQUA/WATER":               "AQUA",
    "WATER/AQUA":               "AQUA",
    "AQUA/WATER/EAU":           "AQUA",
    "WATER/AQUA/EAU":           "AQUA",
    "DEIONIZED WATER":          "AQUA",
    "PURIFIED WATER":           "AQUA",
    "DISTILLED WATER":          "AQUA",
    "WATERAQUAEAU":             "AQUA",
    "WASSERAQUAEAU":            "AQUA",
    "AQUA (WATER)":             "AQUA",
    "WATER (AQUA)":             "AQUA",
    "WATER (AQUA/EAU)":         "AQUA",
    "AQUA (WATER/EAU)":         "AQUA",
    "AQUA/[WATER]":             "AQUA",
    "AQUA [WATER]":             "AQUA",
    "WATER [AQUA]":             "AQUA",
    "WATER/EAU (AQUA)":         "AQUA",
}

FOOTNOTE_PATTERNS = [
    r"^[\*\.\-\d\s]+$",
    r"HERGESTELLT",
    r"ATHERISCH",
    r"BIO-ZUTAT",
    r"NATURLICH",
    r"EXCLUSIVE.*COMPLEX",
    r"^[A-Z]+'S\s",
    r"PPM\)?$",
    r"PPB\)?$",
    r"^\d+\s*%$",
    r"^\d+PPM",
    r"^\d+\s",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def alert_and_fail(message):
    print("PIPELINE FAILURE: {}".format(message))
    try:
        sns = boto3.client("sns", region_name="eu-central-1")
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="Beauty Boba Pipeline Failure - Glue ETL",
            Message="Glue ETL job failed:\n\n{}\n\nJob: {}\nBucket: {}".format(
                message, args["JOB_NAME"], BUCKET)
        )
    except Exception as e:
        print("Could not send SNS alert: {}".format(e))
    raise Exception("Pipeline stopped: {}".format(message))


def clean_price(price_str):
    if price_str is None:
        return None
    cleaned = re.sub(r"[^\d,\.]", " ", str(price_str)).strip()
    match = re.search(r"[\d]+[,\.][\d]+", cleaned)
    if match:
        try:
            return float(match.group().replace(",", "."))
        except ValueError:
            return None
    return None

clean_price_udf = F.udf(clean_price, FloatType())


def protect_chemical_commas(s):
    return re.sub(r"(\d),(\d)", r"\1CHEMCOMMA\2", s)

def restore_chemical_commas(s):
    return s.replace("CHEMCOMMA", ",")

def apply_synonyms(ing):
    return SYNONYMS.get(ing.strip(), ing.strip())

def strip_ingredients_prefix(s):
    patterns = [
        r"^ingredients\s*[/:;,]?\s*inci\s*:\s*",
        r"^ingredients\s*/[^:]+:\s*",
        r"^ingredients\s*[/:;]\s*",
        r"^ingredients\s*:\s*",
        r"^ingredients\s+",
        r"^inci\s*:\s*",
        r"^inci\s+",
    ]
    for pattern in patterns:
        s = re.sub(pattern, "", s, flags=re.IGNORECASE).strip()
    return s

def clean_ingredient_string(raw):
    if raw is None or str(raw).strip() == "":
        return ""
    s = str(raw)
    s = s.replace("\n", " ").replace("\r", " ")
    s = re.sub(r"\s+", " ", s)
    s = strip_ingredients_prefix(s)
    s = re.sub(r"\(Aqua[^)]*\)",  "", s, flags=re.IGNORECASE)
    s = re.sub(r"\(Water[^)]*\)", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\(Eau[^)]*\)",   "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*[|]\s*",  ",", s)
    s = re.sub(r"\s*[*]\s*",  ",", s)
    s = re.sub(r"\s*[\u00b7]\s*", ",", s)
    s = re.sub(r"\s*[\u2022]\s*", ",", s)
    s = re.sub(r"\s*[.]\s*",  ",", s)
    s = s.rstrip(".")
    s = protect_chemical_commas(s)
    s = s.upper()
    return s.strip()

def clean_single_ingredient(ing):
    ing = ing.strip()
    if "/" in ing:
        ing = ing.split("/")[0].strip()
    if "\\" in ing:
        ing = ing.split("\\")[0].strip()
    ing = re.sub(r"\s*\([^)]*\)", "", ing).strip()
    ing = re.sub(r"\s+\d+\.?\d*\s*%", "", ing).strip()
    ing = re.sub(r"^\*+", "", ing).strip()
    ing = re.sub(r"\*+$", "", ing).strip()
    ing = restore_chemical_commas(ing)
    ing_check = ing.replace("A","A").replace("O","O").replace("U","U")
    for pattern in FOOTNOTE_PATTERNS:
        if re.search(pattern, ing_check, re.IGNORECASE):
            return ""
    ing = apply_synonyms(ing)
    return ing.strip()


# ---------------------------------------------------------------------------
# PySpark UDF: explode ingredient string -> array of (ingredient, position)
# ---------------------------------------------------------------------------
ingredient_schema = ArrayType(
    StructType([
        StructField("ingredient", StringType(), True),
        StructField("position",   IntegerType(), True),
    ])
)

def explode_ingredient_string(raw_string):
    if not raw_string:
        return []
    cleaned_string = clean_ingredient_string(raw_string)
    if not cleaned_string:
        return []
    seen     = set()
    position = 1
    result   = []
    for token in cleaned_string.split(","):
        ing = clean_single_ingredient(token)
        if len(ing) < 3:
            continue
        if re.match(r"^CI\s+\d+$", ing):
            continue
        if not ing:
            continue
        if ing in seen:
            continue
        seen.add(ing)
        result.append({"ingredient": ing, "position": position})
        position += 1
    return result

explode_ingredients_udf = F.udf(explode_ingredient_string, ingredient_schema)


# ---------------------------------------------------------------------------
# SECTION 1: Flaconi
# ---------------------------------------------------------------------------
print("Reading Flaconi bronze layer...")

df_products = spark.read.csv(
    SOURCE_FLACONI_PRODUCTS, header=True, sep=";", encoding="UTF-8"
)
df_ingredients = spark.read.csv(
    SOURCE_FLACONI_INGREDIENTS, header=True, sep=",", encoding="UTF-8"
)

print("  Flaconi products   : {}".format(df_products.count()))
print("  Flaconi ingredients: {}".format(df_ingredients.count()))

df_products_clean = df_products.select(
    F.trim(F.col("url")).alias("url"),
    F.trim(F.col("brand")).alias("brand"),
    F.trim(F.col("series")).alias("product_name"),
    clean_price_udf(F.col("price")).alias("price_eur"),
).filter(F.col("url").isNotNull())

df_ingredients_clean = df_ingredients.select(
    F.trim(F.col("url")).alias("url"),
    F.trim(F.col("ingredients")).alias("ingredients_raw"),
).filter(F.col("url").isNotNull())

print("Joining Flaconi products with ingredients...")
df_flaconi = df_products_clean.join(df_ingredients_clean, on="url", how="inner")
print("  After join: {}".format(df_flaconi.count()))

# Filter out missing ingredients (scraper noise ~3-4% expected)
missing_flaconi = df_flaconi.filter(
    F.col("ingredients_raw").isNull() | (F.trim(F.col("ingredients_raw")) == "")
).count()
if missing_flaconi > 0:
    print("Dropping {} Flaconi products with missing ingredients (scraper noise).".format(missing_flaconi))
df_flaconi = df_flaconi.filter(
    F.col("ingredients_raw").isNotNull() & (F.trim(F.col("ingredients_raw")) != "")
)

df_flaconi = df_flaconi \
    .withColumn("product_id", F.concat_ws("_", F.lit("flaconi"),
                F.monotonically_increasing_id().cast(StringType()))) \
    .withColumn("source", F.lit("flaconi"))

print("Exploding Flaconi ingredients...")
df_flaconi = df_flaconi \
    .withColumn("ing_array",  explode_ingredients_udf(F.col("ingredients_raw"))) \
    .withColumn("ing_struct", F.explode(F.col("ing_array"))) \
    .withColumn("ingredient", F.col("ing_struct.ingredient")) \
    .withColumn("position",   F.col("ing_struct.position")) \
    .select("product_id", "source", "brand", "product_name",
            "price_eur", "url", "ingredient", "position")

print("Flaconi silver ready: {} ingredient rows".format(df_flaconi.count()))
print("  Unique products   : {}".format(df_flaconi.select("product_id").distinct().count()))
print("  Unique ingredients: {}".format(df_flaconi.select("ingredient").distinct().count()))


# ---------------------------------------------------------------------------
# SECTION 2: DM
# ---------------------------------------------------------------------------
print("Reading DM bronze layer...")

df_dm_raw = spark.read.csv(
    SOURCE_DM, header=True, sep=",", encoding="UTF-8"
)
print("  DM raw rows: {}".format(df_dm_raw.count()))

df_dm = df_dm_raw.select(
    F.trim(F.col("product_url")).alias("url"),
    F.trim(F.col("brand")).alias("brand"),
    F.trim(F.col("product_name")).alias("product_name"),
    clean_price_udf(F.col("price")).alias("price_eur"),
    F.trim(F.col("ingredients")).alias("ingredients_raw"),
    F.col("rating_search").cast(FloatType()).alias("rating"),
    F.col("review_count_search").alias("review_count"),
).filter(F.col("url").isNotNull())

# Filter out missing ingredients
missing_dm = df_dm.filter(
    F.col("ingredients_raw").isNull() | (F.trim(F.col("ingredients_raw")) == "")
).count()
if missing_dm > 0:
    print("Dropping {} DM products with missing ingredients (scraper noise).".format(missing_dm))
df_dm = df_dm.filter(
    F.col("ingredients_raw").isNotNull() & (F.trim(F.col("ingredients_raw")) != "")
)

df_dm = df_dm \
    .withColumn("product_id", F.concat_ws("_", F.lit("dm"),
                F.monotonically_increasing_id().cast(StringType()))) \
    .withColumn("source", F.lit("dm"))

print("Exploding DM ingredients...")
df_dm = df_dm \
    .withColumn("ing_array",  explode_ingredients_udf(F.col("ingredients_raw"))) \
    .withColumn("ing_struct", F.explode(F.col("ing_array"))) \
    .withColumn("ingredient", F.col("ing_struct.ingredient")) \
    .withColumn("position",   F.col("ing_struct.position")) \
    .select("product_id", "source", "brand", "product_name",
            "price_eur", "url", "ingredient", "position")

print("DM silver ready: {} ingredient rows".format(df_dm.count()))
print("  Unique products   : {}".format(df_dm.select("product_id").distinct().count()))
print("  Unique ingredients: {}".format(df_dm.select("ingredient").distinct().count()))


# ---------------------------------------------------------------------------
# SECTION 3: Write silver layer
# ---------------------------------------------------------------------------
print("Silver Layer Summary")
print("  Flaconi ingredient rows : {}".format(df_flaconi.count()))
print("  DM ingredient rows      : {}".format(df_dm.count()))

print("Writing Flaconi silver to {}...".format(TARGET_FLACONI))
df_flaconi.coalesce(1).write.mode("overwrite").option("header", "true").csv(TARGET_FLACONI)

print("Writing DM silver to {}...".format(TARGET_DM))
df_dm.coalesce(1).write.mode("overwrite").option("header", "true").csv(TARGET_DM)

print("Bronze to Silver ETL complete!")
print("  -> {}".format(TARGET_FLACONI))
print("  -> {}".format(TARGET_DM))

job.commit()
