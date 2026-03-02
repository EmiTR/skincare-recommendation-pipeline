###############################################################
# modules/s3/main.tf
# Creates the pipeline S3 bucket with bronze/silver/gold structure
###############################################################

locals {
  prefix = "${var.project}-${var.environment}"
}

###############################################################
# Pipeline data bucket
###############################################################
resource "aws_s3_bucket" "pipeline" {
  bucket = var.bucket_name

  tags = {
    Project     = var.project
    Environment = var.environment
    Layer       = "all"
  }
}

# Block all public access — this is private pipeline data
resource "aws_s3_bucket_public_access_block" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enable versioning — lets you recover previous data files if something goes wrong
resource "aws_s3_bucket_versioning" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Enable server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

###############################################################
# Folder structure — bronze / silver / gold + glue scripts
# S3 doesn't have real folders, but placeholder objects
# create the logical structure you'll see in the console
###############################################################

resource "aws_s3_object" "bronze_flaconi" {
  bucket  = aws_s3_bucket.pipeline.id
  key     = "raw/flaconi/.keep"
  content = ""
}

resource "aws_s3_object" "bronze_dm" {
  bucket  = aws_s3_bucket.pipeline.id
  key     = "raw/dm/.keep"
  content = ""
}

resource "aws_s3_object" "silver_flaconi" {
  bucket  = aws_s3_bucket.pipeline.id
  key     = "cleaned/flaconi/.keep"
  content = ""
}

resource "aws_s3_object" "silver_dm" {
  bucket  = aws_s3_bucket.pipeline.id
  key     = "cleaned/dm/.keep"
  content = ""
}

resource "aws_s3_object" "gold" {
  bucket  = aws_s3_bucket.pipeline.id
  key     = "output/recommendations/.keep"
  content = ""
}

resource "aws_s3_object" "glue_scripts" {
  bucket  = aws_s3_bucket.pipeline.id
  key     = "scripts/.keep"
  content = ""
}

###############################################################
# EventBridge notification — triggers pipeline when new
# CSV lands in raw/ folder
###############################################################
resource "aws_s3_bucket_notification" "pipeline_trigger" {
  bucket      = aws_s3_bucket.pipeline.id
  eventbridge = true   # sends all S3 events to EventBridge
}
