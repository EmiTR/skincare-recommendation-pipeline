###############################################################
# modules/lambda/main.tf
# Creates two Lambda functions:
# 1. similarity  — computes cosine similarity, writes gold layer
# 2. loader      — loads gold layer from S3 into DynamoDB
###############################################################

locals {
  prefix = "${var.project}-${var.environment}"
}

###############################################################
# Similarity Lambda
###############################################################
resource "aws_lambda_function" "similarity" {
  function_name = "${local.prefix}-similarity"
  role          = var.lambda_role_arn
  runtime       = "python3.11"
  handler       = "handler.lambda_handler"
  timeout       = 300    # 5 minutes — enough for cosine similarity on <10MB
  memory_size   = 512    # MB — enough for scikit-learn + pandas

  # Placeholder zip — you'll replace with your actual code
  filename      = "${path.module}/placeholder.zip"

  environment {
    variables = {
      BUCKET_NAME        = var.bucket_name
      SILVER_FLACONI     = "cleaned/flaconi/"
      SILVER_DM          = "cleaned/dm/"
      GOLD_OUTPUT        = "output/recommendations/"
      DYNAMODB_TABLE     = var.dynamodb_table_name
    }
  }

  tags = {
    Project     = var.project
    Environment = var.environment
  }

  lifecycle {
    ignore_changes = [filename, source_code_hash]
  }
}

###############################################################
# Loader Lambda (loads gold layer → DynamoDB)
###############################################################
resource "aws_lambda_function" "loader" {
  function_name = "${local.prefix}-loader"
  role          = var.lambda_role_arn
  runtime       = "python3.11"
  handler       = "handler.lambda_handler"
  timeout       = 120
  memory_size   = 256

  filename      = "${path.module}/placeholder.zip"

  environment {
    variables = {
      BUCKET_NAME    = var.bucket_name
      GOLD_OUTPUT    = "output/recommendations/"
      DYNAMODB_TABLE = var.dynamodb_table_name
    }
  }

  tags = {
    Project     = var.project
    Environment = var.environment
  }

  lifecycle {
    ignore_changes = [filename, source_code_hash]
  }
}

###############################################################
# CloudWatch Log Groups for Lambda
###############################################################
resource "aws_cloudwatch_log_group" "similarity_logs" {
  name              = "/aws/lambda/${aws_lambda_function.similarity.function_name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "loader_logs" {
  name              = "/aws/lambda/${aws_lambda_function.loader.function_name}"
  retention_in_days = 14
}

###############################################################
# Placeholder zip — needed for initial terraform apply
# You will deploy actual code separately using AWS CLI
###############################################################
data "archive_file" "placeholder" {
  type        = "zip"
  output_path = "${path.module}/placeholder.zip"

  source {
    content  = "def lambda_handler(event, context): return {'statusCode': 200}"
    filename = "handler.py"
  }
}
