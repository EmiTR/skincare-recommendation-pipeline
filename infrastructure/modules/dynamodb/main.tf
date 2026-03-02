###############################################################
# modules/dynamodb/main.tf
# Creates the recommendations table
###############################################################

locals {
  prefix = "${var.project}-${var.environment}"
}

resource "aws_dynamodb_table" "recommendations" {
  name         = "${local.prefix}-recommendations"
  billing_mode = "PAY_PER_REQUEST"  # serverless — no cost when idle
  hash_key     = "flaconi_product_name"

  attribute {
    name = "flaconi_product_name"
    type = "S"
  }

  # Each item will store:
  # - flaconi_product_name (PK)
  # - flaconi_brand
  # - flaconi_price
  # - top_matches: list of 3 DM products with:
  #     dm_product_name, dm_brand, dm_price, similarity_score

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}
