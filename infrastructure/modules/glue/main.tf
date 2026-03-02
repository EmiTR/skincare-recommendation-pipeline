###############################################################
# modules/glue/main.tf
# Creates the Glue ETL job for bronze → silver transformation
###############################################################

locals {
  prefix = "${var.project}-${var.environment}"
}

resource "aws_glue_job" "bronze_to_silver" {
  name         = "${local.prefix}-bronze-to-silver"
  role_arn     = var.glue_role_arn
  glue_version = "4.0"
  worker_type  = "G.1X"     # smallest worker — fine for <10MB
  number_of_workers = 2     # minimum — keeps cost low

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/scripts/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-job-insights"              = "true"
    "--SOURCE_BUCKET"                    = var.bucket_name
    "--SOURCE_FLACONI_PRODUCTS"          = "raw/flaconi/flaconi_gesichtscreme.csv"
    "--SOURCE_FLACONI_INGREDIENTS"       = "raw/flaconi/flaconi_ingredients.csv"
    "--SOURCE_DM"                        = "raw/dm/dm_final.csv"
    "--TARGET_FLACONI"                   = "cleaned/flaconi/"
    "--TARGET_DM"                        = "cleaned/dm/"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}
