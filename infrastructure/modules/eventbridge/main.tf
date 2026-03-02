###############################################################
# modules/eventbridge/main.tf
# Creates EventBridge rule that triggers Step Functions
# whenever a new CSV is uploaded to s3://bucket/raw/
###############################################################

locals {
  prefix = "${var.project}-${var.environment}"
}

resource "aws_cloudwatch_event_rule" "s3_upload_trigger" {
  name        = "${local.prefix}-s3-upload-trigger"
  description = "Triggers pipeline when new CSV is uploaded to raw/ folder"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [var.bucket_name]
      }
      object = {
        key = [{ prefix = "raw/" }]
      }
    }
  })

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

resource "aws_cloudwatch_event_target" "trigger_step_functions" {
  rule      = aws_cloudwatch_event_rule.s3_upload_trigger.name
  target_id = "TriggerPipeline"
  arn       = var.state_machine_arn
  role_arn  = var.eventbridge_role_arn
}
