###############################################################
# modules/sns/main.tf
# Creates SNS topic for pipeline failure alerts
###############################################################

locals {
  prefix = "${var.project}-${var.environment}"
}

resource "aws_sns_topic" "pipeline_alerts" {
  name = "${local.prefix}-pipeline-alerts"

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

# Subscribe your email to receive alerts
resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
  # Note: you will receive a confirmation email — you must click it to activate alerts
}
