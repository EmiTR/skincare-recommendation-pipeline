output "rule_name" { value = aws_cloudwatch_event_rule.s3_upload_trigger.name }
output "rule_arn"  { value = aws_cloudwatch_event_rule.s3_upload_trigger.arn }
