###############################################################
# outputs.tf — Useful values printed after terraform apply
###############################################################

output "pipeline_bucket_name" {
  description = "S3 pipeline bucket name"
  value       = module.s3.bucket_name
}

output "pipeline_bucket_arn" {
  description = "S3 pipeline bucket ARN"
  value       = module.s3.bucket_arn
}

output "dynamodb_table_name" {
  description = "DynamoDB recommendations table name"
  value       = module.dynamodb.table_name
}

output "glue_job_name" {
  description = "Glue ETL job name"
  value       = module.glue.job_name
}

output "similarity_lambda_arn" {
  description = "ARN of the similarity calculation Lambda"
  value       = module.lambda.similarity_function_arn
}

output "state_machine_arn" {
  description = "Step Functions state machine ARN"
  value       = module.step_functions.state_machine_arn
}

output "sns_topic_arn" {
  description = "SNS alert topic ARN"
  value       = module.sns.topic_arn
}
