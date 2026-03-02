variable "project"                  { type = string }
variable "environment"              { type = string }
variable "glue_job_name"            { type = string }
variable "lambda_similarity_arn"    { type = string }
variable "lambda_loader_arn"        { type = string }
variable "sns_topic_arn"            { type = string }
variable "step_functions_role_arn"  { type = string }
