variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "Unique S3 bucket name"
  type        = string
}

variable "project_name" {
  description = "Project name prefix"
  type        = string
  default     = "skincare-pipeline"
}

variable "sns_email" {
  description = "Email for failure alerts"
  type        = string
}
