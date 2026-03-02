variable "project"     { type = string }
variable "environment" { type = string }
variable "bucket_name" { type = string }
variable "aws_region"  { type = string }
variable "account_id"  { 
    type = string 
    sensitive = true 
    }
