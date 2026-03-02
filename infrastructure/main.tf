terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Store state in S3 (remote backend)
  backend "s3" {
    bucket         = "skincare-tfstate-yourname"  # Create this bucket first!
    key            = "terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-locks"
  }
}

# Configure AWS Provider
provider "aws" {
  region = var.aws_region

  # Use your IAM credentials from aws configure
  # Or set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY env vars
}

# S3 Module (Buckets first!)
module "s3" {
  source = "./modules/s3"

  bucket_name   = var.bucket_name
  project_name  = var.project_name
}

# Outputs (visible after terraform apply)
output "s3_bucket_name" {
  value = module.s3.bucket_name
}
