###############################################################
# modules/step_functions/main.tf
# Creates the state machine that orchestrates the full pipeline:
# Glue ETL → Lambda Similarity → Lambda Loader → DynamoDB
# With SNS alert on any failure
###############################################################

locals {
  prefix = "${var.project}-${var.environment}"
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${local.prefix}-pipeline"
  role_arn = var.step_functions_role_arn

  definition = jsonencode({
    Comment = "Beauty Boba skincare recommendation pipeline"
    StartAt = "RunGlueETL"

    States = {

      # Step 1: Run Glue ETL job (bronze → silver)
      RunGlueETL = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = var.glue_job_name
        }
        Next  = "RunSimilarityLambda"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NotifyFailure"
          ResultPath  = "$.error"
        }]
      }

      # Step 2: Run similarity Lambda (silver → gold)
      RunSimilarityLambda = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.lambda_similarity_arn
          "Payload.$"  = "$"
        }
        Next  = "RunLoaderLambda"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NotifyFailure"
          ResultPath  = "$.error"
        }]
      }

      # Step 3: Load gold layer into DynamoDB
      RunLoaderLambda = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.lambda_loader_arn
          "Payload.$"  = "$"
        }
        Next  = "PipelineSuccess"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NotifyFailure"
          ResultPath  = "$.error"
        }]
      }

      # Success state
      PipelineSuccess = {
        Type = "Succeed"
      }

      # Failure: send SNS alert then fail
      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = var.sns_topic_arn
          Message  = "❌ Beauty Boba pipeline failed. Check Step Functions logs for details."
          Subject  = "Pipeline Failure Alert"
        }
        Next = "PipelineFailed"
      }

      PipelineFailed = {
        Type  = "Fail"
        Cause = "Pipeline execution failed — see SNS alert for details"
      }
    }
  })

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}
