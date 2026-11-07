# BQ1 weekly orchestration: a weekly bank file lands in raw/ -> EventBridge -> Step Functions
# (aml_bq1_weekly) -> one EMR Serverless job per step.
#
# BQ2 (aml_bq2_weekly, its role aml-sfn-bq2, the aml-bq2-weekly schedule, SNS + SQS approval gates)
# was built in the console and is not managed here yet; import it once its definition is in
# aws/step-functions/ (see aws/step-functions/README.md).

data "aws_caller_identity" "me" {}

locals {
  account_id   = data.aws_caller_identity.me.account_id
  bq1_sm_name  = "aml_bq1_weekly"
  emr_app_arn  = "arn:aws:emr-serverless:${var.region}:${local.account_id}:/applications/${aws_emrserverless_application.jobs.id}"
  bq1_log_keys = "arn:aws:s3:::${var.bucket}/progress/bq1_weekly/*"
}

# ---------------------------------------------------------------- S3 -> EventBridge
# Sends every bucket event to EventBridge (the rule below picks the one it needs).
# NOTE: this resource owns ALL notification settings of the bucket (none existed on 2026-11-07).
resource "aws_s3_bucket_notification" "lake" {
  bucket      = aws_s3_bucket.lake.id
  eventbridge = true
}

# ---------------------------------------------------------------- state machine
resource "aws_cloudwatch_log_group" "bq1_weekly" {
  name              = "/aws/vendedlogs/states/${local.bq1_sm_name}"
  retention_in_days = 90
}

resource "aws_iam_role" "sfn_bq1" {
  name = "aml-sfn-bq1"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "aws:SourceAccount" = local.account_id } }
    }]
  })
}

# Same shape as aml-sfn-bq2 (console-made), minus SNS/SQS, plus its own run-log prefix.
resource "aws_iam_role_policy" "sfn_bq1" {
  name = "aml-sfn-bq1"
  role = aws_iam_role.sfn_bq1.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "RunJobs"
        Effect   = "Allow"
        Action   = ["emr-serverless:StartJobRun", "emr-serverless:GetJobRun", "emr-serverless:CancelJobRun"]
        Resource = [local.emr_app_arn, "${local.emr_app_arn}/jobruns/*"]
      },
      {
        Sid       = "PassJobRole"
        Effect    = "Allow"
        Action    = "iam:PassRole"
        Resource  = aws_iam_role.emr_job.arn
        Condition = { StringLike = { "iam:PassedToService" = "emr-serverless.amazonaws.com" } }
      },
      {
        Sid      = "SyncJobEvents" # .sync waits for the job through this managed rule
        Effect   = "Allow"
        Action   = ["events:PutTargets", "events:PutRule", "events:DescribeRule"]
        Resource = "arn:aws:events:${var.region}:${local.account_id}:rule/StepFunctionsGetEventsForEMRServerlessJobRule"
      },
      {
        Sid      = "RunLog"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = local.bq1_log_keys
      },
      {
        Sid       = "RunLogMissingKey" # a missing log then reads as NoSuchKey, not AccessDenied
        Effect    = "Allow"
        Action    = "s3:ListBucket"
        Resource  = aws_s3_bucket.lake.arn
        Condition = { StringLike = { "s3:prefix" = ["progress/bq1_weekly/*", "models/final/"] } } # + CheckModel
      },
      {
        Sid    = "Logs" # vended-log delivery; these actions do not support resource scoping
        Effect = "Allow"
        Action = ["logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery", "logs:DeleteLogDelivery",
        "logs:ListLogDeliveries", "logs:PutResourcePolicy", "logs:DescribeResourcePolicies", "logs:DescribeLogGroups"]
        Resource = "*"
      },
    ]
  })
}

resource "aws_sfn_state_machine" "bq1_weekly" {
  name     = local.bq1_sm_name
  role_arn = aws_iam_role.sfn_bq1.arn
  type     = "STANDARD" # .sync waits on EMR jobs: Express does not support it
  definition = templatefile("${path.module}/../step-functions/aml_bq1_weekly.asl.json", {
    bucket         = var.bucket
    application_id = aws_emrserverless_application.jobs.id
    job_role_arn   = aws_iam_role.emr_job.arn
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.bq1_weekly.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  depends_on = [aws_iam_role_policy.sfn_bq1] # logging needs the Logs permissions at create time
}

# ---------------------------------------------------------------- EventBridge rule
# One run per weekly file: only the _SUCCESS marker that replay_week.py writes last matches.
resource "aws_cloudwatch_event_rule" "bq1_weekly_file" {
  name        = "aml-bq1-weekly-file"
  description = "A weekly bank file finished landing in raw/transactions/: start aml_bq1_weekly"
  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = { name = [var.bucket] }
      object = { key = [{ wildcard = "raw/transactions/*/_SUCCESS" }] }
    }
  })
}

resource "aws_iam_role" "events_bq1" {
  name = "aml-events-bq1"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "aws:SourceAccount" = local.account_id } }
    }]
  })
}

resource "aws_iam_role_policy" "events_bq1" {
  name = "start-aml-bq1-weekly"
  role = aws_iam_role.events_bq1.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "states:StartExecution", Resource = aws_sfn_state_machine.bq1_weekly.arn }]
  })
}

resource "aws_cloudwatch_event_target" "bq1_weekly_file" {
  rule     = aws_cloudwatch_event_rule.bq1_weekly_file.name
  arn      = aws_sfn_state_machine.bq1_weekly.arn
  role_arn = aws_iam_role.events_bq1.arn
  # the whole S3 event is the execution input; the state machine reads detail.object.key
}
