# BQ2 weekly pipeline: state machine, approval queue, notifications, schedule.
# The definition is generated: python aws/step-functions/build_bq2_weekly.py (needs the topic ARN and queue URL
# below, so apply the topic and queue first: terraform apply -target=aws_sns_topic.bq2 -target=aws_sqs_queue.bq2_approvals).

variable "bq2_notify_emails" {
  type        = list(string)
  default     = ["weiting.yeo.2024@computing.smu.edu.sg"]
  description = "Who is emailed when a retrain/deploy approval is needed. Empty = no subscriptions."
}

variable "bq2_schedule_enabled" {
  type        = bool
  default     = false
  description = "Keep false until the pipeline is proven: every scheduled run costs EMR money."
}

variable "bq2_schedule" {
  type    = string
  default = "rate(7 days)"
}

data "aws_caller_identity" "me" {}

resource "aws_sns_topic" "bq2" {
  name = "aml-bq2-alerts"
}

resource "aws_sns_topic_subscription" "bq2_email" {
  for_each  = toset(var.bq2_notify_emails)
  topic_arn = aws_sns_topic.bq2.arn
  protocol  = "email"
  endpoint  = each.value
}

# Approval inbox: the state machine puts a task token here; the dashboard reads it and answers with
# SendTaskSuccess / SendTaskFailure.
resource "aws_sqs_queue" "bq2_approvals" {
  name                       = "aml-bq2-approvals"
  message_retention_seconds  = 1209600 # 14 days, longer than the 7-day approval window
  visibility_timeout_seconds = 60
}

resource "aws_cloudwatch_log_group" "bq2_weekly" {
  name              = "/aws/vendedlogs/states/aml_bq2_weekly"
  retention_in_days = 90
}

data "aws_iam_policy_document" "sfn_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn_bq2" {
  name               = "aml-sfn-bq2"
  assume_role_policy = data.aws_iam_policy_document.sfn_trust.json
}

data "aws_iam_policy_document" "sfn_bq2" {
  statement {
    sid       = "RunJobs"
    actions   = ["emr-serverless:StartJobRun", "emr-serverless:GetJobRun", "emr-serverless:CancelJobRun"]
    resources = [aws_emrserverless_application.jobs.arn, "${aws_emrserverless_application.jobs.arn}/jobruns/*"]
  }
  statement {
    sid       = "PassJobRole"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.emr_job.arn]
    condition {
      test     = "StringLike"
      variable = "iam:PassedToService"
      values   = ["emr-serverless.amazonaws.com"]
    }
  }
  statement {
    sid       = "SyncJobEvents" # needed by the .sync integration
    actions   = ["events:PutTargets", "events:PutRule", "events:DescribeRule"]
    resources = ["arn:aws:events:${var.region}:${data.aws_caller_identity.me.account_id}:rule/StepFunctionsGetEventsForEMRServerlessJobRule"]
  }
  statement {
    sid     = "Pointer"
    actions = ["s3:GetObject", "s3:PutObject"]
    resources = ["arn:aws:s3:::${var.bucket}/progress/bq2_weekly.json", "arn:aws:s3:::${var.bucket}/progress/bq2/*", # skip markers
    "arn:aws:s3:::${var.bucket}/models/bq2r/*"]
  }
  statement {
    sid       = "Notify"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.bq2.arn]
  }
  statement {
    sid       = "Approvals"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.bq2_approvals.arn]
  }
  statement {
    sid = "Logs"
    actions = [
      "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery", "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries", "logs:PutResourcePolicy", "logs:DescribeResourcePolicies", "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "sfn_bq2" {
  name   = "aml-sfn-bq2"
  role   = aws_iam_role.sfn_bq2.id
  policy = data.aws_iam_policy_document.sfn_bq2.json
}

resource "aws_sfn_state_machine" "bq2_weekly" {
  name       = "aml_bq2_weekly"
  role_arn   = aws_iam_role.sfn_bq2.arn
  definition = file("${path.module}/../step-functions/aml_bq2_weekly.asl.json")
  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.bq2_weekly.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  # The live definition is deployed by the BQ2 team (aws/step-functions/aml_bq2_weekly.asl.json, via the console
  # or CLI) when they switch it over; an infrastructure apply must never swap it under a running pipeline.
  lifecycle {
    ignore_changes = [definition]
  }
}

# Weekly trigger, OFF by default (state = DISABLED).
data "aws_iam_policy_document" "scheduler_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler_bq2" {
  name               = "aml-scheduler-bq2"
  assume_role_policy = data.aws_iam_policy_document.scheduler_trust.json
}

resource "aws_iam_role_policy" "scheduler_bq2" {
  role = aws_iam_role.scheduler_bq2.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "states:StartExecution", Resource = aws_sfn_state_machine.bq2_weekly.arn }]
  })
}

resource "aws_scheduler_schedule" "bq2_weekly" {
  name                = "aml-bq2-weekly"
  schedule_expression = var.bq2_schedule
  state               = var.bq2_schedule_enabled ? "ENABLED" : "DISABLED"
  flexible_time_window { mode = "OFF" }
  target {
    arn      = aws_sfn_state_machine.bq2_weekly.arn
    role_arn = aws_iam_role.scheduler_bq2.arn
    input    = "{}"
  }
}

# The dashboard's approval buttons: a separate low-privilege user (create access keys by hand, never in git).
resource "aws_iam_user" "bq2_approver" {
  name = "aml-bq2-approver"
}

locals {
  dashboard_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["sqs:ReceiveMessage", "sqs:DeleteMessage"], Resource = aws_sqs_queue.bq2_approvals.arn },
      { Effect = "Allow", Action = ["states:SendTaskSuccess", "states:SendTaskFailure"], Resource = "*" }, # these actions take no resource ARN
      # read-only dashboard access: Athena in the capped workgroup, the BQ2 tables, and the small result files
      { Effect = "Allow", Action = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults"],
      Resource = "arn:aws:athena:${var.region}:${data.aws_caller_identity.me.account_id}:workgroup/${aws_athena_workgroup.aml.name}" },
      { Effect = "Allow", Action = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"],
        Resource = ["arn:aws:glue:${var.region}:${data.aws_caller_identity.me.account_id}:catalog",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.me.account_id}:database/analytics",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.me.account_id}:table/analytics/bq2r_*"] },
      { Effect = "Allow", Action = ["s3:GetObject"],
        Resource = ["arn:aws:s3:::${var.bucket}/analytics/bq2r_*", "arn:aws:s3:::${var.bucket}/models/bq2r/*",
      "arn:aws:s3:::${var.bucket}/progress/*", "arn:aws:s3:::${var.bucket}/athena-results/*"] },
      { Effect = "Allow", Action = ["s3:PutObject", "s3:GetBucketLocation"],
      Resource = ["arn:aws:s3:::${var.bucket}/athena-results/*", "arn:aws:s3:::${var.bucket}"] },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = "arn:aws:s3:::${var.bucket}",
      Condition = { StringLike = { "s3:prefix" = ["analytics/bq2r_*", "models/bq2r/*", "progress/*"] } } },
    ]
  })
}

resource "aws_iam_user_policy" "bq2_approver" {
  user   = aws_iam_user.bq2_approver.name
  policy = local.dashboard_policy
}
