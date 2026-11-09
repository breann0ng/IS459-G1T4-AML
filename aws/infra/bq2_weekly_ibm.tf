# BQ2 weekly pipeline for IBM HI-Large (real data + the fan-in / fan-out new-tactic test).
# Reuses what bq2_weekly.tf (SAML-D) creates: the Step Functions role, the SNS topic, the approval queue,
# the EMR Serverless app and its job role.
# The definition is aws/orchestration/state-machines/aml_bq2_weekly_ibm.asl.json; like aml_bq2_weekly, the BQ2 team deploys
# definition changes themselves (ignore_changes below), so an infrastructure apply never swaps it mid-run.
# The machine and its three policies were made with the CLI and are adopted with the import blocks at the end.
# No schedule: every execution is started by hand (each scored week runs Spark jobs that cost money).

resource "aws_cloudwatch_log_group" "bq2_weekly_ibm" {
  name              = "/aws/vendedlogs/states/aml_bq2_weekly_ibm"
  retention_in_days = 90
}

# The HI-Large run keeps everything under s3://<bucket>/bq2_ibm/ (its own curated/, analytics/, models/, progress/,
# jobs/), so the team's batch HI-Large and models/ are never touched. The Step Functions role needs its week pointer,
# the per-week "already built" markers and the decision files there (the EMR job role already covers the bucket).
resource "aws_iam_role_policy" "sfn_bq2_ibm" {
  name = "aml-sfn-bq2-ibm"
  role = aws_iam_role.sfn_bq2.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject"]
      Resource = ["arn:aws:s3:::${var.bucket}/bq2_ibm/progress/*", "arn:aws:s3:::${var.bucket}/bq2_ibm/models/bq2r/*"]
    }]
  })
}

resource "aws_sfn_state_machine" "bq2_weekly_ibm" {
  name       = "aml_bq2_weekly_ibm"
  role_arn   = aws_iam_role.sfn_bq2.arn
  definition = file("${path.module}/../orchestration/state-machines/aml_bq2_weekly_ibm.asl.json")
  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.bq2_weekly_ibm.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  lifecycle {
    ignore_changes = [definition]
  }
}

# The dashboard (EC2 role and the approver user) reads the HI-Large results under bq2_ibm/ (read-only).
locals {
  dashboard_ibm_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject"], Resource = "arn:aws:s3:::${var.bucket}/bq2_ibm/*" },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = "arn:aws:s3:::${var.bucket}",
      Condition = { StringLike = { "s3:prefix" = ["bq2_ibm/*"] } } },
    ]
  })
}

resource "aws_iam_role_policy" "dashboard_bq2_ibm" {
  name   = "aml-dashboard-bq2-ibm"
  role   = aws_iam_role.dashboard.id
  policy = local.dashboard_ibm_policy
}

resource "aws_iam_user_policy" "bq2_approver_ibm" {
  name   = "aml-bq2-approver-ibm"
  user   = aws_iam_user.bq2_approver.name
  policy = local.dashboard_ibm_policy
}

# ---------------------------------------------------------------- adopt what was built with the CLI
import {
  to = aws_sfn_state_machine.bq2_weekly_ibm
  id = "arn:aws:states:${var.region}:${data.aws_caller_identity.me.account_id}:stateMachine:aml_bq2_weekly_ibm"
}
import {
  to = aws_iam_role_policy.sfn_bq2_ibm
  id = "aml-sfn-bq2:aml-sfn-bq2-ibm"
}
import {
  to = aws_iam_role_policy.dashboard_bq2_ibm
  id = "aml-dashboard-ec2:aml-dashboard-bq2-ibm"
}
import {
  to = aws_iam_user_policy.bq2_approver_ibm
  id = "aml-bq2-approver:aml-bq2-approver-ibm"
}
