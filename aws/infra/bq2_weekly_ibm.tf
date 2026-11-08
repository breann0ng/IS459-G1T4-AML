# BQ2 weekly pipeline for IBM HI-Large (real data + the fan-in / fan-out new-tactic test).
# Reuses what bq2_weekly.tf (SAML-D) creates: the Step Functions role, the SNS topic, the approval queue,
# the EMR Serverless app and its job role. Needs bq2_weekly.tf in the same folder (branch wt-bq2-saml).
# The definition is generated: python aws/step-functions/build_bq2_weekly_ibm.py --account <id>
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
  definition = file("${path.module}/../step-functions/aml_bq2_weekly_ibm.json")
  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.bq2_weekly_ibm.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }
}
