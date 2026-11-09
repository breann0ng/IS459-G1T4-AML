# BQ1 orchestration (the live flow since 2026-11-09):
#
#   raw/transactions/set=<ds>/week=<Mon>/_SUCCESS --(aml-prepare-weekly-file)--> aml_prepare
#       Clean -> ParsePatterns -> Features; marker progress/prepare/<ds>/<week>.json; emits DataPrepared
#   DataPrepared (an HI-*-live week with features) --(aml-bq1-data-prepared)--> aml_bq1_score
#       Score -> Name -> WeekCheck -> Summary; writes analytics/live_*; run log progress/bq1/score/<execution>.json
#   aml_bq1_train: started by hand; trains, gates and promotes models/bq1_champion.json
#   aml-bq1-run-ended: an aml_prepare / aml_bq1_score run FAILED, TIMED_OUT or ABORTED -> email (aml-bq1-weekly-alerts)
#
# Definitions: aws/orchestration/state-machines/*.asl.json and the rules' event patterns
# aws/orchestration/eventbridge/*.json, used as they are (they carry the bucket, account and EMR app ids
# literally, the same text as the console shows). IAM policies: aws/infra/policies/. The job code is s3://<bucket>/jobs/,
# deployed from main with aws/deploy/deploy.sh.
# The three machines, their two roles and the two trigger rules were built with the CLI and adopted here with
# import blocks (Terraform >= 1.5). Once applied, the import blocks are no-ops and can stay or go.
#
# Retired 2026-11-09 (removed from this file, so `terraform apply` deletes them): aml_bq1_weekly (one machine
# for the whole week; its definition is in git history), its rule aml-bq1-weekly-file (already
# disabled), the cleanup machine aml_bq1_abort and its rule aml-bq1-run-aborted, and their roles and log groups.
#
# BQ2: SAML-D (aml_bq2_weekly) is in bq2_weekly.tf. HI-Large (aml_bq2_weekly_ibm) is in bq2_weekly_ibm.tf on
# Tasha's branch and is not applied yet.

# data.aws_caller_identity.me is declared in bq2_weekly.tf (one per module)

locals {
  account_id = data.aws_caller_identity.me.account_id
  sfn_dir    = "${path.module}/../orchestration/state-machines"
  rules_dir  = "${path.module}/../orchestration/eventbridge"
  policy_dir = "${path.module}/policies"
}

# ---------------------------------------------------------------- BQ1's own EMR Serverless app
# BQ1 runs on its own application so it never waits behind BQ2 or ad-hoc runs on the shared
# aml-jobs app (on 2026-11-07 a week's clean starved for 20 min while another job held all 16 vCPU).
# One job at a time, because one BQ1 job (driver 4 + 3 executors x 4) can use the whole 16 vCPU:
# the next step waits in EMR's own queue (QUEUED, not billed) instead of starting and starving.
# Idle costs nothing (auto-stop). Account quota: 64 vCPU, so aml-jobs (16) + aml-bq1 (16) fit.
# Same settings as aml-jobs otherwise (Glue Data Catalog metastore, persistent UI, cost per job).
resource "aws_emrserverless_application" "bq1" {
  name          = "aml-bq1"
  release_label = var.emr_release_label
  type          = "spark"
  architecture  = "X86_64"

  auto_start_configuration { enabled = true }
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  maximum_capacity { # the auto-scaling ceiling AND the cost cap
    cpu    = "16 vCPU"
    memory = "80 GB" # FeaturesV2: 3 executors x 14 GB + a 24 GB driver = 66 GB
  }

  scheduler_configuration {
    max_concurrent_runs   = 3 # TEMPORARY: matches the CLI change of 2026-11-08 (Joey's runs); set back to 1 once he has moved off
    queue_timeout_minutes = 120
  }

  runtime_configuration {
    classification = "spark-defaults"
    properties = {
      "spark.hadoop.hive.metastore.client.factory.class" = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    }
  }

  monitoring_configuration {
    managed_persistence_monitoring_configuration { enabled = true }
  }

  job_level_cost_allocation_configuration { enabled = true }
}

# A second, identical app for test runs (dataset HI-Medium-test), so a test never shares capacity
# with whatever is running on aml-bq1. Idle costs nothing. 16 + 16 + 16 (aml-jobs) = 48 of the 64 vCPU quota.
resource "aws_emrserverless_application" "bq1_test" {
  name          = "aml-bq1-test"
  release_label = var.emr_release_label
  type          = "spark"
  architecture  = "X86_64"

  auto_start_configuration { enabled = true }
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }
  maximum_capacity {
    cpu    = "16 vCPU"
    memory = "80 GB" # FeaturesV2: 3 executors x 14 GB + a 24 GB driver = 66 GB
  }
  scheduler_configuration {
    max_concurrent_runs   = 1
    queue_timeout_minutes = 120
  }
  runtime_configuration {
    classification = "spark-defaults"
    properties = {
      "spark.hadoop.hive.metastore.client.factory.class" = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    }
  }
  monitoring_configuration {
    managed_persistence_monitoring_configuration { enabled = true }
  }
  job_level_cost_allocation_configuration { enabled = true }
}

# ---------------------------------------------------------------- S3 -> EventBridge
# Sends every bucket event to EventBridge (aml-prepare-weekly-file picks the _SUCCESS markers).
# NOTE: this resource owns ALL notification settings of the bucket (none existed on 2026-11-07).
resource "aws_s3_bucket_notification" "lake" {
  bucket      = aws_s3_bucket.lake.id
  eventbridge = true
}

# ---------------------------------------------------------------- roles
# aml-sfn-pipeline: what the three machines may do (EMR jobs, nested aml_prepare, DataPrepared, run logs,
# markers, the champion pointer, aml-bq1-alerts). aml-events-pipeline: EventBridge may start the two machines.
resource "aws_iam_role" "sfn_pipeline" {
  name               = "aml-sfn-pipeline"
  assume_role_policy = file("${local.policy_dir}/aml_pipeline_trust.json")
}

resource "aws_iam_role_policy" "sfn_pipeline" {
  name   = "aml-sfn-pipeline"
  role   = aws_iam_role.sfn_pipeline.id
  policy = file("${local.policy_dir}/aml_pipeline_role_policy.json")
}

resource "aws_iam_role" "events_pipeline" {
  name               = "aml-events-pipeline"
  description        = "EventBridge -> aml_prepare (weekly file) and aml_bq1_score (DataPrepared)"
  assume_role_policy = file("${local.policy_dir}/aml_events_trust.json")
}

resource "aws_iam_role_policy" "events_pipeline" {
  name   = "aml-events-pipeline"
  role   = aws_iam_role.events_pipeline.id
  policy = file("${local.policy_dir}/aml_events_role_policy.json")
}

# ---------------------------------------------------------------- state machines
resource "aws_sfn_state_machine" "prepare" {
  name       = "aml_prepare"
  role_arn   = aws_iam_role.sfn_pipeline.arn
  type       = "STANDARD" # .sync waits on EMR jobs: Express does not support it
  definition = file("${local.sfn_dir}/aml_prepare.asl.json")
}

resource "aws_sfn_state_machine" "bq1_score" {
  name       = "aml_bq1_score"
  role_arn   = aws_iam_role.sfn_pipeline.arn
  type       = "STANDARD"
  definition = file("${local.sfn_dir}/aml_bq1_score.asl.json")
}

resource "aws_sfn_state_machine" "bq1_train" {
  name       = "aml_bq1_train"
  role_arn   = aws_iam_role.sfn_pipeline.arn
  type       = "STANDARD"
  definition = file("${local.sfn_dir}/aml_bq1_train.asl.json")
}

# ---------------------------------------------------------------- triggers
# One prepare run per weekly file: only the _SUCCESS marker that replay_week.py writes last matches.
resource "aws_cloudwatch_event_rule" "prepare_weekly_file" {
  name          = "aml-prepare-weekly-file"
  description   = "A weekly bank file finished landing in raw/transactions/set=<dataset>/week=<Monday>/: start aml_prepare (it emits DataPrepared)"
  event_pattern = file("${local.rules_dir}/week_arrived.json")
}

resource "aws_cloudwatch_event_target" "prepare_weekly_file" {
  rule      = aws_cloudwatch_event_rule.prepare_weekly_file.name
  target_id = "aml-prepare"
  arn       = aws_sfn_state_machine.prepare.arn
  role_arn  = aws_iam_role.events_pipeline.arn
  input_transformer { # aml_prepare reads detail.object.key (set=<dataset>/week=<Monday>)
    input_paths    = { key = "$.detail.object.key" }
    input_template = "{\"detail\": {\"object\": {\"key\": <key>}}}"
  }
}

resource "aws_cloudwatch_event_rule" "bq1_data_prepared" {
  name          = "aml-bq1-data-prepared"
  description   = "aml_prepare finished an HI-*-live week with features: start aml_bq1_score (SAML weeks go to BQ2, never here)"
  event_pattern = file("${local.rules_dir}/data_prepared.json")
}

resource "aws_cloudwatch_event_target" "bq1_data_prepared" {
  rule      = aws_cloudwatch_event_rule.bq1_data_prepared.name
  target_id = "aml-bq1-score"
  arn       = aws_sfn_state_machine.bq1_score.arn
  role_arn  = aws_iam_role.events_pipeline.arn
}

# ---------------------------------------------------------------- failure emails
# Step Functions reports every run's end state to EventBridge ("Step Functions Execution Status Change").
# aml-bq1-run-ended emails the team when an aml_prepare or aml_bq1_score run fails, times out or is stopped.
# (aml_bq1_score's own WeekCheck alerts go to aml-bq1-alerts below.)

variable "bq1_notify_emails" {
  type        = list(string)
  default     = ["is459g1t4@gmail.com"]
  description = "Who is emailed when an aml_prepare or aml_bq1_score run fails, times out or is stopped. Each address must confirm the AWS email once."
}

resource "aws_sns_topic" "bq1_alerts" {
  name = "aml-bq1-weekly-alerts"
}

resource "aws_sns_topic_policy" "bq1_alerts" {
  arn = aws_sns_topic.bq1_alerts.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "EventBridgePublishesRunFailures"
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sns:Publish"
      Resource  = aws_sns_topic.bq1_alerts.arn
      Condition = { ArnEquals = { "aws:SourceArn" = aws_cloudwatch_event_rule.bq1_run_ended.arn } }
    }]
  })
}

resource "aws_sns_topic_subscription" "bq1_alerts_email" {
  for_each  = toset(var.bq1_notify_emails)
  topic_arn = aws_sns_topic.bq1_alerts.arn
  protocol  = "email"
  endpoint  = each.value
}

resource "aws_cloudwatch_event_rule" "bq1_run_ended" {
  name        = "aml-bq1-run-ended"
  description = "An aml_prepare or aml_bq1_score run failed, timed out or was aborted: email the team (aml-bq1-weekly-alerts)"
  event_pattern = file("${local.rules_dir}/run_failed.json")
}

resource "aws_cloudwatch_event_target" "bq1_run_ended_email" {
  rule = aws_cloudwatch_event_rule.bq1_run_ended.name
  arn  = aws_sns_topic.bq1_alerts.arn
  input_transformer {
    input_paths = {
      arn     = "$.detail.executionArn"
      machine = "$.detail.stateMachineArn"
      name    = "$.detail.name"
      status  = "$.detail.status"
    }
    input_template = "\"AML pipeline run <status>: <name> (<machine>). Run logs: aml_prepare -> s3://${var.bucket}/progress/prepare/{dataset}/{week}.json, aml_bq1_score -> s3://${var.bucket}/progress/bq1/score/<name>.json. Console (input, error, step): https://${var.region}.console.aws.amazon.com/states/home?region=${var.region}#/v2/executions/details/<arn>\""
  }
}

# aml-bq1-alerts: the weekly check's emails from inside aml_bq1_score (capacity / precision / gate), made with
# the CLI on 2026-11-08. Only the topic is managed; its email subscriptions are left as they are.
resource "aws_sns_topic" "bq1_check_alerts" {
  name = "aml-bq1-alerts"
}

# ---------------------------------------------------------------- adopt what was built with the CLI
import {
  to = aws_iam_role.sfn_pipeline
  id = "aml-sfn-pipeline"
}
import {
  to = aws_iam_role_policy.sfn_pipeline
  id = "aml-sfn-pipeline:aml-sfn-pipeline"
}
import {
  to = aws_iam_role.events_pipeline
  id = "aml-events-pipeline"
}
import {
  to = aws_iam_role_policy.events_pipeline
  id = "aml-events-pipeline:aml-events-pipeline"
}
import {
  to = aws_sfn_state_machine.prepare
  id = "arn:aws:states:${var.region}:${local.account_id}:stateMachine:aml_prepare"
}
import {
  to = aws_sfn_state_machine.bq1_score
  id = "arn:aws:states:${var.region}:${local.account_id}:stateMachine:aml_bq1_score"
}
import {
  to = aws_sfn_state_machine.bq1_train
  id = "arn:aws:states:${var.region}:${local.account_id}:stateMachine:aml_bq1_train"
}
import {
  to = aws_cloudwatch_event_rule.prepare_weekly_file
  id = "aml-prepare-weekly-file"
}
import {
  to = aws_cloudwatch_event_target.prepare_weekly_file
  id = "aml-prepare-weekly-file/aml-prepare"
}
import {
  to = aws_cloudwatch_event_rule.bq1_data_prepared
  id = "aml-bq1-data-prepared"
}
import {
  to = aws_cloudwatch_event_target.bq1_data_prepared
  id = "aml-bq1-data-prepared/aml-bq1-score"
}
import {
  to = aws_sns_topic.bq1_check_alerts
  id = "arn:aws:sns:${var.region}:${local.account_id}:aml-bq1-alerts"
}
