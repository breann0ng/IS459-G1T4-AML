output "emr_application_id" { value = aws_emrserverless_application.jobs.id }
output "emr_job_role_arn" { value = aws_iam_role.emr_job.arn }
output "athena_workgroup" { value = aws_athena_workgroup.aml.name }
output "glue_databases" { value = [for d in aws_glue_catalog_database.layer : d.name] }
output "bq1_emr_application_id" { value = aws_emrserverless_application.bq1.id }
output "bq1_test_emr_application_id" { value = aws_emrserverless_application.bq1_test.id }
output "bq1_state_machine_arns" {
  value = {
    prepare = aws_sfn_state_machine.prepare.arn
    score   = aws_sfn_state_machine.bq1_score.arn
    train   = aws_sfn_state_machine.bq1_train.arn
  }
}
output "bq1_event_rules" { value = [aws_cloudwatch_event_rule.prepare_weekly_file.name, aws_cloudwatch_event_rule.bq1_data_prepared.name] }
output "bq1_alerts_topic_arn" { value = aws_sns_topic.bq1_alerts.arn }
