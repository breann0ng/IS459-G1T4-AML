output "emr_application_id" { value = aws_emrserverless_application.jobs.id }
output "emr_job_role_arn" { value = aws_iam_role.emr_job.arn }
output "athena_workgroup" { value = aws_athena_workgroup.aml.name }
output "glue_databases" { value = [for d in aws_glue_catalog_database.layer : d.name] }
