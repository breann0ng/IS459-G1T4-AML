# Athena SQL

| Folder | What | Run by |
|---|---|---|
| `ddl/` | Glue databases + the `pipeline_runs` run-log table | once (Terraform or Athena console) |
| `views/` | `case_queue`, `bq1_daily`, `bq2_weekly`, `pipeline_health`: what the Streamlit pages read | Athena, read by `app/` |
| `run-log/` | `log_start`, `log_success`, `log_failure`, `last_success` | Step Functions states |
| `maintenance/` | Iceberg `OPTIMIZE` (compaction) + `VACUUM` (snapshot expiry) | `aml-weekly` |

`views/`, `run-log/` and `maintenance/` are still to write (person 5). Query results go to
`s3://<bucket>/athena-results/` (7-day lifecycle).
