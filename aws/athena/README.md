# Athena SQL

| File | What | Run by |
|---|---|---|
| `ddl/create_bq1_live_tables.sql` | External tables over `analytics/live_*`: the weekly BQ1 results the dashboard reads | once, in Athena workgroup `aml` |
| `ddl/create_pipeline_runs.sql` | `pipeline_runs` run-log table | once, in Athena workgroup `aml` |

The Glue databases themselves come from `aws/infra/glue_athena.tf`. Query results go to
`s3://<bucket>/athena-results/` (7-day lifecycle).
