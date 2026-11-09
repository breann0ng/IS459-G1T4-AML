-- Run log: one row per Step Functions run. Replaces the S3 .done markers.
-- Next dates to process = the day after max(date_to) WHERE status = 'SUCCEEDED'.
CREATE TABLE IF NOT EXISTS analytics.pipeline_runs (
  run_id           string,     -- Step Functions execution name
  pipeline         string,     -- aml-daily | aml-weekly
  dataset          string,
  date_from        date,
  date_to          date,
  status           string,     -- RUNNING | SUCCEEDED | FAILED
  started_at       timestamp,
  finished_at      timestamp,
  rows_cleaned     bigint,
  alerts           bigint,
  model_version    string,     -- MLflow model version used for scoring
  vcpu_hours       double,     -- billed by EMR Serverless (cost per run)
  memory_gb_hours  double,
  snapshot_ids     string,     -- Iceberg snapshots written (time travel / audit)
  error            string
)
LOCATION 's3://<bucket>/analytics/pipeline_runs/'
TBLPROPERTIES ('table_type' = 'ICEBERG');
