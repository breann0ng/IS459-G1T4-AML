# Step Functions (to write)

- `aml_daily.json`: hot path, started by the EventBridge rule on a new `raw/transactions/` file.
  read run log -> stop if running/done -> log start -> clean -> features (v2, txn, v3) -> score ->
  name groups -> log success. Each EMR Serverless step: 2 retries, catch -> log failure.
  Logging: CloudWatch, level ERROR, include execution data, 90-day retention.
- `aml_weekly.json`: cold path, EventBridge Scheduler. BQ2 monitor -> drift? -> validate (train_txn) ->
  final_train (registers a challenger) -> log. A person promotes the model.

The commands each step runs are in `aws/config/pipeline.yaml`.
