# Step Functions

- `aml_bq2_weekly.json` (**generated**, do not hand-edit): the BQ2 weekly run, one simulated week per execution.
  Rebuild with `python aws/step-functions/build_bq2_weekly.py --bucket ... --app-id ... --role-arn ... --topic-arn ... --queue-url ...`.
  Flow: pick week (input `{"week_index": N}`, else pointer `progress/bq2_weekly.json`) -> parallel
  [Part 1 drift | score deployed model -> eval `--as-of` week end] -> read `models/bq2r/<ds>/decision_latest.json`
  -> no alert: advance pointer. Alert: SNS email -> **gate 1** (SQS task token, 7-day timeout) -> train challenger
  -> score -> eval -> SNS email -> **gate 2** -> `bq2_deploy.py` (writes `deployed.json`) -> advance pointer.
  Reject or timeout skips to the pointer advance. No automatic retries (each retry costs EMR money).
  Infra for it: `aws/infra/bq2_weekly.tf` (schedule is created DISABLED).
- `aml_daily.json`: hot path, started by the EventBridge rule on a new `raw/transactions/` file.
  read run log -> stop if running/done -> log start -> clean -> features (v2, txn, v3) -> score ->
  name groups -> log success. Each EMR Serverless step: 2 retries, catch -> log failure.
  Logging: CloudWatch, level ERROR, include execution data, 90-day retention.
- `aml_weekly.json`: cold path, EventBridge Scheduler. BQ2 monitor -> drift? -> validate (train_txn) ->
  final_train (registers a challenger) -> log. A person promotes the model.

The commands each step runs are in `aws/config/pipeline.yaml`.
