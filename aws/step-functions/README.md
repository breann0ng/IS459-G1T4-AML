# Step Functions

| File | What |
|---|---|
| `aml_daily.json` | **BQ1 hot path** (written, validated, not deployed yet). One new day of transactions: run log check → clean → features (v2, payment, v3) → score → name patterns → log. |
| `aml_daily_trigger.json` | EventBridge rule pattern: a day file lands in `raw/transactions/set=<dataset>/date=<day>/` |
| `aml_daily_role_policy.json` | What the state machine's IAM role may do (EMR Serverless jobs, pass the job role, Athena run log) |
| `aml_weekly.json` | BQ2 cold path (to write): EventBridge Scheduler → BQ2 monitor → drift? → retrain a challenger → log. A person promotes the model. |

The commands and Spark settings match `local/runs/run_final_serverless.py`; settings in `aws/config/pipeline.yaml`.

## aml-daily, step by step

1. **Read input.** From the S3 event key (`raw/transactions/set=HI-Large/date=2022-09-12/part-….csv`) or by hand: `{"dataset": "HI-Large", "date": "2022-09-12"}`.
2. **Already ran?** `aws/sql/run-log/already_ran.sql` in Athena. If this day is running or done, stop (each file of a day fires an event; only the first one runs).
3. **Log start** (`log_start.sql`), then five EMR Serverless jobs, each started and awaited (`.sync`), 2 retries, and a catch that logs the failure (`log_failure.sql`):

| Step | Job | In daily mode (`AML_RUN_DATE` = the day) |
|---|---|---|
| Clean | `clean_transactions_spark.py` | Cleans the day's week so far (Monday to the day) from the day folders and rewrites only that week. Week numbers come from the full clean. |
| Features v2 | `build_features_spark.py` | Same week so far, reading 21 days of history |
| Payment features | `build_txn_features_spark.py <dataset> features_v2` | Same |
| Features v3 | `build_features_v3_spark.py` | Same |
| Score | `score_final_spark.py` | Scores only the day with `models/final/`; writes `analytics/live_*` (one `run_date` partition per day) |
| Name patterns | `final_group_namer.py` | Names the day's alerts, linking back up to `max_linked_days` days of earlier alerts; writes `analytics/live_alerts_named` |

4. **Log success** (`log_success.sql`).

Every job runs exactly as before when `AML_RUN_DATE` is not set, so the one-off evaluation (`final_*` tables, the dashboard numbers) is untouched by daily runs.

## Deploy (Terraform, next to `aws/infra/`)

- State machine: `templatefile("aml_daily.json", { bucket, emr_app_id, emr_job_role_arn, athena_workgroup })`, type STANDARD, logging level ERROR with execution data, 90-day log group.
- Its role: `aml_daily_role_policy.json` (also needs `region`, `account_id`), trusted by `states.amazonaws.com`.
- EventBridge: turn on **EventBridge notifications** on the bucket (`aws_s3_bucket_notification`, `eventbridge = true`); a rule with `aml_daily_trigger.json`, target = the state machine, with a role allowed to `states:StartExecution`.
- Once in Athena (workgroup `aml`): `aws/sql/ddl/create_pipeline_runs.sql`.

## Before the first daily run

1. The full one-off run has finished (`local/runs/run_final_serverless.py`): `models/final/` and the curated tables of every earlier day exist.
2. Deploy the code from `main`: `aws/deploy/deploy.sh`.
3. Stage the days to replay, once: `split_days_spark.py HI-Large 2022-09-12 2022-09-25` (one CSV per day in `staging/`).
4. Replay a day: `python local/simulate/replay_day.py HI-Large 2022-09-12 --bucket is459-g1t4-aml`. The file lands in `raw/`, EventBridge starts aml-daily. Replay days in order, starting on a Monday.

Notes
- A daily run rewrites the curated tables of the replayed week from the replayed days. To rebuild the full evaluation afterwards, run the full clean and features again (`run_final_serverless.py`).
- The full clean (no `AML_RUN_DATE`) reads every file under `raw/transactions/set=<dataset>/`, replayed day folders included; exact duplicates are dropped, so the result is the same.
