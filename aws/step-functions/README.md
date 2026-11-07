# Step Functions

Two weekly state machines. Each step is one EMR Serverless job (`emr-serverless:startJobRun.sync`):
Step Functions starts it, waits for it, retries it twice, and logs a failure. The jobs themselves are
the scripts in `aws/jobs/` (uploaded to `s3://<bucket>/jobs/` by `aws/deploy/deploy.sh`); a state
machine only names the S3 path and the arguments, so the two folders are linked by S3 paths only.

| State machine | Started by | Does |
|---|---|---|
| `aml_bq1_weekly.asl.json` (BQ1) | EventBridge **rule**: a weekly bank file finished landing in `raw/transactions/set=<dataset>/week=<Monday>/` (its `_SUCCESS` marker) | clean -> patterns -> features v2 -> txn -> v3 -> score -> name patterns |
| `aml_bq2_weekly` (BQ2) | EventBridge **Scheduler** `aml-bq2-weekly` (disabled for the demo) | Part 1 + Part 2 score/eval -> alert? -> SNS + SQS approval -> retrain challenger -> approval -> deploy. Built in the console; **not in the repo yet** |

Terraform (`aws/infra/orchestration.tf`) creates the BQ1 state machine, its role, its CloudWatch log
group (ERROR, 90 days), the S3 -> EventBridge switch and the rule. `${bucket}`, `${application_id}`
and `${job_role_arn}` in the `.asl.json` are filled in by `templatefile`.

## aml_bq1_weekly

```
ParseInput -> ValidInput? -> Setup -> ReadRunLog -> AlreadyDone? --(running/done)--> Skip
  -> FirstWeek? / ReadPrevWeekLog -> PrevWeekDone? --(no)--> PrevWeekNotDone (fail)
  -> LogStart -> Clean -> ParsePatterns -> HistoryOnly? --(yes)--> LogSuccess
  -> FeaturesV2 -> TxnFeatures -> FeaturesV3 -> Score -> NamePatterns -> LogSuccess
every job: 2 retries, then LogFailure -> Failed
```

* **Input**: the S3 event (the key gives dataset + week), or by hand
  `{"dataset": "HI-Large-live", "week_start": "2022-09-12"}`; add `"force": true` to rerun a finished week.
  Weeks are Mondays; the Sunday and the previous week are computed (JSONata).
* **Run log**: `s3://<bucket>/progress/bq1_weekly/<dataset>/<week>.json` with `status`
  RUNNING / SUCCEEDED / FAILED (+ failed step and error). A second event for the same week is skipped.
* **Order**: a week's features read the 21 days before it, so a week only runs when the previous week
  SUCCEEDED (except the dataset's `first_week`). Replay weeks one at a time (`replay_week.py --wait`).
* **History weeks** (ending before `score_from`) are only cleaned: they exist so the first scored week
  has its look-back. Settings per dataset are in `ParseInput` -> `datasets`.
* **Score** runs from `score_from` to this Sunday (the weeks so far) because `score_final_spark.py`
  replaces the dataset's `analytics/final_*` tables; the namer then names every alert so far.
* `HI-Large-live` is a weekly replay of HI-Large under its own dataset name, so it never overwrites the
  batch `HI-Large` results (BQ1 dashboard, Tasha's BQ2 HI-Large tab).

## Setup and demo (once the jobs are deployed and Terraform applied)

Needs `models/final/stage1.txt` and `models/final/namer_group.txt` (only `meta.json` is in S3 now).

```bash
B=is459-g1t4-aml
# 1. once: split the Kaggle file into weekly files (EMR Serverless job, ~3 history + 2 test weeks)
#    split_weeks_spark.py HI-Large HI-Large-live 2022-08-22 2022-09-19
# 2. once: the live dataset's answer key and accounts file
aws s3 cp s3://$B/raw/patterns/set=HI-Large/HI-Large_Patterns.txt s3://$B/raw/patterns/set=HI-Large-live/
aws s3 cp s3://$B/raw/accounts/set=HI-Large/HI-Large_accounts.csv s3://$B/raw/accounts/set=HI-Large-live/
# 3. history weeks (clean only), in order
for w in 2022-08-22 2022-08-29 2022-09-05; do python local/simulate/replay_week.py HI-Large-live $w --wait; done
# 4. demo: each replay starts one aml_bq1_weekly run (watch it in the Step Functions console)
python local/simulate/replay_week.py HI-Large-live 2022-09-12
python local/simulate/replay_week.py HI-Large-live 2022-09-19
```
