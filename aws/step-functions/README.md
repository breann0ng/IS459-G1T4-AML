# Step Functions

Two weekly state machines. Each step is one EMR Serverless job (`emr-serverless:startJobRun.sync`):
Step Functions starts it, waits for it, retries it, and logs a failure. The jobs themselves are
the scripts in `aws/jobs/` (uploaded to `s3://<bucket>/jobs/` by `aws/deploy/deploy.sh`); a state
machine only names the S3 path and the arguments, so the two folders are linked by S3 paths only.

| State machine | Started by | Does |
|---|---|---|
| `aml_bq1_weekly.asl.json` (BQ1, **deployed**) | EventBridge **rule**: a weekly bank file finished landing in `raw/transactions/set=<dataset>/week=<Monday>/` (its `_SUCCESS` marker) | clean -> patterns -> features v2 -> txn -> v3 -> score -> name patterns |
| `aml_bq2_weekly.json` (BQ2, **deployed**) | EventBridge **Scheduler** `aml-bq2-weekly` (created disabled) | Part 1 + Part 2 score/eval -> alert? -> SNS + SQS approval -> retrain challenger -> approval -> deploy. See "aml_bq2_weekly" below |
| `aml_daily.json` (BQ1 daily variant, **not deployed**) | EventBridge rule `aml_daily_trigger.json` on day files | See "aml-daily (not deployed)" below |

Terraform: `aws/infra/orchestration.tf` (BQ1) and `aws/infra/bq2_weekly.tf` (BQ2). The BQ1 file creates the weekly state machine, its role, its CloudWatch
log group (ERROR, 90 days), the S3 -> EventBridge switch and the rule. `${bucket}`, `${application_id}`
and `${job_role_arn}` in the `.asl.json` are filled in by `templatefile`.

## aml_bq1_weekly

```
ParseInput -> ValidInput? -> Setup -> ReadRunLog -> AlreadyDone? --(running/done)--> Skip
  -> FirstWeek? / ReadPrevWeekLog -> PrevWeekDone? --(no)--> PrevWeekNotDone (fail)
  -> LogStart -> Clean -> ParsePatterns -> HistoryOnly? --(yes)--> LogSuccess
  -> FeaturesV2 -> TxnFeatures -> FeaturesV3 -> CheckModel -> ModelReady? --(no)--> LogSuccess
  -> Score -> NamePatterns -> LogSuccess
every job: 3 retries after 5, 10, 20 min (rides out other jobs holding the shared EMR app), then LogFailure -> Failed
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
* **Score and name** run on this week only, with `AML_RUN_DATE` = the Monday (and, for the namer,
  `AML_RUN_TO` = the Sunday): results go to `analytics/live_*`, one `run_date` partition per week. The
  namer still links back up to `max_linked_days` of earlier alerts. The full-evaluation `final_*` tables
  (and so the dashboard) are never touched. Both are skipped until `models/final/stage1.txt` and
  `namer_group.txt` exist (`CheckModel`).
* `HI-Large-live` is a weekly replay of HI-Large under its own dataset name, so it never overwrites the
  batch `HI-Large` results (BQ1 dashboard, Tasha's BQ2 HI-Large tab).

## Setup and demo (once the jobs are deployed and Terraform applied)

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

## aml-daily (not deployed)

En Rui's daily variant of BQ1: `aml_daily.json`, `aml_daily_trigger.json`, `aml_daily_role_policy.json`,
deployed by `local/runs/deploy_aml_daily.py` (boto3, not Terraform), replayed with
`local/simulate/replay_day.py` after `aws/jobs/load/split_days_spark.py`. One day file -> the day's week
so far is re-cleaned and re-featured (`AML_RUN_DATE`), the day is scored into `live_*`; the run log is the
Athena table `analytics.pipeline_runs` (`aws/sql/run-log/*.sql`). It works on `HI-Large` itself, so it
needs the full HI-Large clean first and rewrites HI-Large's curated weeks.

The team runs BQ1 **weekly** (`aml_bq1_weekly`): one run per weekly file instead of one per day avoids
~6 EMR job start-ups a day and re-cleaning the week so far every day. The jobs keep both modes (a week
argument for weekly, `AML_RUN_DATE` for daily; neither = the whole dataset), so the files above still work.

## aml_bq2_weekly

`aml_bq2_weekly.json` (**generated**, do not hand-edit): the BQ2 weekly run, one simulated week per execution.
  Rebuild with `python aws/step-functions/build_bq2_weekly.py --bucket ... --app-id ... --role-arn ... --topic-arn ... --queue-url ...`.
  Flow: pick week (input `{"week_index": N}`, else pointer `progress/bq2_weekly.json`) -> parallel
  [Part 1 drift | score deployed model -> eval `--as-of` week end] -> read `models/bq2r/<ds>/decision_latest.json`
  -> no alert: advance pointer. Alert: SNS email -> **gate 1** (SQS task token, 7-day timeout) -> train challenger
  -> score -> eval -> SNS email -> **gate 2** -> `bq2_deploy.py` (writes `deployed.json`) -> advance pointer.
  Reject or timeout skips to the pointer advance. No automatic retries (each retry costs EMR money).
  Infra for it: `aws/infra/bq2_weekly.tf` (schedule is created DISABLED).
