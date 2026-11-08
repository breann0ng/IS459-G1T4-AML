# Step Functions

Two weekly state machines. Each step is one EMR Serverless job (`emr-serverless:startJobRun.sync`):
Step Functions starts it, waits for it, retries it, and logs a failure. The jobs themselves are
the scripts in `aws/jobs/` (uploaded to `s3://<bucket>/jobs/` by `aws/deploy/deploy.sh`); a state
machine only names the S3 path and the arguments, so the two folders are linked by S3 paths only.

| State machine | Started by | Does |
|---|---|---|
| `aml_bq1_weekly.asl.json` (BQ1, **deployed**) | EventBridge **rule**: a weekly bank file finished landing in `raw/transactions/set=<dataset>/week=<Monday>/` (its `_SUCCESS` marker) | clean -> patterns -> features v2 -> txn -> v3 -> score -> name patterns |
| `aml_bq2_weekly.json` (BQ2, **deployed**) | EventBridge **Scheduler** `aml-bq2-weekly` (created disabled) | Part 1 + Part 2 score/eval -> alert? -> SNS + SQS approval -> retrain challenger -> approval -> deploy. See "aml_bq2_weekly" below |
| `aml_bq2_weekly_ibm.json` (BQ2, IBM HI-Large, Tasha; **generated** by `build_bq2_weekly_ibm.py`) | By hand, `{"week_index": N}` or the pointer `progress/bq2_weekly_ibm.json` | Same design as `aml_bq2_weekly`, for HI-Large and the fan-in / fan-out new-tactic test (HI-Large-NT): Part 1 x2 | build features -> score both models (each skipped when `progress/bq2/<step>_<week>.done` exists) -> prep -> eval x2 `--as-of` -> same gates (challenger = BQ1 model). 9 scored + 4 label-only weeks. Models in `models/bq2_ibm/`; infra `aws/infra/bq2_weekly_ibm.tf` |
| `aml_daily.json` (BQ1 daily variant, **not deployed**) | EventBridge rule `aml_daily_trigger.json` on day files | See "aml-daily (not deployed)" below |
| `aml_bq1_joey.asl.json` (BQ1, Joey, **in progress**) | By hand (later: Joey's own rule on `_manifest.json`) | Many weeks in one run: clean weeks in parallel -> patterns -> features + score per week in parallel -> name weeks in order. See "aml_bq1_joey" below |

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

## aml_bq1_joey (in progress)

Joey's faster, isolated version of `aml_bq1_weekly`. It runs the same jobs on the same frozen model, so
the features and alerts must come out identical; it only changes how the jobs are ordered and grouped.
It uses its own dataset name (`HI-Medium-joey`), EMR app (`aml-bq1-joey`), role (`aml-sfn-bq1-joey`),
code folder (`dev/joey/jobs/`) and run log (`progress/bq1_joey/`), so it never touches a teammate's run.

```
Input {dataset, first_week, score_from, last_week, code_prefix, app_id, start_at?}
 -> LogStart
 -> CleanWeeks       Map over every week first_week..last_week, 3 at a time      (clean)
      ...all weeks must finish...
 -> ParsePatterns    once, for the whole dataset
 -> FeatureAndScore  Map over the scored weeks score_from..last_week              (build_week_features -> score)
 -> NameWeeks        Map over the scored weeks, 1 at a time, oldest first         (name)
 -> LogSuccess        any failure -> LogFailure -> Fail (no retries)
```

What happens, and why in this order:

1. **No training.** The model was trained once (4 Nov: `train_txn.py` -> `group_stage2.py` ->
   `final_train.py`) and is frozen in `models/bq1/`. This pipeline only predicts. Retraining belongs to
   BQ2 (`aml_bq2_weekly`: retrain challenger -> approval -> deploy).
2. **Cleaning runs in parallel.** Each week's file is cleaned on its own (`week=<Monday>` folder in,
   one `week` partition out), so up to 3 weeks are cleaned at the same time.
3. **Wait, then features + score per week, in parallel.** This step needs the cleaning to have
   finished: a week's features look back 21 days into the cleaned payments of earlier weeks, and they
   take the pattern labels from `curated/scheme_txns`, which ParsePatterns builds from all the cleaned
   weeks. After that the scored weeks are independent: week N reads earlier weeks' cleaned payments,
   never their features (every read of a feature table is cut to the week's own dates). So each week
   runs as its own branch: `build_week_features.py` (features v2 -> txn -> v3 in one Spark
   application) then `score_final_spark.py`, which gives every payment its laundering probability and
   flags those above the threshold (0.9763). Two weeks at once need about 32 vCPU on the app; the Map
   runs 1 at a time until the app's cap is raised.
4. **Naming starts after every week is scored, and runs one week at a time.** The namer groups each
   alert with alerts up to `max_linked_days` (20) days earlier, so a week needs the earlier weeks'
   *scores* (`live_alert_rows`), which are all done by then. It never reads earlier weeks' *names*,
   so the order is not needed for correctness; one at a time is because each namer job takes a 48 GB
   driver and the app's cap is 64 GB.

History weeks (before `score_from`) are only cleaned: they exist so the first scored week has its 21 days
of look-back. Each run writes `progress/bq1_joey/<dataset>/<run>.json`: status, failed step and error, and
each step's start, end and EMR job id (to compare with `aml_bq1_weekly`). `start_at` (e.g. `"name"`) re-runs
only the last part after a fix.
