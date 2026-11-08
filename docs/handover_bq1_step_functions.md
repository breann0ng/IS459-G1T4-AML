# BQ1 Step Functions: handover from En Rui to Breann (8 Nov 2026)

Branch: `enrui-bq1-sfn-precision` (pushed). Nothing from it is deployed in the shared account yet.

## What is ready

| Piece | Where | State |
|---|---|---|
| **Daily mode in the jobs** | `aws/jobs/clean/clean_transactions_spark.py`, the 3 feature builders, `aws/jobs/bq1/score_final_spark.py`, `aws/jobs/bq1/final_group_namer.py` (+ one optional argument in `scheme_stage2.write_results`) | Written, compiled. **Not run on EMR yet.** Off unless `AML_RUN_DATE` is set, so normal runs are unchanged. |
| State machine (daily) | `aws/step-functions/aml_daily.json` | Passed AWS `ValidateStateMachineDefinition`; input parsing, Spark settings and run-log SQL passed `TestState`. |
| Trigger, role policy | `aws/step-functions/aml_daily_trigger.json`, `aml_daily_role_policy.json` | Trigger checked with `TestEventPattern` |
| Run log | `aws/sql/run-log/*.sql`, `aws/sql/ddl/create_pipeline_runs.sql` | SQL checked through `TestState` only |
| Day splitter | `aws/jobs/load/split_days_spark.py` | Not run |
| Laptop scripts (no Terraform, no main) | `local/runs/deploy_branch.py`, `local/runs/deploy_aml_daily.py`, `local/runs/run_final_serverless.py` (`--code-prefix`, `--run-name`, `--stage-days`), `local/simulate/replay_day.py` | Dry runs / plan only |
| Two-queue precision view | `local/reports/final_report.py` (`queues`), `app/app_bq1.py` | Done |

## How daily mode works (one switch: `AML_RUN_DATE=YYYY-MM-DD`)

| Job | With `AML_RUN_DATE` |
|---|---|
| clean | Reads `raw/transactions/set=<dataset>/date=<Monday..day>/*.csv`, rewrites only that week. Week numbers come from week 1 of the existing `curated/transactions` **for the same dataset name**. |
| features v2 / payment / v3 | Builds Monday..day of that week, with the usual 21-day look-back |
| score | Scores only that day; writes `analytics/live_*` with a `run_date` partition (never `final_*`) |
| name | Names that day's alerts, reading earlier `live_alert_rows` days for group links; writes `analytics/live_alerts_named` |

Your `aml_bq1_weekly` already passes `AML_RUN_DATE` / `AML_RUN_TO`, so the two designs are close: a weekly run could call these jobs once per day, or the jobs could take a range.

## Naming: mine vs yours

| | Mine (branch) | Yours (shared account) | What to do |
|---|---|---|---|
| State machine | `aml-daily` | `aml_bq1_weekly` | Keep yours; take what helps from `aml_daily.json` |
| Code in S3 | `dev/enrui/jobs/` (`deploy_branch.py`) | `jobs/` / `dev/breann/jobs/` | `deploy_branch.py --prefix <yours>` uploads this branch anywhere except `jobs/` |
| Model folder | `models/<run>/` via `AML_FINAL_RUN` (`enrui`) | `models/final/` (only `meta.json` today) | See "Model" below |
| Live dataset name | same name as the full clean (`HI-Medium`) | `HI-Medium-live`, `HI-Large-live` | **Must match a dataset already in `curated/transactions`**, or daily clean stops with "needs the full clean first". Either use `set=HI-Medium/date=…`, or run one full clean of `HI-Medium-live` first. |
| Raw layout | `raw/transactions/set=<ds>/date=YYYY-MM-DD/*.csv` | `staging/…/set=<ds>-live/week=YYYY-MM-DD/` + `_SUCCESS` | Team standard is now one folder per **day**: the daily clean reads `date=` folders |
| Trigger | any `.csv` under `set=*/date=*/` | `week=<Monday>/_SUCCESS` | Each day file starts one run; the run log skips repeats |
| Output tables | `analytics/live_*` (per `run_date`) | — | Dashboard reads `final_*` JSON; `live_*` is for the daily alert list |
| Run log | `analytics.pipeline_runs`, `pipeline = 'aml-daily'` | (check yours) | Same DDL file |

## Model: no training run needed

The shared `models/final/` holds only `meta.json`. The trained final model (stage 1 + group namer, `txn_v3m_xp` + `enrui_breann_mld20_grp`, threshold 0.9763) is in En Rui's personal bucket: `s3://4am-bucket1/models/enrui_breann_mld20/` (`stage1.txt` 14 MB, `namer_group.txt` 22 MB, `meta.json`). Copy the three files into the shared bucket under the model folder you choose (e.g. `models/final/`, after checking BQ2 is fine with `meta.json` being replaced, or `models/bq1/` with `AML_FINAL_RUN=bq1`). En Rui can do the copy (it needs both accounts' credentials).

## Running on HI-Medium instead of HI-Large

HI-Medium is a **training** dataset: the final model learnt from its first 75% of days (`VALIDATION_SHARE = 0.25` in `train_txn.py`). Only replay its **last 25% of days** (the validation days, about 12 to 16 Sep 2022) and say in the demo that these days chose the threshold, so the numbers are slightly optimistic. Earlier HI-Medium days were trained on and must not be presented as results.

Order:
1. Copy the model (above).
2. Upload code: `deploy_branch.py --profile g1t4 --prefix <yours>`.
3. Day files: `run_final_serverless.py --code-prefix <yours> --run-name <model folder> --stage-days HI-Medium 2022-09-12 2022-09-16` (writes `staging/transactions/set=HI-Medium/date=…/`).
4. Deploy: your state machine, or `deploy_aml_daily.py --code-prefix <yours> --run-name <model folder>` (plan first, then `--apply`).
5. Replay Monday first: `replay_day.py HI-Medium 2022-09-12 --profile g1t4`.

## Not tested yet (first EMR run will tell)
- The brace glob `date={…}` in the daily clean's input path
- `run_date` partition filters in the namer (pyarrow reads it as a string)
- The EMR Serverless `.sync` parameter names at run time; Athena `UPDATE` on the Iceberg run-log table
