# Orchestration: state machines + EventBridge rules

How the datasets flow through them: [`docs/pipeline.md`](../../docs/pipeline.md).
Everything here is deployed by Terraform ([`aws/infra/orchestration.tf`](../infra/orchestration.tf),
[`aws/infra/bq2_weekly.tf`](../infra/bq2_weekly.tf)), which reads these files as they are. They carry the
account (390402569482), bucket (`is459-g1t4-aml`) and EMR app ids literally, the same text the console shows.

```
state-machines/   one .asl.json per Step Functions state machine
eventbridge/      one event pattern per EventBridge rule (Terraform adds the target)
examples/         example inputs for starting a machine by hand
```

## The weekly flow

```
replay_week.py ("the bank") copies one week from staging/ to raw/, then writes _SUCCESS last
        │
        ▼  eventbridge/week_arrived.json          rule aml-prepare-weekly-file
   aml_prepare        Clean → ParsePatterns → FeaturesV2 → FeaturesTxn → FeaturesV3
        │             marker progress/prepare/<dataset>/<week>.json; emits DataPrepared
        ▼  eventbridge/data_prepared.json         rule aml-bq1-data-prepared  (HI-*-live only)
   aml_bq1_score      Score → Name → WeekCheck → Summary
                      SAML weeks are picked up by aml_bq2_weekly instead (its own schedule)

any aml_prepare / aml_bq1_score run FAILED, TIMED_OUT or ABORTED
        ▼  eventbridge/run_failed.json            rule aml-bq1-run-ended  → SNS aml-bq1-weekly-alerts (email)
```

Only the training sets (HI-Small, HI-Medium) are prepared whole, by `aml_bq1_train`. HI-Large and SAML
arrive one week at a time, like a bank feed: `aml_prepare` refuses them without a week.

## State machines

| File | Machine | Started by | Status |
|---|---|---|---|
| `aml_prepare.asl.json` | `aml_prepare` | rule `aml-prepare-weekly-file`; `aml_bq1_train`, `aml_bq1_score`, `aml_bq2_weekly` | Live version is backed up at `s3://is459-g1t4-aml/backup/sfn/20261109T0800-aml_prepare.asl.json`; this file adds weekly-only HI-Large/SAML, first-week-of-feed and `*-demo` → aml-bq1-dev (9 Nov) |
| `aml_bq1_train.asl.json` | `aml_bq1_train` | by hand (`examples/bq1_train.json`) | Live. Champion rolled back on 9 Nov to `bq1` (better on HI-Large-live 2022-09-26: precision 0.571 vs 0.478, F1 0.550 vs 0.504) |
| `aml_bq1_score.asl.json` | `aml_bq1_score` | rule `aml-bq1-data-prepared`, or by hand | Live. Tested end to end on HI-Large-live |
| `aml_bq2_weekly.asl.json` | `aml_bq2_weekly` | weekly schedule (BQ2 team) | Live definition is deployed by the BQ2 team. This file passes the week to `aml_prepare` |
| `aml_bq2_weekly_ibm.asl.json` | `aml_bq2_weekly_ibm` | BQ2 team | Not deployed yet; runs the shared `jobs/` code |

## EventBridge rules

| File | Rule | Matches | Target |
|---|---|---|---|
| `week_arrived.json` | `aml-prepare-weekly-file` | S3 `Object Created` for `raw/transactions/*/week=*/_SUCCESS` (only the marker, not each part file) | `aml_prepare` (dataset and week are read from the key) |
| `data_prepared.json` | `aml-bq1-data-prepared` | `DataPrepared` from `aml_prepare` for an `HI-*-live` week with features | `aml_bq1_score` |
| `run_failed.json` | `aml-bq1-run-ended` | an `aml_prepare` or `aml_bq1_score` run ends FAILED, TIMED_OUT or ABORTED | SNS `aml-bq1-weekly-alerts` (is459g1t4@gmail.com) |

## Deploy

```bash
cd aws/infra && terraform plan && terraform apply        # machines, rules, targets, roles
```

One machine only, without Terraform:

```bash
aws stepfunctions update-state-machine \
  --state-machine-arn arn:aws:states:us-east-1:390402569482:stateMachine:aml_prepare \
  --definition file://aws/orchestration/state-machines/aml_prepare.asl.json
```

`aml_bq1_score_v2` and `aml_bq2_dryrun` also exist in the account. They are teammates' tests and are not
part of this pipeline.
