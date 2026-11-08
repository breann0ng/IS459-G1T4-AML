# State machines

How the datasets flow through them: [`docs/pipeline.md`](../../docs/pipeline.md).
All are JSONata or JSONPath definitions with the account (390402569482), bucket (`is459-g1t4-aml`)
and EMR apps written in. Edit the JSON here, then redeploy it with the command at the bottom.

| File | State machine | Status (8 Nov 2026) |
|---|---|---|
| `aml_prepare.asl.json` | `aml_prepare` | **Live**, identical to AWS. SAML-R/SAML-C input tested (route + clean) |
| `aml_bq1_train.asl.json` | `aml_bq1_train` | **Live**, identical to AWS. Last run promoted `bq1-20261108-1332`; rolled back on 9 Nov to `bq1` (better on HI-Large-live 2022-09-26: account-day precision 0.571 vs 0.478, F1 0.550 vs 0.504, same recall). Old pointer: `s3://is459-g1t4-aml/backup/champion/` |
| `aml_bq1_score.asl.json` | `aml_bq1_score` | **Live**, identical to AWS. Tested end to end on HI-Large-live 2022-09-26 (9 Nov): score 10.5 min, name 4 min, check + summary 3.5 min |
| `aml_bq2_weekly.asl.json` | `aml_bq2_weekly` | **Not deployed yet.** Same as AWS except the 9 feature states are replaced by one `PrepareData` call to `aml_prepare`. Needs `iam/aml_sfn_bq2_prepare_policy.json` on role `aml-sfn-bq2` first |
| `aml_bq2_weekly_ibm.asl.json` | `aml_bq2_weekly_ibm` | **Not deployed yet.** Same as AWS except the code path is the shared `jobs/` instead of the `bq2_ibm/jobs/` copy |
| `legacy/aml_bq1_weekly.asl.json` | `aml_bq1_weekly` | Live and managed by Terraform (`aws/infra/orchestration.tf`), but no longer started: its S3 rule `aml-bq1-weekly-file` is disabled. Replaced by `aml_prepare` + `aml_bq1_score` (rules below) |

Both "not deployed" files pass `ValidateStateMachineDefinition`. Neither has been run. Deploy them
together with the BQ2 team and test `aml_bq2_weekly` on one SAML week first.

## Folders

- `iam/`:
  - `aml_pipeline_*`: role `aml-sfn-pipeline` (aml_prepare, aml_bq1_train, aml_bq1_score).
  - `aml_events_*`: role `aml-events-pipeline` (EventBridge -> aml_prepare and aml_bq1_score).
  - `aml_sfn_bq2_prepare_policy.json`: lets `aml-sfn-bq2` start `aml_prepare` and wait for it.
- `inputs/`: example inputs. File names start with the machine they are for (`prepare_*`, `bq1_*`).
  - `champion_bootstrap.json`: the first `models/bq1_champion.json`.
  - `eventbridge/`: the two rules of the automatic weekly flow:
    1. `raw/transactions/set=<dataset>/week=<Monday>/_SUCCESS` -> `aml_prepare` (which emits `DataPrepared`).
    2. `DataPrepared` for a `*-live` dataset -> `aml_bq1_score`.
    3. `aml-bq1-run-ended` (`rule_run_ended.json` / `targets_run_ended.json`): an `aml_prepare` or
       `aml_bq1_score` run FAILED / TIMED_OUT / ABORTED -> email via SNS `aml-bq1-weekly-alerts`
       (is459g1t4@gmail.com). Live since 9 Nov; tested with a BadInput run (delivered). The message
       leaves out the run input: inserting that JSON into the text made every delivery fail.

## Switching the weekly trigger to the new flow (done, 9 Nov 2026)

Live: `aml-prepare-weekly-file` and `aml-bq1-data-prepared` are enabled, the legacy `aml-bq1-weekly-file`
is disabled. S3-started runs use the code in `s3://is459-g1t4-aml/jobs/` (the event carries no
`code_prefix`), so `jobs/` must hold `main`'s code. These were the commands:

```bash
aws events put-rule --name aml-prepare-weekly-file --event-pattern file://aws/step-functions/inputs/eventbridge/rule_weekly_file.json
aws events put-targets --rule aml-prepare-weekly-file --targets file://aws/step-functions/inputs/eventbridge/targets_weekly_file.json
aws events put-rule --name aml-bq1-data-prepared --event-pattern file://aws/step-functions/inputs/eventbridge/rule_data_prepared.json
aws events put-targets --rule aml-bq1-data-prepared --targets file://aws/step-functions/inputs/eventbridge/targets_data_prepared.json
aws events disable-rule --name aml-bq1-weekly-file          # the legacy rule (Terraform): or remove it there
```

## Redeploy one machine

```bash
aws stepfunctions update-state-machine \
  --state-machine-arn arn:aws:states:us-east-1:390402569482:stateMachine:aml_prepare \
  --definition file://aws/step-functions/aml_prepare.asl.json
```

`aml_bq1_score_v2` and `aml_bq2_dryrun` also exist in the account. They are teammates' tests and
are not part of this pipeline.
