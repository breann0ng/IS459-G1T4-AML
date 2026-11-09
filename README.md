# IS459 G1T4 · Anti-money-laundering pipeline (BQ1 + BQ2)

- **BQ1:** detect laundering early while keeping false alerts manageable (LightGBM stage 1 + group namer stage 2).
- **BQ2:** are tactics changing, does detection fall, and can retraining recover it?

Details and results: [`docs/bq1.md`](docs/bq1.md) · [`docs/bq2.md`](docs/bq2.md).

## Repository layout: grouped by where things run

```
aws/                      ★ DEPLOYED TO AWS
  jobs/                     code -> s3://<bucket>/jobs/ (one flat folder + jobs.zip), runs on EMR Serverless
    setup/                    one-off, before a weekly feed starts: load_kaggle.sh · saml_ramp_spark.py
                              (SAML-D -> IBM layout) · split_weeks_spark.py (whole file -> weekly files in staging/)
    prepare/                  the aml_prepare steps: clean · parse_patterns_spark · 3 feature builders
    bq1/                      train_txn · group_stage2 · final_train · score_final_spark · final_group_namer
                              scheme_stage2 · shape_rules · bq1_checks
    bq2/                      bq2_common · part1 tactics · part2 train/score/eval · deploy · HI-Large scoring
  orchestration/            how the jobs are run (start here: README.md)
    state-machines/           aml_prepare · aml_bq1_train · aml_bq1_score · aml_bq2_weekly · aml_bq2_weekly_ibm
    eventbridge/              week_arrived · data_prepared · run_failed: one event pattern per rule
    examples/                 example inputs for starting a machine by hand
  infra/                    Terraform: EMR apps, bucket, roles (policies/), state machines, rules, alerts, budget
  athena/                   ddl/: tables the dashboard queries
  deploy/                   deploy.sh · build_venv.sh · requirements-jobs.txt
app/                      ★ DEPLOYED TO EC2 (Streamlit dashboard)
local/                    ✗ NEVER DEPLOYED
  simulate/                 replay_week.py: plays "the bank" (one week staging/ -> raw/, starts aml_prepare)
  reports/                  final_report · txn_report · stage2_report · bq2_ramp_report · bq2_ibm_report
  experiments/              bq1_rounds/ · perf/
  analysis/                 notebooks, DuckDB cleaner
results/                  ✗ evidence for the report: final/ · experiments/ · bq2/
docs/                     ✗ pipeline.md · bq1.md · bq2.md · improving_bq1.md · figures/
```

How the datasets flow through the machines: [`docs/pipeline.md`](docs/pipeline.md).
Only the training sets (HI-Small, HI-Medium) are processed whole. HI-Large and SAML arrive one week at a
time, like a bank feed: `split_weeks_spark.py` stages the weeks once, then `replay_week.py` drops them into
`raw/` one by one, and each `_SUCCESS` marker starts `aml_prepare`.

| Folder | Deployed to | How |
|---|---|---|
| `aws/jobs/` | `s3://<bucket>/jobs/` -> EMR Serverless | `aws/deploy/deploy.sh` (from `main`) |
| `aws/orchestration/`, `aws/infra/` | Step Functions, EventBridge, IAM, SNS, EMR apps, S3, Glue/Athena | `terraform apply` in `aws/infra/` |
| `aws/athena/` | Athena | once, by hand |
| `app/` | EC2 `aml-dashboard` | `aws/infra/dashboard_ec2.tf` |
| `local/`, `results/`, `docs/` | nowhere | — |

Retired code (old EMR-cluster scripts, the daily simulation, the single weekly state machine) is in git
history before 9 Nov 2026.

## BQ2 weekly pipeline (SAML-D)

`aml_bq2_weekly` runs one simulated week per execution: feature check, score the deployed model (incremental: only the last 7 days are rescored), Part 1/Part 2 evaluation, an alert if rule R1-R3 fires, then two human approvals (retrain, deploy) answered on the dashboard's BQ2 tab (name + passcode in SSM `/aml/bq2/approver_passcode`). Its jobs run on their own EMR app `aml-bq2-saml` so they do not queue behind other pipelines. Design and results: [`docs/bq2.md`](docs/bq2.md).

## S3 bucket (data, never code you edit)

```
jobs/  staging/  raw/ (bronze)  curated/ (silver)  features/ (gold, ML)  analytics/ (gold)
models/ (trained model files)  mlflow/  logs/  athena-results/
```

## Final model (BQ1)

En Rui's stage 1 with alert expansion (`txn_v3m_xp`) + stage 2 with context, group vote and
Breann's longer cross-day linking (`enrui_breann_mld20`), on Breann's optimised Spark ETL.
Training is `aml_bq1_train` (`aws/orchestration/examples/bq1_train.json`). Baseline namers (`--baselines`) are experiments only.

## Running locally

Each folder runs as-is: scripts that import across folders (`bq2/`, `prepare/`, `local/reports/`)
add the sibling `aws/jobs/*` folders to `sys.path`; on EMR Serverless everything is one flat
`jobs.zip`, so nothing changes. Point any script at the lake with `AML_ROOT=s3://<bucket>`
(or `s3://<bucket>/dev/<name>` for your own test area).

## Never commit

AWS `credentials`, `.pem` keys, `kaggle.json` (all in `.gitignore`).
