# IS459 G1T4 · Anti-money-laundering pipeline (BQ1 + BQ2)

- **BQ1:** detect laundering early while keeping false alerts manageable (LightGBM stage 1 + group namer stage 2).
- **BQ2:** are tactics changing, does detection fall, and can retraining recover it?

Details and results: [`docs/bq1.md`](docs/bq1.md) · [`docs/bq2.md`](docs/bq2.md).

## Repository layout: grouped by where things run

```
aws/                      ★ DEPLOYED TO AWS
  step-functions/           the state machines, exactly as deployed (start here: README.md)
    aml_prepare               shared data stage: SAML-D convert · clean · patterns · features
    aml_bq1_train             HI-Small + HI-Medium -> detector + namer -> quality gate -> champion
    aml_bq1_score             HI-Large weekly files -> champion scores + names patterns
    aml_bq2_weekly            SAML-R: tactic change, detection drop, retrain on alert
    aml_bq2_weekly_ibm        HI-Large / HI-Large-NT (fan-in, fan-out hidden): same BQ2 rules
    iam/ · inputs/ · legacy/  role policies, example inputs + EventBridge rules, old weekly machine
  jobs/                     code -> s3://<bucket>/jobs/ (one flat folder + jobs.zip), runs on EMR Serverless
    prepare/                  every aml_prepare step: saml_ramp · clean · parse_patterns · 3 feature builders
    bq1/                      train_txn · group_stage2 · final_train · score_final_spark · final_group_namer
                              scheme_stage2 · shape_rules · bq1_checks
    bq2/                      bq2_common · part1 tactics · part2 train/score/eval · deploy · HI-Large scoring
  sql/                      Athena: ddl/ · views/ · run-log/ · maintenance/
  infra/                    Terraform (EMR app, bucket, budgets, legacy weekly trigger)
  config/pipeline.yaml      bucket, region, final model settings
  deploy/                   deploy.sh · build_venv.sh · requirements-jobs.txt
app/                      ★ DEPLOYED TO STREAMLIT CLOUD
local/                    ✗ NEVER DEPLOYED
  simulate/                 replay_week.py · replay_day.py: play "the bank" (staging/ -> raw/)
  reports/                  final_report · txn_report · stage2_report · bq2_ramp_report · bq2_ibm_report
  runs/                     deploy_branch.py · run_final_serverless.py
  experiments/              bq1_rounds/ · perf/
  analysis/                 notebooks, DuckDB cleaner
  legacy-emr/               old EMR-cluster scripts (run_bq2_*.sh, rebuild_final.sh, ...)
results/                  ✗ evidence for the report: final/ · experiments/ · bq2/
docs/                     ✗ pipeline.md · bq1.md · bq2.md · improving_bq1.md · figures/
```

How the datasets flow through the machines: [`docs/pipeline.md`](docs/pipeline.md).

| Folder | Deployed to | How |
|---|---|---|
| `aws/jobs/` | `s3://<bucket>/jobs/` -> EMR Serverless | `aws/deploy/deploy.sh` (from `main`) |
| `aws/step-functions/` | Step Functions | `aws stepfunctions update-state-machine` (see its README) |
| `aws/sql/` | Athena + Glue Data Catalog | by hand |
| `aws/infra/` | EMR app, bucket, budgets, legacy weekly trigger | `terraform apply` |
| `app/` | Streamlit Community Cloud | linked to this repo |
| `local/`, `results/`, `docs/` | nowhere | — |

## S3 bucket (data, never code you edit)

```
jobs/  staging/  raw/ (bronze)  curated/ (silver)  features/ (gold, ML)  analytics/ (gold)
models/ (trained model files)  mlflow/  logs/  athena-results/
```

## Final model (BQ1)

En Rui's stage 1 with alert expansion (`txn_v3m_xp`) + stage 2 with context, group vote and
Breann's longer cross-day linking (`enrui_breann_mld20`), on Breann's optimised Spark ETL.
Exact commands: `aws/config/pipeline.yaml`. Baseline namers (`--baselines`) are experiments only.

## Running locally

Each folder runs as-is: scripts that import across folders (`bq2/`, `prepare/`, `local/reports/`)
add the sibling `aws/jobs/*` folders to `sys.path`; on EMR Serverless everything is one flat
`jobs.zip`, so nothing changes. Point any script at the lake with `AML_ROOT=s3://<bucket>`
(or `s3://<bucket>/dev/<name>` for your own test area).

## Never commit

AWS `credentials`, `.pem` keys, `kaggle.json` (all in `.gitignore`).
