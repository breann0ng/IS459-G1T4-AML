# IS459 G1T4 · Anti-money-laundering pipeline (BQ1 + BQ2)

- **BQ1:** detect laundering early while keeping false alerts manageable (LightGBM stage 1 + group namer stage 2).
- **BQ2:** are tactics changing, does detection fall, and can retraining recover it?

Details and results: [`docs/bq1.md`](docs/bq1.md) · [`docs/bq2.md`](docs/bq2.md).

## Try the dashboards yourself (about 5 minutes, no AWS account needed)

The dashboards read small result files that are already in this repository (`app/data/`), so they run on any laptop.
You need **Python 3.10 or newer** and **Git**.

**1. Get the code** (the BQ1 dashboard lives on the `enrui-ui-nav` branch):

```bash
git clone -b enrui-ui-nav https://github.com/breann0ng/IS459-G1T4-AML.git
cd IS459-G1T4-AML
```

**2. Install the packages** into a fresh environment:

| Windows (PowerShell) | macOS / Linux |
|---|---|
| `python -m venv .venv` | `python3 -m venv .venv` |
| `.venv\Scripts\activate` | `source .venv/bin/activate` |
| `pip install -r app/requirements.txt` | `pip install -r app/requirements.txt` |

**3. Start a dashboard.** Run it from inside the `app` folder, so the dark theme and text sizes in
`app/.streamlit/config.toml` are picked up:

```bash
cd app
streamlit run bq1_site.py                      # BQ1: Overview + BQ1 · Early detection (opens http://localhost:8501)
streamlit run app_bq2.py --server.port 8502    # BQ2: tactic change & detection (opens http://localhost:8502)
```

Each command opens the dashboard in your browser. Stop it with **Ctrl+C** in the terminal. Both can run at the same
time in two terminals (hence the different ports).

**What to look at**

- **BQ1** (`bq1_site.py`): the **Overview** page states both business questions with their answers and the pipeline.
  The **BQ1 · Early detection** page has four tabs: *Alerts & accuracy* (drag the slider to set how many alerts a day
  the team can review), *Speed* (pick a pattern and when a scheme counts as detected), *Who checks what* and
  *Archived findings*. Every chart has a **Takeaway** and a **How to read this, and why it matters** box; hover any
  **(?)** to see how a number is calculated.
- **BQ2** (`app_bq2.py`): two tabs, *HI-Large · IBM AML* and *SAML-D · new tactics test*.

If `streamlit` is "not recognised", the environment is not active: repeat the `activate` line from step 2.

### Updating the BQ1 results as the weekly pipeline finishes more weeks (team only, needs AWS)

The BQ1 page reads `app/data/dashboard_data_live_HI-Large-live.json`: every HI-Large week that the Step Functions
pipeline `aml_bq1_weekly` has finished, merged (counts added up, rates recomputed). After a new week finishes
(`progress/bq1_weekly/HI-Large-live/<week>.json` says `SUCCEEDED`), refresh it from the repository root (PowerShell):

```powershell
$env:AWS_PROFILE = "g1t4"; $env:AML_ROOT = "s3://is459-g1t4-aml"; $env:AML_LIVE = "1"
cd local\reports
..\..\.venv\Scripts\python final_report.py HI-Large-live > ..\..\results\final\final_report_live.txt
Move-Item -Force dashboard_data_live_HI-Large-live.json ..\..\app\data\
cd ..\..
```

Then commit the JSON. Scheme timings on the Speed tab stay from the one-off run of 12–25 Sep, because the weekly
pipeline cuts schemes at the week boundary. If the live file is missing, the page falls back to the one-off run.

## Repository layout: grouped by where things run

```
aws/                      ★ DEPLOYED TO AWS
  jobs/                     code -> s3://<bucket>/jobs/, runs on EMR Serverless
    load/                     load_kaggle.sh · saml_ramp_spark.py              -> raw/
    clean/                    clean_transactions_spark.py · parse_patterns_spark.py -> curated/
    features/                 build_features_spark.py · build_txn_features_spark.py · build_features_v3_spark.py
    bq1/                      train_txn · final_train · score_final_spark · final_group_namer
                              group_stage2 · scheme_stage2 · shape_rules           -> models/, analytics/
    bq2/                      bq2_common · bq2_part1_tactics_spark · bq2_part2_{train,score,eval}
  sql/                      Athena: ddl/ · views/ · run-log/ · maintenance/
  step-functions/           aml_daily.json · aml_weekly.json (to write)
  infra/                    Terraform (to write)
  config/pipeline.yaml      bucket, region, final model settings: the single source of truth
  deploy/                   deploy.sh · build_venv.sh · requirements-jobs.txt
app/                      ★ DEPLOYED TO STREAMLIT CLOUD: bq1_site.py (menu) → home.py · app_bq1.py │ app_bq2.py │ app_stage2.py (experiment log)
local/                    ✗ NEVER DEPLOYED
  simulate/                 replay_day.py: plays "the bank" (staging/ -> raw/)
  reports/                  final_report.py · txn_report.py · stage2_report.py · bq2_ramp_report.py
  experiments/              bq1_rounds/ · perf/
  analysis/                 notebooks, DuckDB cleaner
  legacy-emr/               rebuild_final.sh · run_bq2_*.sh · install_libs.sh (until Step Functions works)
results/                  ✗ evidence for the report: final/ · experiments/ · bq2/
docs/                     ✗ bq1.md · bq2.md · improving_bq1.md · figures/
```

| Folder | Deployed to | How | Owner |
|---|---|---|---|
| `aws/jobs/` | `s3://<bucket>/jobs/` -> EMR Serverless | `aws/deploy/deploy.sh` (from `main` only) | persons 2-4 write, person 1 deploys |
| `aws/sql/` | Athena + Glue Data Catalog | Terraform / Step Functions | person 5 |
| `aws/step-functions/` | Step Functions | Terraform | person 5 writes, person 1 applies |
| `aws/infra/` | every AWS resource | `terraform apply` | person 1 |
| `app/` | Streamlit Community Cloud | linked to this repo | person 5 |
| `local/`, `results/`, `docs/` | nowhere | — | everyone |

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

Each folder runs as-is: scripts that import across folders (`bq2/`, `load/`, `local/reports/`)
add the sibling `aws/jobs/*` folders to `sys.path`; on EMR Serverless everything is one flat
`jobs.zip`, so nothing changes. Point any script at the lake with `AML_ROOT=s3://<bucket>`
(or `s3://<bucket>/dev/<name>` for your own test area).

## Never commit

AWS `credentials`, `.pem` keys, `kaggle.json` (all in `.gitignore`).
