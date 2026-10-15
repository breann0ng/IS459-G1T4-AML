# IS459 G1T4 — AML Detection & Drift Monitoring Pipeline

Big data pipeline on AWS over the [IBM Transactions for Anti Money Laundering](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml) dataset.

**BQ1 — Detection & trade-off:** How can we automatically detect laundering patterns early, while keeping false alerts low enough for analysts to manage?

**BQ2 — Drift monitoring:** Are criminals changing their tactics over time, and can we tell when our detection is falling behind?

## Architecture

```
Kaggle CLI ──► S3 landing/ (raw CSVs as-is, aws s3 cp)
                  │
                  ▼  Glue Spark  01_partition_raw
             S3 raw/dataset=…/week=…/
                  │
                  ▼  Glue Spark  02_clean_transactions
             S3 curated/transactions (Iceberg)
                                              Glue Spark  03_load_schemes
             landing/Patterns.txt ──────────► DynamoDB  aml-schemes   (lookup / drill-down)
                                          └─► S3 curated/schemes (Parquet, for bulk joins)
                  │
                  ▼  Glue Spark  10_bq1_detectors / 20_bq2_drift
             S3 analytics/ (sweep_results, alert_log, weekly_drift, …)
                  │
                  ▼
             Athena ──► Streamlit dashboard

Orchestration: Step Functions (Map over weeks) + EventBridge + SNS email on flagged weeks
```

### Changes since the proposal (prof's feedback)

| Proposal | Now | Why |
|---|---|---|
| pandas splitter cuts `Trans.csv` into weeks | Raw CSVs uploaded as-is; **Glue Spark** partitions by `dataset`/`week` | Single-machine pandas does not scale with data size |
| Glue **Python** job parses `Patterns.txt` → S3 Iceberg | Glue **Spark** job parses it → **DynamoDB** (+ Parquet copy) | Minimise Python jobs; DynamoDB for scheme lookups |
| Each uploaded week triggers preparation | Step Functions **Map** iterates over week partitions | Keeps the weekly-arrival simulation for BQ2 without a Python script |

## Repo layout

| Path | Contents |
|---|---|
| `infra/` | CloudFormation template (S3, DynamoDB, Glue, IAM, Step Functions) |
| `glue/` | Glue Spark job scripts; `glue/common/` shared helpers |
| `orchestration/` | Step Functions state machine definition |
| `dashboard/` | Streamlit app querying Athena |
| `scripts/` | Download / upload / deploy helper scripts |
| `tests/` | PySpark unit tests on small fixtures |
| `sample_data/` | Tiny hand-made fixtures (real data is gitignored) |

## Local development

Develop and test jobs locally on **HI-Small** before running on Glue (saves DPU-hours).

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
bash scripts/download_data.sh HI-Small   # needs ~/.kaggle/kaggle.json
pytest
```

Requires Java 11/17 for local PySpark.

## Deploy & run on AWS

_TODO — filled in as infra is built (this section doubles as the setup video script)._

1. `bash scripts/deploy_infra.sh`
2. `bash scripts/upload_raw.sh HI-Large`
3. Start the Step Functions execution
4. `streamlit run dashboard/app.py`

## Team

Breann, Lesley, Joey, Tasha, En Rui, Wei Ting
