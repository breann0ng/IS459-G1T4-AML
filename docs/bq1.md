# IS459 G1T4 · Anti-money-laundering pipeline (BQ1 + BQ2)

> **Layout note (all-combined):** paths below are from the old EMR layout. `pipeline/` and `bq2/` scripts now live in `aws/jobs/<load|clean|features|bq1|bq2>/`, reports in `local/reports/`, run scripts in `local/legacy-emr/`. To run the legacy EMR scripts, copy `aws/jobs/*/*` and `local/legacy-emr/*` into one folder on the primary node (`aws/deploy/deploy.sh` builds exactly that flat folder in `build/jobs/`). See the root README.

Big data pipeline on AWS EMR (Spark) over the IBM AML dataset, with SAML-D added for BQ2.

- [BQ1 · Early laundering detection](#bq1-early-laundering-detection)
- [BQ2 · Tactic change and detection](#bq2-tactic-change-and-detection)

## BQ1: Early laundering detection

**BQ1:** How can we automatically detect laundering patterns early, while keeping false alerts low enough for analysts to manage?

A two-stage machine-learning pipeline on AWS EMR (Spark) using the IBM AML synthetic dataset:

- **Stage 1 · detection**: LightGBM scores every payment (179 features); an account-day is an alert when its best payment score passes the frozen threshold.
- **Stage 2 · naming**: alerts are grouped with linked accounts, and a second model names the pattern (fan-in, cycle, layering, …).

Trained on HI-Small + HI-Medium, tested once on unseen HI-Large (12–25 Sep 2022).

![Pipeline](docs/figures/pipeline.png)

## Results (HI-Large test)

| | Value |
|---|---|
| Alerts that are real laundering (precision, account-day) | 90.1% |
| Laundering account-days caught (recall) | 52.0% (85.5% for named patterns, 0.7% for unclassified) |
| F1 (validation Small / Medium → test Large) | 0.645 / 0.677 → 0.659 |
| Alerts per day | ~2,269 |
| Schemes flagged | 93.1% |
| Schemes still running when the end-of-day alert arrives | 47.8% |
| Stage 2 macro F1, end to end (experimental) | 0.255 (layering 0.478) |

Full numbers: [`results/final/final_report.txt`](results/final/final_report.txt) and [`results/final/validate_report.txt`](results/final/validate_report.txt).

## Repository layout

```
IS459-G1T4-AML/
├── pipeline/          the final pipeline: every script rebuild_final.sh runs (keep in one folder; they import each other)
├── experiments/       scripts for the experiment rounds (round 3 steps, diagnostics)
├── bq2/               BQ2 pipeline: SAML-D ramp, part 1 (tactics), part 2 (detection, retraining)
├── app/               Streamlit pages for BQ1 and BQ2 (+ their data and theme)
├── results/           report outputs: BQ1 final test, experiment rounds, BQ2 report
└── docs/figures/      pipeline diagram, app screenshot, experiment charts
```

### pipeline/ (in run order)

| Step | Script | Runs on | Does |
|---|---|---|---|
| 0 | `install_libs.sh` | EMR bootstrap action | Installs LightGBM, pandas, pyarrow on every node |
| — | `rebuild_final.sh` | primary node | Runs every step below in order; resumable via S3 markers |
| 1 | `clean_transactions_spark.py` | Spark | Raw CSV → clean transactions + labels |
| 2 | `parse_patterns_spark.py` | Spark | `Patterns.txt` answer key → schemes (labels only, never model input) |
| 3 | `build_features_spark.py` | Spark | Account-day features (v2) |
| 4 | `build_txn_features_spark.py` | Spark | Payment-level features |
| 5 | `build_features_v3_spark.py` | Spark | v3 features (owner, cycles, trends) |
| 6 | `train_txn.py` | primary | Validation run `txn_v3m`: sets the threshold |
| 7 | `final_train.py` | primary | Trains the frozen stage 1 model + group namer |
| 8 | `score_final_spark.py` | Spark | Scores HI-Large; stage 1 metrics, early detection |
| 9 | `final_group_namer.py` | primary | Stage 2: names the alerts, end-to-end grading |
| 10 | `final_report.py` | laptop | Results → `final_report.txt` + `dashboard_data.json` |
| helpers | `group_stage2.py`, `scheme_stage2.py`, `txn_report.py` | — | Imported by the scripts above |

## How to run

**Pipeline (AWS Learner Lab).** Upload `pipeline/` to `s3://<bucket>/scripts/`, create an EMR cluster (1 primary + 4 core `m5.xlarge`, bootstrap action `install_libs.sh`), then on the primary node:

```bash
export AML_BUCKET=<bucket>
aws s3 cp s3://$AML_BUCKET/scripts/ . --recursive
nohup bash rebuild_final.sh > rebuild.log 2>&1 &
```

Step-by-step guide: the BQ1 Final Pipeline Rerun Runbook (team doc).

**Report (laptop).**

```bash
pip install -r requirements.txt
set AML_ROOT=s3://<bucket>          # Windows; export on macOS/Linux
python pipeline/final_report.py HI-Large > results/final/final_report.txt
```

**Streamlit app.**

```bash
cd app
streamlit run app_bq1.py
```

![BQ1 app](docs/figures/bq1_streamlit.png)

**Stage 2 page (validation only, no HI-Large).** After `group_stage2.py` runs on the cluster:

```bash
set AML_ROOT=s3://<bucket>
python pipeline/stage2_report.py              # leaderboard of every *_grp run, writes app/data/stage2_data.json
cd app
streamlit run app_stage2.py
```

## Data

IBM Transactions for Anti-Money Laundering (Kaggle, `ealtman2019/ibm-transactions-for-anti-money-laundering-aml`). Not stored in this repo: the pipeline downloads it straight into S3.

## Never commit

AWS `credentials`, `.pem` keys, `kaggle.json`. They are listed in `.gitignore`.

## Limits

- HI-Large was tested on a two-week window to fit the lab budget.
- Payments are scored when their day closes, because the features describe the whole day. The `0.0` median hours to first flag and the 97.1% share from first flag in `final_report.txt` assume same-moment scoring, so they are not reported as results.
- Unclassified laundering (no known pattern) is mostly missed.
- Stage 2 over-names layering on HI-Large, so its pattern names are experimental hints.
- Results are not broken down by payment channel.
- Synthetic data: real banks' volumes and patterns will differ.

## BQ2: Tactic change and detection

**BQ2:** Are criminals changing their laundering tactics over time, and when they do, does our detection rate fall, and can retraining recover it?

- **Part 1 · Are tactics changing?** Confirmed laundering cases only, no model. Each week's laundering profile (typology mix, payment format, amount, cross-currency, cross-bank, hour) is compared with a baseline. A week counts as a tactic change when the Jensen-Shannon distance passes the baseline noise level, the chi-square test gives p < 0.01, it lasts 2 weeks, and honest payments don't change the same way.
- **Part 2 · Is detection affected?** The BQ1 detection recipe is trained before the change (v0) and graded every week against confirmed cases (4-week investigation lag). Alert rules on recall trigger retraining (v1), and v0 and v1 are compared on the same weeks.

| | HI-Large (IBM) | SAML-D with new tactics |
|---|---|---|
| Tactics changed? | No: all 6 dimensions stable | Yes: payment format confirmed in Week 24, 2 weeks after the new tactics began |
| Detection affected? | No alert, no retraining | Alert in Week 27; v0 caught 40% of the new tactics |
| Retraining | not needed | v1 live from Week 32: new-tactic recall 40% → 70%, all laundering 64% → 85%, precision 98.6% → 99.5% |

SAML-D (another synthetic AML dataset with 17 laundering typologies): Oztas et al. 2023, "Enhancing Anti-Money Laundering: Development of a Synthetic Transaction Monitoring Dataset", IEEE ICEBE, [doi 10.1109/ICEBE59045.2023.00028](https://doi.org/10.1109/ICEBE59045.2023.00028), [Kaggle](https://www.kaggle.com/datasets/berkanoztas/synthetic-transaction-monitoring-dataset-aml). Four of its tactics (structuring, smurfing, cash withdrawal, deposit-send) are held back for the first 21 weeks and then switched on gradually (SAML-R); a control copy never gets them (SAML-C).

Design, scripts and assumptions: [`bq2/README.md`](bq2/README.md). Full numbers: [`results/bq2/bq2_ramp_report.txt`](results/bq2/bq2_ramp_report.txt).

### How to run BQ2

**Pipeline (EMR primary node).** Needs `SAML-D.csv` at `s3://<bucket>/raw/saml/` and BQ1's `models/final/`. Upload `pipeline/` and `bq2/` to `s3://<bucket>/scripts/`, then:

```bash
export AML_BUCKET=<bucket>
aws s3 cp s3://$AML_BUCKET/scripts/ . --recursive
nohup bash run_bq2_ramp.sh > bq2r.log 2>&1 &      # resumable; about 1.5 hours
```

**Report and app (laptop).**

```bash
set AML_ROOT=s3://<bucket>          # Windows; export on macOS/Linux
python bq2/bq2_ramp_report.py > results/bq2/bq2_ramp_report.txt    # also writes app/data/bq2_data.json
cd app
streamlit run app_bq2.py
```

### BQ2 limits

- Controlled test: the change in SAML-R is built in, so it shows the method works, not that real criminals changed.
- Labels here are complete; a real bank mostly confirms the cases its model flagged, so it also needs a review sample below the threshold.
- SAML-D has no bank IDs or owners and was mapped onto IBM's format (bank = country, fixed exchange rates); compare SAML numbers within SAML only.
- Part of v1's gain on established tactics comes from 10 more weeks of training data.
