# IS459 G1T4 · BQ2: Are criminals changing their tactics, and is detection affected?

> **Layout note (all-combined):** paths below are from the old EMR layout. `pipeline/` and `bq2/` scripts now live in `aws/jobs/<prepare|bq1|bq2>/`, reports in `local/reports/`, run scripts in `local/legacy-emr/`. To run the legacy EMR scripts, copy `aws/jobs/*/*` and `local/legacy-emr/*` into one folder on the primary node (`aws/deploy/deploy.sh` builds exactly that flat folder in `build/jobs/`). See the root README.

**BQ2:** Are criminals changing their laundering tactics over time, and when they do, does our detection rate fall, and can retraining recover it?

## Design

| | Question | Data | Method |
|---|---|---|---|
| **Part 1** | Are tactics changing? | Confirmed cases only (labels + pattern names), **no model** | Weekly profile of laundering (pattern, format, amount, cross-currency, cross-bank, hour) vs a baseline: Jensen-Shannon distance + smoothed PSI (size), chi-square (real or noise), persistence (2 weeks), honest-traffic control, CUSUM (when it started) |
| **Part 2** | Does the change affect detection? | Scores + confirmed cases | BQ1 stage 1 recipe trained before the change (v0), graded week by week; alert rules → retrain with the confirmed cases (v1) → compare v0 and v1 on the same weeks |

**Datasets**
- **HI-Large (IBM):** Part 1 only. Tactics barely change after the simulator warm-up, so no alert and no retraining are needed. IBM's 8 patterns are also close relatives (fan-in, fan-out, gather-scatter, scatter-gather share the same shapes), so even a held-out pattern hardly hurts detection.
- **SAML-R (ramp):** SAML-D ([Oztas et al. 2023](https://doi.org/10.1109/ICEBE59045.2023.00028); [Kaggle](https://www.kaggle.com/datasets/berkanoztas/synthetic-transaction-monitoring-dataset-aml)), 17 more varied typologies. Four "emerging" tactics (structuring, smurfing, cash withdrawal, deposit-send) are absent for the first 17 weeks, then switched on step by step: 10% → 25% → 50% → 100% of their schemes.
- **SAML-C (control):** the same, but the emerging tactics never arrive. Part 1 must stay quiet on it; any change it reports is a false alarm.

This is a **controlled test**: the change in SAML-R is one we built in, so it shows whether the method detects and measures a change, not that real criminals changed. Real-world evidence that a behaviour shift breaks models: the Elliptic Bitcoin dataset's dark-market shutdown.

## Scripts (run order)

| Step | Script | Runs on | Does |
|---|---|---|---|
| — | `run_bq2_ramp.sh` | primary | Runs everything below in order; resumable via S3 markers (`progress/bq2r/`) |
| 1 | `saml_ramp_spark.py` | Spark | SAML-D.csv → SAML-R and SAML-C in the IBM layout |
| 2 | `pipeline/` BQ1 steps, **unchanged** | Spark | clean + answer key (both), features (SAML-R) |
| 3 | `bq2_part1_tactics_spark.py` | Spark | Part 1 on SAML-R, SAML-C, HI-Large |
| 4 | `bq2_part2_train.py` | primary | v0: BQ1 recipe (from `models/final/meta.json`) on SAML-R weeks 1–17 |
| 5 | `bq2_part2_score.py` | primary | scores a version on later weeks |
| 6 | `bq2_part2_eval.py` | primary | weekly recall, alert rules, `decision.json` (retrain cut-off, deploy date); after v1, the comparison |
| 7 | train / score / eval again | primary | v1: retrained with cases confirmed up to the alert week, deployed after the 4-week label lag |
| 7b | `run_bq2_round2.sh` | primary | Round 2: new data baseline (Weeks 34–39), monitor v1 against v1's own validation; if alerted, train v2, score, compare v1 vs v2 |
| 8 | `bq2_ramp_report.py` | laptop | results → `results/bq2/bq2_ramp_report.txt` + `app/data/bq2_data.json` |
| — | `bq2_common.py` | — | shared settings: emerging typologies, dates, doses, alert rules |

## HI-Large tab: Joey's method on IBM + the fan-in / fan-out new-tactic test (Tasha)

HI-Large's tactics do not change, so on top of Part 1 on the real data we test a **held-out pattern**: fan-in and
fan-out laundering are left out of every week before 3 Oct 2022 (Week 10), so they appear as a brand-new tactic.
The bank's model then is the BQ1 stage 1 recipe trained **without** them (`models/final_nofan`); the BQ1 final
model (`models/final`, `final_train.py txn_v3m`, the same stage 1 as the En Rui + Breann final run) shows what
retraining with the confirmed cases gives. Both are graded with the **same** Part 1 and Part 2 rules as SAML-D. New scripts: `aws/jobs/bq2/`.

| Step | Script | Does |
|---|---|---|
| — | `local/legacy-emr/run_bq2_ibm.sh` | runs everything below; resumable (`progress/bq2/`) |
| 1 | `bq2_ibm_score_spark.py` | per week: features (unchanged BQ1 steps) + scores with both models (Spark: ~13M payments a week) |
| 2 | `bq2_part1_tactics_spark.py` | Part 1 on HI-Large, real data, then with `BQ2_HIDE=fan_in,fan_out BQ2_HIDE_BEFORE=2022-10-03` (tag `_nt`) |
| 3 | `bq2_ibm_part2_prep_spark.py` | scores in Part 2's format (`analytics/bq2r_scores`, datasets `HI-Large` and `HI-Large-NT`) and each model's validation baseline (`models/bq2r/<ds>/<v>/meta.json`, HI-Small + HI-Medium validation days) |
| 4 | `bq2_part2_eval.py` | Joey's rules R1-R3, unchanged (`BQ2_EMERGING=fan_in,fan_out BQ2_FIRST_DOSE=2022-10-03`) |
| 5 | `local/reports/bq2_ibm_report.py` | laptop: `results/bq2/bq2_ibm_report.txt` + `app/data/bq2_ibm_data.json` (HI-Large tab, "New-tactic test") |

Window: Weeks 5-13 (29 Aug - 30 Oct 2022), baseline Weeks 5-6 (as in Joey's HI-Large Part 1). Limits: the hidden
payments still exist in the raw data before Week 10 (removed from grading and Part 1 only), and with the 4-week
label lag the retrained model would go live after the last scored day, so the BQ1 model line stands in for it.

## Assumptions to state

- SAML-D has no bank IDs, owners or second amount: bank = country, every account its own owner, amount received from fixed exchange rates, payment types mapped onto IBM's 7 formats. Owner features are constant on SAML-D, so the model ignores them.
- Compare SAML numbers **within SAML only** (v0 vs v1, before vs after); not with BQ1's IBM numbers.
- Confirmed cases arrive 4 weeks after the week they happen (`LABEL_LAG_DAYS`), and our labels are complete; a real bank mostly confirms what its model flagged, so a below-threshold review sample would be needed to find missed cases.
- Results are payment-level (BQ1's account-day view and alert expansion are not applied).
