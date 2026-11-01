# Improving BQ1 precision and recall: how to test

**Goal:** beat the current BQ1 detection model, measured fairly so the result can be trusted.

## 1. The rule: one validation setup, HI-Large untouched

Don't use a random 80/20 split, and don't tune on HI-Small or HI-Medium alone. Use the setup already built into `pipeline/train_txn.py`:

| | What | Why |
|---|---|---|
| **Training data** | HI-Small + HI-Medium **together**, one model | The final model is trained on both, so experiments must be too |
| **Split** | By **time**: first **75%** of each dataset's days = train, last **25%** = validation (`VALIDATION_SHARE = 0.25`) | A random split puts the same accounts and schemes in train and validation, and the features use 7–21 days of history, so scores come out too high. A time split matches real use: learn from the past, catch the future |
| **Reported** | Validation scores **per dataset** (Small and Medium separately) | Shows whether a change helps in general or only on one bank |
| **HI-Large** | **Never used** while experimenting | It is the final test. Testing on it repeatedly and keeping the best makes the result meaningless |

## 2. Baseline to beat (validation run `txn_v3m`)

| Validation | Precision | Recall | **F1** | PR-AUC | Alerts/day |
|---|---|---|---|---|---|
| HI-Medium · account-day | 0.909 | 0.539 | **0.677** | 0.677 | 2,149 |
| HI-Small · account-day | 0.894 | 0.505 | **0.645** | 0.637 | 424 |
| HI-Medium · payment | 0.894 | 0.555 | 0.685 | 0.670 | 1,428 |
| HI-Small · payment | 0.889 | 0.539 | 0.671 | 0.661 | 290 |

- Recall on the named patterns: 0.889 (Medium) / 0.877 (Small). Recall on **unclassified** laundering: **0.008 / 0.005**. This is where most of the room is: unclassified laundering is about 40% of all laundering.
- Current HI-Large test result, for the final comparison only: account-day precision 0.901, recall 0.520, F1 **0.659**.
- Setup: LightGBM preset `p3`, 179 payment features (`txn_features_v3`), threshold = best F1 on validation (0.9753), stage 2 with `--merge-layering`.

**Main metric:** account-day F1, which is what an analyst reviews. Also watch:
- **PR-AUC**, which doesn't depend on the threshold, so a real model improvement raises it;
- **precision vs recall**, to see which one moved;
- **unclassified recall**.

## 3. How to run one experiment

1. **Change one thing per run** (one feature group, one parameter, one sampling change), so you know what caused the difference.
2. Give every run a new `--config` name. Results are saved under that name in `analytics/txn_results`.
3. On the EMR **primary node** (about 20 minutes per run):
   ```bash
   export AML_ROOT=s3://<bucket>
   python3 -u train_txn.py --txn-table txn_features_v3 --stage2-features features_v3 \
       --preset p3 --merge-layering --config <your_run_name>
   ```
   Useful options:
   - `--param num_leaves=255` (repeatable): override any LightGBM parameter;
   - `--preset p1|p2|p3`;
   - `--drop-format`: ablation, removes payment-format features;
   - `--no-stage2`: faster, if you're only working on detection.
   - **Do not use `--test`**: it scores the HI-Large test set.
4. Compare with the baseline on the laptop. This reads small result tables only:
   ```
   set AML_ROOT=s3://<bucket>
   python pipeline\txn_report.py txn_v3m <your_run_name>
   ```
5. Keep a log: run name · what changed · F1 Small / Medium · PR-AUC · precision · recall · keep or drop.

**New features** need the feature tables rebuilt first. Edit the feature script (`build_txn_features_spark.py` / `build_features_v3_spark.py`), run it with `spark-submit` for HI-Small and HI-Medium, then train. Don't rebuild the HI-Large window until the very end.

## 4. When you've chosen the best version (once)

1. `python3 -u final_train.py <your_run_name>` trains the frozen models on the same training days with the new settings, and saves them to `models/final/`. Back up the current `models/final/` first if you want to keep it.
2. If features changed: rebuild the HI-Large test window, 12–25 Sep 2022 (same steps as `feat_large` / `txn_large` / `v3_large` in `rebuild_final.sh`).
3. Score HI-Large **once**: the `score` and `name` steps of `rebuild_final.sh`. The simplest way is to delete the `score.done` and `name.done` markers in `progress/rebuild/` (plus the three `*_large` markers if features changed) and rerun the script.
4. Report the new HI-Large result **next to** the old one (F1 0.659), and the validation gain that justified it.

## 5. Practical setup

- **Data:** you need a bucket with the full pipeline data. Your teammate's copy is `s3://pw-aml-g1t4-c`. Either get read access the same way it was copied (a bucket policy for your account), or copy it into your own bucket with `aws s3 sync`.
- **Cluster:** 1 primary + 4 core `m5.xlarge`, with the bootstrap action `s3://<bucket>/scripts/install_libs.sh` (it installs LightGBM on every node). Java 17 on the primary (see the runbook). Don't `sudo pip install` on a running cluster: it breaks the AWS CLI.
- **Budget:** each training run is about 20 minutes of a running cluster. Terminate the cluster when you're not running something.
- **Never commit:** AWS `credentials`, `.pem` keys, `kaggle.json`.

## 6. Where to look first

- **Unclassified laundering** (recall under 1%): it barely overlaps the scheme accounts, so it likely needs new signals, not tuning. Check `experiments/unclassified_profile.py` for what is known about it.
- **Precision is already about 0.90:** gains are more likely on recall. Report both, so a recall gain bought with many more false alerts is visible.
- **Stage 2** (pattern naming) over-names layering on HI-Large: a separate improvement, judged by the end-to-end macro F1 in the same report.
