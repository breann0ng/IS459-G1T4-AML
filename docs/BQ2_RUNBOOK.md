# BQ2 runbook: tactics over time and "is detection falling behind?"

**BQ2:** Are criminals changing their tactics over time, and can we tell when our detection is falling behind?

## What the BQ2 run does

1. **Two frozen models.**
   - `all`: the BQ1 final model (`models/final`).
   - `nofan`: the same model, settings and features, but trained **without any fan-in / fan-out laundering** (`models/final_nofan`). It has never seen those patterns, like any model facing a genuinely new tactic.
2. **Weekly scoring.** Both models score **10 weeks of HI-Large** (weeks 5–14, 29 Aug – 5 Nov 2022), 2 weeks at a time.
3. **The analysis** (`bq2_analysis_spark.py`) runs two scenarios:
   - **real**: the data as it is.
   - **new_tactic**: fan-in / fan-out laundering is removed from every week **before week 9**, so to the monitor it looks like a brand-new tactic appearing in week 9.
4. **What it measures:**
   - **BQ2a, are tactics changing?** Each week's mix of laundering (pattern, payment format, amount band, cross-currency, cross-bank) compared with the baseline weeks 5–7, using PSI (0.1 = moderate, 0.2 = large) and a chi-square test.
   - **BQ2b, is detection falling behind?** Each week's precision, recall (overall, the new tactic, other patterns, unclassified), alerts per day, and schemes alerted before they finish.
     - **Alarm:** recall below the baseline's mean − 2 sd, two weeks in a row.
     - Labels arrive **2 weeks late** in a real bank, so the alarm is only *known* 2 weeks later.
   - **Early warning, no labels:** alerts per day (z-score), shift in the scores of the day's top 2,000 accounts (the "high-risk tail", PSI), shift in all risk scores (PSI) and shift in the top features (PSI), all known the same week. A shift only counts if it is also statistically significant (chi-square p < 0.01), so a few odd schemes in a quiet week can't raise an alarm.
   - **Lead time** = the week the label alarm is known − the week of the first early warning.
5. **Outputs:** `analytics/bq2/HI-Large.json`, which `bq2_report.py` prints and the Streamlit page `app/app_bq2.py` shows.

## New and changed files (branch `tasha-bq2`)

| File | What |
|---|---|
| `pipeline/rebuild_bq2.sh` | Runs everything in order. **Resumable** (markers in `progress/bq2/`), and **skips any step whose output is already in the bucket** |
| `pipeline/bq2_score_spark.py` | Scores a date window with both models in one pass |
| `pipeline/bq2_analysis_spark.py` | BQ2a + BQ2b + early warning + alarms |
| `pipeline/bq2_report.py` | Laptop: prints the report, writes `app/data/bq2_data.json` |
| `app/app_bq2.py` | BQ2 Streamlit page |
| `pipeline/train_txn.py`, `final_train.py` | New options `--exclude-patterns`, `--model-dir`, `--stage1-only` (defaults unchanged, so BQ1 runs exactly as before) |
| `pipeline/install_libs.sh` | Enrui's fix that keeps the AWS CLI working after pip installs |

## Run it (AWS Learner Lab, us-east-1)

### 0. Choose: copy the team's data (fast) or rebuild (slow)

- **Copy (recommended).** A teammate's bucket already has the cleaned data, the HI-Small / HI-Medium features, `models/final`, and HI-Large weeks 7–8.
  1. The bucket owner adds a bucket policy giving your account read access. Template below; replace the account ID and bucket name.
  2. Copy with `aws s3 sync` (step 4).
  3. The script then only does what's new: the `nofan` model (about 40 min), HI-Large features for 8 new weeks, scoring, and the analysis.
- **Rebuild.** Skip the copy; the script downloads from Kaggle and rebuilds everything, like `rebuild_final.sh`. It takes many more cluster hours and needs `~/.kaggle/kaggle.json`.

Bucket policy for the **team bucket** (the owner pastes it in S3 → bucket → Permissions → Bucket policy):

```json
{"Version": "2012-10-17", "Statement": [{
  "Sid": "Bq2ReadForTasha", "Effect": "Allow",
  "Principal": {"AWS": "arn:aws:iam::<TASHA_ACCOUNT_ID>:root"},
  "Action": ["s3:ListBucket", "s3:GetObject"],
  "Resource": ["arn:aws:s3:::<TEAM_BUCKET>", "arn:aws:s3:::<TEAM_BUCKET>/*"]}]}
```

### 1. Bucket and scripts

Use your own bucket (e.g. `is459-g1t4-aml-tasha`). Upload the **whole `pipeline/` folder** of this branch to `s3://<bucket>/scripts/`.

### 2. Cluster

Create the EMR cluster the same way as in the team's **BQ1 Final Pipeline Rerun Runbook**:
- 1 primary + 4 core `m5.xlarge`
- bootstrap action `s3://<bucket>/scripts/install_libs.sh`

Then do the runbook's first-login setup on the primary node (Java 17, etc.).

### 3. Get the scripts onto the primary node

```bash
export AML_BUCKET=<your bucket>
mkdir -p ~/bq2 && cd ~/bq2
aws s3 cp s3://$AML_BUCKET/scripts/ . --recursive
```

### 4. Copy the team's data (only for the copy option)

```bash
TEAM=<team bucket>
aws s3 sync s3://$TEAM/curated/      s3://$AML_BUCKET/curated/      --exclude "*quarantine*"
aws s3 sync s3://$TEAM/models/final/ s3://$AML_BUCKET/models/final/
aws s3 sync s3://$TEAM/raw/accounts/ s3://$AML_BUCKET/raw/accounts/
```

### 5. Run

```bash
nohup bash rebuild_bq2.sh > rebuild_bq2.log 2>&1 &
tail -f rebuild_bq2.log          # Ctrl+C stops watching, not the run
```

- **If the lab session times out or the cluster stops:** start the lab again, create a new cluster, repeat steps 3 and 5. Finished steps are skipped.
- **Every step logs to `logs/<step>.log`.** If one fails, the run stops and prints the last 20 lines.
- **When it prints `ALL DONE`, terminate the cluster.**

### 6. Report and dashboard (laptop)

```bash
git checkout tasha-bq2
pip install -r requirements.txt
export AML_ROOT=s3://<your bucket>       # Windows: set AML_ROOT=s3://<your bucket>
mkdir -p results/bq2
python pipeline/bq2_report.py HI-Large > results/bq2/bq2_report.txt
cd app && streamlit run app_bq2.py
```

The laptop needs AWS credentials for your Learner Lab: AWS Details → AWS CLI → Show → paste into `~/.aws/credentials`. They change every session.

## Settings (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `MON_FROM`, `MON_TO` | `2022-08-29`, `2022-11-06` | Monitored weeks (Monday to Sunday) |
| `HIDE` / `BQ2_HIDE` | `fan_in,fan_out` | The "new tactic" (pattern names as in `pattern_label`) |
| `BQ2_INTRO_WEEK` | 5th monitored week (week 9) | When the new tactic appears |
| `BQ2_BASELINE_WEEKS` | `3` | Weeks that define "normal" |
| `BQ2_LABEL_DELAY` | `2` | Weeks until labels are known |

To try another new tactic (e.g. cycles), run `validate_nofan` and `final_train_nofan` again with `HIDE=cycle`. Use a new `--model-dir`, and delete those two markers in `progress/bq2/` first.

## Limits

- **Synthetic data.** IBM's simulator may barely change tactics over 3 months, so the **controlled test** is what shows the monitor works. The **real** scenario honestly reports whether tactics shifted.
- **The new tactic is simulated in the evaluation.** Fan-in / fan-out payments still exist in the data before week 9. They're only removed from grading and from the label-free signals. The features of *other* accounts that traded with them are unchanged.
- **Scoring is end of day**, as in BQ1. Hours-to-alert assume the list is published after the day closes.
- **The "age" features are left out of the drift monitor.** `r_age_days`, `s_age_days` and `t_pair_age_d` count days since an account or pair was first seen *in the data read*. That restarts with every 2-week chunk (21 days of history) and differs between training (HI-Small / HI-Medium read whole, at most 10–16 days) and HI-Large (up to 34 days). In the local test they moved strongly every week by construction. Worth flagging for BQ1 too: they're top-10 features, so the model sees ages on HI-Large that it never saw in training.
- **Whole-population signals are weak against a small new tactic.** Laundering is a tiny share of all payments, so whole-score and feature drift barely move; the high-risk-tail and alerts-per-day signals are the ones to watch. In the local test (synthetic data) they moved after the new tactic appeared but stayed under the alarm lines. The HI-Large run gives the real answer.

## Local test (synthetic data, before the AWS run)

The whole run (team pipeline + BQ2) was run end to end on synthetic HI-Small / HI-Medium / HI-Large data (Spark local, Java 17), with all 8 patterns, unclassified laundering, and a built-in tactic change from week 10 (more cycles, more Bitcoin).
- Every step ran.
- The blind model dropped the fan-in / fan-out payments from training and validation and set its own threshold.
- **BQ2a** flagged the payment-format shift two weeks after it started.
- **BQ2b:** in the controlled test, the blind model's recall on the new tactic fell to 7–37% in weeks 9–11, while the current model stayed higher. The monitor raised "falling behind" for the blind model only.
- The numbers themselves are not results (fake data).
