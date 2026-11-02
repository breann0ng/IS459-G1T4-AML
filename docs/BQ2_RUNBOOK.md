# BQ2 runbook: tactics over time and "is detection falling behind?"

**BQ2:** Are criminals changing their tactics over time, and can we tell when our detection is falling behind?

## What the BQ2 run does

1. **Rebuilds the BQ1 final model** (`models/final`), exactly as `rebuild_final.sh` does: LightGBM, 179 features, trained on HI-Small + HI-Medium.
2. **Trains a second, "blind" model** (`models/final_nofan`): same settings, but it **never sees fan-in / fan-out laundering**, like any model facing a genuinely new tactic.
3. **Scores 8 weeks of HI-Large** (weeks 6–13, 5 Sep – 30 Oct 2022) with both models, 2 weeks at a time.
4. **Analyses two scenarios:**
   - **real**: the data as it is.
   - **new_tactic**: fan-in / fan-out laundering is removed from every week **before week 10 (3 Oct)**, so to the monitor it looks like a brand-new tactic appearing in week 10.
5. **Measures:**
   - **BQ2a, are tactics changing?** Each week's mix of laundering (pattern, payment format, amount band, cross-currency, cross-bank) compared with the baseline weeks 6–8. A shift counts if PSI ≥ 0.2 **and** chi-square p < 0.01, two weeks in a row.
   - **BQ2b, is detection falling behind?** Each week's precision, recall (overall, the new tactic, other patterns, unclassified), alerts per day, and schemes alerted before they finish.
     - **Alarm:** recall below the baseline's mean − 2 sd, two weeks in a row.
     - Labels arrive **2 weeks late** in a real bank, so the alarm is only *known* 2 weeks later.
   - **Early warning, no labels:** alerts per day (z-score), shift in the scores of each day's top 2,000 accounts (the "high-risk tail"), shift in all scores, and shift in the top features. All are known the same week, and a shift only counts if it is also significant (p < 0.01).
   - **Lead time** = the week the label alarm is known − the week of the first early warning.
6. **Outputs:** `analytics/bq2/HI-Large.json`, which `bq2_report.py` prints and the Streamlit page `app/app_bq2.py` shows.

## New and changed files (branch `tasha-bq2`)

| File | What |
|---|---|
| `pipeline/rebuild_bq2.sh` | Runs everything in order. **Resumable** (markers in `progress/bq2/`), and skips any step whose output is already in the bucket |
| `pipeline/check_cluster.sh` | Checks the cluster is ready (Java 17, libraries, bucket, Kaggle token, scripts) |
| `pipeline/bq2_score_spark.py` | Scores a date window with both models in one pass |
| `pipeline/bq2_analysis_spark.py` | BQ2a + BQ2b + early warning + alarms |
| `pipeline/bq2_report.py` | Laptop: prints the report, writes `app/data/bq2_data.json` |
| `app/app_bq2.py` | BQ2 Streamlit page |
| `pipeline/train_txn.py`, `final_train.py` | New options `--exclude-patterns`, `--model-dir`, `--stage1-only` (defaults unchanged, so BQ1 runs exactly as before) |
| `pipeline/install_libs.sh` | Enrui's fix that keeps the AWS CLI working after pip installs |

---

# Start to finish (your own AWS Learner Lab, nothing shared)

You need: the repo on your laptop, a Kaggle account, and the Learner Lab.

## Part A · Laptop (15 min)

**1. Get the branch.** If it is already on GitHub, run `git fetch origin` then `git checkout tasha-bq2`. If you only have the bundle file:

```bash
cd ~/IS459-G1T4-AML                     # your copy of the repo
git fetch ~/Downloads/tasha-bq2.bundle tasha-bq2:tasha-bq2
git checkout tasha-bq2
git push -u origin tasha-bq2            # so the team can see it
```

**2. Kaggle API token.**
1. Go to kaggle.com → your picture → **Settings** → **API** → **Create Legacy API Key** (older page: "Create New Token").
2. It downloads `kaggle.json`, a tiny file with your username and key. Keep it private and never commit it.
3. Open the dataset page once while logged in: kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml.

## Part B · AWS setup (30 min)

**3. Start the lab.**
1. Click **Start Lab** and wait for the green dot, then click **AWS**.
2. Top right: region **N. Virginia (us-east-1)**.
3. Note the **budget left** shown on the lab page.

**4. S3 bucket.** Use your bucket from before (`is459-g1t4-aml-tasha`), or create one (S3 → Create bucket, defaults).

**5. Upload the scripts.**
1. In the bucket, **Create folder** `scripts`, open it, then **Upload → Add files**.
2. Select **every file inside your laptop's `pipeline/` folder** (not the folder itself), so the files sit directly in `scripts/`.
3. Check that `s3://<bucket>/scripts/install_libs.sh` exists.

**6. SSH key.**
1. Learner Lab page → **AWS Details** → **Download PEM** (`labsuser.pem`).
2. On a Mac, in Terminal: `mv ~/Downloads/labsuser.pem ~/.ssh/ && chmod 400 ~/.ssh/labsuser.pem`

**7. Create the EMR cluster.** Go to EMR → **Create cluster** and set:

| Setting | Value |
|---|---|
| Name | `aml-bq2` |
| Amazon EMR release | **emr-7.x** (the newest 7.x; it uses Java 17) |
| Application bundle | **Spark** (Spark interactive is fine) |
| Primary | 1 × **m5.xlarge** |
| Core | 4 × **m5.xlarge** (remove the Task group) |
| EBS root volume (if offered) | 30 GiB or more |
| Cluster termination | **Automatically terminate after idle time: 2 hours** (protects your budget; training on the primary can look "idle" for ~30 min, so don't set it shorter) |
| Bootstrap actions → Add | Name `libs`, script location `s3://<bucket>/scripts/install_libs.sh` |
| Security configuration and EC2 key pair | **vockey** |
| Amazon EMR service role | **EMR_DefaultRole** |
| EC2 instance profile | **EMR_EC2_DefaultRole** |

**Create cluster.** It takes about 10–15 minutes to reach **Waiting**. If a role or instance type is refused, send me the exact error; Learner Lab sometimes blocks a choice.

**8. Let your laptop SSH in (once per cluster).**
1. Open the cluster → **Primary node** security group → **Edit inbound rules**.
2. **Add rule:** type **SSH**, source **My IP** → Save.
3. Copy the **Primary node public DNS** from the cluster summary.

## Part C · Run it (a few hours of cluster time; you can leave it running)

**9. Log in and set up.** On your laptop:

```bash
ssh -i ~/.ssh/labsuser.pem hadoop@<primary public DNS>
```

Then, **on the cluster**:

```bash
export AML_BUCKET=is459-g1t4-aml-tasha
echo "export AML_BUCKET=$AML_BUCKET" >> ~/.bashrc
mkdir -p ~/bq2 && cd ~/bq2
aws s3 cp s3://$AML_BUCKET/scripts/ . --recursive
mkdir -p ~/.kaggle && nano ~/.kaggle/kaggle.json   # paste the contents of kaggle.json, Ctrl+O Enter, Ctrl+X
chmod 600 ~/.kaggle/kaggle.json
bash check_cluster.sh
```

Every line of `check_cluster.sh` should say **OK**. If it says it fixed Java, run `source ~/.bashrc` and check again.

**10. Start the run.**

```bash
nohup bash rebuild_bq2.sh > rebuild_bq2.log 2>&1 &
tail -f rebuild_bq2.log        # Ctrl+C stops WATCHING; the run keeps going
```

The log shows a `start` / `done` line per step. The order is:
1. Downloads: 9 files, HI-Large is about 16 GB.
2. Clean + answer key.
3. HI-Small / HI-Medium features.
4. `validate` and `final_train`: the BQ1 model.
5. `validate_nofan` and `final_train_nofan`: the blind model.
6. Per 2-week chunk: `feat_large_*`, `txn_large_*`, `v3_large_*`, `bq2_score_*`.
7. `bq2_analysis`, then **ALL DONE**.

You can close the laptop. The run lives on the cluster.

**11. Keep the lab alive.**
- A lab session lasts 4 hours. Click **Start Lab** again before the timer ends to reset it.
- **If the session ends anyway or the cluster dies:**
  1. Start the lab.
  2. EMR → select the old cluster → **Clone** → Create.
  3. Repeat steps 8–10.

  Finished steps are skipped (their markers are in S3), so you only lose the step that was running.
- **If a step says FAILED**, the log prints its last 20 lines. Send them to me; `logs/<step>.log` has the full log.

**12. When the log says ALL DONE, terminate the cluster** (EMR → Terminate). It costs money every hour it runs.

## Part D · Results on your laptop (10 min)

**13. Credentials.**
1. Learner Lab → AWS Details → AWS CLI → **Show**.
2. Paste the three lines under `[default]` into `~/.aws/credentials`. They change every session.
3. Make sure `~/.aws/config` has `region = us-east-1`.

**14. Report and dashboard.**

```bash
cd ~/IS459-G1T4-AML
pip3 install -r requirements.txt
export AML_ROOT=s3://is459-g1t4-aml-tasha
mkdir -p results/bq2
python3 pipeline/bq2_report.py HI-Large > results/bq2/bq2_report.txt
cd app && streamlit run app_bq2.py
```

**15. Commit the results** (not the data):

```bash
git add results/bq2/bq2_report.txt app/data/bq2_data.json
git commit -m "BQ2 results on HI-Large"
git push
```

---

## Shortcut if a teammate shares their bucket later

The script skips every step whose output already exists. With read access to a teammate's bucket, run this on the cluster before step 10:

```bash
TEAM=<team bucket>
aws s3 sync s3://$TEAM/curated/      s3://$AML_BUCKET/curated/      --exclude "*quarantine*"
aws s3 sync s3://$TEAM/models/final/ s3://$AML_BUCKET/models/final/
aws s3 sync s3://$TEAM/raw/accounts/ s3://$AML_BUCKET/raw/accounts/
```

Bucket policy the owner adds (S3 → bucket → Permissions → Bucket policy):

```json
{"Version": "2012-10-17", "Statement": [{
  "Sid": "Bq2Read", "Effect": "Allow",
  "Principal": {"AWS": "arn:aws:iam::<YOUR_ACCOUNT_ID>:root"},
  "Action": ["s3:ListBucket", "s3:GetObject"],
  "Resource": ["arn:aws:s3:::<TEAM_BUCKET>", "arn:aws:s3:::<TEAM_BUCKET>/*"]}]}
```

## Settings (environment variables, set before step 10)

| Variable | Default | Meaning |
|---|---|---|
| `MON_FROM`, `MON_TO` | `2022-09-05`, `2022-10-30` | Monitored weeks (Monday to Sunday). 10 weeks: `2022-08-29` / `2022-11-06` |
| `HIDE` / `BQ2_HIDE` | `fan_in,fan_out` | The "new tactic" (pattern names as in `pattern_label`) |
| `BQ2_INTRO_WEEK` | 5th monitored week (week 10) | When the new tactic appears |
| `BQ2_BASELINE_WEEKS` | `3` | Weeks that define "normal" |
| `BQ2_LABEL_DELAY` | `2` | Weeks until labels are known |
| `BQ2_TAIL_RANK` | `2000` | Size of the daily "high-risk tail" |

To try another new tactic (e.g. cycles), set `HIDE=cycle` and use a new `--model-dir`. Delete the `validate_nofan` and `final_train_nofan` markers in `progress/bq2/` first.

## Limits

- **Synthetic data.** IBM's simulator may barely change tactics over 2 months, so the **controlled test** is what shows the monitor works. The **real** scenario honestly reports whether tactics shifted.
- **The new tactic is simulated in the evaluation.** Fan-in / fan-out payments still exist in the data before week 10. They're only removed from grading and from the label-free signals.
- **Scoring is end of day**, as in BQ1. Hours-to-alert assume the list is published after the day closes.
- **The "age" features are left out of the drift monitor.** `r_age_days`, `s_age_days` and `t_pair_age_d` count days since an account or pair was first seen *in the data read*. That restarts with every 2-week chunk (21 days of history) and differs between training (HI-Small / HI-Medium read whole, at most 10–16 days) and HI-Large (up to 34 days). Worth flagging for BQ1 too: they're top-10 features, so the model sees ages on HI-Large it never saw in training.
- **Whole-population signals are weak against a small new tactic.** Laundering is a tiny share of all payments; the high-risk-tail and alerts-per-day signals are the ones to watch.

## Local test (synthetic data, before the AWS run)

The whole run (team pipeline + BQ2) was run end to end on synthetic HI-Small / HI-Medium / HI-Large data (Spark local, Java 17), with all 8 patterns, unclassified laundering, and a built-in tactic change from week 10 (more cycles, more Bitcoin).
- Every step ran.
- The blind model dropped the fan-in / fan-out payments from training and validation and set its own threshold.
- **BQ2a** flagged the payment-format shift two weeks after it started.
- **BQ2b:** in the controlled test, the blind model's recall on the new tactic fell to 7–37% in the weeks after it appeared, while the current model stayed higher. The monitor raised "falling behind" for the blind model only.
- The numbers themselves are not results (fake data).
