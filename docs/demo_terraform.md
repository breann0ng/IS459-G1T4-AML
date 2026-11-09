# Demo script: one Terraform for BQ1 + BQ2, then each pipeline

About 10 minutes. **DO** = what is on screen; **SAY** = what the speaker says, word for word or close to it.
Speakers: **Breann** (scenes 1-5), **Wei Ting** (scene 6), **Tasha** (scene 7).
Region us-east-1, bucket `is459-g1t4-aml`, profile `is459`. **Blur the account ID** (top-right menu, ARNs) in editing.

## Before filming (not filmed)

- `git pull` on main; dry run `cd aws/infra && AWS_PROFILE=is459 terraform init && AWS_PROFILE=is459 terraform plan`.
  It must say **16 to import, 1 to add, 9 to change, 14 to destroy**. Anything else: stop and check.
- Team: nobody applies Terraform from another branch, and no BQ2 week is started during filming.
- Wei Ting checks why `aml_bq2_weekly` failed on 2026-11-09 06:07 UTC.
- Tabs open: Step Functions > State machines; EventBridge > Rules; S3 > `is459-g1t4-aml`. Terminal font large, zoom 125%.

---

## Scene 1 · The infrastructure is code (~2 min, Breann)

**DO:** Editor, folder `aws/infra/` expanded.
**SAY:** "Everything our pipeline runs on in AWS is defined in this one folder. BQ1, BQ2, the dashboard: one
Terraform configuration, so nothing important exists only because someone clicked it in the console."

**DO:** Open `backend.tf`.
**SAY:** "The Terraform state is shared in our S3 bucket, with a lock file. All four of us plan and apply against
the same view of AWS, and two people can never apply at the same time."

**DO:** Open `orchestration.tf`, scroll to the three `aws_sfn_state_machine` blocks.
**SAY:** "This is BQ1. Three state machines: `aml_prepare` cleans the data and builds the features,
`aml_bq1_score` scores each week, and `aml_bq1_train` trains a new model. Each definition is just a JSON file
from our repo, so the workflow is versioned in git like any other code."

**DO:** Scroll to the two `aws_cloudwatch_event_rule` blocks.
**SAY:** "These two rules make it event-driven. When a bank's weekly file lands in S3, the first rule starts
`aml_prepare`. When the data is ready, `aml_prepare` publishes an event, and the second rule starts scoring. No
schedule, no one pressing a button."

**DO:** Open `bq2_weekly.tf`, then `bq2_weekly_ibm.tf`.
**SAY:** "BQ2 is in the same configuration: Wei Ting's pipeline on the SAML-D data and Tasha's on IBM HI-Large.
They share one role, one alert topic and one approval queue, so a person has to approve before any model is
retrained or deployed."

**DO:** Scroll to an `import { ... }` block.
**SAY:** "Some of these pipelines were first built by hand while we were testing. These import blocks adopt them
into Terraform instead of recreating them, so nothing running is interrupted."

**DO:** Open `aws/orchestration/state-machines/aml_bq1_score.asl.json` for a few seconds.
**SAY:** "And this is what a definition looks like: the exact JSON Step Functions runs."

## Scene 2 · Plan and apply (~2 min, Breann; cut the waiting)

**DO:** Terminal: `cd aws/infra` then `AWS_PROFILE=is459 terraform plan`. Scroll to the summary line.
**SAY:** "Before changing anything, Terraform shows exactly what it will do. Sixteen imports: the pipelines we
built by hand, now taken over by code. Nine small changes, mostly tags. One new log group. And fourteen
deletions: that is our first version of BQ1, one big state machine for the whole week, which we replaced with
the separate prepare and score pipelines."

**DO:** `AWS_PROFILE=is459 terraform apply`, type `yes`. Cut to `Apply complete!`.
**SAY:** "Apply."

**DO:** `AWS_PROFILE=is459 terraform plan` again.
**SAY:** "And now: no changes. What is running in AWS is exactly what is in git."

**DO:** Browser: Step Functions > State machines.
**SAY:** "Five pipelines: prepare, train and score for BQ1, and the two BQ2 pipelines."

**DO:** EventBridge > Rules; hover `aml-prepare-weekly-file`, `aml-bq1-data-prepared`, `aml-bq1-run-ended`.
**SAY:** "And the three rules: start on a new file, start scoring when the data is ready, and email the team if
any run fails."

---

## Scene 3 · `aml_prepare` (~1.5 min, Breann) - run live

**DO:** Open `aml_prepare` > Definition graph.
**SAY:** "`aml_prepare` is the data stage both BQ1 and BQ2 use. It cleans a week of payments, extracts the
laundering patterns, and builds three sets of features, each one a Spark job on EMR Serverless."

**DO:** Start execution, input:
```json
{"dataset": "HI-Large-live", "week": "2022-10-24"}
```
**SAY:** "Let me ask it to prepare the week of the 24th of October."

**DO:** The run succeeds in seconds. Open S3 `progress/prepare/HI-Large-live/2022-10-24.json`.
**SAY:** "It finished straight away, because that week is already prepared. Every week has a marker file like
this one, recording each step, its job and how long it took. So if BQ1 and BQ2 both ask for the same week, the
work is done only once, and a rerun never duplicates data."

**DO:** Open past execution `29bde462-...` (2026-11-09 00:58 UTC) > Graph view.
**SAY:** "Here is the real run for that week: clean, patterns, then the three feature jobs, about seventy
minutes for nearly fifteen million payments. At the end it publishes a `DataPrepared` event, which is what
starts scoring."

## Scene 4 · `aml_bq1_train` (~1 min, Breann) - show `train-nofan-20261108-b`

**DO:** Open the execution > Input tab.
**SAY:** "Training is started by hand, rarely. This run trained on HI-Small and HI-Medium only. HI-Large is our
test data and the model never sees it."

**DO:** Graph view; click the gate step.
**SAY:** "It prepares the training data, trains the detector and the pattern namer, and then hits a quality
gate. The new model only replaces the current one if it passes on validation data. Otherwise we get an email and
nothing changes."

**DO:** Open S3 `models/bq1_champion.json`.
**SAY:** "This file points at the model in production. This challenger passed the gate, but when we tested it on
new weeks it did worse than the old model, so we rolled the pointer back. Because scoring always reads this one
file, switching models is a one-line change, with no redeploy."

## Scene 5 · `aml_bq1_score` (~1.5 min, Breann) - show `cap-rescore-0912-1024-...`

**DO:** Open the execution > Input tab.
**SAY:** "This is the weekly scoring pipeline. Normally the `DataPrepared` event starts it for one week. Here we
ran our seven test weeks, mid-September to the end of October."

**DO:** Graph view; click **Score**.
**SAY:** "For each week: read which model is in production, score every payment, and decide the alerts. An
account becomes an alert above the model's threshold, plus accounts it sent money to that also look risky. And
then a daily capacity: analysts can review 2,500 alerts a day, so on a busy day only the 2,500 most suspicious
go out."

**DO:** Click **Name**.
**SAY:** "Stage two groups linked alerts and names the laundering pattern, like fan-out or cycle, so the analyst
knows what they are looking at."

**DO:** Click **WeekCheck**; open S3 `progress/bq1/HI-Large-live/2022-09-26/check.json`.
**SAY:** "Then a check on the analysts' workload. This week it lists the 30th of September as a capped day. That
was a month-end payment burst: twice the usual payments, and the model raised over thirteen thousand alerts.
The cap kept the 2,500 most suspicious, and the check flags the week so the team knows it happened."

**DO:** Show the email in `is459g1t4@gmail.com` (optional).
**SAY:** "And if any run fails, the whole team gets an email like this."

## Scene 5b · The results in Athena and on the dashboard (~2 min, Breann)

**DO:** Athena > Query editor, workgroup **aml**, database **analytics**. Run:
```sql
SELECT run_date, alerts, true_alerts, ROUND("precision", 3) AS "precision", ROUND(recall, 3) AS recall
FROM analytics.live_results
WHERE dataset = 'HI-Large-live' AND level = 'account_day' AND cutoff = 'capacity2500'
ORDER BY run_date;
```
**SAY:** "Every week the pipeline writes its results to S3 as Parquet, and Athena queries them with plain SQL.
One row per week: the alerts analysts got, how many were real, and how much laundering they caught. When a new
week is scored, its row appears here by itself; there is no loading step."

**DO:** Run:
```sql
SELECT date, level, alerts, ROUND(true_alerts * 1.0 / alerts, 2) AS share_real
FROM analytics.live_daily
WHERE dataset = 'HI-Large-live' AND level IN ('account_day_raised', 'account_day_used')
  AND date IN (DATE '2022-09-30', DATE '2022-10-30')
ORDER BY date, level;
```
**SAY:** "And here are the two payment-burst days. 'Raised' is what the model wanted to send: over ten thousand
alerts, and only about one in five real. 'Used' is what analysts actually got: 2,500, and most of them real."

**DO:** Browser: the dashboard > BQ1 page. Point at the headline, then the **Daily capacity** box.
**SAY:** "The dashboard reads the same tables through Athena, so it always shows the latest weeks. The headline
is what analysts actually get. This box names the days the capacity was reached and shows what the numbers would
be without it, so nothing is hidden."

**DO:** Drag the slider down to 1,000, then back to the model's setting.
**SAY:** "And this is the decision the Tuning Team makes: how many alerts a day. Fewer alerts are almost all
real but catch less laundering; the model's setting is the balance point."

---

## Scene 6 · `aml_bq2_weekly` (~1 min, Wei Ting) - show `week14-fix-151200`

**DO:** Open `aml_bq2_weekly` > Definition graph.
**SAY:** "BQ2 asks whether criminals are changing their tactics, and whether our detection is falling because
of it. This pipeline runs once a week on the SAML-D data."

**DO:** Open the execution > Input: `{"week_index": 14}`.
**SAY:** "It keeps track of which week it is up to, so each run does the next week. Here it ran week 14."

**DO:** Graph view; the parallel branch.
**SAY:** "Two checks run side by side: are the patterns of laundering shifting, and is the live model catching
less of it."

**DO:** Click the approval step (`WaitRetrain`).
**SAY:** "If both say yes, it emails us and stops here, waiting for a person to approve retraining on the
dashboard. A model is never retrained or deployed without a human signing off: once to retrain, and once more
to deploy."

## Scene 7 · `aml_bq2_weekly_ibm` (~1 min, Tasha) - show `auto-index7-...`

**DO:** Open `aml_bq2_weekly_ibm` > the execution > Input: `{"week_index": 7}`.
**SAY:** "This is the same BQ2 flow on the IBM HI-Large data, real for our purposes, one week per run."

**DO:** Graph view.
**SAY:** "HI-Large's tactics don't change on their own, so we also run a test version where fan-in and fan-out
laundering is hidden until the 3rd of October. To the pipeline it looks like a brand-new tactic appearing, and
we can check that it notices the shift and that retraining wins back detection."

**DO:** Back to the state machine list.
**SAY:** "So that is the whole system: one Terraform configuration, three BQ1 pipelines and two BQ2 pipelines,
all event-driven, all in git, and a person approving every model change."

---

## If something goes wrong on camera

- `plan` shows a destroy that is not listed above: stop, do not apply. Someone applied from another branch; compare
  `terraform state list` with `aws/infra/*.tf`.
- `Error acquiring the state lock`: someone else is applying. Wait; never `force-unlock` during their apply.
- An execution fails: the failure email (rule `aml-bq1-run-ended`) is itself worth showing.
