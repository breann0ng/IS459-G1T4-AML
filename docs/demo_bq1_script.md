# BQ1 demo: talk track and Q&A prep (En Rui)

About 4 minutes, live on the BQ1 site (`cd app` → `streamlit run bq1_site.py`, see the README).
Backup screenshots, in order, in `docs/figures/demo/` (1_overview.png … 5_bq1_archived_findings.png).
Every number below is on the dashboard (HI-Large test, 14 unseen days, 12–25 Sep 2022, 24.4M payments, scored by
the weekly Step Functions pipeline; the Speed tab's scheme timings come from the one-off run of the same days).
If more weeks are added, re-read the numbers off the page before presenting.

## Talk track

**1 · The problem (Overview page, BQ1 card) · 30 s**
> Banks flag far more than analysts can check. In Singapore's S$3B case, 8 of 9 fined banks had flagged transactions
> that nobody properly investigated. So BQ1 asks: can we catch laundering early while keeping false alerts low enough
> for analysts to manage? We built this for the Transaction Monitoring Tuning Team, who set the daily alert volume.

**2 · Alerts and accuracy (BQ1 page, first tab) · 60 s**
> *[Point at the four numbers.]* At the model's own setting the team gets about 2,300 alerts a day, and 89% are real
> laundering. *[Drag the slider to 1,000, then back.]* Fewer alerts are almost all real; more alerts add mostly false
> ones: past the model's setting only about 14 in 100 extra alerts are real.
> **So the 48% of laundering we miss won't be found by hiring more analysts: it needs new signals in the model.**
> *[Scroll to Two queues.]* One cheap fix we found: alerts our pattern namer can't name are only 17% real. Moving them
> to a second queue lifts the main queue to 94.4% real while it still catches 51% of laundering.

**3 · Speed (second tab) · 60 s**
> 93% of schemes get an alert at some point, but only 47% while they are still running. The median alert comes after
> 13 hours, and the median scheme lasts 17. *[Point at the window chart, Archived findings tab, or say it.]*
> **The model isn't the slow part; the daily schedule is.** A scheme has to run about a day to be caught in time by a
> daily alert: 40% of 6–24 hour schemes, 88% of 1–3 day schemes. Scoring payments as they arrive would catch 93% of
> schemes under 6 hours. That is the case for live scoring, an architecture change, not a model change.

**4 · Who checks what (third tab) · 45 s**
> Each pattern gets red, yellow or green from cut-offs fixed on validation data before testing.
> **Analysts are needed to catch more and catch sooner, not to filter noise**: every named pattern's alerts are 93–99%
> real. Red is random (finishes too fast) and unclassified laundering (no known shape, almost never caught).

**5 · Archived findings: two guesses (fourth tab) · 30 s**
> Each alert also names its likely pattern, so the analyst knows where to start. On HI-Large one guess is right 35% of
> the time and two guesses 61%: reliable for layering, a hint for the rest. A wrong name never hides an alert: the risk
> score alone decides that.

**6 · Close · 15 s**
> In one line: 9 in 10 alerts are real at a volume a team can staff, but half the schemes finish before a daily alert
> arrives, so the next step is live scoring and new signals for the laundering that follows no known pattern.

## Likely questions, with answers

| Question | Answer (and where it is) |
|---|---|
| Why is recall only 52%? | About 40% of HI-Large laundering follows no known pattern, and the model catches under 1% of it. On laundering with a known pattern it catches about 85%. (Alerts tab, takeaway box) |
| Did you tune on the test set? | No. The threshold and the red/yellow/green cut-offs were fixed on HI-Small and HI-Medium validation days; HI-Large was scored once. The two-queue idea was spotted on test, so we checked it on validation first: 91.2% → 94.1% and 89.7% → 92.1%. |
| Why only 14 days of HI-Large? | It is a held-out test of 24.4M payments, never seen in training. These results come from the team's weekly Step Functions pipeline, which scores one week at a time with the same frozen model; more weeks are added as they finish. |
| Precision dropped from an earlier run? | 0.901 → 0.898 with identical PR-AUC (0.662): the model ranks the same; only the retrained threshold moved. That is noise, not a regression. |
| Why LightGBM? | Fast on 179 tabular features at this scale, handles imbalance, and runs inside Spark on EMR Serverless (the model is sent to the data). |
| What is "early"? | Measured against each scheme's own first and last payment from the answer key, not a fixed clock. (Speed tab) |
| Why is naming so much worse on HI-Large (35% / 61%) than on validation (54% / 81%)? | Most HI-Large patterns get named layering, the most common pattern, when the namer sees too little of the scheme (often a single payment). That is why the page shows two names and calls them a starting hint, not a verdict. |
| Would this work on real bank data? | IBM's data is synthetic; real volumes and patterns differ. The pipeline and the way we measure (precision at a daily budget, caught while running) carry over; the numbers would need re-measuring. |
| What does it cost to run? | Each step is an EMR Serverless job that stops after 15 idle minutes, so cost follows data. The feature jobs are the bulk: about 11 vCPU-hours for one week of data (measured on a HI-Medium week). |
| Why use the one-off run for speed? | The weekly pipeline scores one week at a time, so a scheme that crosses a week boundary is cut short: its timing would look shorter than it was. The one-off run scored the same 14 days in one go and measures whole schemes. |
