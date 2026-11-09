# The AWS pipeline: which data goes through which state machine

Account 390402569482, region us-east-1, bucket `is459-g1t4-aml`. Every step is one EMR Serverless
job. Every state machine is in [`aws/step-functions/`](../aws/step-functions/) exactly as it runs.

```
                       ┌──────────────────────────── aml_prepare (shared) ────────────────────────────┐
 HI-Small  (whole) ───►│                                                                              │──► aml_bq1_train
 HI-Medium (whole) ───►│  [ConvertSaml: SAML only] → Clean → ParsePatterns → FeaturesV2 → FeaturesTxn  │      (detector, namer,
 HI-Large-live (week)─►│                                                      → FeaturesV3             │       gate, champion)
 SAML-R / SAML-C ─────►│  progress marker per dataset/week: a second caller returns at once           │──► aml_bq1_score
                       └──────────────────────────────────────────────────────────────────────────────┘      (HI-Large weeks)
                                                    │
                                                    └──► aml_bq2_weekly (SAML-R)
 HI-Large / HI-Large-NT ───────────────────────────────► aml_bq2_weekly_ibm (own data area, see below)
```

| Dataset | Prepared by | Then | What it answers |
|---|---|---|---|
| HI-Small, HI-Medium | `aml_prepare` (whole file) | `aml_bq1_train` | BQ1: train the detector + namer; gate on validation days; promote the champion |
| HI-Large-live (weekly files) | `aml_prepare` (one week) | `aml_bq1_score` | BQ1: score each new week with the champion, name the patterns, check analyst load |
| SAML-R (ramp) / SAML-C (control) | `aml_prepare` (whole; SAML-D converted first) | `aml_bq2_weekly` | BQ2: do tactics change, does detection drop (R1–R3), does retraining recover it |
| HI-Large-NT (fan-in / fan-out hidden before 2022-10-03) | `aml_bq2_weekly_ibm` (own features) | its v0 model scores the weeks, v1 is the challenger | BQ2 on real IBM data: is a "new" tactic caught |

SAML-D has its own columns, so `aml_prepare` first converts it to the IBM layout
(`saml_ramp_spark.py`, writes both SAML-R and SAML-C). From Clean onwards every dataset takes the
same steps and the same code.

## aml_prepare

Input `{"dataset": "HI-Large-live", "week": "2022-09-12"}` (one weekly file) or
`{"dataset": "HI-Medium"}` / `{"dataset": "SAML-R"}` (whole dataset). Options: `features` (default
true; false = clean only), `force`, `force_features`, `convert` (SAML: redo the conversion),
`check_previous` (week mode: the previous week must be cleaned, for the 21-day look-back), `emit`
(publish `DataPrepared` when done), `app_id`, `code_prefix`, `feature_version`.

Progress marker: `s3://is459-g1t4-aml/progress/prepare/<dataset>/<week|full>.json`. A finished
dataset or week returns at once when its feature version matches the requested version.
Interrupted runs preserve completed feature stages and resume from the first unfinished one.
An execution known to be running blocks a second caller, including a forced call. If its status
cannot be checked, the lookup is retried and the caller fails without changing the marker.
The S3 marker check is still not an atomic lock: simultaneous first callers can race.

Feature releases use one version for V2, transaction features, and V3. The default is
`2026-11-09.3`, set in `Setup.Assign.feature_version` in `aml_prepare.asl.json`. When any feature
builder changes, upload the updated scripts and `jobs.zip`, bump that default, and deploy the
state machine. All existing callers then use the new default. Editing Python alone does not
invalidate completion markers. The version is a completion label, not a code selector; the
S3 entry scripts and `jobs.zip` must contain the intended code.

- A missing or different marker version resets all feature flags and rebuilds
  `FeaturesV2 -> FeaturesTxn -> FeaturesV3`, preserving completed cleaning. Legacy markers
  without a version therefore trigger one feature rebuild when features are next requested.
- The same version retains saved checkpoints. For example, a V3 failure resumes at V3.
- `force_features: true` resets all feature flags for the requested version.
- `force: true` resets cleaning and feature progress. A failure records only progress from
  that rebuild, rather than inheriting completion flags from an older successful run.
- `features: false` preserves the actual saved feature version; a forced clean clears it.

A direct preparation call can override the default, for example:

```json
{"dataset": "HI-Medium", "feature_version": "2026-11-09.4"}
```

Use that same override when resuming the run. For normal pipeline releases, bump the default
instead so nested BQ1/BQ2 callers request the same version. Output table names remain unchanged;
different versions are not stored as separate feature tables.

Local workflow regression checks evaluate the actual JSONata expressions with simulated service
responses (no AWS calls): install `pytest` and `jsonata-python==0.7.1`, then run
`python -m pytest tests/test_aml_prepare.py`. Validate the definition with AWS before deployment;
local checks do not replace an EMR integration run.

Sizing (tested on HI-Medium, 8 Nov 2026):

| Step | Driver | Executors | Notes |
|---|---|---|---|
| Clean | 1 vCPU, 4 GB | 1 × 4 vCPU, 10 + 2 GB, 100 GB disk | A whole HI-Medium needs the 100 GB disk. 3 executors were slower on one week (262 s vs 186–234 s) |
| FeaturesV2 | 2 vCPU, 10 + 4 GB | ≤ 3 × 2 vCPU, 11 + 5 GB, 100 GB disk | Each feature family is checkpointed, so the driver stays small. 6 + 2 GB was killed in the cycles family |
| FeaturesTxn, FeaturesV3 | 4 vCPU, 20 + 2 GB | ≤ 3 × 4 vCPU, 10 + 3 GB | |

Every job waits about 100 s for EMR workers to start. Pre-initialised capacity cuts that to about
3 s, but it is billed while the app is started, so it is off. Turn it on for a long backfill only.

The 9 fan-in / fan-out / gather-scatter window features cost about 9 minutes per FeaturesV2 run.
The builder computes them only while some model still uses them: the BQ1 champion,
`models/final` (the recipe BQ2 retrains with) or any `models/bq2r/<dataset>/<version>`.
`AML_FAN_WINDOWS=1|0` forces it either way.

## aml_bq1_train

Input `{"train_datasets": ["HI-Small", "HI-Medium"]}`. Optional `"drop_features": "fan_windows"`
trains without the 9 window features. On validation (last 25 % of days) account-day F1 was 0.6690
without them vs 0.6694 with them, inside the seed-to-seed range of 0.6680 to 0.6707.

Steps:
1. `aml_prepare` for each training set (in parallel).
2. TrainDetector, then TrainNamer, then FinalTrain.
3. QualityGate checks precision, F1 and alerts per day on validation.
4. Promote the model to `models/bq1_champion.json`, or keep the old champion and send an SNS message.

It never reads HI-Large: that is the one-time test set.

## aml_bq1_score

Input: the `DataPrepared` event, or `{"dataset": "HI-Large-live", "week": "2022-09-12"}`, or a
backfill `{"first_week", "score_from", "last_week"}`. For each week:
1. `aml_prepare` the week.
2. Score it with the champion.
3. Name the patterns.
4. WeekCheck: SNS when the alerts per day exceed what the analysts can review.

## aml_bq2_weekly (SAML-R) and aml_bq2_weekly_ibm (HI-Large)

Each execution simulates one week:
- **Part 1** checks whether tactics changed: JS divergence > max(0.10, leave-one-out noise) and
  chi-square p < 0.01. This is a report and does not trigger retraining.
- **Part 2** scores the week with the deployed version and evaluates it after a 28-day label lag.
  It alerts when one of these holds for 2 weeks in a row:
  - R1: recall < validation − 0.10, with at least 30 cases.
  - R2: recall on a new typology < 0.50, with at least 20 cases.
  - R3: recall on a known typology < validation − 0.20, with at least 20 cases.
- **On an alert:** SNS, then a person approves retraining (SQS task token). The challenger v1 is
  trained, scored and compared with v0. A second approval deploys it.

`aml_bq2_weekly` gets its SAML-R features from `aml_prepare` (step `PrepareData`, on the BQ2 EMR
app). `aml_bq2_weekly_ibm` runs the shared code in `s3://is459-g1t4-aml/jobs/` but keeps its own
data area `s3://is459-g1t4-aml/bq2_ibm/` (its own HI-Large clean and features, and the
`final_nofan` model). Moving it onto `aml_prepare`'s `HI-Large-live` tables needs one HI-Large run
to check, so it has not been done.

## Deploy

```bash
AML_BUCKET=is459-g1t4-aml bash aws/deploy/deploy.sh        # code: every aws/jobs/*/* -> s3://.../jobs/ + jobs.zip
aws stepfunctions update-state-machine \
  --state-machine-arn arn:aws:states:us-east-1:390402569482:stateMachine:aml_prepare \
  --definition file://aws/step-functions/aml_prepare.asl.json   # same for the other machines
```

See [`aws/step-functions/README.md`](../aws/step-functions/README.md) for what is live, what is
not deployed yet, and the IAM and EventBridge changes that go with it.
