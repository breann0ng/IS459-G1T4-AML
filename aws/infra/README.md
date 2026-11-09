# Terraform: AWS foundation (person 1 applies)

| File | Creates / adopts |
|---|---|
| `backend.tf` | Shared state in `s3://is459-g1t4-aml/tfstate/` with locking |
| `s3.tf` | **Imports** the lake bucket; blocks public access; lifecycle: `logs/` 90 d, `athena-results/` 7 d, `raw/` -> Glacier Instant Retrieval after 90 d, abort stuck uploads |
| `glue_athena.tf` | Glue databases `curated`, `features`, `analytics`; Athena workgroup `aml` (results -> `athena-results/`, 10 GB scan limit per query) |
| `budgets.tf` | Monthly AWS Budget, **tracking only** by default (no emails, no deny actions). No CloudWatch billing alarm: billing metrics are controlled by the payer account. |
| `iam.tf` + `policies/` | **Imports** the EMR Serverless job role `aml-emr-serverless-job` and its bucket + Glue policy. `policies/` also holds the Step Functions and EventBridge role policies used by `orchestration.tf` |
| `emr.tf` | **Imports** the EMR Serverless app `00ga27nqsrmqmp09` (16 vCPU / 64 GB cap, auto-stop 15 min, 2 concurrent runs, 120 min queue timeout) |

| `orchestration.tf` | BQ1: EMR apps `aml-bq1` / `aml-bq1-test`; S3 -> EventBridge; state machines `aml_prepare`, `aml_bq1_score`, `aml_bq1_train` (definitions from `aws/orchestration/state-machines/`, rule patterns from `aws/orchestration/eventbridge/`) and their roles `aml-sfn-pipeline` / `aml-events-pipeline`; rules `aml-prepare-weekly-file` and `aml-bq1-data-prepared` and their targets; failure emails (`aml-bq1-run-ended` -> SNS `aml-bq1-weekly-alerts`); topic `aml-bq1-alerts`. The CLI-built pieces are adopted with `import` blocks |
| `bq2_weekly.tf` | BQ2 SAML-D: `aml_bq2_weekly` (definition changes are deployed by the BQ2 team, not by apply), its role, its own EMR app `aml-bq2-saml`, weekly schedule (off), SNS `aml-bq2-alerts`, SQS approval queue, approver user |
| `dashboard_ec2.tf` + `dashboard_user_data.sh` | Streamlit dashboard EC2 `aml-dashboard` (t4g.micro), its role/instance profile and security group. The user data file is byte-for-byte what the instance runs: changing it replaces the instance |

| `bq2_weekly_ibm.tf` | BQ2 IBM HI-Large: `aml_bq2_weekly_ibm` (definition deployed by the BQ2 team, not by apply), its log group and its bq2_ibm/ policies (SFN role, dashboard, approver); adopted with `import` blocks |
Retired 2026-11-09 and removed from the config (apply deletes them): `aml_bq1_weekly`, `aml-bq1-weekly-file`,
`aml_bq1_abort`, `aml-bq1-run-aborted`, their roles and log groups.

## First run (from a laptop with Terraform >= 1.10 and AWS credentials for us-east-1, or CloudShell)

```bash
cd aws/infra
terraform init
terraform plan                                    # READ THIS before applying
terraform apply
```

**What a good first plan looks like:** "4 to import" (bucket, job role, its policy, the EMR app)
and only small *in-place* changes (tags, the new lifecycle/public-access/budget/Glue/Athena resources
being *created*). **Stop if the plan says it will destroy or replace the bucket, the role or the EMR app**:
that means a setting here differs from what exists (e.g. the release label: check with
`aws emr-serverless get-application --application-id 00ga27nqsrmqmp09 --query application.releaseLabel`).

If the bucket already has lifecycle rules made in the console, `s3.tf` replaces them: copy any you need in first.
