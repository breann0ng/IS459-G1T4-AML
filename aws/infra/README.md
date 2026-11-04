# Terraform: AWS foundation (person 1 applies)

| File | Creates / adopts |
|---|---|
| `backend.tf` | Shared state in `s3://is459-g1t4-aml/tfstate/` with locking |
| `s3.tf` | **Imports** the lake bucket; blocks public access; lifecycle: `logs/` 90 d, `athena-results/` 7 d, `raw/` -> Glacier Instant Retrieval after 90 d, abort stuck uploads |
| `glue_athena.tf` | Glue databases `curated`, `features`, `analytics`; Athena workgroup `aml` (results -> `athena-results/`, 10 GB scan limit per query) |
| `budgets.tf` | Monthly budget, **tracking only** by default (no emails, no deny actions) + a CloudWatch alarm on estimated charges with no actions (shows "In alarm" in CloudWatch). Needs "Receive CloudWatch billing alerts" turned on once in Billing preferences. |
| `iam.tf` + `policies/` | **Imports** the EMR Serverless job role `aml-emr-serverless-job` and its bucket + Glue policy |
| `emr.tf` | **Imports** the EMR Serverless app `00ga27nqsrmqmp09` (16 vCPU / 64 GB cap, auto-stop 15 min, 2 concurrent runs, 120 min queue timeout) |

Later (once each is tested): Step Functions + their CloudWatch log groups (ERROR, 90 d), EventBridge
rule + schedule, Step Functions/EventBridge/crawler roles, Streamlit's read-only IAM user.

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
