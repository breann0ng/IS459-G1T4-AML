# Terraform (to write, person 1)

Creates: S3 bucket layout + lifecycle rules (logs/ 90 d, athena-results/ 7 d, raw/ -> Glacier after 90 d),
EMR Serverless application (max capacity, auto-stop) + job role, Glue databases, Athena workgroup
(scan limit), Step Functions state machines + CloudWatch log groups (ERROR, 90 d), EventBridge rule
(new raw/ file) + weekly schedule, AWS Budgets alerts (no deny actions), team IAM logins.
Pin the provider version and `region = "ap-southeast-2"`.
