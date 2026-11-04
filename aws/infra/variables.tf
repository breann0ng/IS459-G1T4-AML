variable "region" {
  type    = string
  default = "us-east-1" # the bucket's region: never mix regions
}

variable "bucket" {
  type    = string
  default = "is459-g1t4-aml"
}

variable "emr_application_id" {
  type        = string
  default     = "00ga27nqsrmqmp09"
  description = "EMR Serverless app created in the console; imported, not recreated"
}

variable "emr_release_label" {
  type        = string
  default     = "emr-spark-8.0"
  description = "Check with: aws emr-serverless get-application --application-id <id> --query application.releaseLabel"
}

variable "job_role_name" {
  type    = string
  default = "aml-emr-serverless-job"
}

variable "budget_limit_usd" {
  type    = number
  default = 50
}

variable "budget_emails" {
  type        = list(string)
  description = "Who gets the cost alerts (person 1). Set in terraform.tfvars."
}

variable "athena_scan_limit_gb" {
  type    = number
  default = 10 # per query; stops one bad query scanning the whole lake
}
