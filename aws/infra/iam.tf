# Job role every EMR Serverless job runs as (made in the console on 2026-11-04, imported here).
# The service-linked role AWSServiceRoleForAmazonEMRServerless is AWS's own and is not managed here.
import {
  to = aws_iam_role.emr_job
  id = var.job_role_name
}

import {
  to = aws_iam_role_policy.emr_job
  id = "${var.job_role_name}:aml-bucket-and-glue"
}

resource "aws_iam_role" "emr_job" {
  name               = var.job_role_name
  assume_role_policy = file("${path.module}/policies/emr_job_role_trust.json")
}

resource "aws_iam_role_policy" "emr_job" {
  name   = "aml-bucket-and-glue"
  role   = aws_iam_role.emr_job.id
  policy = templatefile("${path.module}/policies/emr_job_role.json.tftpl", { bucket = var.bucket })
}
