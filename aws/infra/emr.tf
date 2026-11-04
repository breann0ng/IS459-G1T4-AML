# EMR Serverless application created in the console (2026-11-04) and proven with the HI-Small
# clean job (99 s, 0.396 vCPU-h). Imported: Terraform adopts it instead of creating a second one.
import {
  to = aws_emrserverless_application.jobs
  id = var.emr_application_id
}

resource "aws_emrserverless_application" "jobs" {
  name          = "aml-jobs"
  release_label = var.emr_release_label
  type          = "spark"
  architecture  = "X86_64"

  auto_start_configuration { enabled = true }
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15 # no idle billing
  }

  maximum_capacity { # the auto-scaling ceiling AND the cost cap
    cpu    = "16 vCPU"
    memory = "64 GB"
  }

  scheduler_configuration {
    max_concurrent_runs   = 2   # a 3rd job queues instead of failing on capacity
    queue_timeout_minutes = 120 # a jammed queue fails the same afternoon
  }

  lifecycle {
    prevent_destroy = true
    # Set in the console (Glue Data Catalog as metastore, Java 17, job-level cost allocation).
    # Not managed here yet so the import cannot wipe them; bring them in once `terraform plan`
    # shows their exact values.
    ignore_changes = [runtime_configuration, monitoring_configuration, interactive_configuration]
  }
}
