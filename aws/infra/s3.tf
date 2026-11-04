# The lake bucket already exists: imported, never recreated, never destroyed by Terraform.
import {
  to = aws_s3_bucket.lake
  id = var.bucket
}

resource "aws_s3_bucket" "lake" {
  bucket = var.bucket
  lifecycle { prevent_destroy = true }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# NOTE: this resource owns ALL lifecycle rules on the bucket; add new rules here, not in the console.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "logs-expire-90d" # Spark event logs + EMR Serverless job logs
    status = "Enabled"
    filter { prefix = "logs/" }
    expiration { days = 90 }
  }

  rule {
    id     = "athena-results-expire-7d" # query output only, never data
    status = "Enabled"
    filter { prefix = "athena-results/" }
    expiration { days = 7 }
  }

  rule {
    id     = "raw-to-glacier-ir-90d" # bronze kept for replay/audit; Instant Retrieval so rebuilds still read it
    status = "Enabled"
    filter { prefix = "raw/" }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }

  rule {
    id     = "abort-incomplete-uploads-7d" # streamed Kaggle uploads that died halfway
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}
