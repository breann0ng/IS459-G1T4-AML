# Glue Data Catalog: one database per lake layer (replaces aws/sql/ddl/create_databases.sql).
resource "aws_glue_catalog_database" "layer" {
  for_each     = toset(["curated", "features", "analytics"])
  name         = each.key
  location_uri = "s3://${var.bucket}/${each.key}/"
}

resource "aws_athena_workgroup" "aml" {
  name = "aml"
  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_scan_limit_gb * 1024 * 1024 * 1024
    result_configuration {
      output_location = "s3://${var.bucket}/athena-results/"
    }
  }
}
