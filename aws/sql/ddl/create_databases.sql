-- Glue Data Catalog databases, one per lake layer. Run once in Athena (replace <bucket>).
CREATE DATABASE IF NOT EXISTS curated   LOCATION 's3://<bucket>/curated/';
CREATE DATABASE IF NOT EXISTS features  LOCATION 's3://<bucket>/features/';
CREATE DATABASE IF NOT EXISTS analytics LOCATION 's3://<bucket>/analytics/';
