-- Athena tables over the weekly BQ1 results (analytics/live_*), written by aml_bq1_score's Score and Name steps.
-- The BQ1 dashboard (app/aws_data.py load_bq1_live) queries these, so each finished week shows up on its own.
-- Catalog entries only: no data is copied or changed. Partition projection finds each new week's run_date
-- partition, so no MSCK REPAIR / crawler is needed after a run.
-- analytics.live_results and analytics.live_alerts_named already exist (same layout); these are the other four.
-- config (live_stage2) = the champion model's validated_run (models/<run>/meta.json): add a value here when a
-- model with a new config is promoted.

CREATE EXTERNAL TABLE IF NOT EXISTS analytics.live_daily (
  `date` date, `rows` bigint, laundering bigint, alerts bigint, true_alerts bigint, level string)
PARTITIONED BY (dataset string, run_date string)
STORED AS PARQUET
LOCATION 's3://is459-g1t4-aml/analytics/live_daily/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.dataset.type' = 'enum',
  'projection.dataset.values' = 'HI-Large-live,HI-Medium-live,HI-Medium-test',
  'projection.run_date.type' = 'date',
  'projection.run_date.format' = 'yyyy-MM-dd',
  'projection.run_date.range' = '2022-08-01,2022-12-31',
  'projection.run_date.interval' = '1',
  'projection.run_date.interval.unit' = 'DAYS',
  'storage.location.template' = 's3://is459-g1t4-aml/analytics/live_daily/dataset=${dataset}/run_date=${run_date}/');

CREATE EXTERNAL TABLE IF NOT EXISTS analytics.live_budget (
  level string, k_per_day bigint, alerts bigint, true_alerts bigint, `precision` double, recall double, f1 double)
PARTITIONED BY (dataset string, run_date string)
STORED AS PARQUET
LOCATION 's3://is459-g1t4-aml/analytics/live_budget/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.dataset.type' = 'enum',
  'projection.dataset.values' = 'HI-Large-live,HI-Medium-live,HI-Medium-test',
  'projection.run_date.type' = 'date',
  'projection.run_date.format' = 'yyyy-MM-dd',
  'projection.run_date.range' = '2022-08-01,2022-12-31',
  'projection.run_date.interval' = '1',
  'projection.run_date.interval.unit' = 'DAYS',
  'storage.location.template' = 's3://is459-g1t4-aml/analytics/live_budget/dataset=${dataset}/run_date=${run_date}/');

CREATE EXTERNAL TABLE IF NOT EXISTS analytics.live_patterns (
  pattern_label string, positives bigint, caught bigint, level string, caught_used bigint)
PARTITIONED BY (dataset string, run_date string)
STORED AS PARQUET
LOCATION 's3://is459-g1t4-aml/analytics/live_patterns/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.dataset.type' = 'enum',
  'projection.dataset.values' = 'HI-Large-live,HI-Medium-live,HI-Medium-test',
  'projection.run_date.type' = 'date',
  'projection.run_date.format' = 'yyyy-MM-dd',
  'projection.run_date.range' = '2022-08-01,2022-12-31',
  'projection.run_date.interval' = '1',
  'projection.run_date.interval.unit' = 'DAYS',
  'storage.location.template' = 's3://is459-g1t4-aml/analytics/live_patterns/dataset=${dataset}/run_date=${run_date}/');

CREATE EXTERNAL TABLE IF NOT EXISTS analytics.live_stage2 (
  pattern string, true_cases bigint, named_as bigint, correct bigint,
  `precision` double, recall double, f1 double, mode string, run_at string)
PARTITIONED BY (dataset string, config string, run_date string)
STORED AS PARQUET
LOCATION 's3://is459-g1t4-aml/analytics/live_stage2/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.dataset.type' = 'enum',
  'projection.dataset.values' = 'HI-Large-live,HI-Medium-live,HI-Medium-test',
  'projection.config.type' = 'enum',
  'projection.config.values' = 'txn_v3m_xp',
  'projection.run_date.type' = 'date',
  'projection.run_date.format' = 'yyyy-MM-dd',
  'projection.run_date.range' = '2022-08-01,2022-12-31',
  'projection.run_date.interval' = '1',
  'projection.run_date.interval.unit' = 'DAYS',
  'storage.location.template' = 's3://is459-g1t4-aml/analytics/live_stage2/dataset=${dataset}/config=${config}/run_date=${run_date}/');
