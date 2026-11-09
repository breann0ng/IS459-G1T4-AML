-- BQ2 analytics tables (external, read-only over Parquet written by the EMR jobs).
-- Run in workgroup `aml`, database `analytics`. Replace ${BUCKET} with the team bucket name.
-- Partition projection on `dataset` means no MSCK REPAIR is ever needed.
-- Do NOT create a table over analytics/bq2r_scores (917 MB); the dashboard never needs it.

CREATE EXTERNAL TABLE IF NOT EXISTS analytics.bq2r_design (
  week_start string, typology string, `group` string,
  payments_saml_d bigint, payments_ramp bigint, payments_control bigint, dose double)
PARTITIONED BY (dataset string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/analytics/bq2r_design/'
TBLPROPERTIES ('projection.enabled'='true','projection.dataset.type'='enum',
  'projection.dataset.values'='SAML-R,SAML-C,HI-Large',
  'storage.location.template'='s3://${BUCKET}/analytics/bq2r_design/dataset=${dataset}/');

-- Part 1: tactic drift. Same schema for the *_nb (no-balance) variants; create both.
CREATE EXTERNAL TABLE IF NOT EXISTS analytics.bq2r_part1_summary (
  population string, dimension string, js_threshold double, baseline_weeks bigint,
  weeks_after bigint, weeks_changed bigint, weeks_lasting bigint, weeks_tactic_change bigint,
  first_change string, max_js_after double, change_started string,
  cusum_alarms string, cusum_unconfirmed string)
PARTITIONED BY (dataset string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/analytics/bq2r_part1_summary/'
TBLPROPERTIES ('projection.enabled'='true','projection.dataset.type'='enum',
  'projection.dataset.values'='SAML-R,SAML-C,HI-Large',
  'storage.location.template'='s3://${BUCKET}/analytics/bq2r_part1_summary/dataset=${dataset}/');

CREATE EXTERNAL TABLE IF NOT EXISTS analytics.bq2r_part1_summary_nb (
  population string, dimension string, js_threshold double, baseline_weeks bigint,
  weeks_after bigint, weeks_changed bigint, weeks_lasting bigint, weeks_tactic_change bigint,
  first_change string, max_js_after double, change_started string,
  cusum_alarms string, cusum_unconfirmed string)
PARTITIONED BY (dataset string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/analytics/bq2r_part1_summary_nb/'
TBLPROPERTIES ('projection.enabled'='true','projection.dataset.type'='enum',
  'projection.dataset.values'='SAML-R,SAML-C,HI-Large',
  'storage.location.template'='s3://${BUCKET}/analytics/bq2r_part1_summary_nb/dataset=${dataset}/');

CREATE EXTERNAL TABLE IF NOT EXISTS analytics.bq2r_part1_weekly (
  population string, dimension string, week_start string, n bigint, in_baseline boolean,
  js double, psi double, p_value double, js_threshold double, changed boolean,
  lasting boolean, honest_changed boolean, tactic_change boolean, month_end_week boolean)
PARTITIONED BY (dataset string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/analytics/bq2r_part1_weekly/'
TBLPROPERTIES ('projection.enabled'='true','projection.dataset.type'='enum',
  'projection.dataset.values'='SAML-R,SAML-C,HI-Large',
  'storage.location.template'='s3://${BUCKET}/analytics/bq2r_part1_weekly/dataset=${dataset}/');

CREATE EXTERNAL TABLE IF NOT EXISTS analytics.bq2r_part1_weekly_nb (
  population string, dimension string, week_start string, n bigint, in_baseline boolean,
  js double, psi double, p_value double, js_threshold double, changed boolean,
  lasting boolean, honest_changed boolean, tactic_change boolean, month_end_week boolean)
PARTITIONED BY (dataset string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/analytics/bq2r_part1_weekly_nb/'
TBLPROPERTIES ('projection.enabled'='true','projection.dataset.type'='enum',
  'projection.dataset.values'='SAML-R,SAML-C,HI-Large',
  'storage.location.template'='s3://${BUCKET}/analytics/bq2r_part1_weekly_nb/dataset=${dataset}/');

CREATE EXTERNAL TABLE IF NOT EXISTS analytics.bq2r_part1_shares (
  population string, dimension string, category string, week_start string, n bigint,
  share double, baseline_share double, z double, cusum_up double, cusum_down double, alarm boolean)
PARTITIONED BY (dataset string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/analytics/bq2r_part1_shares/'
TBLPROPERTIES ('projection.enabled'='true','projection.dataset.type'='enum',
  'projection.dataset.values'='SAML-R,SAML-C,HI-Large',
  'storage.location.template'='s3://${BUCKET}/analytics/bq2r_part1_shares/dataset=${dataset}/');

-- Part 2: detection performance
CREATE EXTERNAL TABLE IF NOT EXISTS analytics.bq2r_part2_weekly (
  version string, week_start string, payments bigint, alerts bigint, `precision` double,
  honest_flag_rate double, `group` string, cases bigint, caught bigint, recall double)
PARTITIONED BY (dataset string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/analytics/bq2r_part2_weekly/'
TBLPROPERTIES ('projection.enabled'='true','projection.dataset.type'='enum',
  'projection.dataset.values'='SAML-R,SAML-C',
  'storage.location.template'='s3://${BUCKET}/analytics/bq2r_part2_weekly/dataset=${dataset}/');

-- rules / rules_v1 and typology / typology_v1 are the v0 and v1 outputs of the same eval job.
CREATE EXTERNAL TABLE IF NOT EXISTS analytics.bq2r_part2_rules (
  week_start string, cases bigint, recall double, baseline_recall double,
  r1_recall_drop boolean, r2_new_typology string, r3_typology_drop string,
  any_rule boolean, alert boolean)
PARTITIONED BY (dataset string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/analytics/bq2r_part2_rules/'
TBLPROPERTIES ('projection.enabled'='true','projection.dataset.type'='enum',
  'projection.dataset.values'='SAML-R,SAML-C',
  'storage.location.template'='s3://${BUCKET}/analytics/bq2r_part2_rules/dataset=${dataset}/');

CREATE EXTERNAL TABLE IF NOT EXISTS analytics.bq2r_part2_rules_v1 (
  week_start string, cases bigint, recall double, baseline_recall double,
  r1_recall_drop boolean, r2_new_typology string, r3_typology_drop string,
  any_rule boolean, alert boolean, version string)
PARTITIONED BY (dataset string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/analytics/bq2r_part2_rules_v1/'
TBLPROPERTIES ('projection.enabled'='true','projection.dataset.type'='enum',
  'projection.dataset.values'='SAML-R,SAML-C',
  'storage.location.template'='s3://${BUCKET}/analytics/bq2r_part2_rules_v1/dataset=${dataset}/');

CREATE EXTERNAL TABLE IF NOT EXISTS analytics.bq2r_part2_typology (
  version string, period string, `group` string, typology string,
  cases bigint, caught bigint, recall double)
PARTITIONED BY (dataset string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/analytics/bq2r_part2_typology/'
TBLPROPERTIES ('projection.enabled'='true','projection.dataset.type'='enum',
  'projection.dataset.values'='SAML-R,SAML-C',
  'storage.location.template'='s3://${BUCKET}/analytics/bq2r_part2_typology/dataset=${dataset}/');

CREATE EXTERNAL TABLE IF NOT EXISTS analytics.bq2r_part2_typology_v1 (
  version string, period string, `group` string, typology string,
  cases bigint, caught bigint, recall double)
PARTITIONED BY (dataset string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/analytics/bq2r_part2_typology_v1/'
TBLPROPERTIES ('projection.enabled'='true','projection.dataset.type'='enum',
  'projection.dataset.values'='SAML-R,SAML-C',
  'storage.location.template'='s3://${BUCKET}/analytics/bq2r_part2_typology_v1/dataset=${dataset}/');
