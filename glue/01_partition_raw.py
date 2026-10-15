"""Glue Spark job: split raw *_Trans.csv into weekly partitions.

Replaces the pandas splitter from the proposal.

Input:  s3://<bucket>/landing/dataset=<name>/<name>_Trans.csv
Output: s3://<bucket>/raw/dataset=<name>/week=<NN>/  (CSV, partitioned)

TODO (owner: ___):
- read with glue.common.schema.RAW_TRANS_SCHEMA (header=True, ignore header names)
- compute week number from week_start() relative to the first Monday in the data
- write partitionBy("dataset", "week")
"""
