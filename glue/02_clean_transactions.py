"""Glue Spark job: clean one week of transactions into the curated Iceberg table.

Input:  s3://<bucket>/raw/dataset=<name>/week=<NN>/
Output: curated/transactions (Iceberg, partitioned by dataset, week)

TODO (owner: ___):
- parse timestamp, build from_key / to_key with account_key()
- add is_self_transfer, is_cross_currency, txn_key
- drop exact duplicates and log how many were removed
"""
