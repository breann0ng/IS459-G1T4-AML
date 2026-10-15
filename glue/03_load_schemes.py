"""Glue Spark job: parse *_Patterns.txt into the scheme table.

Replaces the Glue Python job from the proposal.

Input:  s3://<bucket>/landing/dataset=<name>/<name>_Patterns.txt
Output: DynamoDB table aml-schemes (PK scheme_id, SK txn_key, GSI on txn_key)
        s3://<bucket>/curated/schemes/ (Parquet copy for bulk Spark joins)

Approach: spark.read.text -> flag "BEGIN LAUNDERING ATTEMPT" lines -> running
sum of the flag in file order = scheme_id -> parse the CSV lines in between.

TODO (owner: Lesley)
"""
