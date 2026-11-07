"""
replay_day.py                                         (simulation only: plays "the bank")
Copies one day's split file from staging/ into raw/, which fires the EventBridge rule and starts
the aml-daily Step Functions run. Not part of the pipeline. Works from PowerShell or CloudShell
(boto3, no AWS CLI needed).

    python local/simulate/replay_day.py HI-Large 2022-09-12 --profile g1t4
Stage the days first: local/runs/run_final_serverless.py --stage-days HI-Large 2022-09-12 2022-09-25 ...
"""
import argparse
import os

import boto3

ap = argparse.ArgumentParser()
ap.add_argument("dataset")
ap.add_argument("date", help="YYYY-MM-DD")
ap.add_argument("--bucket", default=os.environ.get("AML_BUCKET", "is459-g1t4-aml"))
ap.add_argument("--profile", help="AWS profile for the shared account (g1t4)")
a = ap.parse_args()

s3 = boto3.Session(profile_name=a.profile, region_name="us-east-1").client("s3")
src = f"staging/transactions/set={a.dataset}/date={a.date}/"
dst = f"raw/transactions/set={a.dataset}/date={a.date}/"
keys = [o["Key"] for p in s3.get_paginator("list_objects_v2").paginate(Bucket=a.bucket, Prefix=src)
        for o in p.get("Contents", []) if o["Key"].endswith(".csv")]
if not keys:
    raise SystemExit(f"nothing staged at s3://{a.bucket}/{src}: run the --stage-days step first")
for k in keys:
    s3.copy_object(Bucket=a.bucket, Key=dst + k[len(src):], CopySource={"Bucket": a.bucket, "Key": k})
print(f"replayed {a.dataset} {a.date}: {len(keys)} file(s) s3://{a.bucket}/{src} -> {dst}")
