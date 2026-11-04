"""
replay_day.py                                         (simulation only: plays "the bank")
Copies one day's split file from staging/ into raw/, which fires the EventBridge rule and starts
the aml-daily Step Functions run. Not part of the pipeline.

    python local/simulate/replay_day.py HI-Large 2022-09-12 [--bucket pw-aml-g1t4]
"""
import argparse
import os
import subprocess

ap = argparse.ArgumentParser()
ap.add_argument("dataset")
ap.add_argument("date", help="YYYY-MM-DD")
ap.add_argument("--bucket", default=os.environ.get("AML_BUCKET", "pw-aml-g1t4"))
a = ap.parse_args()
src = f"s3://{a.bucket}/staging/transactions/set={a.dataset}/date={a.date}/"
dst = f"s3://{a.bucket}/raw/transactions/set={a.dataset}/date={a.date}/"
subprocess.run(["aws", "s3", "cp", "--recursive", src, dst], check=True)
print(f"replayed {a.dataset} {a.date}: {src} -> {dst}")
