"""
replay_week.py                                        (simulation only: plays "the bank")
Copies one week's file from staging/ into raw/, then writes an empty _SUCCESS marker last.
Only the marker matches the EventBridge rule, so each replayed week starts exactly one
aml_bq1_weekly Step Functions run (the part files' own S3 events are ignored). Not part of the pipeline.

    python local/simulate/replay_week.py HI-Large-live 2022-09-12 [--wait] [--bucket is459-g1t4-aml]

--wait   poll the run log (progress/bq1_weekly/<dataset>/<week>.json) until the run finishes.
         Use it when replaying several weeks in a row (e.g. the 3 history weeks): a week's features
         read the weeks before it, so the state machine refuses a week whose previous week is not done.

Weeks are Mondays. staging/ is filled once by aws/jobs/load/split_weeks_spark.py.
"""
import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time

ap = argparse.ArgumentParser()
ap.add_argument("dataset")
ap.add_argument("week", help="Monday of the week, YYYY-MM-DD")
ap.add_argument("--bucket", default=os.environ.get("AML_BUCKET", "is459-g1t4-aml"))
ap.add_argument("--wait", action="store_true", help="wait until aml_bq1_weekly finishes this week")
ap.add_argument("--timeout-min", type=int, default=90)
a = ap.parse_args()

if dt.date.fromisoformat(a.week).weekday() != 0:
    sys.exit(f"{a.week} is not a Monday: weeks are named by their Monday")

src = f"s3://{a.bucket}/staging/transactions/set={a.dataset}/week={a.week}/"
dst = f"s3://{a.bucket}/raw/transactions/set={a.dataset}/week={a.week}/"
log = f"s3://{a.bucket}/progress/bq1_weekly/{a.dataset}/{a.week}.json"

subprocess.run(["aws", "s3", "cp", "--recursive", src, dst, "--exclude", "_SUCCESS"], check=True)
# Marker goes last: when it lands, every part file is already in raw/.
subprocess.run(["aws", "s3", "cp", "-", dst + "_SUCCESS"], input=b"", check=True)
print(f"replayed {a.dataset} week {a.week}: {src} -> {dst} (+ _SUCCESS -> starts aml_bq1_weekly)")

if a.wait:
    deadline = time.time() + a.timeout_min * 60
    status = None
    while time.time() < deadline:
        time.sleep(30)
        r = subprocess.run(["aws", "s3", "cp", log, "-"], capture_output=True, text=True)
        body = json.loads(r.stdout) if r.returncode == 0 and r.stdout.strip() else {}
        body = json.loads(body) if isinstance(body, str) else body   # Step Functions stores it as a quoted JSON string
        status = body.get("status")
        print(f"  {time.strftime('%H:%M:%S')} run log: {status or 'not started yet'}")
        if status in ("SUCCEEDED", "SKIPPED", "FAILED"):
            break
    sys.exit(0 if status in ("SUCCEEDED", "SKIPPED") else f"week {a.week} did not succeed: {status}")
