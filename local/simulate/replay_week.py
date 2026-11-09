"""
replay_week.py                                        (simulation only: plays "the bank")
Copies one week's file from staging/ into raw/, then writes an empty _SUCCESS marker last.
Only the marker matches the EventBridge rule aml-prepare-weekly-file, so each replayed week starts
exactly one aml_prepare run (the part files' own S3 events are ignored). Not part of the pipeline.

    python local/simulate/replay_week.py HI-Large-demo 2022-09-12 [--wait] [--bucket is459-g1t4-aml]
    python local/simulate/replay_week.py SAML-R-demo 2023-01-30 --wait

--wait   poll aml_prepare's progress marker (progress/prepare/<dataset>/<week>.json) until the run
         finishes. Use it when replaying several weeks in a row: a week's features read the 21 days
         before it, so aml_prepare refuses a week whose previous week was delivered but not cleaned.
         The first week of a feed (no earlier week in raw/) needs nothing before it.

Weeks are Mondays. staging/ is filled once by aws/jobs/setup/split_weeks_spark.py.
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
ap.add_argument("--wait", action="store_true", help="wait until aml_prepare finishes this week")
ap.add_argument("--timeout-min", type=int, default=90)
a = ap.parse_args()

if dt.date.fromisoformat(a.week).weekday() != 0:
    sys.exit(f"{a.week} is not a Monday: weeks are named by their Monday")

src = f"s3://{a.bucket}/staging/transactions/set={a.dataset}/week={a.week}/"
dst = f"s3://{a.bucket}/raw/transactions/set={a.dataset}/week={a.week}/"
log = f"s3://{a.bucket}/progress/prepare/{a.dataset}/{a.week}.json"

subprocess.run(["aws", "s3", "cp", "--recursive", src, dst, "--exclude", "_SUCCESS"], check=True)
# Marker goes last: when it lands, every part file is already in raw/.
subprocess.run(["aws", "s3", "cp", "-", dst + "_SUCCESS"], input=b"", check=True)
print(f"replayed {a.dataset} week {a.week}: {src} -> {dst} (+ _SUCCESS -> starts aml_prepare)")

if a.wait:
    deadline = time.time() + a.timeout_min * 60
    status = None
    while time.time() < deadline:
        time.sleep(30)
        r = subprocess.run(["aws", "s3", "cp", log, "-"], capture_output=True, text=True)
        body = json.loads(r.stdout) if r.returncode == 0 and r.stdout.strip() else {}
        body = json.loads(body) if isinstance(body, str) else body   # Step Functions stores it as a quoted JSON string
        status = body.get("status")
        print(f"  {time.strftime('%H:%M:%S')} aml_prepare: {status or 'not started yet'}"
              + (f" ({body['steps'][-1]['step']} done)" if body.get("steps") else ""))
        if status in ("SUCCEEDED", "SKIPPED", "FAILED"):
            break
    sys.exit(0 if status == "SUCCEEDED" else f"week {a.week} did not succeed: {status}")
