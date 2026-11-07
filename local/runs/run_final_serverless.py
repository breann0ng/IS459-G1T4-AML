"""
run_final_serverless.py                       (laptop: submits jobs, does no data work itself)
The BQ1 final run on EMR Serverless: local/legacy-emr/rebuild_final.sh without the cluster.
Every step is one EMR Serverless job run, submitted in order and waited on. RESUMABLE: a
finished step leaves s3://<bucket>/progress/serverless/<step>.done, so after a failure fix
it and run the same command again.

    python local/runs/run_final_serverless.py --dry-run            print every job, submit nothing
    python local/runs/run_final_serverless.py                      full run (HI-Large, every day)
    python local/runs/run_final_serverless.py --only clean_HI-Small    one step (ignores its marker)
    python local/runs/run_final_serverless.py --until final_train      stop after that step
    python local/runs/run_final_serverless.py --list                   step names, done or not

Before the first run (see the steps in local/runs/README.md):
    raw/ has HI-Small, HI-Medium, HI-Large       (aws/jobs/load/load_kaggle.sh, in CloudShell)
    jobs/jobs.zip + entry scripts                (aws/deploy/deploy.sh, from main)
    jobs/venv.tar.gz                             (aws/deploy/build_venv.sh, needs Docker)

Commands are the ones in aws/config/pipeline.yaml (stage1_cmd, stage2_cmd, final_train_cmd).
"""
import argparse
import datetime as dt
import shlex
import sys
import time

BUCKET = "is459-g1t4-aml"
REGION = "us-east-1"
APP_ID = "00ga27nqsrmqmp09"
JOB_ROLE = "aml-emr-serverless-job"
LARGE_FROM, LARGE_TO = "2022-08-01", "2022-11-06"   # HI-Large: Monday to Sunday around every payment
CHUNK_WEEKS = 2                                     # each feature chunk also reads 21 days before it

STAGE1 = ("train_txn.py --txn-table txn_features_v3 --stage2-features features_v3 --preset p3 "
          "--merge-layering --expand auto --config txn_v3m_xp")
STAGE2 = ("group_stage2.py txn_v3m_xp --link-days 3 --param n_estimators=800 --param learning_rate=0.03 "
          "--keep-rates 1,0.7,0.5,0.3 --context --group-vote --max-linked-days 20 "
          "--out-config enrui_breann_mld20")
FINAL_TRAIN = "final_train.py txn_v3m_xp enrui_breann_mld20_grp"

# Worker sizes. The app is capped at 16 vCPU / 64 GB in total, and memory + overhead must fit the
# worker (4 vCPU: up to 30 GB, 8 vCPU: up to 60 GB).
SPARK = {   # Spark jobs: driver + up to 3 executors = 16 vCPU
    "spark.driver.cores": "4", "spark.driver.memory": "12g", "spark.driver.maxResultSize": "2g",
    "spark.executor.cores": "4", "spark.executor.memory": "10g", "spark.executor.memoryOverhead": "3g",
    "spark.dynamicAllocation.maxExecutors": "3",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
}
DRIVER_ONLY = {   # plain Python (pandas / LightGBM on one machine): one big driver, no executors
    "spark.driver.cores": "8", "spark.driver.memory": "48g",
    "spark.dynamicAllocation.initialExecutors": "0", "spark.dynamicAllocation.minExecutors": "0",
    "spark.dynamicAllocation.maxExecutors": "1",
    "spark.emr-serverless.driver.disk": "100g",
}


def weeks(start, end, n):
    """Monday-to-Sunday chunks of n weeks covering start..end."""
    d, last = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    while d <= last:
        e = min(d + dt.timedelta(weeks=n) - dt.timedelta(days=1), last)
        yield d.isoformat(), e.isoformat()
        d = e + dt.timedelta(days=1)


def steps(score_from, score_to):
    """(name, kind, 'script args') in run order; mirrors rebuild_final.sh."""
    s = []
    for d in ["HI-Small", "HI-Medium", "HI-Large"]:
        s += [(f"clean_{d}", "spark", f"clean_transactions_spark.py {d}"),
              (f"patterns_{d}", "spark", f"parse_patterns_spark.py {d}")]
    for d in ["HI-Small", "HI-Medium"]:
        s += [(f"feat_{d}", "spark", f"build_features_spark.py {d}"),
              (f"txn_{d}", "spark", f"build_txn_features_spark.py {d} features_v2"),
              (f"v3_{d}", "spark", f"build_features_v3_spark.py {d}")]
    s += [("validate", "driver", STAGE1),
          ("stage2", "driver", STAGE2),
          ("final_train", "driver", FINAL_TRAIN)]
    for a, b in weeks(LARGE_FROM, LARGE_TO, CHUNK_WEEKS):
        s += [(f"feat_HI-Large_{a}", "spark", f"build_features_spark.py HI-Large {a} {b}"),
              (f"txn_HI-Large_{a}", "spark", f"build_txn_features_spark.py HI-Large features_v2 {a} {b}"),
              (f"v3_HI-Large_{a}", "spark", f"build_features_v3_spark.py HI-Large {a} {b}")]
    window = f" {score_from} {score_to}" if score_from else ""
    s += [("score", "spark", f"score_final_spark.py HI-Large{window}"),
          ("name", "driver", "final_group_namer.py HI-Large")]
    return s


def spark_params(kind, bucket):
    env = "./environment/bin/python"
    conf = {
        "spark.archives": f"s3://{bucket}/jobs/venv.tar.gz#environment",
        "spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON": env,
        "spark.emr-serverless.driverEnv.PYSPARK_PYTHON": env,
        "spark.executorEnv.PYSPARK_PYTHON": env,
        # the jobs default to the old bucket; AML_ROOT points them at this one
        "spark.emr-serverless.driverEnv.AML_ROOT": f"s3://{bucket}",
        "spark.executorEnv.AML_ROOT": f"s3://{bucket}",
        "spark.emr-serverless.driverEnv.AWS_REGION": REGION,
        "spark.executorEnv.AWS_REGION": REGION,
        **(SPARK if kind == "spark" else DRIVER_ONLY),
    }
    return f"--py-files s3://{bucket}/jobs/jobs.zip " + " ".join(f"--conf {k}={v}" for k, v in conf.items())


def job_request(name, kind, cmd, bucket, role_arn):
    script, *args = shlex.split(cmd)
    return {
        "applicationId": APP_ID, "executionRoleArn": role_arn, "name": f"aml-{name}",
        "jobDriver": {"sparkSubmit": {"entryPoint": f"s3://{bucket}/jobs/{script}",
                                      "entryPointArguments": args,
                                      "sparkSubmitParameters": spark_params(kind, bucket)}},
        "configurationOverrides": {"monitoringConfiguration": {
            "s3MonitoringConfiguration": {"logUri": f"s3://{bucket}/logs/emr_serverless/"}}},
        "executionTimeoutMinutes": 600,
        "tags": {"project": "aml", "step": name},
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", default=BUCKET)
    ap.add_argument("--profile", help="AWS profile for the shared account")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--only", help="run just this step (even if done)")
    ap.add_argument("--until", help="stop after this step")
    ap.add_argument("--score-from", help="score only these dates (default: every HI-Large day)")
    ap.add_argument("--score-to")
    a = ap.parse_args()
    plan = steps(a.score_from, a.score_to)
    names = [n for n, _, _ in plan]
    for x in (a.only, a.until):
        if x and x not in names:
            sys.exit(f"unknown step {x}; --list shows them")

    if a.dry_run:
        for n, k, c in plan:
            if a.only and n != a.only:
                continue
            r = job_request(n, k, c, a.bucket, f"arn:aws:iam::<account>:role/{JOB_ROLE}")
            print(f"--- {n} [{k}]\n    {r['jobDriver']['sparkSubmit']['entryPoint']} "
                  f"{' '.join(r['jobDriver']['sparkSubmit']['entryPointArguments'])}")
            if n == names[0] or a.only:
                print(f"    {r['jobDriver']['sparkSubmit']['sparkSubmitParameters']}")
            if n == a.until:
                break
        return

    import boto3
    sess = boto3.Session(profile_name=a.profile, region_name=REGION)
    s3, emr = sess.client("s3"), sess.client("emr-serverless")
    role_arn = f"arn:aws:iam::{sess.client('sts').get_caller_identity()['Account']}:role/{JOB_ROLE}"
    marker = lambda n: f"progress/serverless/{n}.done"

    def done(n):
        try:
            s3.head_object(Bucket=a.bucket, Key=marker(n))
            return True
        except s3.exceptions.ClientError:
            return False

    if a.list:
        for n, k, _ in plan:
            print(f"{'done' if done(n) else '    '}  {n:32s} {k}")
        return

    t0 = time.time()
    for n, k, c in plan:
        if a.only and n != a.only:
            continue
        if not a.only and done(n):
            print(f"=== skip {n} (done)")
            continue
        start = time.time()
        # laptop slept / window closed mid-run: re-attach to that job instead of starting it twice
        live = [j for p in emr.get_paginator("list_job_runs").paginate(
                    applicationId=APP_ID, states=["SUBMITTED", "PENDING", "SCHEDULED", "RUNNING", "SUCCESS"])
                for j in p["jobRuns"] if j["name"] == f"aml-{n}"] if not a.only else []
        if live:
            run_id = max(live, key=lambda j: j["createdAt"])["id"]
            print(f"=== {time.strftime('%m-%d %H:%M')} attach {n}  ({run_id}, already submitted)", flush=True)
        else:
            run_id = emr.start_job_run(**job_request(n, k, c, a.bucket, role_arn))["jobRunId"]
            print(f"=== {time.strftime('%m-%d %H:%M')} start {n}  ({run_id})", flush=True)
        state = None
        while state not in ("SUCCESS", "FAILED", "CANCELLED"):
            time.sleep(30)
            run = emr.get_job_run(applicationId=APP_ID, jobRunId=run_id)["jobRun"]
            if run["state"] != state:
                state = run["state"]
                print(f"    {time.strftime('%H:%M')} {state}", flush=True)
        mins = (time.time() - start) / 60
        if state != "SUCCESS":
            print(f"=== FAILED {n} after {mins:.0f} min: {run.get('stateDetails', '')}\n"
                  f"    driver log: s3://{a.bucket}/logs/emr_serverless/applications/{APP_ID}/jobs/{run_id}/"
                  "SPARK_DRIVER/stderr.gz\n    fix it, then run the same command again (finished steps are skipped)")
            sys.exit(1)
        use = run.get("totalResourceUtilization", {})
        s3.put_object(Bucket=a.bucket, Key=marker(n), Body=f"{run_id}\n".encode())
        print(f"=== done {n}: {mins:.0f} min, {use.get('vCPUHour', 0):.2f} vCPU-h, "
              f"{use.get('memoryGBHour', 0):.1f} GB-h", flush=True)
        if n == a.until:
            break
    if a.only or a.until:
        print(f"=== stopped as asked ({(time.time() - t0) / 60:.0f} min)")
        return
    print(f"=== ALL DONE in {(time.time() - t0) / 3600:.1f} h. Laptop: "
          f"AML_ROOT=s3://{a.bucket} python local/reports/final_report.py HI-Large > results/final/final_report.txt")


if __name__ == "__main__":
    main()
