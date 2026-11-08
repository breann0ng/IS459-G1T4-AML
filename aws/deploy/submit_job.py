"""Submit one job to EMR Serverless. DRY RUN by default: prints the command, starts nothing.
    python aws/deploy/submit_job.py bq2_part1_tactics_spark.py SAML-R 2022-10-03 2023-01-29          # dry run
    python aws/deploy/submit_job.py bq2_part1_tactics_spark.py SAML-R 2022-10-03 2023-01-29 --run   # starts a job (costs money)
    python aws/deploy/submit_job.py bq2_part2_train.py SAML-R v0 2023-01-29 --python --run           # Python-only job
Settings: AML_BUCKET (default is459-g1t4-aml), AWS_PROFILE, EMR_APP_ID. Mirrors the HI-Small run that succeeded.
"""
import json
import os
import subprocess
import sys

BUCKET = os.environ.get("AML_BUCKET", "is459-g1t4-aml")
APP = os.environ.get("EMR_APP_ID", "00ga27nqsrmqmp09")
ROLE = os.environ.get("EMR_JOB_ROLE_ARN")          # default: looked up from the account id below
REGION = os.environ.get("AWS_REGION", "us-east-1")
PROFILE = os.environ.get("AWS_PROFILE", "my-profile")
AWS = os.path.join(os.environ.get("LOCALAPPDATA", ""), "Programs", "Amazon", "AWSCLIV2", "aws.exe")
AWS = AWS if os.path.exists(AWS) else "aws"

args = sys.argv[1:]
run, python_only = "--run" in args, "--python" in args
args = [a for a in args if a not in ("--run", "--python")]
if not args:
    sys.exit(__doc__)
script, entry_args = args[0], args[1:]


def aws(*cmd):
    return subprocess.run([AWS, *cmd, "--profile", PROFILE, "--region", REGION],
                          capture_output=True, text=True, check=True).stdout


role = ROLE or "arn:aws:iam::" + json.loads(aws("sts", "get-caller-identity"))["Account"] + ":role/aml-emr-serverless-job"
env = [f"spark.emr-serverless.driverEnv.AML_ROOT=s3://{BUCKET}", f"spark.executorEnv.AML_ROOT=s3://{BUCKET}",
       f"spark.emr-serverless.driverEnv.AWS_REGION={REGION}", f"spark.executorEnv.AWS_REGION={REGION}"]
py = ["spark.archives=s3://%s/jobs/venv.tar.gz#environment" % BUCKET,
      "spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python",
      "spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python",
      "spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"]
sizing = ["spark.driver.cores=4", "spark.driver.memory=12g", "spark.driver.maxResultSize=2g",
          "spark.sql.autoBroadcastJoinThreshold=-1"]
if python_only:                                    # no executors needed: the script runs on the driver
    sizing += ["spark.executor.instances=1", "spark.dynamicAllocation.initialExecutors=1",
               "spark.dynamicAllocation.maxExecutors=1", "spark.executor.cores=1", "spark.executor.memory=2g"]
else:
    # override per run if executors get killed for memory, e.g. EXEC_CORES=2 EXEC_MEM=12g
    sizing += [f"spark.executor.cores={os.environ.get('EXEC_CORES', 4)}",
               f"spark.executor.memory={os.environ.get('EXEC_MEM', '10g')}", "spark.executor.memoryOverhead=3g",
               f"spark.dynamicAllocation.maxExecutors={os.environ.get('MAX_EXEC', 3)}"]
params = f"--py-files s3://{BUCKET}/jobs/jobs.zip " + " ".join(f"--conf {c}" for c in py + env + sizing)
driver = {"sparkSubmit": {"entryPoint": f"s3://{BUCKET}/jobs/{script}", "entryPointArguments": entry_args,
                          "sparkSubmitParameters": params}}
monitor = {"monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": f"s3://{BUCKET}/logs/emr_serverless/"}}}
name = "aml-" + script.rsplit(".", 1)[0] + ("-" + "-".join(entry_args[:2]) if entry_args else "")
cmd = ["emr-serverless", "start-job-run", "--application-id", APP, "--execution-role-arn", role,
       "--name", name[:100], "--job-driver", json.dumps(driver), "--configuration-overrides", json.dumps(monitor)]
if not run:
    print("DRY RUN (add --run to start; this costs money):\n", json.dumps(driver, indent=1), "\nrole:", role)
else:
    print(aws(*cmd))
