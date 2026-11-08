"""Generate aws/step-functions/aml_bq2_weekly.json (the weekly BQ2 state machine). Creates nothing in AWS.
    python aws/step-functions/build_bq2_weekly.py --bucket is459-g1t4-aml --app-id 00ga27nqsrmqmp09 \
        --role-arn arn:aws:iam::<acct>:role/aml-emr-serverless-job --topic-arn <sns> --queue-url <sqs>
One execution = one simulated week. The week comes from a pointer file (progress/bq2_weekly.json) and a
calendar baked into the definition; or pass {"week_index": N} to run a given week.
Human gates: a message with the task token goes to the SQS approval queue; the dashboard reads it and calls
SendTaskSuccess / SendTaskFailure. No Lambda, no automatic retries (every retry costs money).
"""
import argparse
import json
from datetime import date, timedelta

ap = argparse.ArgumentParser()
ap.add_argument("--bucket", required=True)
ap.add_argument("--app-id", required=True)
ap.add_argument("--role-arn", required=True)
ap.add_argument("--topic-arn", required=True)
ap.add_argument("--queue-url", required=True)
ap.add_argument("--region", default="us-east-1")
ap.add_argument("--first-week", default="2023-01-30")      # first week after the baseline (weeks 1-17)
ap.add_argument("--last-week", default="2023-08-14")       # the data ends 2023-08-23
ap.add_argument("--base-from", default="2022-10-03")
ap.add_argument("--base-to", default="2023-01-29")
ap.add_argument("--dataset", default="SAML-R")
ap.add_argument("--dashboard-url", default="", help="optional: the approval emails link to <url>/?approve=<dataset>")
ap.add_argument("--out", default="aws/step-functions/aml_bq2_weekly.json")
a = ap.parse_args()

starts, d = [], date.fromisoformat(a.first_week)
while d <= date.fromisoformat(a.last_week):
    starts.append(d.isoformat())
    d += timedelta(days=7)
ends = [(date.fromisoformat(s) + timedelta(days=6)).isoformat() for s in starts]

ROOT = f"s3://{a.bucket}"
PTR = "progress/bq2_weekly.json"
MODEL = f"models/bq2r/{a.dataset}"
COMMON = [f"spark.emr-serverless.driverEnv.AML_ROOT={ROOT}", f"spark.executorEnv.AML_ROOT={ROOT}",
          f"spark.emr-serverless.driverEnv.AWS_REGION={a.region}", f"spark.executorEnv.AWS_REGION={a.region}",
          f"spark.archives={ROOT}/jobs/venv.tar.gz#environment",
          "spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python",
          "spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python",
          "spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python",
          "spark.driver.cores=4", "spark.driver.memory=12g", "spark.driver.maxResultSize=2g",
          "spark.sql.autoBroadcastJoinThreshold=-1"]
SPARK = COMMON + ["spark.executor.cores=4", "spark.executor.memory=10g", "spark.executor.memoryOverhead=3g",
                  "spark.dynamicAllocation.maxExecutors=3"]
PYONLY = COMMON + ["spark.executor.cores=1", "spark.executor.memory=2g", "spark.executor.instances=1",
                   "spark.dynamicAllocation.initialExecutors=1", "spark.dynamicAllocation.maxExecutors=1"]


def conf(c):
    return f"--py-files {ROOT}/jobs/jobs.zip " + " ".join(f"--conf {x}" for x in c)


def emr(name, script, args, nxt, python_only=True, **extra):
    """Run one EMR Serverless job and wait for it (.sync). `args` is an ASL intrinsic or a list of constants."""
    drv = {"EntryPoint": f"{ROOT}/jobs/{script}", "SparkSubmitParameters": conf(PYONLY if python_only else SPARK)}
    if isinstance(args, str):
        drv["EntryPointArguments.$"] = args
    else:
        drv["EntryPointArguments"] = args
    st = {"Type": "Task", "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",
          "Parameters": {"ApplicationId": a.app_id, "ClientToken.$": "States.UUID()", "ExecutionRoleArn": a.role_arn,
                         "Name": name, "ExecutionTimeoutMinutes": 45, "JobDriver": {"SparkSubmit": drv},
                         "ConfigurationOverrides": {"MonitoringConfiguration": {
                             "S3MonitoringConfiguration": {"LogUri": f"{ROOT}/logs/emr_serverless/"}}}},
          "ResultPath": None}
    st.update(extra)
    st.update({"Next": nxt} if nxt else {"End": True})
    return st


def get_json(key, nxt, path, **extra):
    st = {"Type": "Task", "Resource": "arn:aws:states:::aws-sdk:s3:getObject",
          "Parameters": {"Bucket": a.bucket, "Key": key}, "ResultSelector": {"v.$": "States.StringToJson($.Body)"},
          "ResultPath": path, "Next": nxt}
    st.update(extra)
    return st


def approval(gate, text, nxt, on_reject, extra_ctx=None):
    body = {"token.$": "$$.Task.Token", "gate": gate, "dataset": a.dataset, "week_end.$": "$.week_end",
            "decision.$": "$.decision.v", "message": text}
    body.update(extra_ctx or {})
    return {"Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
            "Parameters": {"QueueUrl": a.queue_url, "MessageBody": body},
            "TimeoutSeconds": 604800, "ResultPath": f"$.{gate}",
            "Catch": [{"ErrorEquals": ["States.ALL"], "ResultPath": "$.rejected", "Next": on_reject}], "Next": nxt}


def notify(subject, text, nxt):
    if a.dashboard_url:          # the link opens the approval page for this dataset's waiting request
        text += f" Review and decide: {a.dashboard_url.rstrip('/')}/?approve={a.dataset}"
    return {"Type": "Task", "Resource": "arn:aws:states:::sns:publish",
            "Parameters": {"TopicArn": a.topic_arn, "Subject": subject,
                           "Message.$": f"States.Format('{text}', $.dataset, $.week_end)"},
            "ResultPath": None, "Next": nxt}


ds = a.dataset
S = {
    # which week: an explicit {"week_index": N} input, else the pointer file, else week 0
    "HasWeek": {"Type": "Choice", "Choices": [{"Variable": "$.week_index", "IsPresent": True, "Next": "InRange?"}],
                "Default": "ReadPointer"},
    "ReadPointer": get_json(PTR, "UsePointer", "$.ptr", Catch=[
        {"ErrorEquals": ["States.ALL"], "ResultPath": None, "Next": "StartAtZero"}]),
    "UsePointer": {"Type": "Pass", "Parameters": {"week_index.$": "$.ptr.v.week_index"}, "Next": "InRange?"},
    "StartAtZero": {"Type": "Pass", "Parameters": {"week_index": 0}, "Next": "InRange?"},
    "InRange?": {"Type": "Choice", "Choices": [{"Variable": "$.week_index", "NumericGreaterThanEquals": len(starts),
                                                 "Next": "DataEnded"}], "Default": "AddCalendar"},
    "DataEnded": {"Type": "Succeed", "Comment": "no weeks left in the data"},
    "AddCalendar": {"Type": "Pass", "Result": {"starts": starts, "ends": ends}, "ResultPath": "$.cal",
                    "Next": "Calendar"},
    "Calendar": {"Type": "Pass", "Parameters": {
        "dataset": ds, "week_index.$": "$.week_index",
        "week_start.$": "States.ArrayGetItem($.cal.starts, $.week_index)",
        "week_end.$": "States.ArrayGetItem($.cal.ends, $.week_index)"}, "Next": "Weekly"},
}

S["Weekly"] = {"Type": "Parallel", "ResultPath": None, "Next": "ReadDecision", "Branches": [
    {"StartAt": "Part1", "States": {"Part1": emr(
        f"bq2-part1-{ds}", "bq2_part1_tactics_spark.py", [ds, a.base_from, a.base_to], None, python_only=False)}},
    {"StartAt": "ScoreDeployed", "States": {
        "ScoreDeployed": emr("bq2-score", "bq2_part2_score.py", "States.Array($.dataset, 'deployed', '-', $.week_end)",
                             "EvalDeployed"),
        "EvalDeployed": emr("bq2-eval", "bq2_part2_eval.py",
                            "States.Array($.dataset, 'deployed', '--as-of', $.week_end)", None)}}]}
S["ReadDecision"] = get_json(f"{MODEL}/decision_latest.json", "Alert?", "$.decision")
S["Alert?"] = {"Type": "Choice", "Choices": [{"Variable": "$.decision.v.alert", "BooleanEquals": True,
                                                "Next": "NotifyRetrain"}], "Default": "Advance"}
S["NotifyRetrain"] = notify(
    "AML BQ2: retrain approval needed",
    "Detection alert for {} (week ending {}). Open the dashboard to approve or reject retraining.", "WaitRetrain")
S["WaitRetrain"] = approval("retrain_gate", "Approve retraining?", "TrainChallenger", "Rejected")
S["TrainChallenger"] = emr("bq2-train", "bq2_part2_train.py",
                           "States.Array($.dataset, $.decision.v.retrain, $.decision.v.retrain_to)", "ScoreChallenger")
S["ScoreChallenger"] = emr("bq2-score-challenger", "bq2_part2_score.py",
                           "States.Array($.dataset, $.decision.v.retrain, '-', $.week_end)", "EvalChallenger")
S["EvalChallenger"] = emr("bq2-eval-challenger", "bq2_part2_eval.py",
                          "States.Array($.dataset, 'deployed', '--as-of', $.week_end)", "NotifyDeploy")
S["NotifyDeploy"] = notify(
    "AML BQ2: deploy approval needed",
    "Challenger model for {} is trained (week ending {}). Review it on the dashboard and approve or reject deployment.",
    "WaitDeploy")
S["WaitDeploy"] = approval("deploy_gate", "Approve deploying the challenger?", "Deploy", "Rejected")
S["Deploy"] = emr("bq2-deploy", "bq2_deploy.py",
                  "States.Array($.dataset, $.decision.v.retrain, $.deploy_gate.approved_by, $.decision.v.deploy_from)",
                  "Advance")
S["Rejected"] = {"Type": "Pass", "Comment": "a person rejected, or the 7-day approval window passed",
                 "Next": "Advance"}
S["Advance"] = {"Type": "Pass", "Parameters": {"week_index.$": "States.MathAdd($.week_index, 1)"},
                "ResultPath": "$.next", "Next": "SavePointer"}
S["SavePointer"] = {"Type": "Task", "Resource": "arn:aws:states:::aws-sdk:s3:putObject",
                    "Parameters": {"Bucket": a.bucket, "Key": PTR,
                                   "Body.$": "States.JsonToString($.next)"}, "ResultPath": None, "End": True}
order = ["HasWeek", "ReadPointer", "UsePointer", "StartAtZero", "InRange?", "DataEnded", "AddCalendar", "Calendar",
         "Weekly", "ReadDecision",
         "Alert?", "NotifyRetrain", "WaitRetrain", "TrainChallenger", "ScoreChallenger", "EvalChallenger",
         "NotifyDeploy", "WaitDeploy", "Deploy", "Rejected", "Advance", "SavePointer"]
sm = {"Comment": f"AML BQ2 weekly run for {ds}: one simulated week per execution",
      "StartAt": "HasWeek", "States": {k: S[k] for k in order}}
with open(a.out, "w") as f:
    json.dump(sm, f, indent=1)
print(f"wrote {a.out}: {len(sm['States'])} states, {len(starts)} weeks ({starts[0]} .. {ends[-1]})")
