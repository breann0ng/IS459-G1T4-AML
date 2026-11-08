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
import os
import re
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
ap.add_argument("--label-only-weeks", type=int, default=0,
                help="extra weeks after the data ends: no new payments, only newly confirmed cases to grade")
ap.add_argument("--base-from", default="2022-10-03")
ap.add_argument("--base-to", default="2023-01-29")
ap.add_argument("--dataset", default="SAML-R")
ap.add_argument("--out", default="aws/step-functions/aml_bq2_weekly.json")
a = ap.parse_args()

starts, d = [], date.fromisoformat(a.first_week)
while d <= date.fromisoformat(a.last_week) + timedelta(days=7 * a.label_only_weeks):
    starts.append(d.isoformat())
    d += timedelta(days=7)
ends = [(date.fromisoformat(s) + timedelta(days=6)).isoformat() for s in starts]
new_data = [s <= a.last_week for s in starts]       # False = label-only week

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


def emr(name, script, args, nxt, python_only=True, timeout=45, **extra):
    """Run one EMR Serverless job and wait for it (.sync). `args` is an ASL intrinsic or a list of constants."""
    drv = {"EntryPoint": f"{ROOT}/jobs/{script}", "SparkSubmitParameters": conf(PYONLY if python_only else SPARK)}
    if isinstance(args, str):
        drv["EntryPointArguments.$"] = args
    else:
        drv["EntryPointArguments"] = args
    st = {"Type": "Task", "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",
          "Parameters": {"ApplicationId": a.app_id, "ClientToken.$": "States.UUID()", "ExecutionRoleArn": a.role_arn,
                         "Name": name, "ExecutionTimeoutMinutes": timeout, "JobDriver": {"SparkSubmit": drv},
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
    "UsePointer": {"Type": "Pass", "Parameters": {"p.$": "States.StringToJson($.ptr.v)"}, "Next": "ReadWeek",
                   "Comment": "the pointer is stored as a quoted JSON string, so it is parsed twice"},
    "ReadWeek": {"Type": "Pass", "Parameters": {"week_index.$": "$.p.week_index"}, "Next": "InRange?"},
    "StartAtZero": {"Type": "Pass", "Parameters": {"week_index": 0}, "Next": "InRange?"},
    "InRange?": {"Type": "Choice", "Choices": [{"Variable": "$.week_index", "NumericGreaterThanEquals": len(starts),
                                                 "Next": "DataEnded"}], "Default": "AddCalendar"},
    "DataEnded": {"Type": "Succeed", "Comment": "no weeks left in the data"},
    "AddCalendar": {"Type": "Pass", "Result": {"starts": starts, "ends": ends, "new_data": new_data},
                    "ResultPath": "$.cal", "Next": "Calendar"},
    "Calendar": {"Type": "Pass", "Parameters": {
        "dataset": ds, "week_index.$": "$.week_index",
        "week_start.$": "States.ArrayGetItem($.cal.starts, $.week_index)",
        "week_end.$": "States.ArrayGetItem($.cal.ends, $.week_index)",
        "new_data.$": "States.ArrayGetItem($.cal.new_data, $.week_index)"}, "Next": "Weekly"},
}

def once(name, label, script, args, nxt, python_only=False):
    """Build a week's feature table only if its marker file is missing (so a re-run costs nothing)."""
    key = f"States.Format('progress/bq2/{name}_{{}}_{{}}.done', $.dataset, $.week_start)"
    return {
        f"Has{label}?": {"Type": "Task", "Resource": "arn:aws:states:::aws-sdk:s3:headObject",
                         "Parameters": {"Bucket": a.bucket, "Key.$": key}, "ResultPath": None, "Next": nxt,
                         "Catch": [{"ErrorEquals": ["States.ALL"], "ResultPath": None, "Next": f"Build{label}"}]},
        f"Build{label}": emr(f"bq2-{name}", script, args, f"Mark{label}", python_only=python_only, timeout=240),
        f"Mark{label}": {"Type": "Task", "Resource": "arn:aws:states:::aws-sdk:s3:putObject",
                         "Parameters": {"Bucket": a.bucket, "Key.$": key, "Body": "done"}, "ResultPath": None,
                         "Next": nxt}}


feature_states = {}
feature_states.update(once("features_v2", "Features", "build_features_spark.py",
                           "States.Array($.dataset, $.week_start, $.week_end)", "HasTxnFeatures?"))
feature_states.update(once("txn_features", "TxnFeatures", "build_txn_features_spark.py",
                           "States.Array($.dataset, 'features_v2', $.week_start, $.week_end)", "HasV3Features?"))
feature_states.update(once("features_v3", "V3Features", "build_features_v3_spark.py",
                           "States.Array($.dataset, $.week_start, $.week_end)", "ScoreDeployed"))

S["Weekly"] = {"Type": "Parallel", "ResultPath": None, "Next": "ReadDecision", "Branches": [
    {"StartAt": "Part1", "States": {"Part1": emr(
        f"bq2-part1-{ds}", "bq2_part1_tactics_spark.py",
        f"States.Array($.dataset, '{a.base_from}', '{a.base_to}', '-', $.week_end)", None, python_only=False)}},
    {"StartAt": "NewData?", "States": {
        "NewData?": {"Type": "Choice", "Choices": [{"Variable": "$.new_data", "BooleanEquals": True,
                                                    "Next": "HasFeatures?"}], "Default": "EvalDeployed",
                     "Comment": "label-only week: no new payments, only newly confirmed cases to grade"},
        **feature_states,
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
S["Rejected"] = {"Type": "Task", "Resource": "arn:aws:states:::sns:publish",
                 "Comment": "a person rejected, or the 7-day approval window passed: tell the team, no approval needed",
                 "Parameters": {"TopicArn": a.topic_arn, "Subject": "AML BQ2: retraining not approved",
                                "Message.$": "States.Format('The alert for {} (week ending {}) was rejected or timed "
                                             "out. The live model is unchanged; monitoring continues.', $.dataset, "
                                             "$.week_end)"},
                 "ResultPath": None, "Next": "Advance"}
S["Advance"] = {"Type": "Pass", "Parameters": {"week_index.$": "States.MathAdd($.week_index, 1)"},
                "ResultPath": "$.next", "Next": "SavePointer"}
S["SavePointer"] = {"Type": "Task", "Resource": "arn:aws:states:::aws-sdk:s3:putObject",
                    "Parameters": {"Bucket": a.bucket, "Key": PTR,
                                   "Body.$": "States.JsonToString($.next)"}, "ResultPath": None, "End": True}
order = ["HasWeek", "ReadPointer", "UsePointer", "ReadWeek", "StartAtZero", "InRange?", "DataEnded", "AddCalendar", "Calendar",
         "Weekly", "ReadDecision",
         "Alert?", "NotifyRetrain", "WaitRetrain", "TrainChallenger", "ScoreChallenger", "EvalChallenger",
         "NotifyDeploy", "WaitDeploy", "Deploy", "Rejected", "Advance", "SavePointer"]
sm = {"Comment": f"AML BQ2 weekly run for {ds}: one simulated week per execution",
      "StartAt": "HasWeek", "States": {k: S[k] for k in order}}
with open(a.out, "w") as f:
    json.dump(sm, f, indent=1)
print(f"wrote {a.out}: {len(sm['States'])} states, {len(starts)} weeks ({starts[0]} .. {ends[-1]})")

# the dashboard reads these facts from S3 (progress/bq2_config.json) instead of copying them; the model-side
# values (datasets, label lag) are parsed from bq2_common.py so that file stays the single source
common = open(os.path.join(os.path.dirname(__file__), "..", "jobs", "bq2", "bq2_common.py"), encoding="utf-8").read()
ramp, control = re.search(r'RAMP, CONTROL = "([^"]+)", "([^"]+)"', common).groups()
cfg = {"ramp": ramp, "control": control, "label_lag_days": int(re.search(r"LABEL_LAG_DAYS = (\d+)", common).group(1)),
       "replay_start": a.first_week, "weeks": len(starts), "week_anchors": {"HI-Large": "2022-08-01"}}
cfg_out = os.path.join(os.path.dirname(a.out), "bq2_config.json")
with open(cfg_out, "w") as f:
    json.dump(cfg, f, indent=1)
print(f"wrote {cfg_out}: upload it to s3://{a.bucket}/progress/bq2_config.json")
