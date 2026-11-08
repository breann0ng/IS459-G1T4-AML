"""Generate aws/step-functions/aml_bq2_weekly_ibm.json: the weekly BQ2 state machine for IBM HI-Large.
Creates nothing in AWS. Same design as build_bq2_weekly.py (SAML-D): one execution = one simulated week, a pointer
file picks the week, the same two approval gates (SQS task token) and the same SNS topic. What differs for HI-Large:
  * the week's payments must be turned into features and scored with Spark (about 13M payments a week), with both
    models: the BQ1 model (models/final) and the one that never saw fan-in / fan-out (models/final_nofan).
    Each build step leaves progress/bq2/<step>_<week>.done, the same markers as run_bq2_ibm.sh, and is skipped
    when its marker exists;
  * two evaluations each week: HI-Large (real data) and HI-Large-NT (fan-in / fan-out hidden before 3 Oct, so they
    look like a brand-new tactic), and Part 1 for both;
  * cases are confirmed 4 weeks late, so after the last scored week the calendar adds label-only weeks: no new
    payments, only grading, until the last scored week's cases are known;
  * the challenger for HI-Large-NT already exists (v1 = the BQ1 model, scored every week by the prep step), so gate 1
    leads straight to grading it instead of training. An alert on the real data only sends an email (no challenger).

    python aws/step-functions/build_bq2_weekly_ibm.py --account 390402569482
"""
import argparse
import json
from datetime import date, timedelta

ap = argparse.ArgumentParser()
ap.add_argument("--account", default="390402569482")
ap.add_argument("--bucket", default="is459-g1t4-aml")
ap.add_argument("--app-id", default="00ga27nqsrmqmp09")
ap.add_argument("--region", default="us-east-1")
ap.add_argument("--role-arn", help="EMR Serverless job role (default: aml-emr-serverless-job in --account)")
ap.add_argument("--topic-arn", help="SNS topic (default: aml-bq2-alerts)")
ap.add_argument("--queue-url", help="SQS approval queue (default: aml-bq2-approvals)")
ap.add_argument("--score-from", default="2022-08-29")      # Week 5: weeks 1-4 are the simulator warm-up
ap.add_argument("--score-to", default="2022-10-30")        # Week 13 (9 scored weeks)
ap.add_argument("--base-from", default="2022-08-29")       # Part 1 baseline = Weeks 5-6
ap.add_argument("--base-to", default="2022-09-11")
ap.add_argument("--hide", default="fan_in,fan_out")        # the staged new tactic ...
ap.add_argument("--intro", default="2022-10-03")           # ... appears on this day (Week 10)
ap.add_argument("--label-lag-days", type=int, default=28)
ap.add_argument("--max-executors", type=int, default=3)   # the team app is capped at 16 vCPU / 64 GB
ap.add_argument("--out", default="aws/step-functions/aml_bq2_weekly_ibm.json")
a = ap.parse_args()
role = a.role_arn or f"arn:aws:iam::{a.account}:role/aml-emr-serverless-job"
topic = a.topic_arn or f"arn:aws:sns:{a.region}:{a.account}:aml-bq2-alerts"
queue = a.queue_url or f"https://sqs.{a.region}.amazonaws.com/{a.account}/aml-bq2-approvals"

DS, NT = "HI-Large", "HI-Large-NT"
starts, scored, d = [], [], date.fromisoformat(a.score_from)
last_known = date.fromisoformat(a.score_to) + timedelta(days=a.label_lag_days)
while d + timedelta(days=6) <= last_known + timedelta(days=6):
    starts.append(d.isoformat())
    scored.append(d <= date.fromisoformat(a.score_to))
    d += timedelta(days=7)
ends = [(date.fromisoformat(s) + timedelta(days=6)).isoformat() for s in starts]

ROOT = f"s3://{a.bucket}"
PTR = "progress/bq2_weekly_ibm.json"
COMMON = [f"spark.emr-serverless.driverEnv.AML_ROOT={ROOT}", f"spark.executorEnv.AML_ROOT={ROOT}",
          f"spark.emr-serverless.driverEnv.AWS_REGION={a.region}", f"spark.executorEnv.AWS_REGION={a.region}",
          f"spark.archives={ROOT}/jobs/venv.tar.gz#environment",
          "spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python",
          "spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python",
          "spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python",
          "spark.driver.cores=4", "spark.driver.memory=12g", "spark.driver.maxResultSize=2g",
          "spark.sql.autoBroadcastJoinThreshold=-1"]
SPARK = COMMON + ["spark.executor.cores=4", "spark.executor.memory=10g", "spark.executor.memoryOverhead=3g",
                  f"spark.dynamicAllocation.maxExecutors={a.max_executors}",
                  "spark.emr-serverless.driverEnv.AML_SHUFFLE=1000", "spark.executorEnv.AML_SHUFFLE=1000",
                  "spark.emr-serverless.driverEnv.AML_BROADCAST_ACCOUNTS=0"]
PYONLY = COMMON + ["spark.executor.cores=1", "spark.executor.memory=2g", "spark.executor.instances=1",
                   "spark.dynamicAllocation.initialExecutors=1", "spark.dynamicAllocation.maxExecutors=1"]
HIDE_ENV = {"BQ2_HIDE": a.hide, "BQ2_HIDE_BEFORE": a.intro}
EMERGE_ENV = {"BQ2_EMERGING": a.hide, "BQ2_FIRST_DOSE": a.intro}


def conf(c, env=None):
    extra = [f"spark.emr-serverless.driverEnv.{k}={v}" for k, v in (env or {}).items()] + \
            [f"spark.executorEnv.{k}={v}" for k, v in (env or {}).items()]
    return f"--py-files {ROOT}/jobs/jobs.zip " + " ".join(f"--conf {x}" for x in c + extra)


def emr(name, script, args, nxt, python_only=False, env=None, minutes=60, **extra):
    """Run one EMR Serverless job and wait for it (.sync). `args` is an ASL intrinsic or a list of constants."""
    drv = {"EntryPoint": f"{ROOT}/jobs/{script}",
           "SparkSubmitParameters": conf(PYONLY if python_only else SPARK, env)}
    if isinstance(args, str):
        drv["EntryPointArguments.$"] = args
    else:
        drv["EntryPointArguments"] = args
    st = {"Type": "Task", "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",
          "Parameters": {"ApplicationId": a.app_id, "ClientToken.$": "States.UUID()", "ExecutionRoleArn": role,
                         "Name": name, "ExecutionTimeoutMinutes": minutes, "JobDriver": {"SparkSubmit": drv},
                         "ConfigurationOverrides": {"MonitoringConfiguration": {
                             "S3MonitoringConfiguration": {"LogUri": f"{ROOT}/logs/emr_serverless/"}}}},
          "ResultPath": None}
    st.update(extra)
    st.update({"Next": nxt} if nxt else {"End": True})
    return st


def guarded(states, step, label, job, nxt):
    """Has<label>? (marker exists -> skip) -> Build<label> -> Mark<label>. Returns the first state's name."""
    key = f"States.Format('progress/bq2/{step}_{{}}.done', $.week_start)"
    states[f"Has{label}?"] = {"Type": "Task", "Resource": "arn:aws:states:::aws-sdk:s3:headObject",
                              "Parameters": {"Bucket": a.bucket, "Key.$": key}, "ResultPath": None, "Next": nxt,
                              "Catch": [{"ErrorEquals": ["States.ALL"], "ResultPath": None,
                                         "Next": f"Build{label}"}]}
    states[f"Build{label}"] = {**{k: v for k, v in job.items() if k != "End"}, "Next": f"Mark{label}"}
    states[f"Mark{label}"] = {"Type": "Task", "Resource": "arn:aws:states:::aws-sdk:s3:putObject",
                              "Parameters": {"Bucket": a.bucket, "Key.$": key, "Body": "done"},
                              "ResultPath": None, "Next": nxt}
    return f"Has{label}?"


def get_json(key, nxt, path, **extra):
    st = {"Type": "Task", "Resource": "arn:aws:states:::aws-sdk:s3:getObject",
          "Parameters": {"Bucket": a.bucket, "Key": key}, "ResultSelector": {"v.$": "States.StringToJson($.Body)"},
          "ResultPath": path, "Next": nxt}
    st.update(extra)
    return st


def approval(gate, text, nxt, on_reject):
    body = {"token.$": "$$.Task.Token", "gate": gate, "dataset": NT, "week_end.$": "$.week_end",
            "decision.$": "$.decision.v", "message": text}
    return {"Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
            "Parameters": {"QueueUrl": queue, "MessageBody": body},
            "TimeoutSeconds": 604800, "ResultPath": f"$.{gate}",
            "Catch": [{"ErrorEquals": ["States.ALL"], "ResultPath": "$.rejected", "Next": on_reject}], "Next": nxt}


def notify(subject, text, nxt, dataset=NT):
    return {"Type": "Task", "Resource": "arn:aws:states:::sns:publish",
            "Parameters": {"TopicArn": topic, "Subject": subject,
                           "Message.$": f"States.Format('{text}', '{dataset}', $.week_end)"},
            "ResultPath": None, "Next": nxt}


S = {
    "HasWeek": {"Type": "Choice", "Choices": [{"Variable": "$.week_index", "IsPresent": True, "Next": "InRange?"}],
                "Default": "ReadPointer"},
    "ReadPointer": get_json(PTR, "UsePointer", "$.ptr", Catch=[
        {"ErrorEquals": ["States.ALL"], "ResultPath": None, "Next": "StartAtZero"}]),
    "UsePointer": {"Type": "Pass", "Parameters": {"week_index.$": "$.ptr.v.week_index"}, "Next": "InRange?"},
    "StartAtZero": {"Type": "Pass", "Parameters": {"week_index": 0}, "Next": "InRange?"},
    "InRange?": {"Type": "Choice", "Choices": [{"Variable": "$.week_index", "NumericGreaterThanEquals": len(starts),
                                                 "Next": "DataEnded"}], "Default": "AddCalendar"},
    "DataEnded": {"Type": "Succeed", "Comment": "every scored week is graded"},
    "AddCalendar": {"Type": "Pass", "Result": {"starts": starts, "ends": ends, "scored": scored}, "ResultPath": "$.cal",
                    "Next": "Calendar"},
    "Calendar": {"Type": "Pass", "Parameters": {
        "dataset": NT, "week_index.$": "$.week_index",
        "week_start.$": "States.ArrayGetItem($.cal.starts, $.week_index)",
        "week_end.$": "States.ArrayGetItem($.cal.ends, $.week_index)",
        "new_data.$": "States.ArrayGetItem($.cal.scored, $.week_index)"}, "Next": "Weekly"},
}

# ---- branch 1: Part 1 (tactics, confirmed cases only), real data and the new-tactic test
tactics = {
    "Part1": emr(f"bq2-part1-{DS}", "bq2_part1_tactics_spark.py", [DS, a.base_from, a.base_to], "Part1NewTactic"),
    "Part1NewTactic": emr(f"bq2-part1-{NT}", "bq2_part1_tactics_spark.py", [DS, a.base_from, a.base_to, "_nt"], None,
                          env=HIDE_ENV),
}

# ---- branch 2: this week's payments -> features -> scores (both models) -> Part 2 tables -> rules
det = {"NewData?": {"Type": "Choice", "Choices": [{"Variable": "$.new_data", "BooleanEquals": True,
                                                   "Next": "HasFeatures?"}], "Default": "Evaluate",
                    "Comment": "label-only weeks: no new payments, only newly confirmed cases to grade"}}
wk = "States.Array('%s', $.week_start, $.week_end)" % DS
guarded(det, "feat_large", "Features", emr("bq2-features", "build_features_spark.py", wk, None, minutes=240),
        "HasTxnFeatures?")
guarded(det, "txn_large", "TxnFeatures",
        emr("bq2-txn-features", "build_txn_features_spark.py",
            "States.Array('%s', 'features_v2', $.week_start, $.week_end)" % DS, None, minutes=240),
        "HasV3Features?")
guarded(det, "v3_large", "V3Features", emr("bq2-v3-features", "build_features_v3_spark.py", wk, None, minutes=240),
        "HasScores?")
guarded(det, "bq2_score", "Scores", emr("bq2-score-both-models", "bq2_ibm_score_spark.py", wk, None, minutes=120),
        "PrepToDate")
det["PrepToDate"] = emr("bq2-prep", "bq2_ibm_part2_prep_spark.py",
                        "States.Array('%s', '%s', $.week_end)" % (DS, a.score_from), "Evaluate", env=HIDE_ENV,
                        Comment="Part 2 tables (both datasets, both versions) for every scored week so far")
asof = "States.Array('{}', 'deployed', '--as-of', $.week_end)"
det["Evaluate"] = {"Type": "Parallel", "ResultPath": None, "End": True, "Branches": [
    {"StartAt": "EvalReal", "States": {"EvalReal": emr("bq2-eval-real", "bq2_part2_eval.py", asof.format(DS), None,
                                                       python_only=True, env=EMERGE_ENV)}},
    {"StartAt": "EvalNewTactic", "States": {"EvalNewTactic": emr(
        "bq2-eval-nt", "bq2_part2_eval.py", asof.format(NT), None, python_only=True, env=EMERGE_ENV)}}]}

S["Weekly"] = {"Type": "Parallel", "ResultPath": None, "Next": "ReadDecision", "Branches": [
    {"StartAt": "Part1", "States": tactics}, {"StartAt": "NewData?", "States": det}]}
S["ReadDecision"] = get_json(f"models/bq2r/{NT}/decision_latest.json", "ReadDecisionReal", "$.decision")
S["ReadDecisionReal"] = get_json(f"models/bq2r/{DS}/decision_latest.json", "Alert?", "$.decision_real")
S["Alert?"] = {"Type": "Choice", "Choices": [
    {"Variable": "$.decision.v.alert", "BooleanEquals": True, "Next": "NotifyRetrain"},
    {"Variable": "$.decision_real.v.alert", "BooleanEquals": True, "Next": "NotifyRealAlert"}], "Default": "Advance"}
S["NotifyRealAlert"] = notify(
    "AML BQ2: detection alert on real HI-Large",
    "Detection alert for {} (week ending {}). No challenger is prepared for the real data: investigate on the dashboard.",
    "Advance", dataset=DS)
S["NotifyRetrain"] = notify(
    "AML BQ2: retrain approval needed",
    "Detection alert for {} (week ending {}). Open the dashboard to approve or reject retraining.", "WaitRetrain")
S["WaitRetrain"] = approval("retrain_gate", "Approve retraining?", "ChallengerReady", "Rejected")
S["ChallengerReady"] = {"Type": "Pass", "Next": "EvalChallenger",
                        "Comment": "v1 = the BQ1 model trained with fan-in / fan-out; PrepToDate scores it every week"}
S["EvalChallenger"] = emr("bq2-eval-challenger", "bq2_part2_eval.py", asof.format(NT), "NotifyDeploy",
                          python_only=True, env=EMERGE_ENV)
S["NotifyDeploy"] = notify(
    "AML BQ2: deploy approval needed",
    "Challenger model for {} is graded (week ending {}). Review it on the dashboard and approve or reject deployment.",
    "WaitDeploy")
S["WaitDeploy"] = approval("deploy_gate", "Approve deploying the challenger?", "Deploy", "Rejected")
S["Deploy"] = emr("bq2-deploy", "bq2_deploy.py",
                  "States.Array('%s', $.decision.v.retrain, $.deploy_gate.approved_by, $.decision.v.deploy_from)" % NT,
                  "Advance", python_only=True)
S["Rejected"] = {"Type": "Pass", "Comment": "a person rejected, or the 7-day approval window passed", "Next": "Advance"}
S["Advance"] = {"Type": "Pass", "Parameters": {"week_index.$": "States.MathAdd($.week_index, 1)"},
                "ResultPath": "$.next", "Next": "SavePointer"}
S["SavePointer"] = {"Type": "Task", "Resource": "arn:aws:states:::aws-sdk:s3:putObject",
                    "Parameters": {"Bucket": a.bucket, "Key": PTR, "Body.$": "States.JsonToString($.next)"},
                    "ResultPath": None, "End": True}

sm = {"Comment": f"AML BQ2 weekly run for IBM {DS} (real data) and {NT} (fan-in / fan-out appear on {a.intro}): "
                 "one simulated week per execution",
      "StartAt": "HasWeek", "States": S}


def check(states):     # every Next / Default / Catch target exists, recursively
    for name, st in states.items():
        assert not ("Next" in st and st.get("End")), f"{name}: both Next and End"
        for t in [st.get("Next"), st.get("Default")] + [c["Next"] for c in st.get("Choices", []) + st.get("Catch", [])]:
            assert t is None or t in states, f"{name} -> {t} missing"
        for b in st.get("Branches", []):
            assert b["StartAt"] in b["States"]
            check(b["States"])


check(S)
with open(a.out, "w") as f:
    json.dump(sm, f, indent=1)
print(f"wrote {a.out}: {len(S)} top-level states; {len(starts)} weeks ({starts[0]} .. {ends[-1]}), "
      f"{sum(scored)} with new payments, {len(starts) - sum(scored)} label-only")
