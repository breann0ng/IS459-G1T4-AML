"""
deploy_aml_daily.py                 (laptop, any branch: the BQ1 daily state machine without Terraform)
Creates or updates, in the shared account:
    IAM role   aml-daily-sfn       what the state machine may do (aws/step-functions/aml_daily_role_policy.json)
    IAM role   aml-daily-events    lets EventBridge start the state machine
    Step Functions state machine   aml-daily (aws/step-functions/aml_daily.json, placeholders filled)
    S3 bucket  EventBridge notifications switched on (other notification settings are kept)
    EventBridge rule  aml-daily-trigger (aws/step-functions/aml_daily_trigger.json) -> aml-daily
    Athena     analytics.pipeline_runs, the run log (aws/sql/ddl/create_pipeline_runs.sql), if missing

    python local/runs/deploy_aml_daily.py --profile g1t4                      show the plan, change nothing
    python local/runs/deploy_aml_daily.py --profile g1t4 --apply              do it
Options: --code-prefix dev/enrui/jobs (code from deploy_branch.py), --run-name enrui (models/<run>/).
Re-running is safe: everything is created or updated in place. Nothing here costs money until a day runs.
"""
import argparse
import json
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SFN_DIR = REPO / "aws" / "step-functions"
APP_ID, JOB_ROLE, REGION = "00ga27nqsrmqmp09", "aml-emr-serverless-job", "us-east-1"
SM_NAME, SFN_ROLE, EVENTS_ROLE, RULE = "aml-daily", "aml-daily-sfn", "aml-daily-events", "aml-daily-trigger"


def fill(text, values):
    for k, v in values.items():
        text = text.replace("${" + k + "}", v)
    assert "${" not in text, "unfilled placeholder"
    return text


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile")
    ap.add_argument("--bucket", default="is459-g1t4-aml")
    ap.add_argument("--code-prefix", default="dev/enrui/jobs")
    ap.add_argument("--run-name", default="enrui")
    ap.add_argument("--workgroup", default="aml")
    ap.add_argument("--apply", action="store_true", help="make the changes (default: only show the plan)")
    a = ap.parse_args()

    import boto3
    s = boto3.Session(profile_name=a.profile, region_name=REGION)
    acct = s.client("sts").get_caller_identity()["Account"]
    iam, sfn, ev, s3, ath = (s.client(x) for x in ("iam", "stepfunctions", "events", "s3", "athena"))
    v = {"bucket": a.bucket, "code_prefix": a.code_prefix.strip("/"), "final_run": a.run_name, "emr_app_id": APP_ID,
         "emr_job_role_arn": f"arn:aws:iam::{acct}:role/{JOB_ROLE}", "athena_workgroup": a.workgroup,
         "region": REGION, "account_id": acct}
    definition = fill((SFN_DIR / "aml_daily.json").read_text(encoding="utf-8"), v)
    policy = fill((SFN_DIR / "aml_daily_role_policy.json").read_text(encoding="utf-8"), v)
    pattern = fill((SFN_DIR / "aml_daily_trigger.json").read_text(encoding="utf-8"), v)
    sm_arn = f"arn:aws:states:{REGION}:{acct}:stateMachine:{SM_NAME}"

    # ---- checks (read-only)
    iam.get_role(RoleName=JOB_ROLE)
    ath.get_work_group(WorkGroup=a.workgroup)
    r = sfn.validate_state_machine_definition(definition=definition, type="STANDARD")
    assert r["result"] == "OK", r["diagnostics"]
    def has(f):
        try:
            f(); return True
        except Exception:
            return False
    plan = [
        (f"IAM role {SFN_ROLE}", "update policy" if has(lambda: iam.get_role(RoleName=SFN_ROLE)) else "create"),
        (f"IAM role {EVENTS_ROLE}", "update policy" if has(lambda: iam.get_role(RoleName=EVENTS_ROLE)) else "create"),
        (f"state machine {SM_NAME}", "update" if has(lambda: sfn.describe_state_machine(stateMachineArn=sm_arn)) else "create"),
        (f"bucket {a.bucket}: EventBridge notifications",
         "already on" if "EventBridgeConfiguration" in s3.get_bucket_notification_configuration(Bucket=a.bucket) else "switch on"),
        (f"EventBridge rule {RULE}", "update" if has(lambda: ev.describe_rule(Name=RULE)) else "create"),
        ("Athena table analytics.pipeline_runs", "exists" if has(lambda: s.client("glue").get_table(
            DatabaseName="analytics", Name="pipeline_runs")) else "create"),
    ]
    print(f"account ...{acct[-4:]} · code s3://{a.bucket}/{v['code_prefix']}/ · models/{a.run_name}/ · definition valid")
    for what, action in plan:
        print(f"  {action:14s} {what}")
    if not a.apply:
        print("plan only: add --apply to make these changes")
        return

    def role(name, service, doc):
        trust = json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Allow",
                            "Principal": {"Service": service}, "Action": "sts:AssumeRole"}]})
        if not has(lambda: iam.get_role(RoleName=name)):
            iam.create_role(RoleName=name, AssumeRolePolicyDocument=trust, Tags=[{"Key": "project", "Value": "aml"}])
        iam.put_role_policy(RoleName=name, PolicyName=name, PolicyDocument=doc)
        return f"arn:aws:iam::{acct}:role/{name}"

    sfn_role = role(SFN_ROLE, "states.amazonaws.com", policy)
    for i in range(12):                  # a new role takes a few seconds before Step Functions can use it
        try:
            if has(lambda: sfn.describe_state_machine(stateMachineArn=sm_arn)):
                sfn.update_state_machine(stateMachineArn=sm_arn, definition=definition, roleArn=sfn_role)
            else:
                sfn.create_state_machine(name=SM_NAME, definition=definition, roleArn=sfn_role, type="STANDARD",
                                         tags=[{"key": "project", "value": "aml"}])
            break
        except sfn.exceptions.ClientError as e:
            if "AccessDenied" not in str(e) and "not authorized" not in str(e) or i == 11:
                raise
            time.sleep(5)
    print(f"  done  state machine {SM_NAME}")

    cfg = {k: x for k, x in s3.get_bucket_notification_configuration(Bucket=a.bucket).items() if k != "ResponseMetadata"}
    if "EventBridgeConfiguration" not in cfg:
        cfg["EventBridgeConfiguration"] = {}
        s3.put_bucket_notification_configuration(Bucket=a.bucket, NotificationConfiguration=cfg)
    print("  done  bucket EventBridge notifications")

    ev_role = role(EVENTS_ROLE, "events.amazonaws.com", json.dumps({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "states:StartExecution", "Resource": sm_arn}]}))
    ev.put_rule(Name=RULE, EventPattern=pattern, State="ENABLED",
                Description="A day file in raw/transactions/set=*/date=*/ starts aml-daily (BQ1)")
    ev.put_targets(Rule=RULE, Targets=[{"Id": "aml-daily", "Arn": sm_arn, "RoleArn": ev_role}])
    print(f"  done  EventBridge rule {RULE}")

    if not has(lambda: s.client("glue").get_table(DatabaseName="analytics", Name="pipeline_runs")):
        ddl = (REPO / "aws" / "sql" / "ddl" / "create_pipeline_runs.sql").read_text(encoding="utf-8")
        ddl = "\n".join(l for l in ddl.splitlines() if not l.strip().startswith("--")).replace("<bucket>", a.bucket)
        q = ath.start_query_execution(QueryString=ddl, WorkGroup=a.workgroup)["QueryExecutionId"]
        while (st := ath.get_query_execution(QueryExecutionId=q)["QueryExecution"]["Status"])["State"] in ("QUEUED", "RUNNING"):
            time.sleep(2)
        if st["State"] != "SUCCEEDED":
            raise SystemExit(f"run-log table: {st.get('StateChangeReason')}")
    print("  done  Athena table analytics.pipeline_runs")
    print(f"deployed. Start a day by hand: Step Functions console -> {SM_NAME} -> Start execution with "
          '{"dataset": "HI-Large", "date": "2022-09-12"}, or replay one with local/simulate/replay_day.py')


if __name__ == "__main__":
    main()
