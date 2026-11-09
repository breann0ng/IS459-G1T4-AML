"""
aws_data.py: BQ2 dashboard data straight from AWS (Athena + S3) and the two approval gates (SQS + Step Functions).
Returns the same dict that local/reports/bq2_ramp_report.py writes to data/bq2_data.json.

Settings come from environment variables or Streamlit secrets (never from code):
    AML_ROOT (s3://bucket), AWS_REGION, ATHENA_WORKGROUP (default aml), BQ2_APPROVAL_QUEUE_URL,
    and credentials: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (the aml-bq2-approver user), or a profile.
"""
import hmac
import json
import os
import time

import boto3

DB = "analytics"
SAML = "SAML-R"


def _cfg(key, default=None):
    if os.environ.get(key):
        return os.environ[key]
    try:
        import streamlit as st
        return st.secrets.get(key, default)
    except Exception:
        return default


def enabled():
    return bool(_cfg("AML_ROOT"))


def _session():
    kw = {"region_name": _cfg("AWS_REGION", "us-east-1")}
    if _cfg("AWS_ACCESS_KEY_ID"):
        kw.update(aws_access_key_id=_cfg("AWS_ACCESS_KEY_ID"), aws_secret_access_key=_cfg("AWS_SECRET_ACCESS_KEY"))
    elif _cfg("AWS_PROFILE"):
        kw["profile_name"] = _cfg("AWS_PROFILE")
    return boto3.Session(**kw)


def _bucket_key(uri):
    b, _, k = uri.removeprefix("s3://").partition("/")
    return b, k


def _cast(v, t):
    if v is None:
        return None
    if t == "boolean":
        return v == "true"
    if t in ("bigint", "integer", "int", "smallint", "tinyint"):
        return int(v)
    if t in ("double", "float", "real", "decimal"):
        return float(v)
    return v


def query(sql):
    """Run one Athena query in the cost-capped workgroup and return a list of dicts."""
    a = _session().client("athena")
    qid = a.start_query_execution(QueryString=sql, WorkGroup=_cfg("ATHENA_WORKGROUP", "aml"),
                                  QueryExecutionContext={"Database": DB})["QueryExecutionId"]
    while True:
        st = a.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.5)
    if st["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena {st['State']}: {st.get('StateChangeReason')}")
    rows, cols = [], None
    for page in a.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        if cols is None:
            cols = [(c["Name"], c["Type"]) for c in page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
        for r in page["ResultSet"]["Rows"]:
            vals = [x.get("VarCharValue") for x in r["Data"]]
            if rows == [] and cols and vals == [n for n, _ in cols]:
                continue                                          # header row of the first page
            rows.append({n: _cast(v, t) for (n, t), v in zip(cols, vals)})
    return rows


def get_json(key, default=None):
    b, k = _bucket_key(f"{_cfg('AML_ROOT')}/{key}")
    try:
        v = json.loads(_session().client("s3").get_object(Bucket=b, Key=k)["Body"].read())
        return json.loads(v) if isinstance(v, str) else v      # the pointer is written as a quoted JSON string
    except Exception:
        return default


def load_bq2():
    """Same keys as bq2_data.json. Tables are tiny (KBs); each query scans well under the 10 MB billing minimum."""
    t = lambda name, where="": query(f"SELECT * FROM {name} {where}")
    pick = ("threshold", "train_from", "train_to", "validation", "train_laundering", "train_patterns",
            "validation_by_pattern")
    cfg = get_json("progress/bq2_config.json", {})
    SAML = cfg.get("ramp", globals()["SAML"])
    out = {"design": t("bq2r_design"),
           "part1_summary": t("bq2r_part1_summary"), "part1_weekly": t("bq2r_part1_weekly"),
           "part2_weekly": t("bq2r_part2_weekly"), "part2_rules": t("bq2r_part2_rules"),
           "part2_typology": t("bq2r_part2_typology"),
           "decision": get_json(f"models/bq2r/{SAML}/decision.json", {}),
           "part1_summary_nb": t("bq2r_part1_summary_nb"), "part1_weekly_nb": t("bq2r_part1_weekly_nb"),
           "part2_rules_v1": t("bq2r_part2_rules_v1"), "part2_typology_v1": t("bq2r_part2_typology_v1"),
           "decision_v1": get_json(f"models/bq2r/{SAML}/decision_v1.json", {}),
           "decision_latest": get_json(f"models/bq2r/{SAML}/decision_latest.json", {}),
           "deployed": get_json(f"models/bq2r/{SAML}/deployed.json", {}),
           "progress": get_json("progress/bq2_weekly.json", {}),
           "config": cfg,
           "models": {}}
    for v in ("v0", "v1", "v2"):
        m = get_json(f"models/bq2r/{SAML}/{v}/meta.json")
        if m:
            out["models"][v] = {k: m[k] for k in pick if k in m}
    return out


def pending_approvals():
    """Messages in the approval queue (each holds a Step Functions task token). They stay hidden for 30 s
    after being read and reappear, so this never consumes them."""
    q = _cfg("BQ2_APPROVAL_QUEUE_URL")
    if not q:
        return []
    r = _session().client("sqs").receive_message(QueueUrl=q, MaxNumberOfMessages=10, VisibilityTimeout=5,
                                                 WaitTimeSeconds=0)
    return [{"handle": m["ReceiptHandle"], **json.loads(m["Body"])} for m in r.get("Messages", [])]


PASSCODE_PARAM = "/aml/bq2/approver_passcode"        # SSM SecureString; never in code or in the S3 app bundle


def passcode_ok(given):
    """Constant-time compare with the stored passcode. No parameter, or no access to it, means nobody can approve."""
    try:
        want = _session().client("ssm").get_parameter(Name=PASSCODE_PARAM, WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return False
    ok = bool(given) and hmac.compare_digest(given.encode(), want.encode())
    if not ok:
        time.sleep(1.5)                               # slows guessing
    return ok


def answer(item, approve, who, passcode=""):
    """Resume the waiting execution, then delete the queue message. Returns an error text or None.
    A message whose execution already ended (timed out, rejected elsewhere) is dropped from the queue."""
    if not passcode_ok(passcode):
        return "Wrong or missing passcode: nothing was sent."
    sfn = _session().client("stepfunctions")
    try:
        if approve:
            sfn.send_task_success(taskToken=item["token"],
                                  output=json.dumps({"approved_by": who, "gate": item["gate"]}))
        else:
            sfn.send_task_failure(taskToken=item["token"], error="Rejected", cause=f"rejected by {who}")
        err = None
    except (sfn.exceptions.TaskTimedOut, sfn.exceptions.InvalidToken, sfn.exceptions.TaskDoesNotExist) as e:
        err = f"This request is no longer active ({type(e).__name__}); it was removed."
    try:
        _session().client("sqs").delete_message(QueueUrl=_cfg("BQ2_APPROVAL_QUEUE_URL"), ReceiptHandle=item["handle"])
    except Exception:
        pass                                                      # a stale handle only means it reappears once
    return err


# ------------------------------------------------------------------ HI-Large (Tasha's weekly pipeline)
IBM_DS, IBM_NT = "HI-Large", "HI-Large-NT"


def ibm_root():
    """The HI-Large pipeline keeps everything under its own prefix (default s3://<bucket>/bq2_ibm)."""
    return f"{_cfg('AML_ROOT')}/{_cfg('BQ2_IBM_PREFIX', 'bq2_ibm')}"


def _ibm_table(name, ds_):
    """One small result table (a few hundred rows), read straight from S3 with pyarrow; [] when absent."""
    import pyarrow.dataset as pads
    from pyarrow import fs
    kw = {"region": _cfg("AWS_REGION", "us-east-1")}
    if _cfg("AWS_ACCESS_KEY_ID"):
        kw.update(access_key=_cfg("AWS_ACCESS_KEY_ID"), secret_key=_cfg("AWS_SECRET_ACCESS_KEY"))
    try:
        t = pads.dataset(f"{ibm_root().removeprefix('s3://')}/analytics/{name}", filesystem=fs.S3FileSystem(**kw),
                         format="parquet", partitioning="hive").to_table().to_pandas()
    except (FileNotFoundError, OSError):
        return []
    t["dataset"] = t["dataset"].astype(str)
    t = t[t["dataset"] == ds_]
    return json.loads(t.to_json(orient="records", date_format="iso"))


def load_ibm():
    """Same keys as data/bq2_ibm_data.json (bq2_ibm_report.py), plus the pipeline's pointer and live model."""
    pre = _cfg("BQ2_IBM_PREFIX", "bq2_ibm")
    out = {"part1": {}, "part2": {}, "meta": {}, "decision": {}, "decision_latest": {}, "deployed": {}}
    for key, tag in (("real", ""), ("new_tactic", "_nt")):
        out["part1"][key] = {k: _ibm_table(f"bq2r_part1_{k}{tag}", IBM_DS) for k in ("weekly", "shares", "summary")}
    for key, ds_ in (("real", IBM_DS), ("new_tactic", IBM_NT)):
        out["part2"][key] = {k: _ibm_table(f"bq2r_part2_{k}", ds_) for k in ("weekly", "rules", "typology")}
        out["decision"][key] = get_json(f"{pre}/models/bq2r/{ds_}/decision.json", {})
        out["decision_latest"][key] = get_json(f"{pre}/models/bq2r/{ds_}/decision_latest.json", {})
        out["deployed"][key] = get_json(f"{pre}/models/bq2r/{ds_}/deployed.json", {})
        out["meta"][key] = {v: get_json(f"{pre}/models/bq2r/{ds_}/{v}/meta.json", {}) for v in ("v0", "v1")}
    out["progress"] = get_json(f"{pre}/progress/bq2_weekly_ibm.json", {})
    return out
