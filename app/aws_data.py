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


def query(sql, timeout=45):
    """Run one Athena query in the cost-capped workgroup and return a list of dicts (TimeoutError after timeout s)."""
    a = _session().client("athena")
    qid = a.start_query_execution(QueryString=sql, WorkGroup=_cfg("ATHENA_WORKGROUP", "aml"),
                                  QueryExecutionContext={"Database": DB})["QueryExecutionId"]
    deadline = time.time() + timeout
    while True:
        st = a.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.time() > deadline:                    # a hung query: cancel it so the page falls back, never spins
            a.stop_query_execution(QueryExecutionId=qid)
            raise TimeoutError(f"Athena query {qid} still running after {timeout}s; cancelled")
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


# ------------------------------------------------------------------ BQ1 weekly results (aml_bq1_score -> analytics.live_*)
# Same keys as data/dashboard_data_live_HI-Large-live.json, so app_bq1.py and home.py show every finished week
# without a new export: each week is pooled from the tables the weekly pipeline writes (Athena tables in
# aws/athena/ddl/create_bq1_live_tables.sql). Scheme timings (speed) still come from the one-off run's file.
BQ1_LIVE_DS = "HI-Large-live"
NAMED = ("cycle", "fan_in", "fan_out", "gather_scatter", "layering", "random", "scatter_gather")
_bq1_cache = {}


def bq1_config():
    """The champion model's validated_run: the config partition its naming results are written under."""
    champ = get_json("models/bq1_champion.json", {}) or {}
    meta = get_json(f"models/{champ.get('run', 'bq1')}/meta.json", {}) or {}
    return meta.get("validated_run", "txn_v3m_xp")


def used_row(rows):
    """The alerts the analysts get, the last rule the scoring step applied: daily capacity (capacity<K>) >
    alert expansion (expand<LOW>) > the frozen threshold. Same order as bq1_checks.used_row."""
    for prefix in ("capacity", "expand"):
        x = rows[rows["cutoff"].astype(str).str.startswith(prefix)]
        if not x.empty:
            return x.iloc[0]
    return rows[rows["cutoff"] == "frozen_threshold"].iloc[0]


def load_bq1_live(dataset=BQ1_LIVE_DS, weeks=None, ttl=600):
    """Cached for ttl seconds (shared by every page and viewer of this server), so a page view costs no query."""
    import copy
    key = (dataset, tuple(weeks or ()))
    hit = _bq1_cache.get(key)
    if not hit or time.time() - hit[0] > ttl:
        hit = _bq1_cache[key] = (time.time(), _bq1_live(dataset, weeks))
    return copy.deepcopy(hit[1])                                 # pages may change their copy


def _recs(df):
    return json.loads(df.to_json(orient="records", date_format="iso"))


def _prf(tp, alerts, pos):
    p = tp / alerts if alerts else 0.0
    r = tp / pos if pos else 0.0
    return p, r, (2 * p * r / (p + r) if p + r else 0.0)


def _bq1_live(ds, weeks=None):
    import pandas as pd
    from concurrent.futures import ThreadPoolExecutor
    cfg = bq1_config()
    w = f"dataset = '{ds}'" + (f" AND run_date IN ({', '.join(repr(str(x)) for x in weeks)})" if weeks else "")
    wc = f"{w} AND config = '{cfg}'"
    # the answer key's class, as stage 2 names it: stack + bipartite = layering; no alert-day laundering = not_laundering
    true_cls = ("CASE WHEN label = 0 THEN 'not_laundering' WHEN true_pattern IS NULL THEN 'unclassified' "
                "WHEN true_pattern IN ('stack', 'bipartite') THEN 'layering' ELSE true_pattern END")
    alerts = (f"SELECT date, acct, score, predicted_pattern, pattern_confidence, second_pattern, second_confidence, "
              f"group_id, g_accounts, g_nodes, g_payments, g_days, g_max_in, g_max_out, g_has_cycle, g_longest, "
              f"{true_cls} AS true_pattern, row_number() OVER (PARTITION BY group_id ORDER BY score DESC) AS rg "
              f"FROM live_alerts_named WHERE {wc} AND label = 1")
    one_per_group = f"SELECT * FROM ({alerts}) WHERE rg = 1 AND true_pattern IN {NAMED}"
    sql = {
        "results": f"SELECT * FROM live_results WHERE {w}",
        "daily": f"SELECT * FROM live_daily WHERE {w} ORDER BY level, date",
        "budget": f"SELECT level, k_per_day, sum(alerts) AS alerts, sum(true_alerts) AS true_alerts "
                  f"FROM live_budget WHERE {w} GROUP BY level, k_per_day",
        "patterns": f"SELECT level, pattern_label, sum(positives) AS positives, sum(caught) AS caught, "
                    f"sum(caught_used) AS caught_used FROM live_patterns WHERE {w} GROUP BY level, pattern_label",
        "stage2": f"SELECT pattern, mode, sum(true_cases) AS true_cases, sum(named_as) AS named_as, "
                  f"sum(correct) AS correct, max(run_at) AS run_at FROM live_stage2 "
                  f"WHERE {wc} AND pattern NOT LIKE 'MACRO%' GROUP BY pattern, mode",
        "named": f"SELECT {true_cls} AS true_pattern, predicted_pattern, second_pattern, count(*) AS n "
                 f"FROM live_alerts_named WHERE {wc} GROUP BY 1, 2, 3",
        "groups": f"{one_per_group} ORDER BY g_nodes DESC, score DESC LIMIT 40",
        "examples": f"SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY predicted_pattern ORDER BY score DESC) "
                    f"AS rp FROM ({one_per_group})) WHERE rp <= 3 ORDER BY score DESC",
    }
    with ThreadPoolExecutor(max_workers=len(sql)) as ex:
        res = dict(zip(sql, ex.map(query, sql.values())))
    r = pd.DataFrame(res["results"])
    if r.empty:
        raise RuntimeError(f"no analytics.live_results rows for {ds}")
    for c in ("threshold", "pr_auc", "recall_patterns", "recall_unclassified"):
        r[c] = pd.to_numeric(r[c], errors="coerce")                         # empty for some cutoffs

    # stage 1: counts add up over the weeks; ratios are recomputed from them. pr_auc and the pattern recalls have
    # no counts behind them, so they are averaged weighted by each week's laundering (positives).
    s1 = []
    for (level, cutoff), x in r.groupby(["level", "cutoff"], sort=False):
        a, tp, pos, days = (int(x[c].sum()) for c in ("alerts", "true_alerts", "positives", "days"))
        p, rc, f1 = _prf(tp, a, pos)
        wavg = lambda c: (float((x[c] * x["positives"]).sum() / x.loc[x[c].notna(), "positives"].sum())
                          if x[c].notna().any() else None)
        thr = x["threshold"].dropna()
        s1.append({"level": level, "cutoff": cutoff, "threshold": float(thr.iloc[0]) if len(thr) else None,
                   "units": x["units"].iloc[0], "precision": p, "recall": rc, "f1": f1,
                   "alerts_per_day": a / days if days else 0.0, "pr_auc": wavg("pr_auc"),
                   "recall_patterns": wavg("recall_patterns"), "recall_unclassified": wavg("recall_unclassified"),
                   "validated_run": x["validated_run"].iloc[0], "date_from": x["date_from"].min(),
                   "date_to": x["date_to"].max(), "dataset": ds, "run_date": x["run_date"].min(),
                   "alerts": a, "true_alerts": tp, "positives": pos, "days": days, "payments": int(x["payments"].sum())})
    s1 = pd.DataFrame(s1)
    pos_by_level = s1.groupby("level")["positives"].first().to_dict()
    ad = s1[s1["level"] == "account_day"]
    used = used_row(ad)                                                       # the alerts analysts get

    b = pd.DataFrame(res["budget"])
    if not b.empty:
        prf = [_prf(t, a, pos_by_level.get(l, 0)) for t, a, l in zip(b["true_alerts"], b["alerts"], b["level"])]
        b[["precision", "recall", "f1"]] = pd.DataFrame(prf, index=b.index)
        b["dataset"] = ds
        b = b.sort_values(["level", "k_per_day"])

    pt = pd.DataFrame(res["patterns"])
    if not pt.empty:
        pt["dataset"] = ds
        pt["recall"] = pt["caught"] / pt["positives"]
        pt["recall_used"] = pt["caught_used"] / pt["positives"]
        pt = pt.sort_values(["level", "pattern_label"])

    s2 = pd.DataFrame(res["stage2"])
    if not s2.empty:
        prf = [_prf(c, n, t) for c, n, t in zip(s2["correct"], s2["named_as"], s2["true_cases"])]
        s2[["precision", "recall", "f1"]] = pd.DataFrame(prf, index=s2.index)
        macro = []
        for mode, x in s2[s2["pattern"].isin(NAMED)].groupby("mode"):           # as the namer writes it
            macro.append({"pattern": f"MACRO ({len(x)} patterns)", "mode": mode, "true_cases": int(x["true_cases"].sum()),
                          "named_as": int(x["named_as"].sum()), "correct": int(x["correct"].sum()),
                          "precision": x["precision"].mean(), "recall": x["recall"].mean(), "f1": x["f1"].mean(),
                          "run_at": x["run_at"].max()})
        s2 = pd.concat([s2, pd.DataFrame(macro)], ignore_index=True)
        s2["config"], s2["dataset"] = cfg, ds
        s2 = s2.sort_values(["pattern", "mode"])

    nm = pd.DataFrame(res["named"])
    out = {"dataset": ds, "stage1": _recs(s1), "daily": res["daily"], "budget": _recs(b), "patterns": _recs(pt),
           "stage2": _recs(s2), "example_groups": _recs(pd.DataFrame(res["groups"]).drop(columns=["rg"], errors="ignore")),
           "alert_examples": _recs(pd.DataFrame(res["examples"]).drop(columns=["rg", "rp"], errors="ignore")),
           "weeks": sorted(r["run_date"].unique().tolist()), "config": cfg,
           "source": "weekly pipeline (aml_bq1_score), live from Athena analytics.live_*"}
    if not nm.empty:
        real = nm["true_pattern"] != "not_laundering"
        out["confusion"] = _recs(nm.groupby(["true_pattern", "predicted_pattern"], as_index=False)["n"].sum())
        g = nm.assign(t=nm["n"] * real).groupby("predicted_pattern", as_index=False)[["n", "t"]].sum()
        out["alerts_by_predicted_pattern"] = _recs(pd.DataFrame(
            {"predicted_pattern": g["predicted_pattern"], "alerts": g["n"], "truly_laundering": g["t"] / g["n"]}))
        # two queues: alerts the namer cannot match to a pattern ('unclassified') go to a second queue
        q = [{"queue": "all", "alerts": int(used["alerts"]), "true_alerts": int(used["true_alerts"])}]
        for name, m in (("main", nm["predicted_pattern"] != "unclassified"),
                        ("second", nm["predicted_pattern"] == "unclassified")):
            q.append({"queue": name, "alerts": int(nm.loc[m, "n"].sum()), "true_alerts": int(nm.loc[m & real, "n"].sum())})
        for x in q:
            x.update(positives=int(used["positives"]), days=int(used["days"]))
            x["precision"], x["recall"], _ = _prf(x["true_alerts"], x["alerts"], x["positives"])
        out["queues"] = q
        # how often the first guess, or either of the two guesses, is the true pattern (known patterns only)
        k = nm[nm["true_pattern"].isin(NAMED)]
        hit1 = k["n"] * (k["predicted_pattern"] == k["true_pattern"])
        hit2 = k["n"] * ((k["predicted_pattern"] == k["true_pattern"]) | (k["second_pattern"] == k["true_pattern"]))
        t2 = (k.assign(h1=hit1, h2=hit2).groupby("true_pattern", as_index=False)[["n", "h1", "h2"]].sum())
        t2 = pd.concat([t2, pd.DataFrame([{"true_pattern": "ALL", "n": t2["n"].sum(), "h1": t2["h1"].sum(),
                                           "h2": t2["h2"].sum()}])], ignore_index=True)
        out["top2"] = _recs(pd.DataFrame({"true_pattern": t2["true_pattern"], "cases": t2["n"],
                                          "top1": t2["h1"] / t2["n"], "top2": t2["h2"] / t2["n"]}))
    return out
