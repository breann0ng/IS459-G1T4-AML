"""
bq1_checks.py                                   (BQ1, plain Python on a small EMR Serverless driver)
The two automatic checks the BQ1 state machines (aml_bq1_train / aml_bq1_score) branch on. Each one reads metric
tables the other jobs already wrote and saves a small JSON file to S3, which Step Functions
reads with s3:getObject (Step Functions cannot read Parquet itself).

    python3 bq1_checks.py gate <stage1_config> <model_run>   [--min-precision 0.85] [--min-f1 0.55]
                                                             [--max-f1-drop 0.02]
        After training. Is the new model good enough to replace the champion?
        Reads   analytics/txn_results (config=<stage1_config>, split=validation, the alerts stage 2 gets)
                models/bq1_champion.json -> models/<champion>/gate.json (if a champion exists)
        Writes  models/<model_run>/gate.json   {"passed": bool, "reasons": [...], "metrics": {...}}
        Passes when validation precision >= min-precision, F1 >= min-f1, and F1 is no more than
        max-f1-drop below the current champion's (champion/challenger: never deploy a worse model).
        Validation only (HI-Small + HI-Medium, last 25% of days): HI-Large is never used to choose.

    python3 bq1_checks.py week <dataset> <week_monday>       [--capacity 2500] [--min-precision 0.70]
        After scoring a week. Is the alert queue still manageable for the analysts?
        Reads   analytics/live_results (dataset, run_date=<week_monday>), account-day level
        Writes  progress/bq1/<dataset>/<week_monday>/check.json
                {"alerts_per_day", "precision", "recall", "over_capacity", "low_precision", "notify"}
        Precision / recall use the dataset's labels. In a real bank labels arrive weeks later
        (investigations), so these would be graded with a lag; alerts_per_day needs no labels.
"""
import argparse
import json
import os

import pandas as pd
import pyarrow.dataset as ds

import train_txn as T
import scheme_stage2 as S


def get_json(uri):
    fs, p = T.fs_path(uri)
    try:
        with fs.open_input_stream(p) as f:
            return json.loads(f.read().decode())
    except (FileNotFoundError, OSError):
        return None


def put_json(uri, obj):
    fs, p = T.fs_path(uri)
    fs.create_dir(p.rsplit("/", 1)[0], recursive=True)
    with fs.open_output_stream(p) as f:
        f.write(json.dumps(obj, indent=1, default=float).encode())
    print(f"[check] wrote {uri}\n{json.dumps(obj, indent=1, default=float)}")


def pooled(rows):
    """Precision / recall / F1 over all rows' counts (not the mean of per-dataset ratios)."""
    a, tp, pos = rows["alerts"].sum(), rows["true_alerts"].sum(), rows["positives"].sum()
    p = tp / a if a else 0.0
    r = tp / pos if pos else 0.0
    return {"alerts": int(a), "true_alerts": int(tp), "positives": int(pos), "precision": p, "recall": r,
            "f1": 2 * p * r / (p + r) if p + r else 0.0}


def gate(a):
    res = S.load_table("txn_results", ds.field("config") == a.stage1_config)
    val = res[(res["split"] == "validation") & (res["level"] == "account_day_rollup")
              & (res["cutoff"] == "alerts_used")]
    if val.empty:
        raise SystemExit(f"no validation rows in analytics/txn_results for config={a.stage1_config}")
    m = pooled(val)
    m["alerts_per_day"] = {d: float(x) for d, x in zip(val["dataset"], val["alerts_per_day"])}
    m["threshold"] = float(val["threshold_val_best_f1"].iloc[0])

    reasons = []
    if m["precision"] < a.min_precision:
        reasons.append(f"validation precision {m['precision']:.3f} < {a.min_precision}")
    if m["f1"] < a.min_f1:
        reasons.append(f"validation F1 {m['f1']:.3f} < {a.min_f1}")
    champ = get_json(f"{T.ROOT}/models/bq1_champion.json")
    champ_gate = get_json(f"{T.ROOT}/models/{champ['run']}/gate.json") if champ else None
    if champ_gate and champ["run"] != a.model_run:
        best = champ_gate["metrics"]["f1"]
        if m["f1"] < best - a.max_f1_drop:
            reasons.append(f"F1 {m['f1']:.3f} is more than {a.max_f1_drop} below the champion "
                           f"{champ['run']} ({best:.3f})")
    put_json(f"{T.ROOT}/models/{a.model_run}/gate.json", {
        "passed": not reasons, "reasons": reasons, "model_run": a.model_run,
        "stage1_config": a.stage1_config, "champion": champ["run"] if champ else None,
        "rules": {"min_precision": a.min_precision, "min_f1": a.min_f1, "max_f1_drop": a.max_f1_drop},
        "metrics": m, "checked_at": T.RUN_AT})


def week(a):
    t = T.open_dataset(f"{T.ANALYTICS}/live_results").to_table().to_pandas()
    t = t[(t["dataset"].astype(str) == a.dataset) & (t["run_date"].astype(str) == a.week)
          & (t["level"] == "account_day")]
    if t.empty:
        raise SystemExit(f"no analytics/live_results for {a.dataset} run_date={a.week}")
    # The alerts the analysts actually get: with alert expansion on, the expanded row; else the threshold.
    xp = t[t["cutoff"].astype(str).str.startswith("expand")]
    row = (xp if not xp.empty else t[t["cutoff"] == "frozen_threshold"]).iloc[0]
    out = {"dataset": a.dataset, "week": a.week, "cutoff": row["cutoff"],
           "alerts": int(row["alerts"]), "alerts_per_day": float(row["alerts_per_day"]),
           "precision": float(row["precision"]), "recall": float(row["recall"]),
           "capacity_per_day": a.capacity, "min_precision": a.min_precision,
           "model_run": os.environ.get("AML_FINAL_RUN"), "checked_at": T.RUN_AT}
    out["over_capacity"] = out["alerts_per_day"] > a.capacity
    out["low_precision"] = out["precision"] < a.min_precision
    out["notify"] = out["over_capacity"] or out["low_precision"]
    put_json(f"{T.ROOT}/progress/bq1/{a.dataset}/{a.week}/check.json", out)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("gate")
    g.add_argument("stage1_config")
    g.add_argument("model_run")
    g.add_argument("--min-precision", type=float, default=0.85)
    g.add_argument("--min-f1", type=float, default=0.55)
    g.add_argument("--max-f1-drop", type=float, default=0.02)
    w = sub.add_parser("week")
    w.add_argument("dataset")
    w.add_argument("week")
    w.add_argument("--capacity", type=float, default=2500)
    w.add_argument("--min-precision", type=float, default=0.70)
    a = ap.parse_args()
    gate(a) if a.cmd == "gate" else week(a)


if __name__ == "__main__":
    main()
