"""
final_report.py
Collect the final HI-Large results from S3 into one small file for the BQ1 dashboard.
Laptop is fine (small tables only):
    python final_report.py HI-Large > final_report.txt      (also writes dashboard_data.json)
"""

import json
import sys

import numpy as np
import pandas as pd

import txn_report as R

DATASET = sys.argv[1] if len(sys.argv) > 1 else "HI-Large"


def tbl(name):
    t = R.load(name)
    return t[t["dataset"] == DATASET].copy() if not t.empty and "dataset" in t else t


def clean(df):
    df = df.copy()
    for c in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            df[c] = df[c].astype(str)
        elif df[c].dtype == object:
            df[c] = df[c].map(lambda v: str(v) if v is not None and not isinstance(v, (str, int, float, bool)) else v)
    return json.loads(df.replace({np.nan: None}).to_json(orient="records", date_format="iso"))


def main():
    pd.set_option("display.width", 220)
    res, daily, budget = tbl("final_results"), tbl("final_daily"), tbl("final_budget")
    pats, sch, st2 = tbl("final_patterns"), tbl("final_schemes"), tbl("final_stage2")
    named = tbl("final_alerts_named")
    if res.empty:
        raise SystemExit("No analytics/final_results yet: run score_final_spark.py first.")

    print(f"=== {DATASET} · STAGE 1 (frozen threshold {res['threshold'].dropna().iloc[0]:.4f})")
    print(res[["level", "cutoff", "alerts", "true_alerts", "positives", "precision", "recall", "f1",
               "alerts_per_day", "pr_auc", "recall_patterns", "recall_unclassified"]].round(3).to_string(index=False))
    print("\n=== detection per pattern")
    p = pats.assign(recall=pats["caught"] / pats["positives"])
    print(p[["level", "pattern_label", "positives", "caught", "recall"]].sort_values(["level", "pattern_label"]).round(3).to_string(index=False))

    sch["caught"] = sch["caught"].astype(bool)
    by = sch.groupby("pattern_type").agg(schemes=("scheme_id", "count"), caught=("caught", "mean"),
                                         median_hours_to_first_flag=("hours_to_first_flag", "median"),
                                         median_duration_h=("duration_h", "median"),
                                         share_from_first_flag=("share_from_first_flag", "mean"),
                                         alert_before_end_eod=("caught_before_end_eod", "mean")).reset_index()
    tot = {"schemes": len(sch), "caught": float(sch["caught"].mean()),
           "median_hours_to_first_flag": float(sch["hours_to_first_flag"].median()),
           "median_duration_h": float(sch["duration_h"].median()),
           "share_from_first_flag": float(sch["share_from_first_flag"].mean()),
           "alert_before_end_eod": float(sch["caught_before_end_eod"].fillna(False).astype(bool).mean())}
    print("\n=== schemes: caught and how early")
    print(by.round(3).to_string(index=False))
    print(tot)

    if not st2.empty:
        print("\n=== STAGE 2 END TO END (group namer)")
        e = st2[st2["mode"] == "end_to_end"]
        print(e[["pattern", "true_cases", "named_as", "correct", "precision", "recall", "f1"]].round(3).to_string(index=False))

    # confusion of the analyst's alert list (predicted vs true), and example groups
    conf, examples = [], []
    if not named.empty:
        conf = clean(named.groupby(["true_pattern", "predicted_pattern"]).size().rename("n").reset_index())
        big = (named[named["g_nodes"] >= 3].sort_values(["g_nodes", "score"], ascending=False)
                    .drop_duplicates("group_id").head(40))
        examples = clean(big[["group_id", "date", "predicted_pattern", "pattern_confidence", "g_accounts", "g_nodes",
                              "g_payments", "g_days", "g_max_in", "g_max_out", "g_has_cycle", "g_longest", "true_pattern"]])
        alerts_by_pred = clean(named.groupby("predicted_pattern").agg(
            alerts=("score", "size"), truly_laundering=("label", "mean")).reset_index())
    else:
        alerts_by_pred = []

    out = {"dataset": DATASET, "stage1": clean(res), "daily": clean(daily.sort_values(["level", "date"])),
           "budget": clean(budget.sort_values(["level", "k_per_day"])), "patterns": clean(p),
           "schemes_by_pattern": clean(by), "schemes_total": tot,
           "scheme_hours_hist": clean(sch.loc[sch["caught"], "hours_to_first_flag"].clip(upper=96)
                                       .pipe(lambda s: pd.cut(s, [-0.01, 1, 3, 6, 12, 24, 48, 96]).value_counts().sort_index()
                                             .rename("schemes").reset_index().assign(bucket=lambda d: d.iloc[:, 0].astype(str)))[["bucket", "schemes"]]),
           "stage2": clean(st2) if not st2.empty else [], "confusion": conf, "example_groups": examples,
           "alerts_by_predicted_pattern": alerts_by_pred}
    with open("dashboard_data.json", "w") as f:
        json.dump(out, f, indent=1, default=str)
    print("\nwrote dashboard_data.json")


if __name__ == "__main__":
    main()
