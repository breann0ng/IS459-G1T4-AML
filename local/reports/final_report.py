"""
final_report.py
Collect the final HI-Large results from S3 into one small file for the BQ1 dashboard.
Laptop is fine (small tables only):
    python final_report.py HI-Large > final_report.txt      (also writes dashboard_data.json)

Weekly pipeline (Step Functions aml_bq1_weekly writes analytics/live_*, one run_date partition per week):
    AML_LIVE=1 python final_report.py HI-Large-live          (writes dashboard_data_live_HI-Large-live.json)
Every finished week is merged: counts are added up and rates recomputed (never averaged), so rerunning
after each new week keeps the dashboard current.
"""

import json
import os
import sys

import numpy as np
import pandas as pd

import txn_report as R

DATASET = sys.argv[1] if len(sys.argv) > 1 else "HI-Large"
RUN = os.environ.get("AML_FINAL_RUN", "final")     # a named final run: export AML_FINAL_RUN=<name> first
LIVE = bool(os.environ.get("AML_LIVE"))            # the weekly pipeline's live_* tables, all finished weeks


def tbl(name):
    if LIVE:
        t = R.load(f"live_{name[len('final_'):]}")
    else:
        t = R.load(name if RUN == "final" else f"final_{RUN}_{name[len('final_'):]}")
    return t[t["dataset"] == DATASET].copy() if not t.empty and "dataset" in t else t


def prf(tp, alerts, pos):
    p = tp / alerts if alerts else np.nan
    r = tp / pos if pos else np.nan
    return p, r, (2 * p * r / (p + r) if p and r and p + r else np.nan)


def merge_weeks(res, budget, pats, st2, sch, named, daily):
    """One row per week -> one row for all weeks: add the counts, then recompute every rate."""
    weeks = sorted(res["run_date"].astype(str).unique())
    pats = (pats.groupby(["level", "pattern_label"], as_index=False)[["positives", "caught", "caught_used"]].sum()
                .assign(dataset=DATASET))
    first = res.sort_values("run_date").groupby(["level", "cutoff"], as_index=False).first()
    summed = res.groupby(["level", "cutoff"], as_index=False)[["alerts", "true_alerts", "positives", "days", "payments"]].sum()
    out = first.drop(columns=["alerts", "true_alerts", "positives", "days", "payments"]).merge(summed, on=["level", "cutoff"])
    out[["precision", "recall", "f1"]] = [prf(r.true_alerts, r.alerts, r.positives) for r in out.itertuples()]
    out["alerts_per_day"] = out["alerts"] / out["days"]
    w = res.assign(wpos=res["positives"] * res["pr_auc"]).groupby(["level", "cutoff"])[["wpos", "positives"]].sum(min_count=1)
    out["pr_auc"] = out.set_index(["level", "cutoff"]).index.map(lambda k: w.loc[k, "wpos"] / w.loc[k, "positives"]
                                                                 if w.loc[k, "wpos"] == w.loc[k, "wpos"] else np.nan)
    out["date_from"], out["date_to"] = res["date_from"].astype(str).min(), res["date_to"].astype(str).max()
    for lvl in out["level"].unique():                      # pattern / unclassified recall from the merged per-pattern counts
        p = pats[pats["level"] == lvl]
        named_p, unc = p[p["pattern_label"] != "unclassified"], p[p["pattern_label"] == "unclassified"]
        m = (out["level"] == lvl) & out["recall_patterns"].notna()
        out.loc[m, "recall_patterns"] = named_p["caught"].sum() / named_p["positives"].sum()
        out.loc[m, "recall_unclassified"] = unc["caught"].sum() / max(unc["positives"].sum(), 1)
    fz = out[out["cutoff"] == "frozen_threshold"].set_index("level")["positives"]
    budget = budget.groupby(["level", "k_per_day"], as_index=False)[["alerts", "true_alerts"]].sum().assign(dataset=DATASET)
    budget[["precision", "recall", "f1"]] = [prf(r.true_alerts, r.alerts, fz.get(r.level, np.nan)) for r in budget.itertuples()]
    if not st2.empty:
        keep = [c for c in ["config"] if c in st2]
        body = st2[~st2["pattern"].str.startswith("MACRO")]
        g = body.groupby(["pattern", "mode", *keep], as_index=False)[["true_cases", "named_as", "correct"]].sum()
        g[["precision", "recall", "f1"]] = [prf(r.correct, r.named_as, r.true_cases) for r in g.itertuples()]
        macro = (g[g["pattern"] != "unclassified"].groupby(["mode", *keep], as_index=False)
                  [["true_cases", "named_as", "correct", "precision", "recall", "f1"]]
                  .agg({"true_cases": "sum", "named_as": "sum", "correct": "sum", "precision": "mean", "recall": "mean",
                        "f1": "mean"}).assign(pattern="MACRO (7 patterns)"))
        st2 = pd.concat([g, macro], ignore_index=True).assign(dataset=DATASET, run_at=str(st2["run_at"].max()))
    sch = sch.drop_duplicates("scheme_id")
    return out, budget, pats, st2, sch, named, daily.sort_values(["level", "date"]), weeks


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
    weeks = []
    if LIVE:
        res, budget, pats, st2, sch, named, daily, weeks = merge_weeks(res, budget, pats, st2, sch, named, daily)
        print(f"=== live: {len(weeks)} finished weeks merged: {', '.join(weeks)}")

    print(f"=== {DATASET} · STAGE 1 (frozen threshold {res['threshold'].dropna().iloc[0]:.4f})")
    print(res[["level", "cutoff", "alerts", "true_alerts", "positives", "precision", "recall", "f1",
               "alerts_per_day", "pr_auc", "recall_patterns", "recall_unclassified"]].round(3).to_string(index=False))
    print("\n=== detection per pattern")
    p = pats.assign(recall=pats["caught"] / pats["positives"])
    show = ["level", "pattern_label", "positives", "caught", "recall"]
    if "caught_used" in p and (p["caught_used"] != p["caught"]).any():    # alert expansion on
        p["recall_used"] = p["caught_used"] / p["positives"]
        show += ["caught_used", "recall_used"]
    print(p[show].sort_values(["level", "pattern_label"]).round(3).to_string(index=False))

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

    # two guesses (namer runs with second_pattern): of truly laundering, patterned alerts, how often
    # the true pattern is the first guess / one of the two guesses
    top2, alert_examples = [], []
    if not named.empty and "second_pattern" in named:
        tp = named[(named["label"] == 1) & ~named["true_pattern"].isin(["unclassified", "not_laundering"])]
        t = tp.assign(top1=tp["true_pattern"] == tp["predicted_pattern"],
                      top2=(tp["true_pattern"] == tp["predicted_pattern"]) | (tp["true_pattern"] == tp["second_pattern"]))
        t2 = t.groupby("true_pattern").agg(cases=("top1", "size"), top1=("top1", "mean"), top2=("top2", "mean")).reset_index()
        top2 = clean(pd.concat([t2, pd.DataFrame([{"true_pattern": "ALL", "cases": len(t),
                                                   "top1": t["top1"].mean(), "top2": t["top2"].mean()}])]))
        ex = (named.sort_values("score", ascending=False).drop_duplicates("group_id")
                   .groupby("predicted_pattern").head(3))
        alert_examples = clean(ex[["date", "acct", "score", "predicted_pattern", "pattern_confidence", "second_pattern",
                                   "second_confidence", "group_id", "g_accounts", "g_payments", "true_pattern"]])
    print(f"\n=== two guesses: {len(top2)} rows")

    # two queues: alerts the namer cannot match to any pattern ("unclassified") go to a second queue.
    # Validation (txn_v3m_xp, account namer): precision 0.912 -> 0.941 (HI-Medium), 0.897 -> 0.921 (HI-Small).
    queues = []
    if not named.empty:
        ad_pos = int(res[(res["level"] == "account_day") & (res["cutoff"] == "frozen_threshold")]["positives"].iloc[0])
        n_days = named["date"].nunique()
        second = named["predicted_pattern"] == "unclassified"
        for q, x in [("all", named), ("main", named[~second]), ("second", named[second])]:
            queues.append({"queue": q, "alerts": len(x), "true_alerts": int(x["label"].sum()), "positives": ad_pos,
                           "days": n_days, "precision": float(x["label"].mean()) if len(x) else None,
                           "recall": float(x["label"].sum() / ad_pos)})
        print("\n=== queues (unclassified alerts -> second queue)")
        print(pd.DataFrame(queues).round(3).to_string(index=False))

    # one row per scheme for the speed chart (hours from the scheme's first payment)
    per_scheme = sch.assign(
        hours_to_alert=(pd.to_datetime(sch["alert_ready_time"]) - pd.to_datetime(sch["start_time"])).dt.total_seconds() / 3600)
    per_scheme = clean(per_scheme[["pattern_type", "n_payments", "caught", "duration_h", "hours_to_first_flag",
                                   "hours_to_alert", "caught_before_end_eod"]].round(2))

    out = {"dataset": DATASET, "stage1": clean(res), "daily": clean(daily.sort_values(["level", "date"])),
           "budget": clean(budget.sort_values(["level", "k_per_day"])), "patterns": clean(p),
           "schemes_by_pattern": clean(by), "schemes_total": tot,
           "scheme_hours_hist": clean(sch.loc[sch["caught"], "hours_to_first_flag"].clip(upper=96)
                                       .pipe(lambda s: pd.cut(s, [-0.01, 1, 3, 6, 12, 24, 48, 96]).value_counts().sort_index()
                                             .rename("schemes").reset_index().assign(bucket=lambda d: d.iloc[:, 0].astype(str)))[["bucket", "schemes"]]),
           "stage2": clean(st2) if not st2.empty else [], "confusion": conf, "example_groups": examples,
           "alerts_by_predicted_pattern": alerts_by_pred,
           "schemes": per_scheme, "top2": top2, "alert_examples": alert_examples, "queues": queues,
           "source": "weekly pipeline (aml_bq1_weekly)" if LIVE else "one-off run", "weeks": weeks}
    out_file = (f"dashboard_data_live_{DATASET}.json" if LIVE
                else "dashboard_data.json" if RUN == "final" else f"dashboard_data_{RUN}.json")
    with open(out_file, "w") as f:
        json.dump(out, f, indent=1, default=str)
    print(f"\nwrote {out_file} (run {RUN})")


if __name__ == "__main__":
    main()
