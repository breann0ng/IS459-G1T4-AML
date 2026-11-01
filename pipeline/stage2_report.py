"""
stage2_report.py
Compare stage 2 (pattern naming) experiments on the VALIDATION days and write the data for
app/app_stage2.py. HI-Large is never read. Laptop is fine (small results tables only):

    set AML_ROOT=s3://<bucket>
    python stage2_report.py                          every *_grp run, best one picked by macro F1
    python stage2_report.py s2_leaves63_grp          show this run against the baseline
    python stage2_report.py --baseline txn_v3m_grp s2_leaves63_grp

Reads   analytics/group_stage2, analytics/group_stage2_confusion (group_stage2.py runs)
        analytics/txn_results (stage 1 of the same train_txn.py run, for context)
Writes  ../app/data/stage2_data.json
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

import txn_report as R

OUT = Path(__file__).resolve().parent.parent / "app" / "data" / "stage2_data.json"


def records(df):
    return json.loads(df.replace({np.nan: None}).to_json(orient="records"))


def main():
    ap = argparse.ArgumentParser(description="Stage 2 validation report + dashboard data")
    ap.add_argument("pick", nargs="?", help="group_stage2.py config to show (default: best macro F1)")
    ap.add_argument("--baseline", default="txn_v3m_grp")
    a = ap.parse_args()
    pd.set_option("display.width", 220)

    g = R.load("group_stage2")
    if g.empty:
        raise SystemExit("No analytics/group_stage2 yet: run group_stage2.py on the cluster first.")
    if a.baseline not in set(g["config"]):
        raise SystemExit(f"Baseline {a.baseline} not found. Runs: {', '.join(sorted(g['config'].unique()))}")
    e2e = g[g["mode"] == "end_to_end"]
    macro = e2e[e2e["pattern"].str.startswith("MACRO")]

    # ---- leaderboard: the group namer of every run, plus the old account namer of the baseline
    board = macro[macro["method"] == "group"].copy()
    board["run"] = board["config"]
    old = macro[(macro["method"] == "account") & (macro["config"] == a.baseline)].copy()
    old["run"] = "account namer (before group shape)"
    board = pd.concat([board, old], ignore_index=True)
    board["settings_json"] = board.get("settings_json", pd.Series(dtype=str)).fillna("{}")
    board = board[["run", "config", "method", "dataset", "stage1_config", "precision", "recall", "f1",
                   "settings_json", "run_at"]]
    mean_f1 = board[board["method"] == "group"].groupby("config")["f1"].mean()
    pick = a.pick or mean_f1.idxmax()
    if pick not in mean_f1.index:
        raise SystemExit(f"{pick} not found. Runs: {', '.join(sorted(mean_f1.index))}")
    if g.loc[g["config"] == pick, "stage1_config"].iloc[0] != g.loc[g["config"] == a.baseline, "stage1_config"].iloc[0]:
        print("WARNING: the picked run and the baseline name the alerts of DIFFERENT stage 1 runs; "
              "the comparison mixes stage 1 and stage 2 changes.")

    print("=== STAGE 2 · validation · end to end · macro over named patterns (unclassified excluded)")
    show = board.pivot_table(index="run", columns="dataset", values=["precision", "recall", "f1"])
    show.columns = [f"{m}_{d}" for m, d in show.columns]
    show["mean_f1"] = show[[c for c in show.columns if c.startswith("f1_")]].mean(axis=1)
    print(show.sort_values("mean_f1", ascending=False).round(3).to_string())
    print(f"\nbaseline {a.baseline} · showing {pick}")

    per = g[(g["method"] == "group") & g["config"].isin([a.baseline, pick])
            & ~g["pattern"].str.startswith("MACRO")]
    per = per[["config", "dataset", "mode", "pattern", "true_cases", "named_as", "correct",
               "precision", "recall", "f1"]]
    conf = R.load("group_stage2_confusion")
    conf = (conf[(conf["config"] == pick) & (conf["method"] == "group")]
            [["dataset", "true_pattern", "predicted_pattern", "n"]] if not conf.empty else conf)
    if conf.empty:
        print(f"(no confusion counts for {pick}: rerun it with the current group_stage2.py)")

    res = R.load("txn_results")
    s1_cfg = g.loc[g["config"] == pick, "stage1_config"].iloc[0]
    r = res[(res["config"] == s1_cfg) & (res["cutoff"] == "val_best_f1") & (res["split"] == "validation")]
    s1 = r[r["level"] == "account_day_rollup"][["dataset", "precision", "recall", "f1", "alerts_per_day"]]
    s1 = s1.merge(r[r["level"] == "transaction"][["dataset", "recall_patterns", "recall_unclassified"]],
                  on="dataset", how="left")

    out = {"baseline": a.baseline, "pick": pick, "stage1_config": s1_cfg,
           "board": records(board), "per_pattern": records(per),
           "confusion": records(conf) if not conf.empty else [], "stage1": records(s1)}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=1, default=str))
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
