"""
bq2_report.py
Fetch the BQ2 results (analytics/bq2/<dataset>.json, written by bq2_analysis_spark.py), print a
plain-text report, and copy the file to app/data/bq2_data.json for the Streamlit page.
Laptop is fine:
    export AML_ROOT=s3://<bucket>          (Windows: set AML_ROOT=s3://<bucket>)
    python pipeline/bq2_report.py HI-Large > results/bq2/bq2_report.txt
"""

import json
import os
import sys
from pathlib import Path

import pandas as pd

ROOT = os.environ.get("AML_ROOT", "s3://pw-aml-g1t4")
DATASET = sys.argv[1] if len(sys.argv) > 1 else "HI-Large"


def fetch(uri):
    import pyarrow.fs as pafs
    if uri.startswith("s3://"):
        fs, path = pafs.S3FileSystem(region=os.environ.get("AWS_REGION", "us-east-1")), uri[5:]
    else:
        fs, path = pafs.LocalFileSystem(), os.path.abspath(uri)
    with fs.open_input_stream(path) as f:
        return f.read().decode()


def pct(x):
    return "-" if x is None or pd.isna(x) else f"{x:.1%}"


def main():
    body = fetch(f"{ROOT}/analytics/bq2/{DATASET}.json")
    d = json.loads(body)
    out = Path(__file__).resolve().parent.parent / "app" / "data" / "bq2_data.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(body)
    pd.set_option("display.width", 220)
    pd.set_option("display.max_columns", 30)
    wk = pd.DataFrame(d["weeks"])
    print(f"=== BQ2 · {d['dataset']} · weeks {wk['week'].min()}-{wk['week'].max()} "
          f"({d['window'][0]} to {d['window'][1]})")
    print(f"baseline weeks {d['baseline_weeks']}; controlled test: {'+'.join(d['hidden'])} appear in week "
          f"{d['intro_week']}; labels assumed {d['label_delay_weeks']} weeks late")
    for m, v in d["models"].items():
        print(f"  model {m}: {v['folder']}, threshold {v['threshold']:.4f}, never saw {v['excluded_patterns'] or 'nothing'}")

    shift = pd.DataFrame(d["tactics_shift"])
    print("\n=== BQ2a · tactic shift vs baseline (PSI; 0.1 moderate, 0.2 large)")
    for scen in ("real", "new_tactic"):
        t = shift[shift["scenario"] == scen].pivot(index="week", columns="dimension", values="psi")
        print(f"-- {scen}\n{t.round(3).to_string()}")

    card = pd.DataFrame(d["scorecard"])
    print("\n=== BQ2b · weekly detection (overall)")
    o = card[card["group"] == "overall"][["scenario", "model", "week", "alerts_per_day", "precision", "recall",
                                          "precision_top1000", "positives"]]
    print(o.round(3).to_string(index=False))
    print("\n=== recall by group, controlled test")
    g = card[(card["scenario"] == "new_tactic") & (card["group"] != "overall")]
    print(g.pivot_table(index=["group", "model"], columns="week", values="recall").round(2).to_string())

    sc = pd.DataFrame(d["scheme_card"])
    if not sc.empty:
        print("\n=== schemes alerted before their last payment (end-of-day alerts)")
        print(sc.pivot_table(index=["scenario", "group", "model"], columns="week", values="before_end").round(2).to_string())

    dr = pd.DataFrame(d["drift"])
    print("\n=== early warning (no labels)")
    print(dr[["scenario", "model", "week", "alerts_per_day", "alerts_z", "score_psi", "feature_psi_max",
              "feature_psi_top"]].round(3).to_string(index=False))

    al = pd.DataFrame(d["alarms"])
    print("\n=== alarms (alarm_week = week the problem happened; known_week = week we could know)")
    print(al[al["alarm_week"].notna()].to_string(index=False))
    print("\n=== headline")
    for h in d["headline"]:
        print(f"  {h['scenario']:>10} / {h['model']:>5}: falling behind in week {h['falling_behind_week']} "
              f"({h['falling_behind_signal']}), known week {h['falling_behind_known_week']}; first early "
              f"warning week {h['early_warning_week']} ({h['early_warning_signal']}); lead time "
              f"{h['lead_time_weeks']} week(s)")
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
