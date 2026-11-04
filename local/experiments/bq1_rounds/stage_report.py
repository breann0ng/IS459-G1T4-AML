"""
stage_report.py
Stage 1 vs stage 2 precision / recall / F1 for one or more saved runs (no retraining).

Run (EMR master node or laptop with AWS credentials):
    python3 stage_report.py lgbm_p2 xgb_p2

Stage 1  "is this account-day laundering?"      read from analytics/ml_results (val_best_f1 row)
Stage 2  "which pattern is this laundering?"
         oracle      from analytics/ml_confusion: graded on every true laundering account-day,
                     ignoring stage 1 (a diagnostic of the namer on its own)
         end to end  from ml_alerts + ml_pattern_results: only the account-days stage 1 flagged
                     (score >= its val_best_f1 threshold) are named, as in real use
                     label precision = alerts named X that are X / all alerts named X
                     recall          = alerts named X that are X / all true X
         (for train_txn.py runs the same tables are in analytics/txn_stage2)
         precision(X) = correctly named X / all named X
         recall(X)    = correctly named X / all true X
         macro        = plain average over the patterns ("unclassified" reported separately;
                        a --merge-layering run has 7 classes, with "layering" for bipartite + stack)
"""

import os
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs

BUCKET = "pw-aml-g1t4"
ROOT = os.environ.get("AML_ROOT", f"s3://{BUCKET}")
PATTERNS = ["fan_in", "fan_out", "gather_scatter", "scatter_gather", "cycle",
            "bipartite", "stack", "random"]


def fs_path(uri):
    """S3 with patient retries (a Wi-Fi blip should not kill a long run); local paths as is."""
    if uri.startswith("s3://"):
        fs = pafs.S3FileSystem(region=os.environ.get("AWS_REGION", "us-east-1"),
                               connect_timeout=30, request_timeout=300,
                               retry_strategy=pafs.AwsStandardS3RetryStrategy(max_attempts=20))
        return fs, uri[len("s3://"):]
    return pafs.FileSystem.from_uri(uri)


def plain(schema):
    """Runs saved on different machines may store text as string or large_string (same thing):
    treat both as string so their files can be read together."""
    big = {pa.large_string(): pa.string(), pa.large_binary(): pa.binary()}
    return pa.schema([pa.field(f.name, big.get(f.type, f.type)) for f in schema])


def load(name):
    """Read a results table. Older and newer runs may have different columns, so the schemas
    of all files are merged (columns missing in older runs come back empty)."""
    fs, path = fs_path(f"{ROOT}/analytics/{name}")
    d = ds.dataset(path, filesystem=fs, format="parquet", partitioning="hive")
    schema = pa.unify_schemas([plain(f.physical_schema) for f in d.get_fragments()] + [plain(d.schema)])
    return ds.dataset(path, filesystem=fs, format="parquet", partitioning="hive",
                      schema=schema).to_table().to_pandas()


def f1(p, r):
    return 2 * p * r / (p + r) if p and r and (p + r) else 0.0


def end_to_end(cfg, res, pres, alerts):
    """Stage 2 graded only on what stage 1 flagged (round 1 style runs of train_ml.py)."""
    print("\nSTAGE 2 END TO END (only stage 1's alerts are named; false alerts named X count as wrong)")
    for (dname, split), r in res[res["config"] == cfg].groupby(["dataset", "split"]):
        thr = float(r["threshold_val_best_f1"].iloc[0])
        al = alerts[(alerts["config"] == cfg) & (alerts["dataset"] == dname) & (alerts["split"] == split)
                    & (alerts["score"] >= thr)]
        pr = pres[(pres["config"] == cfg) & (pres["dataset"] == dname) & (pres["split"] == split)
                  & (pres["cutoff"] == "val_best_f1")]
        if al.empty or pr.empty:
            continue
        named = al["predicted_pattern"].value_counts()
        rows = []
        for _, p in pr.iterrows():
            n = int(named.get(p["pattern"], 0)); c = int(p["detected_and_named"]); t = int(p["positives"])
            prec = c / n if n else 0.0; rec = c / t if t else 0.0
            rows.append({"pattern": p["pattern"], "true_cases": t, "named_as": n, "correct": c,
                         "label_precision": prec, "e2e_recall": rec, "e2e_f1": f1(prec, rec)})
        t = pd.DataFrame(rows)
        pat = t[t["pattern"] != "unclassified"]
        t = pd.concat([t, pd.DataFrame([{"pattern": f"MACRO ({len(pat)} classes)",
                                         "true_cases": int(pat["true_cases"].sum()), "named_as": int(pat["named_as"].sum()),
                                         "correct": int(pat["correct"].sum()), "label_precision": pat["label_precision"].mean(),
                                         "e2e_recall": pat["e2e_recall"].mean(), "e2e_f1": pat["e2e_f1"].mean()}])],
                      ignore_index=True)
        print(f"-- {dname} {split}  ({len(al)} alerts, {int((al['is_true'] == 1).sum())} true)")
        print(t.round(3).to_string(index=False))


def main(configs):
    res, conf = load("ml_results"), load("ml_confusion")
    try:
        pres, alerts = load("ml_pattern_results"), load("ml_alerts")
    except Exception:
        pres = alerts = None
    pd.set_option("display.width", 200)
    for cfg in configs:
        print(f"\n=================== {cfg}")
        s1 = res[(res["config"] == cfg) & (res["cutoff"] == "val_best_f1")]
        print("STAGE 1 (laundering yes/no, at the best-F1 threshold)")
        cols = ["dataset", "split", "precision", "recall_acct_day", "f1", "alerts_per_day"]
        cols += [c for c in ("recall_patterns", "recall_unclassified") if c in s1.columns]
        print(s1[cols].round(3).to_string(index=False))

        c = conf[conf["config"] == cfg]
        print("\nSTAGE 2 ORACLE (which pattern, on every true laundering account-day; diagnostic)")
        for (dname, split), g in c.groupby(["dataset", "split"]):
            m = g.pivot_table(index="true_pattern", columns="predicted_pattern", values="n",
                              aggfunc="sum", fill_value=0)
            names = set(m.index) | set(m.columns)
            classes = [p for p in PATTERNS if p in names] + sorted(names - set(PATTERNS) - {"unclassified"})
            rows = []
            for p in classes + ["unclassified"]:
                tp = m.at[p, p] if p in m.index and p in m.columns else 0
                named = m[p].sum() if p in m.columns else 0
                true = m.loc[p].sum() if p in m.index else 0
                prec = tp / named if named else 0.0
                rec = tp / true if true else 0.0
                rows.append({"pattern": p, "true_cases": int(true), "named_as": int(named),
                             "precision": prec, "recall": rec, "f1": f1(prec, rec)})
            t = pd.DataFrame(rows)
            pat = t[t["pattern"].isin(classes)]
            macro = {"pattern": f"MACRO ({len(classes)} classes)", "true_cases": int(pat["true_cases"].sum()),
                     "named_as": int(pat["named_as"].sum()), "precision": pat["precision"].mean(),
                     "recall": pat["recall"].mean(), "f1": pat["f1"].mean()}
            t = pd.concat([t, pd.DataFrame([macro])], ignore_index=True)
            print(f"-- {dname} {split}")
            print(t.round(3).to_string(index=False))
        if pres is not None:
            end_to_end(cfg, res[res["cutoff"] == "val_best_f1"], pres, alerts)


if __name__ == "__main__":
    main(sys.argv[1:] or ["lgbm_p2"])
