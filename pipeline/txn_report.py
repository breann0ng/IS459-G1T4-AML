"""
txn_report.py
Print the saved results of train_txn.py runs, straight from S3 (no cluster, no logs needed).
Reads only small results tables, so it is fine to run on a laptop.

    python txn_report.py                       list every config that has results
    python txn_report.py txn_e2e txn_p1 txn_p2 txn_p3 txn_noformat txn_merge > step1_results.txt

For each config:
    STAGE 1   payment level and account-day level, at the validation best-F1 threshold
    STAGE 2   end to end (only stage 1's alerts are named; false alerts count as wrong),
              per pattern + macro, with the "caught" and "oracle" F1 alongside
"""

import os
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs

BUCKET = "pw-aml-g1t4"
ROOT = os.environ.get("AML_ROOT", f"s3://{BUCKET}")


def fs_path(uri):
    if uri.startswith("s3://"):
        fs = pafs.S3FileSystem(region=os.environ.get("AWS_REGION", "us-east-1"),
                               retry_strategy=pafs.AwsStandardS3RetryStrategy(max_attempts=20))
        return fs, uri[len("s3://"):]
    return pafs.FileSystem.from_uri(uri)


def plain(schema):
    big = {pa.large_string(): pa.string(), pa.large_binary(): pa.binary()}
    return pa.schema([pa.field(f.name, big.get(f.type, f.type)) for f in schema])


def load(name):
    fs, path = fs_path(f"{ROOT}/analytics/{name}")
    try:
        d = ds.dataset(path, filesystem=fs, format="parquet", partitioning="hive")
    except (FileNotFoundError, OSError):
        return pd.DataFrame()
    schema = pa.unify_schemas([plain(f.physical_schema) for f in d.get_fragments()] + [plain(d.schema)])
    return ds.dataset(path, filesystem=fs, format="parquet", partitioning="hive",
                      schema=schema).to_table().to_pandas()


def group_report(cfg):
    """Results of group_stage2.py (config <stage1>_grp): account vs group namer, end to end."""
    g = load("group_stage2")
    g = g[g["config"] == cfg] if not g.empty else g
    print(f"\n=================== {cfg}  (group_stage2.py)")
    if g.empty:
        print("  (no results saved: the run did not finish)")
        return
    for dname, x in g.groupby("dataset"):
        e = x[x["mode"] == "end_to_end"]
        a = e[e["method"] == "account"].set_index("pattern")
        b = e[e["method"] == "group"].set_index("pattern")
        t = a[["true_cases"]].copy()
        for m, s in (("account", a), ("group", b)):
            t[f"{m}_P"], t[f"{m}_R"], t[f"{m}_F1"] = s["precision"], s["recall"], s["f1"]
        t["change_F1"] = t["group_F1"] - t["account_F1"]
        for m in ("logreg", "rules"):                  # baselines, only with --baselines
            if (e["method"] == m).any():
                t[f"{m}_F1"] = e[e["method"] == m].set_index("pattern")["f1"]
        c = x[x["mode"] == "caught"].pivot_table(index="pattern", columns="method", values="f1")
        t["caught_account_F1"], t["caught_group_F1"] = c.get("account"), c.get("group")
        order = [p for p in t.index if not p.startswith("MACRO")] + [p for p in t.index if p.startswith("MACRO")]
        if "max_linked_days" in x and x["max_linked_days"].notna().any():
            print(f"   LINK_DAYS={int(x['link_days'].iloc[0])} MAX_LINKED_DAYS={int(x['max_linked_days'].iloc[0])}")
        print(f"-- {dname} · END TO END (account = today's namer, group = + group shape)")
        print(t.loc[order].round(3).to_string())


def main(configs):
    res, st2, imp = load("txn_results"), load("txn_stage2"), load("txn_feature_importance")
    if res.empty:
        raise SystemExit("No analytics/txn_results yet.")
    if not configs:
        print("Configs with results:", ", ".join(sorted(res["config"].unique())))
        return
    pd.set_option("display.width", 220)
    for cfg in configs:
        if cfg.endswith("_grp"):
            group_report(cfg)
            continue
        r = res[res["config"] == cfg]
        print(f"\n=================== {cfg}")
        if r.empty:
            print("  (no results saved: the run did not finish)")
            continue
        print(f"run at {r['run_at'].iloc[0]} · {int(r['n_features'].iloc[0])} features · "
              f"threshold {r['threshold_val_best_f1'].iloc[0]:.4f} · params {r['params_json'].iloc[0]}")
        print("STAGE 1 (validation best-F1 threshold)")
        s1 = r[r["cutoff"] == "val_best_f1"]
        cols = ["dataset", "level", "precision", "recall", "f1", "alerts_per_day", "pr_auc",
                "recall_patterns", "recall_unclassified"]
        print(s1[[c for c in cols if c in s1]].sort_values(["dataset", "level"]).round(3).to_string(index=False))
        if not imp.empty:
            top = imp[imp["config"] == cfg].sort_values("importance", ascending=False).head(15)
            if len(top):
                tot = imp.loc[imp["config"] == cfg, "importance"].sum()
                print("top features (share of total gain): " + ", ".join(
                    f"{f} {v / tot:.1%}" for f, v in zip(top["feature"], top["importance"])))
        s = st2[st2["config"] == cfg] if not st2.empty else st2
        if s.empty:
            print("STAGE 2: (not in this run)")
            continue
        print("\nSTAGE 2 END TO END (label precision / recall / F1 over stage 1's alerts) "
              "+ caught F1 + oracle F1 (diagnostic)")
        for dname, g in s.groupby("dataset"):
            e = g[g["mode"] == "end_to_end"].set_index("pattern")
            c = g[g["mode"] == "caught"].set_index("pattern")["f1"]
            o = g[g["mode"] == "oracle"].set_index("pattern")["f1"]
            t = e[["true_cases", "named_as", "correct", "precision", "recall", "f1"]].rename(
                columns={"precision": "label_precision", "recall": "e2e_recall", "f1": "e2e_f1"})
            t["caught_f1"], t["oracle_f1"] = c.reindex(t.index), o.reindex(t.index)
            order = [p for p in t.index if not p.startswith("MACRO")] + [p for p in t.index if p.startswith("MACRO")]
            print(f"-- {dname}")
            print(t.loc[order].round(3).to_string())


if __name__ == "__main__":
    main(sys.argv[1:])
