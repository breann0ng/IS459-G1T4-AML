"""
train_txn.py
Stage D of BQ1: transaction-level model (is THIS PAYMENT laundering?).
Kept alongside train_ml.py (account-day model); this one gives a number comparable to the
published per-payment F1 (IBM GFP + XGBoost ~0.63 on HI-Small, different split).

Plain Python on one machine (laptop or EMR master), after build_txn_features_spark.py:
    python3 train_txn.py                                    LightGBM, defaults below
    python3 train_txn.py --param num_leaves=63 --config txn_t2
    python3 train_txn.py --preset p2 --config txn_p2            tuning preset
    python3 train_txn.py --drop-format --config txn_noformat     ablation: no payment format
    python3 train_txn.py --merge-layering --config txn_merge     stage 2 with 7 classes
    python3 train_txn.py --txn-table txn_features_v3 --stage2-features features_v3 --preset p3 --config txn_v3
                                                            round 3 step 2 features
    python3 train_txn.py --train-datasets HI-Small --config txn_small   train/validate on one dataset
    python3 train_txn.py --config txn_final --test          final run only

Split (same rule as train_ml.py cross_dataset)
    train       first 75% of the days of each TRAIN_DATASETS dataset
    validation  last 25% of those days (must be >= FULL_FROM in build_txn_features_spark.py)
    test        every day of TEST_DATASET, only with --test
Threshold = best F1 on validation, applied unchanged to test.

Reported (per dataset x split x cut-off, written to analytics/txn_results)
    txn level        precision, recall, F1, PR-AUC, alerts/day, recall on scheme vs
                     unclassified laundering
    account-day view the same model rolled up: an account-day's score = the highest score of
                     any payment it sent or received, so it can be compared with train_ml.py

Stage 2 end to end (written to analytics/txn_stage2, alerts to analytics/txn_alerts)
    The pattern namer (same as train_ml.py's stage 2: LightGBM, default settings, trained on
    the true laundering account-days of the training period, features from --stage2-features)
    labels ONLY the account-days this model flagged, exactly as it would in real use.
    end_to_end  per pattern X, over the alerts:
                  label precision = alerts named X that really are X / all alerts named X
                                    (false alerts named X count as wrong)
                  recall          = alerts named X that really are X / ALL true X that day range
                                    (missed by stage 1 = not named)
    caught      naming accuracy on the true laundering stage 1 caught (no false alerts)
    oracle      every true laundering account-day, ignoring stage 1 (diagnostic only; the
                number stage_report.py shows)
"""

import argparse
import warnings
import gc
import resource
import json
import math
import os
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
from lightgbm import LGBMClassifier
from sklearn.utils.class_weight import compute_sample_weight
import xxhash
from sklearn.metrics import average_precision_score, precision_recall_curve

# =============================================================================== SETTINGS
BUCKET = "pw-aml-g1t4"
ROOT = os.environ.get("AML_ROOT", f"s3://{BUCKET}")
TRAIN_DATASETS = ["HI-Small", "HI-Medium"]
TEST_DATASET = "HI-Large"
VALIDATION_SHARE = 0.25
CONFIG_NAME = "txn_lgbm"
PARAMS = {"n_estimators": 600, "num_leaves": 63, "learning_rate": 0.05, "subsample": 0.8,
          "subsample_freq": 1, "colsample_bytree": 0.6, "min_child_samples": 50,
          "random_state": 42, "verbose": -1, "scale_pos_weight": "auto"}
# Tuning presets for --preset (override PARAMS); same idea as train_ml.py Task A
PRESETS = {
    "p1": {"scale_pos_weight": 1},                                     # no up-weighting
    "p2": {"n_estimators": 1000, "num_leaves": 127, "learning_rate": 0.03,
           "min_child_samples": 100, "scale_pos_weight": 1},           # bigger, slower
    "p3": {"n_estimators": 1000, "num_leaves": 127, "learning_rate": 0.03,
           "min_child_samples": 100, "scale_pos_weight": 1,
           "colsample_bytree": 0.4},                                   # p2, fewer features per tree
}
SCORE_CUTOFFS = [0.5, 0.9, 0.95, 0.99]
TOP_K_PER_DAY = [100, 1000, 5000]
BATCH_SIZE = 100_000          # rows read at a time; lower it if memory is tight
PATTERNS = ["fan_in", "fan_out", "gather_scatter", "scatter_gather", "cycle",
            "bipartite", "stack", "random"]
# Round 3 step 2 feature groups (in the *_v3 tables), for --new-groups
NEW_T = ["t_same_owner", "t_cross_country", "t_amt_vs_sender", "t_amt_vs_receiver", "t_sender_new"]


def group_of(col):
    """'x' = unusual for this account, 'o' = owner, 't' = new payment features, None = v2."""
    base = col[2:] if col.startswith(("s_", "r_")) else col
    if base.startswith("x_"):
        return "x"
    if base.startswith("o_"):
        return "o"
    if col in NEW_T:
        return "t"
    return None


def keep_groups(cols, groups):
    return [c for c in cols if group_of(c) is None or group_of(c) in groups]


STAGE2_PARAMS = {"n_estimators": 400, "num_leaves": 31, "learning_rate": 0.05, "subsample": 0.8,
                 "subsample_freq": 1, "colsample_bytree": 0.8, "random_state": 42, "verbose": -1}
STAGE2_META = ["dataset", "week", "acct_bank", "acct", "date", "label", "first_laundering_ts",
               "pattern_label"]
# =========================================================================================

META = ["dataset", "week", "txn_id", "date", "s_id", "r_id", "label", "pattern_label"]
CURATED, ANALYTICS = f"{ROOT}/curated", f"{ROOT}/analytics"
RUN_AT = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
RUN_ID = time.strftime("%Y%m%d%H%M%S", time.gmtime())


def fs_path(uri):
    """S3 with patient retries (a Wi-Fi blip should not kill a long run); local paths as is."""
    if uri.startswith("s3://"):
        fs = pafs.S3FileSystem(region=os.environ.get("AWS_REGION", "us-east-1"),
                               connect_timeout=30, request_timeout=300,
                               retry_strategy=pafs.AwsStandardS3RetryStrategy(max_attempts=20))
        return fs, uri[len("s3://"):]
    return pafs.FileSystem.from_uri(uri)


warnings.filterwarnings("ignore", message="X does not have valid feature names")


def spark_id(bank, acct):
    """Same value as Spark's xxhash64(cast(bank as string), acct): the s_id / r_id key."""
    h = xxhash.xxh64_intdigest(str(bank).encode(), seed=42)
    h = xxhash.xxh64_intdigest(str(acct).encode(), seed=h)
    return h - (1 << 64) if h >= (1 << 63) else h


def pattern_of(labels, merge):
    p = pd.Series(labels).where(pd.Series(labels).isin(PATTERNS), "unclassified")
    return (p.replace(merge) if merge else p).to_numpy()


def class_table(true, pred, classes, total_true=None):
    """Per class P / R / F1. total_true: true counts to use as the recall denominator."""
    rows = []
    for c in classes:
        correct = int(((true == c) & (pred == c)).sum())
        named = int((pred == c).sum())
        n_true = int(total_true.get(c, 0) if total_true is not None else (true == c).sum())
        p = correct / named if named else 0.0
        r = correct / n_true if n_true else 0.0
        rows.append({"pattern": c, "true_cases": n_true, "named_as": named, "correct": correct,
                     "precision": p, "recall": r, "f1": 2 * p * r / (p + r) if p + r else 0.0})
    t = pd.DataFrame(rows)
    pat = t[(t["pattern"] != "unclassified") & (t["true_cases"] > 0)]
    macro = {"pattern": f"MACRO ({len(pat)} patterns)", "true_cases": int(pat["true_cases"].sum()),
             "named_as": int(pat["named_as"].sum()), "correct": int(pat["correct"].sum()),
             "precision": pat["precision"].mean(), "recall": pat["recall"].mean(), "f1": pat["f1"].mean()}
    return pd.concat([t, pd.DataFrame([macro])], ignore_index=True)


def mem():
    """Peak memory of this process so far (Linux reports kB)."""
    return f"{resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 ** 2:.1f} GB peak"


def open_dataset(uri):
    fs, path = fs_path(uri)
    return ds.dataset(path, filesystem=fs, format="parquet", partitioning="hive")


def write_results(df, name):
    fs, path = fs_path(f"{ANALYTICS}/{name}")
    part = ds.partitioning(pa.schema([("dataset", pa.string()), ("model", pa.string()),
                                      ("config", pa.string())]), flavor="hive")
    ds.write_dataset(pa.Table.from_pandas(df, preserve_index=False), path, filesystem=fs,
                     format="parquet", partitioning=part,
                     basename_template=f"part-{RUN_ID}-{{i}}.parquet",
                     existing_data_behavior="delete_matching")


def value(text):
    for cast in (int, float):
        try:
            return cast(text)
        except ValueError:
            pass
    return text


def best_f1_threshold(y, s):
    p, r, t = precision_recall_curve(y, s)
    f = 2 * p * r / np.maximum(p + r, 1e-12)
    return float(t[np.argmax(f[:-1])])


def metrics(y, mask, days, scheme=None):
    tp = int((y & mask).sum()); n_alert = int(mask.sum()); n_pos = int(y.sum())
    p = tp / n_alert if n_alert else 0.0
    r = tp / n_pos if n_pos else 0.0
    row = {"alerts": n_alert, "true_alerts": tp, "positives": n_pos, "precision": p, "recall": r,
           "f1": 2 * p * r / (p + r) if p + r else 0.0, "alerts_per_day": n_alert / max(days, 1)}
    if scheme is not None:
        for name, m in (("recall_patterns", y & scheme), ("recall_unclassified", y & ~scheme)):
            row[name] = (m & mask).sum() / m.sum() if m.any() else None
    return row


def topk(date, s, k):
    order = np.lexsort((-s, date))
    d = date[order]
    start = np.r_[0, np.flatnonzero(d[1:] != d[:-1]) + 1]
    rank = np.arange(len(d)) - np.repeat(start, np.diff(np.r_[start, len(d)]))
    m = np.zeros(len(s), dtype=bool)
    m[order[rank < k]] = True
    return m


def main():
    global CONFIG_NAME, TRAIN_DATASETS
    ap = argparse.ArgumentParser(description="Transaction-level laundering model")
    ap.add_argument("--config", default=CONFIG_NAME)
    ap.add_argument("--train-datasets", default=",".join(TRAIN_DATASETS),
                    help="comma-separated datasets to train and validate on, e.g. HI-Small "
                         "(default HI-Small,HI-Medium; never HI-Large)")
    ap.add_argument("--param", action="append", default=[], metavar="KEY=VALUE")
    ap.add_argument("--test", action="store_true", help="also score TEST_DATASET (final run only)")
    ap.add_argument("--preset", choices=list(PRESETS), help="tuning preset (p1, p2, p3)")
    ap.add_argument("--drop-format", action="store_true",
                    help="ablation: remove every payment-format column (t_fmt_*, s_/r_n_formats)")
    ap.add_argument("--drop-account-features", action="store_true",
                    help="ablation: payment's own features only (no s_/r_ columns)")
    ap.add_argument("--txn-table", default="txn_features",
                    help="payment feature table (default txn_features; txn_features_v3 for step 2)")
    ap.add_argument("--stage2-features", default="features_v2",
                    help="account-day feature table for the pattern namer (default features_v2)")
    ap.add_argument("--merge-layering", action="store_true", help="stage 2: bipartite + stack -> layering")
    ap.add_argument("--no-stage2", action="store_true", help="skip the end-to-end stage 2")
    ap.add_argument("--new-groups", default="x,o,t",
                    help="v3 feature groups to use: x = unusual for this account, o = owner, "
                         "t = new payment features; e.g. --new-groups x (default all)")
    a = ap.parse_args()
    CONFIG_NAME = a.config
    TRAIN_DATASETS = [d for d in a.train_datasets.split(",") if d]
    if not TRAIN_DATASETS or TEST_DATASET in TRAIN_DATASETS:
        ap.error(f"--train-datasets must be non-empty and must not include {TEST_DATASET}")
    PARAMS.update(PRESETS.get(a.preset, {}))
    PARAMS.update({k: value(v) for k, v in (p.split("=", 1) for p in a.param)})

    sample_ds = open_dataset(f"{CURATED}/{a.txn_table}_sample")
    keep = [c for c in sample_ds.schema.names if c not in ("txn_id", "s_id", "r_id", "week", "pattern_label")]
    sample = sample_ds.to_table(columns=keep, filter=ds.field("dataset").isin(TRAIN_DATASETS)).to_pandas()
    sample["date"] = pd.to_datetime(sample["date"]).dt.date
    feat = [c for c in sample.columns if c not in META]
    if a.drop_account_features:
        feat = [c for c in feat if c.startswith("t_")]
    a.groups = {g for g in a.new_groups.split(",") if g}
    feat = keep_groups(feat, a.groups)
    if a.drop_format:
        feat = [c for c in feat if not (c.startswith("t_fmt_") or c.endswith("n_formats"))]
    train_end = {}
    for d in TRAIN_DATASETS:
        days = sorted(sample.loc[sample["dataset"] == d, "date"].unique())
        if days:
            train_end[d] = days[max(0, math.ceil(len(days) * (1 - VALIDATION_SHARE)) - 1)]
    in_train = np.zeros(len(sample), dtype=bool)
    for d, end in train_end.items():
        in_train |= (sample["dataset"] == d).to_numpy() & (sample["date"] <= end).to_numpy()
    train = sample[in_train]
    n_pos, n_neg = int(train["label"].sum()), int((train["label"] == 0).sum())
    print(f"[train] last training day {({k: str(v) for k, v in train_end.items()})}; rows={len(train)} "
          f"laundering={n_pos} normal={n_neg} features={len(feat)}")
    params = dict(PARAMS)
    if params.get("scale_pos_weight") == "auto":
        params["scale_pos_weight"] = n_neg / max(n_pos, 1)
    t0 = time.time()
    model = LGBMClassifier(**params).fit(train[feat].to_numpy(np.float32), train["label"].to_numpy())
    print(f"[train] {time.time() - t0:.0f}s ({mem()})")
    del sample, train
    gc.collect()

    # ---- score validation (+ test) in batches
    full = open_dataset(f"{CURATED}/{a.txn_table}")
    jobs = [(d, (ds.field("dataset") == d) & (ds.field("date") > pa.scalar(e, pa.date32())), "validation")
            for d, e in train_end.items()]
    if a.test:
        jobs.append((TEST_DATASET, ds.field("dataset") == TEST_DATASET, "test"))
    arrays = {}
    for d, flt, split in jobs:
        parts = {k: [] for k in ("s", "y", "date", "scheme", "s_id", "r_id")}
        scan = full.scanner(columns=feat + ["date", "label", "pattern_label", "s_id", "r_id"],
                            filter=flt, batch_size=BATCH_SIZE,
                            batch_readahead=1, fragment_readahead=1)    # read one piece at a time
        for b in scan.to_batches():
            if b.num_rows == 0:
                continue
            p = b.to_pandas()
            del b
            parts["s"].append(model.predict_proba(p[feat].to_numpy(np.float32))[:, 1].astype(np.float32))
            parts["y"].append(p["label"].to_numpy(bool))
            parts["date"].append(pd.to_datetime(p["date"]).to_numpy("datetime64[D]"))
            parts["scheme"].append(~p["pattern_label"].isin(["none", "unclassified"]).to_numpy())
            parts["s_id"].append(p["s_id"].to_numpy()); parts["r_id"].append(p["r_id"].to_numpy())
            del p
        if parts["s"]:
            arrays[(d, split)] = {k: np.concatenate(v) for k, v in parts.items()}
            print(f"[score] {d} {split}: {len(arrays[(d, split)]['s'])} payments ({mem()})")
        else:
            print(f"[score] {d} {split}: no rows in txn_features (check FULL_FROM)")

    val = [v for (d, sp), v in arrays.items() if sp == "validation"]
    thr = best_f1_threshold(np.concatenate([v["y"] for v in val]), np.concatenate([v["s"] for v in val]))
    print(f"[threshold] validation best-F1 = {thr:.4f}")

    rows, rollups = [], {}
    for (d, sp), v in sorted(arrays.items()):
        s, y, date = v["s"], v["y"], v["date"]
        days = len(np.unique(date))
        pr_auc = float(average_precision_score(y, s)) if y.any() else None
        masks = {str(c): s >= c for c in SCORE_CUTOFFS}
        masks.update({f"top{k}/day": topk(date, s, k) for k in TOP_K_PER_DAY})
        masks["val_best_f1"] = s >= thr
        for cut, m in masks.items():
            rows.append({"dataset": d, "split": sp, "level": "transaction", "cutoff": cut,
                         "pr_auc": pr_auc, **metrics(y, m, days, v["scheme"])})
        # account-day roll-up: max payment score per (account, day), sent or received
        ad = pd.DataFrame({"id": np.r_[v["s_id"], v["r_id"]], "date": np.r_[date, date],
                           "s": np.r_[s, s], "y": np.r_[y, y]})
        ad = ad.groupby(["id", "date"], sort=False).agg(s=("s", "max"), y=("y", "max"))
        rollups[(d, sp)] = ad
        ay, as_ = ad["y"].to_numpy(bool), ad["s"].to_numpy()
        adate = ad.index.get_level_values("date").to_numpy()
        a_auc = float(average_precision_score(ay, as_)) if ay.any() else None
        for cut, m in {"val_best_f1": as_ >= thr, "top100/day": topk(adate, as_, 100),
                       "top1000/day": topk(adate, as_, 1000)}.items():
            rows.append({"dataset": d, "split": sp, "level": "account_day_rollup", "cutoff": cut,
                         "pr_auc": a_auc, **metrics(ay, m, days)})
    res = pd.DataFrame(rows)
    res["model"], res["config"], res["threshold_val_best_f1"] = "lightgbm", CONFIG_NAME, thr
    res["params_json"], res["n_features"], res["run_at"] = json.dumps(PARAMS), len(feat), RUN_AT
    res["txn_table"], res["stage2_features"] = a.txn_table, a.stage2_features
    res["new_groups"] = ",".join(sorted(a.groups))
    res["train_end_dates"] = json.dumps({k: str(v) for k, v in train_end.items()})
    write_results(res, "txn_results")

    imp = pd.DataFrame({"feature": feat, "importance": model.booster_.feature_importance("gain")})
    imp = imp.sort_values("importance", ascending=False)
    imp["dataset"], imp["model"], imp["config"], imp["run_at"] = "+".join(TRAIN_DATASETS), "lightgbm", CONFIG_NAME, RUN_AT
    write_results(imp, "txn_feature_importance")

    if not a.no_stage2:
        stage2_end_to_end(a, train_end, rollups, thr)

    with pd.option_context("display.width", 220, "display.max_columns", 20):
        show = res[res["cutoff"].isin(["val_best_f1", "top100/day", "top1000/day"])]
        print(show[["dataset", "split", "level", "cutoff", "alerts", "precision", "recall", "f1",
                    "recall_patterns", "recall_unclassified", "alerts_per_day", "pr_auc"]]
              .round(4).to_string(index=False))
        print("\n[top features]", ", ".join(imp["feature"].head(12)))
    print(f"\nSaved analytics/txn_results under config={CONFIG_NAME}")


def stage2_end_to_end(a, train_end, rollups, thr):
    """Train the namer, run it on this model's account-day alerts, grade end to end."""
    merge = {"bipartite": "layering", "stack": "layering"} if a.merge_layering else {}
    classes = [p for p in PATTERNS if p not in merge] + sorted(set(merge.values())) + ["unclassified"]
    # ---- train (true laundering account-days of the training period)
    smp = open_dataset(f"{CURATED}/{a.stage2_features}_sample").to_table(
        filter=ds.field("dataset").isin(list(train_end)) & (ds.field("label") == 1)).to_pandas()
    smp["date"] = pd.to_datetime(smp["date"]).dt.date
    smp = smp[[dt_ <= train_end[d] for d, dt_ in zip(smp["dataset"], smp["date"])]]
    f2 = keep_groups([c for c in smp.columns if c not in STAGE2_META], a.groups)
    y2 = pattern_of(smp["pattern_label"], merge)
    namer = LGBMClassifier(objective="multiclass", **STAGE2_PARAMS)
    namer.fit(smp[f2].to_numpy(np.float32), y2, sample_weight=compute_sample_weight("balanced", y2))
    print(f"\n[stage2] namer trained on {len(smp)} laundering account-days, {len(f2)} features "
          f"({a.stage2_features}), merge={merge or 'none'} ({mem()})")
    del smp

    full2 = open_dataset(f"{CURATED}/{a.stage2_features}")
    tables, alerts_out = [], []
    for (d, sp), ad in sorted(rollups.items()):
        flagged = ad[ad["s"] >= thr].reset_index()[["id", "date", "s"]]
        flagged["date"] = pd.to_datetime(flagged["date"]).dt.date
        keys = set(zip(flagged["id"], flagged["date"]))
        start = min(flagged["date"]) if len(flagged) else None
        flt = ds.field("dataset") == d
        if sp == "validation":
            flt = flt & (ds.field("date") > pa.scalar(train_end[d], pa.date32()))
        keep = []
        scan = full2.scanner(columns=["acct_bank", "acct", "date", "label", "pattern_label"] + f2,
                             filter=flt, batch_size=BATCH_SIZE, batch_readahead=1, fragment_readahead=1)
        for b in scan.to_batches():
            p = b.to_pandas()
            p["date"] = pd.to_datetime(p["date"]).dt.date
            p["id"] = [spark_id(x, y) for x, y in zip(p["acct_bank"], p["acct"])]
            p["flagged"] = [(i, t) in keys for i, t in zip(p["id"], p["date"])]
            p = p[p["flagged"] | (p["label"] == 1)]
            if len(p):
                keep.append(p)
        g = pd.concat(keep, ignore_index=True)
        g["pred"] = namer.predict(g[f2].to_numpy(np.float32))
        g["conf"] = namer.predict_proba(g[f2].to_numpy(np.float32)).max(axis=1)
        g["true"] = np.where(g["label"] == 1, pattern_of(g["pattern_label"], merge), "not_laundering")
        n_found = int(g["flagged"].sum())
        print(f"[stage2] {d} {sp}: {len(flagged)} alerts ({n_found} matched to account-day rows), "
              f"{int((g['label'] == 1).sum())} true laundering account-days")
        lab = g[g["label"] == 1]
        al = g[g["flagged"]]
        total_true = lab["true"].value_counts().to_dict()
        caught = al[al["label"] == 1]
        for mode, t in [("end_to_end", class_table(al["true"].to_numpy(), al["pred"].to_numpy(), classes, total_true)),
                        ("caught", class_table(caught["true"].to_numpy(), caught["pred"].to_numpy(), classes)),
                        ("oracle", class_table(lab["true"].to_numpy(), lab["pred"].to_numpy(), classes))]:
            t["dataset"], t["split"], t["mode"] = d, sp, mode
            tables.append(t)
        with pd.option_context("display.width", 220):
            e2e, cau, ora = tables[-3], tables[-2], tables[-1]
            show = e2e[["pattern", "true_cases", "named_as", "correct", "precision", "recall", "f1"]].rename(
                columns={"precision": "label_precision", "recall": "e2e_recall", "f1": "e2e_f1"})
            show["caught_f1"], show["oracle_f1"] = cau["f1"].to_numpy(), ora["f1"].to_numpy()
            print(f"-- {d} {sp}   e2e = named right among this model's alerts (false alerts count as wrong);"
                  f" caught = only true alerts; oracle = all true laundering (diagnostic)")
            print(show.round(3).to_string(index=False))
            fa = al[al["label"] == 0]
            if len(fa):
                print(f"   false alerts ({len(fa)}) were named: "
                      + ", ".join(f"{k} {v}" for k, v in fa["pred"].value_counts().head(5).items()))
        out = al[["id", "acct_bank", "acct", "date", "label", "pred", "conf", "true"]].rename(
            columns={"label": "is_true", "pred": "predicted_pattern", "conf": "pattern_confidence",
                     "true": "true_pattern"})
        out = out.merge(flagged.rename(columns={"s": "score"}), how="left", on=["id", "date"]).drop(columns="id")
        out["dataset"], out["split"] = d, sp
        alerts_out.append(out)
    t = pd.concat(tables, ignore_index=True)
    t["model"], t["config"], t["stage2_features"], t["threshold"], t["run_at"] = (
        "lightgbm", CONFIG_NAME, a.stage2_features, thr, RUN_AT)
    write_results(t, "txn_stage2")
    al = pd.concat(alerts_out, ignore_index=True)
    al["model"], al["config"], al["run_at"] = "lightgbm", CONFIG_NAME, RUN_AT
    write_results(al, "txn_alerts")
    print("[stage2] saved analytics/txn_stage2 and analytics/txn_alerts")


if __name__ == "__main__":
    main()
