"""
scheme_stage2.py
Stage C of BQ1: name the pattern from the SHAPE OF THE GROUP, not from one account-day alone.

Plain Python on one machine (laptop or EMR master), after train_ml.py:
    pip3 install --user pandas pyarrow scikit-learn lightgbm networkx
    python3 scheme_stage2.py lgbm_p2                       uses that run's alerts + threshold
    python3 scheme_stage2.py lgbm_p2 --merge-layering      bipartite + stack -> "layering"
    python3 scheme_stage2.py lgbm_final --test             final run only (scores the test set)

Why
    train_ml.py names the pattern from one account's own numbers. But "fan-out" vs
    "scatter-gather" vs "bipartite" is a property of the whole group of accounts moving the
    money. This script rebuilds that group and gives the stage 2 model the group's shape.

How
    1. Nodes   = suspicious account-days.
                 Training: the true laundering account-days of the training period.
                 Scoring:  "alerts" = account-days stage 1 flagged (score >= its val_best_f1
                           threshold); the honest, end-to-end number.
                           "oracle" = every true laundering account-day; same population as
                           the stage 2 table in stage_report.py (an upper bound).
    2. Edges   = payments between two nodes on the same day, plus "same account, days within
                 LINK_DAYS" (a scheme can run over several days).
    3. Groups  = connected components. For each group: size, sources / sinks / middles,
                 max fan-in / fan-out, cycle, longest path, layers, density, reciprocity,
                 amount spread, days spanned.
       Role    = this account inside its group: in / out degree, source / sink / middle,
                 distance from the first source and to the last sink, share of the group.
    4. Two LightGBM classifiers, same settings, same rows:
                 "account"  account-day features only (what train_ml.py does)
                 "group"    account-day features + group shape + role
       The difference between them is what the group context adds.

Reads   analytics/ml_results, analytics/ml_alerts (the run named on the command line)
        curated/<features>_sample (training rows), curated/<features> (scoring rows)
        curated/transactions (only payments touching the nodes)
Writes  analytics/scheme_stage2/      per class precision / recall / F1, per mode and method
        analytics/scheme_stage2_pred/ every graded account-day with both predictions
"""

import argparse
import json
import os
import re
import time
from collections import defaultdict

import networkx as nx
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as pafs
from lightgbm import LGBMClassifier
from sklearn.utils.class_weight import compute_sample_weight

# =============================================================================== SETTINGS
ROOT = os.environ["AML_ROOT"]      # s3://<bucket>; no default, so a job never writes to the wrong bucket
PATTERNS = ["fan_in", "fan_out", "gather_scatter", "scatter_gather", "cycle",
            "bipartite", "stack", "random"]
LINK_DAYS = 2              # same account on days this close = same group
MAX_PATH_NODES = 3000      # groups bigger than this skip the (slow) longest-path measure
STAGE2_PARAMS = {"n_estimators": 400, "num_leaves": 31, "learning_rate": 0.05,
                 "subsample": 0.8, "subsample_freq": 1, "colsample_bytree": 0.8,
                 "random_state": 42, "verbose": -1}
BATCH_SIZE = 200_000
# =========================================================================================

META = ["dataset", "week", "acct_bank", "acct", "date", "label", "first_laundering_ts",
        "pattern_label"]


# AML_DROP_FEATURES: leave feature columns out of every model (detector and namer) in one place.
# A preset name or a regex; unset = all features (the default). "fan_windows" = the 9 sliding
# fan-in / fan-out / gather-scatter windows (s_/r_ prefixed in the detector, bare in the namer):
# on validation they changed account-day F1 by less than the seed-to-seed noise (8 Nov 2026).
DROP_PRESETS = {"fan_windows": r"(^|_)(fanin|fanout|gs)_max_\d+d$"}


def drop_features(cols):
    name = os.environ.get("AML_DROP_FEATURES", "").strip()
    if not name:
        return list(cols)
    rx = DROP_PRESETS.get(name, name)
    dropped = [c for c in cols if re.search(rx, c)]
    print(f"[features] AML_DROP_FEATURES={name}: dropped {len(dropped)}: {', '.join(dropped)}", flush=True)
    return [c for c in cols if c not in dropped]


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


def open_dataset(uri):
    fs, path = fs_path(uri)
    return ds.dataset(path, filesystem=fs, format="parquet", partitioning="hive")


def plain(schema):
    """Runs saved on different machines may store text as string or large_string (same thing):
    treat both as string so their files can be read together."""
    big = {pa.large_string(): pa.string(), pa.large_binary(): pa.binary()}
    return pa.schema([pa.field(f.name, big.get(f.type, f.type)) for f in schema])


def load_table(name, flt=None):
    """Read an analytics table, merging schemas of older and newer runs."""
    fs, path = fs_path(f"{ANALYTICS}/{name}")
    d = ds.dataset(path, filesystem=fs, format="parquet", partitioning="hive")
    schemas = [plain(f.physical_schema) for f in d.get_fragments()] + [plain(d.schema)]
    types = {}                              # a column saved as int in old runs and float in new ones -> float
    for s in schemas:
        for f in s:
            types.setdefault(f.name, set()).add(f.type)
    num = lambda t: pa.types.is_integer(t) or pa.types.is_floating(t)
    mixed = {n for n, ts in types.items() if len(ts) > 1 and all(map(num, ts))}
    schema = pa.unify_schemas([pa.schema([pa.field(f.name, pa.float64()) if f.name in mixed else f for f in s])
                               for s in schemas])
    return ds.dataset(path, filesystem=fs, format="parquet", partitioning="hive",
                      schema=schema).to_table(filter=flt).to_pandas()


def write_results(df, name, config, extra_parts=()):
    """extra_parts: more string partition columns already in df (e.g. run_date for daily runs)."""
    fs, path = fs_path(f"{ANALYTICS}/{name}")
    df = df.assign(config=config)
    part = ds.partitioning(pa.schema([(c, pa.string()) for c in ("dataset", "config", *extra_parts)]),
                           flavor="hive")
    ds.write_dataset(pa.Table.from_pandas(df, preserve_index=False), path, filesystem=fs,
                     format="parquet", partitioning=part,
                     basename_template=f"part-{RUN_ID}-{{i}}.parquet",
                     existing_data_behavior="delete_matching")


def key(bank, acct):
    return bank.astype(str) + "|" + acct.astype(str)


def true_class(df, merge):
    p = df["pattern_label"].where(df["pattern_label"].isin(PATTERNS), "unclassified")
    return p.replace(merge) if merge else p


# ============================================================================ graph part
def read_payments(dataset, start, end, accounts):
    """Payments in [start, end] whose sender AND receiver are in `accounts` (bank|acct)."""
    tx = open_dataset(f"{CURATED}/transactions")
    flt = ((ds.field("dataset") == dataset)
           & (ds.field("txn_date") >= pa.scalar(start, pa.date32()))
           & (ds.field("txn_date") <= pa.scalar(end, pa.date32())))
    names = pa.array(sorted({a.split("|", 1)[1] for a in accounts}))
    cols = ["txn_time", "txn_date", "from_bank", "from_account", "to_bank", "to_account",
            "amount_paid", "payment_currency", "payment_format"]
    out = []
    for b in tx.scanner(columns=cols, filter=flt, batch_size=BATCH_SIZE,
                          batch_readahead=1, fragment_readahead=1).to_batches():
        if b.num_rows == 0:
            continue
        m = pc.and_(pc.is_in(b["from_account"], names), pc.is_in(b["to_account"], names))
        b = b.filter(m)
        if b.num_rows:
            out.append(b.to_pandas())
    if not out:
        return pd.DataFrame(columns=cols + ["src", "dst"])
    p = pd.concat(out, ignore_index=True)
    p["src"], p["dst"] = key(p["from_bank"], p["from_account"]), key(p["to_bank"], p["to_account"])
    p = p[p["src"].isin(accounts) & p["dst"].isin(accounts) & (p["src"] != p["dst"])]
    p["date"] = pd.to_datetime(p["txn_date"]).dt.date
    p["amount_paid"] = p["amount_paid"].astype(float)
    return p


def build_groups(nodes, payments):
    """nodes: DataFrame with node key (k) and date. Returns node -> group id, and the
    payments that are edges (both ends are nodes on the payment's day)."""
    node_set = set(zip(nodes["k"], nodes["date"]))
    e = payments[[(s, d) in node_set and (t, d) in node_set
                  for s, t, d in zip(payments["src"], payments["dst"], payments["date"])]]
    uf = nx.utils.UnionFind(node_set)
    for s, t, d in zip(e["src"], e["dst"], e["date"]):
        uf.union((s, d), (t, d))
    by_acct = defaultdict(list)
    for k, d in node_set:
        by_acct[k].append(d)
    for k, days in by_acct.items():
        days.sort()
        for a, b in zip(days, days[1:]):
            if (b - a).days <= LINK_DAYS:
                uf.union((k, a), (k, b))
    gid = {n: i for i, s in enumerate(uf.to_sets()) for n in s}
    return gid, e


CYCLE_BOUND = 12           # longest cycle measured (simple_cycles length_bound)


def _longest_cycle(G):
    """Longest simple cycle up to CYCLE_BOUND. Stops as soon as one of length CYCLE_BOUND is found
    (nothing longer is ever counted), so the value is the same as max() over every cycle."""
    best = 0
    for c in nx.simple_cycles(G, length_bound=CYCLE_BOUND):
        if len(c) > best:
            best = len(c)
            if best >= CYCLE_BOUND:
                break
    return best


def _one_group(members, e):
    """Feature rows (dicts with _idx) for one group: members = its nodes, e = its edges (or None)."""
    accts = members["k"].unique()
    G = nx.DiGraph()
    G.add_nodes_from(accts)
    if e is not None:
        G.add_edges_from(zip(e["src"], e["dst"]))
    n = G.number_of_nodes()
    indeg, outdeg = dict(G.in_degree()), dict(G.out_degree())
    src = [a for a in G if indeg[a] == 0 and outdeg[a] > 0]
    snk = [a for a in G if outdeg[a] == 0 and indeg[a] > 0]
    mid = [a for a in G if indeg[a] > 0 and outdeg[a] > 0]
    has_cycle = not nx.is_directed_acyclic_graph(G)
    longest, from_src, to_snk = 0, {}, {}
    if not has_cycle and 1 < n <= MAX_PATH_NODES:
        order = list(nx.topological_sort(G))
        for a in order:                                  # longest path from any source
            from_src[a] = max((from_src[p] + 1 for p in G.predecessors(a)), default=0)
        for a in reversed(order):                        # longest path to any sink
            to_snk[a] = max((to_snk[s] + 1 for s in G.successors(a)), default=0)
        longest = max(from_src.values(), default=0)
    elif has_cycle and n <= MAX_PATH_NODES:
        longest = _longest_cycle(G) if n <= 200 else -1
    m = G.number_of_edges()
    recip = sum(1 for a, b in G.edges if G.has_edge(b, a)) / m if m else 0.0
    if e is not None and len(e):
        amt = np.log1p(e["amount_paid"].to_numpy())
        amt_cv, n_pay = float(np.std(amt) / (np.mean(amt) + 1e-9)), len(e)
        n_ccy, n_fmt = e["payment_currency"].nunique(), e["payment_format"].nunique()
        span_h = (e["txn_time"].max() - e["txn_time"].min()).total_seconds() / 3600
    else:
        amt_cv = n_pay = n_ccy = n_fmt = span_h = 0
    days = members["date"]
    shape = {
        "g_accounts": n, "g_nodes": len(members), "g_edges": m, "g_payments": n_pay,
        "g_days": (max(days) - min(days)).days + 1,
        "g_sources": len(src), "g_sinks": len(snk), "g_middles": len(mid),
        "g_isolated": n - len(src) - len(snk) - len(mid),
        "g_src_share": len(src) / n, "g_snk_share": len(snk) / n, "g_mid_share": len(mid) / n,
        "g_max_in": max(indeg.values(), default=0), "g_max_out": max(outdeg.values(), default=0),
        "g_max_in_share": max(indeg.values(), default=0) / n,
        "g_max_out_share": max(outdeg.values(), default=0) / n,
        "g_density": m / (n * (n - 1)) if n > 1 else 0.0,
        "g_has_cycle": int(has_cycle), "g_longest": longest, "g_reciprocity": recip,
        "g_amount_cv": amt_cv, "g_currencies": n_ccy, "g_formats": n_fmt,
        "g_span_h": span_h, "g_pay_per_edge": n_pay / m if m else 0.0,
    }
    rows = []
    for idx, a in zip(members.index, members["k"]):
        i, o = indeg.get(a, 0), outdeg.get(a, 0)
        preds, succs = list(G.predecessors(a)), list(G.successors(a))
        rows.append({"_idx": idx, **shape,
                     "r_in": i, "r_out": o, "r_in_share": i / n, "r_out_share": o / n,
                     "r_role": 0 if i == 0 and o == 0 else (1 if i == 0 else (2 if o == 0 else 3)),
                     "r_from_src": from_src.get(a, -1), "r_to_snk": to_snk.get(a, -1),
                     "r_pred_out_avg": np.mean([outdeg[p] for p in preds]) if preds else 0.0,
                     "r_succ_in_avg": np.mean([indeg[s] for s in succs]) if succs else 0.0,
                     "r_hub": int(max(i, o) == max(shape["g_max_in"], shape["g_max_out"]) and max(i, o) > 1)})
    return rows


def _timed_group(task):
    g, members, e = task
    t0 = time.time()
    rows = _one_group(members, e)
    return g, len(members), time.time() - t0, rows


def _workers():
    """Cores this process may use (AML_GROUP_WORKERS overrides; 1 = no pool)."""
    if os.environ.get("AML_GROUP_WORKERS"):
        return max(1, int(os.environ["AML_GROUP_WORKERS"]))
    try:
        return max(1, len(os.sched_getaffinity(0)))
    except AttributeError:
        return max(1, os.cpu_count() or 1)


def group_features(nodes, gid, edges):
    """One row per node: shape of its group + its role inside the group.
    Groups are independent, so they are measured in parallel worker processes (same values, any order)."""
    nodes = nodes.copy()
    nodes["gid"] = [gid[(k, d)] for k, d in zip(nodes["k"], nodes["date"])]
    edges = edges.copy()
    edges["gid"] = [gid[(s, d)] for s, d in zip(edges["src"], edges["date"])]
    eg = dict(tuple(edges.groupby("gid")))
    tasks = [(g, members, eg.get(g)) for g, members in nodes.groupby("gid")]
    tasks.sort(key=lambda t: -(0 if t[2] is None else len(t[2])))   # big groups first: no long tail
    workers = min(_workers(), len(tasks))
    t0, done = time.time(), []
    if workers > 1:
        import multiprocessing as mp
        ctx = mp.get_context("fork" if "fork" in mp.get_all_start_methods() else "spawn")
        with ctx.Pool(workers) as pool:
            done = list(pool.imap_unordered(_timed_group, tasks, chunksize=8))
    else:
        done = [_timed_group(t) for t in tasks]
    slow = sorted(done, key=lambda d: -d[2])[:3]
    print(f"[groups] {len(tasks)} groups on {workers} processes in {time.time() - t0:.0f}s; slowest: "
          + ", ".join(f"{n} nodes {s:.0f}s" for _, n, s, _ in slow))
    rows = [r for _, _, _, rs in done for r in rs]
    return pd.DataFrame(rows).set_index("_idx").loc[nodes.index]


def with_groups(rows, payments_by_ds):
    """Add group + role columns to rows (account-days of several datasets)."""
    parts = []
    for d, r in rows.groupby("dataset"):
        r = r.copy()
        r["k"] = key(r["acct_bank"], r["acct"])
        gid, edges = build_groups(r, payments_by_ds[d])
        gf = group_features(r, gid, edges)
        print(f"[groups] {d}: {len(r)} account-days -> {len(set(gid.values()))} groups, "
              f"{len(edges)} linking payments, largest group {int(gf['g_nodes'].max())} account-days")
        parts.append(pd.concat([r.drop(columns="k"), gf], axis=1))
    return pd.concat(parts)


def payments_for(rows):
    out = {}
    for d, r in rows.groupby("dataset"):
        accts = set(key(r["acct_bank"], r["acct"]))
        out[d] = read_payments(d, min(r["date"]), max(r["date"]), accts)
    return out


# ============================================================================ model part
def fit(X, y):
    model = LGBMClassifier(objective="multiclass", **STAGE2_PARAMS)
    model.fit(X, y, sample_weight=compute_sample_weight("balanced", y))
    return model


def per_class(true, pred, classes):
    rows = []
    for c in classes:
        tp = int(((true == c) & (pred == c)).sum())
        n_true, n_pred = int((true == c).sum()), int((pred == c).sum())
        p = tp / n_pred if n_pred else 0.0
        r = tp / n_true if n_true else 0.0
        rows.append({"pattern": c, "true_cases": n_true, "named_as": n_pred, "precision": p,
                     "recall": r, "f1": 2 * p * r / (p + r) if p + r else 0.0})
    t = pd.DataFrame(rows)
    pat = t[(t["pattern"] != "unclassified") & (t["true_cases"] > 0)]
    macro = {"pattern": f"MACRO ({len(pat)} classes)", "true_cases": int(pat["true_cases"].sum()),
             "named_as": int(pat["named_as"].sum()), "precision": pat["precision"].mean(),
             "recall": pat["recall"].mean(), "f1": pat["f1"].mean()}
    return pd.concat([t, pd.DataFrame([macro])], ignore_index=True)


def main():
    ap = argparse.ArgumentParser(description="Group-level (scheme-level) stage 2")
    ap.add_argument("config", help="CONFIG_NAME of a train_ml.py run, e.g. lgbm_p2")
    ap.add_argument("--model", default="lightgbm", help="stage 1 model of that run")
    ap.add_argument("--merge-layering", action="store_true", help="bipartite + stack -> layering")
    ap.add_argument("--test", action="store_true", help="also grade the test split (final run)")
    ap.add_argument("--out-config", help="name to save under (default <config>_group)")
    a = ap.parse_args()
    merge = {"bipartite": "layering", "stack": "layering"} if a.merge_layering else {}
    out_cfg = a.out_config or f"{a.config}_group"
    splits = ["validation", "test"] if a.test else ["validation"]

    # ---- the stage 1 run: threshold, training end dates, feature table
    res = load_table("ml_results", (ds.field("config") == a.config) & (ds.field("model") == a.model))
    res = res[res["cutoff"] == "val_best_f1"]
    if res.empty:
        raise SystemExit(f"No ml_results for config={a.config} model={a.model}: run train_ml.py first.")
    thr = float(res["threshold_val_best_f1"].iloc[0])
    train_end = {k: pd.Timestamp(v).date() for k, v in json.loads(res["train_end_dates"].iloc[0]).items()}
    ftab = res["features_table"].dropna().iloc[0] if "features_table" in res and res["features_table"].notna().any() else "features"
    print(f"[run] stage 1 = {a.config}/{a.model}, threshold {thr:.4f}, features {ftab}, "
          f"train end {({k: str(v) for k, v in train_end.items()})}, merge={merge or 'none'}")

    # ---- training rows: true laundering account-days of the training period
    sample = open_dataset(f"{CURATED}/{ftab}_sample").to_table(
        filter=ds.field("dataset").isin(list(train_end)) & (ds.field("label") == 1)).to_pandas()
    sample["date"] = pd.to_datetime(sample["date"]).dt.date
    train = sample[[r["date"] <= train_end[r["dataset"]] for _, r in sample.iterrows()]].copy()
    feat_cols = [c for c in sample.columns if c not in META]
    print(f"[train] {len(train)} laundering account-days, {len(feat_cols)} account-day features")
    t0 = time.time()
    train = with_groups(train, payments_for(train))
    print(f"[train] groups built in {time.time() - t0:.0f}s")
    group_cols = [c for c in train.columns if c.startswith(("g_", "r_"))]
    y = true_class(train, merge).to_numpy()
    models = {"account": (feat_cols, fit(train[feat_cols].fillna(0.0), y)),
              "group": (feat_cols + group_cols, fit(train[feat_cols + group_cols].fillna(0.0), y))}
    classes = [c for c in PATTERNS if c not in merge] + sorted(set(merge.values())) + ["unclassified"]

    # ---- scoring rows
    alerts = load_table("ml_alerts", (ds.field("config") == a.config) & (ds.field("model") == a.model))
    alerts = alerts[alerts["split"].isin(splits) & (alerts["score"] >= thr)]
    alerts["date"] = pd.to_datetime(alerts["date"]).dt.date
    full = open_dataset(f"{CURATED}/{ftab}")
    score_sets = sorted(set(alerts["dataset"]))
    cols = META + [c for c in feat_cols if c not in META]
    pieces = []
    for d in score_sets:
        start = min(alerts.loc[alerts["dataset"] == d, "date"])
        akeys = set(key(alerts.loc[alerts["dataset"] == d, "acct_bank"],
                        alerts.loc[alerts["dataset"] == d, "acct"]) + "@"
                    + alerts.loc[alerts["dataset"] == d, "date"].astype(str))
        flt = (ds.field("dataset") == d) & (ds.field("date") >= pa.scalar(start, pa.date32()))
        for b in full.scanner(columns=[c for c in cols if c != "dataset" and c != "week"] + ["dataset"],
                              filter=flt, batch_size=BATCH_SIZE, batch_readahead=1,
                              fragment_readahead=1).to_batches():
            if b.num_rows == 0:
                continue
            p = b.to_pandas()
            p["date"] = pd.to_datetime(p["date"]).dt.date
            k = key(p["acct_bank"], p["acct"]) + "@" + p["date"].astype(str)
            p["flagged"] = k.isin(akeys).to_numpy()
            p = p[p["flagged"] | (p["label"] == 1)]
            if len(p):
                pieces.append(p)
    scoring = pd.concat(pieces, ignore_index=True)
    split_of = alerts.drop_duplicates(["dataset", "acct_bank", "acct", "date"]).set_index(
        ["dataset", "acct_bank", "acct", "date"])["split"]

    all_tables, all_preds = [], []
    for mode, nodes in [("alerts", scoring[scoring["flagged"]]),
                        ("oracle", scoring[scoring["label"] == 1])]:
        print(f"\n=================== mode = {mode}  ({len(nodes)} account-days)")
        g = with_groups(nodes.copy(), payments_for(nodes))
        graded = g[g["label"] == 1].copy()          # only true laundering is graded
        idx = pd.MultiIndex.from_frame(graded[["dataset", "acct_bank", "acct", "date"]])
        graded["split"] = split_of.reindex(idx).fillna("validation").to_numpy()
        graded["true_pattern"] = true_class(graded, merge).to_numpy()
        for method, (cols_m, model) in models.items():
            graded[f"pred_{method}"] = model.predict(graded[cols_m].fillna(0.0))
        for (d, sp), gg in graded.groupby(["dataset", "split"]):
            if sp not in splits:
                continue
            for method in models:
                t = per_class(gg["true_pattern"].to_numpy(), gg[f"pred_{method}"].to_numpy(), classes)
                t["dataset"], t["split"], t["mode"], t["method"] = d, sp, mode, method
                all_tables.append(t)
            show = all_tables[-2][["pattern", "true_cases", "f1"]].rename(columns={"f1": "f1_account"})
            show["f1_group"] = all_tables[-1]["f1"].to_numpy()
            show["precision_group"] = all_tables[-1]["precision"].to_numpy()
            show["recall_group"] = all_tables[-1]["recall"].to_numpy()
            print(f"-- {d} {sp}   (f1_account = account-day features only, f1_group = + group shape)")
            print(show.round(3).to_string(index=False))
        all_preds.append(graded[["dataset", "acct_bank", "acct", "date", "split", "true_pattern",
                                 "pred_account", "pred_group"]].assign(mode=mode))

    out = pd.concat(all_tables, ignore_index=True)
    out["stage1_config"], out["threshold"], out["run_at"] = a.config, thr, RUN_AT
    write_results(out, "scheme_stage2", out_cfg)
    preds = pd.concat(all_preds, ignore_index=True)
    preds["run_at"] = RUN_AT
    write_results(preds, "scheme_stage2_pred", out_cfg)

    imp = pd.Series(models["group"][1].feature_importances_, index=models["group"][0])
    print("\n[group model] top features:", ", ".join(imp.sort_values(ascending=False).head(10).index))
    print(f"\nSaved analytics/scheme_stage2 and scheme_stage2_pred under config={out_cfg}")
    print("Read: 'alerts' = honest end-to-end (named right among what stage 1 caught); "
          "'oracle' = upper bound on all true laundering.")


if __name__ == "__main__":
    main()
