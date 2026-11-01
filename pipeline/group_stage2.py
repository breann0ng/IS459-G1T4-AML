"""
group_stage2.py
Round 3, step 3: name the pattern from the GROUP's shape, fixed version of Task C.

Plain Python on the EMR primary node (needs scheme_stage2.py in the same folder):
    pip3 install --user pandas pyarrow scikit-learn lightgbm networkx
    python3 group_stage2.py txn_v3m                 stage 1 + namer = that train_txn.py run
    python3 group_stage2.py txn_v3m --param num_leaves=63 --out-config s2_leaves63
                                                    one namer / grouping setting changed

What changed from Task C (scheme_stage2.py)
    1. Groups are built from the PAYMENT model's alerts (txn_alerts of the given run), which
       catch most of each scheme at ~0.9 precision, instead of round 1's alerts (~31% recall).
    2. Trained on deliberately INCOMPLETE groups: the training schemes are rebuilt with only
       KEEP_RATES of their account-days (100%, 70%, 50%), so the namer has seen fragments.
    3. Busy accounts no longer chain separate schemes together: an account with more than
       HUB_DEGREE linked counterparties does not join groups, and an account is only linked
       across days if it has at most MAX_LINKED_DAYS suspicious days.
    4. Graded END TO END, exactly like train_txn.py: only stage 1's alerts are named, false
       alerts named X count as wrong, missed laundering counts against recall.
    Compared on the same alerts:
        account   the run's own namer (predicted_pattern in txn_alerts): today's stage 2
        group     account-day features + 35 group-shape / role features

Reads   analytics/txn_alerts, analytics/txn_results (the run), curated/<features>(_sample),
        curated/transactions (only payments between the accounts involved)
Writes  analytics/group_stage2/            per class P / R / F1 per dataset, method and mode
        analytics/group_stage2_confusion/  alerts per true x predicted pattern (dashboard)
"""

import argparse
import json
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import networkx as nx
from collections import defaultdict

import scheme_stage2 as S

KEEP_RATES = [1.0, 0.7, 0.5]     # training copies: whole schemes, and two fragment levels
HUB_DEGREE = 30                  # accounts linked to more counterparties than this don't join groups
MAX_LINKED_DAYS = 3              # link an account across days only if it has this few suspicious days
SEED = 42
META = S.META


def build_groups(nodes, payments):
    """Like Task C, but busy accounts cannot chain separate schemes into one giant group."""
    node_set = set(zip(nodes["k"], nodes["date"]))
    e = payments[[(s, d) in node_set and (t, d) in node_set
                  for s, t, d in zip(payments["src"], payments["dst"], payments["date"])]]
    deg = defaultdict(set)
    for s, t in zip(e["src"], e["dst"]):
        deg[s].add(t); deg[t].add(s)
    hub = {a for a, cp in deg.items() if len(cp) > HUB_DEGREE}
    uf = nx.utils.UnionFind(node_set)
    for s, t, d in zip(e["src"], e["dst"], e["date"]):
        if s not in hub and t not in hub:
            uf.union((s, d), (t, d))
    by_acct = defaultdict(list)
    for k, d in node_set:
        by_acct[k].append(d)
    for k, days in by_acct.items():
        if k in hub or len(days) > MAX_LINKED_DAYS:
            continue
        days.sort()
        for a, b in zip(days, days[1:]):
            if (b - a).days <= S.LINK_DAYS:
                uf.union((k, a), (k, b))
    gid = {n: i for i, s in enumerate(uf.to_sets()) for n in s}
    same = [gid[(s, d)] == gid[(t, d)] for s, t, d in zip(e["src"], e["dst"], e["date"])]
    return gid, e[same], len(hub)


def with_groups(rows, payments_by_ds, label=""):
    parts = []
    for d, r in rows.groupby("dataset"):
        r = r.copy()
        r["k"] = S.key(r["acct_bank"], r["acct"])
        gid, edges, n_hub = build_groups(r, payments_by_ds[d])
        gf = S.group_features(r, gid, edges)
        print(f"[groups] {label} {d}: {len(r)} account-days -> {len(set(gid.values()))} groups "
              f"({len(r) / max(len(set(gid.values())), 1):.1f} per group), {len(edges)} linking payments, "
              f"{n_hub} busy accounts kept apart, largest group {int(gf['g_nodes'].max())}")
        parts.append(pd.concat([r.drop(columns="k"), gf], axis=1))
    return pd.concat(parts)


def class_table(true, pred, classes, total_true=None):
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


def value(text):
    for cast in (int, float):
        try:
            return cast(text)
        except ValueError:
            pass
    return text


def main():
    global KEEP_RATES, HUB_DEGREE, MAX_LINKED_DAYS
    ap = argparse.ArgumentParser(description="Group-level stage 2, end to end")
    ap.add_argument("stage1", help="config of a train_txn.py run, e.g. txn_v3m")
    ap.add_argument("--features", default="features_v3", help="account-day table (namer features)")
    ap.add_argument("--out-config", help="name to save under (default <stage1>_grp; _grp is "
                                             "added if missing, txn_report.py needs it)")
    ap.add_argument("--param", action="append", default=[], metavar="KEY=VALUE",
                    help="override a namer LightGBM setting, e.g. --param num_leaves=63 (repeatable)")
    ap.add_argument("--keep-rates", help=f"training fragment levels, e.g. 1,0.7 (default "
                                         f"{','.join(map(str, KEEP_RATES))})")
    ap.add_argument("--hub-degree", type=int, default=HUB_DEGREE)
    ap.add_argument("--max-linked-days", type=int, default=MAX_LINKED_DAYS)
    ap.add_argument("--link-days", type=int, default=S.LINK_DAYS)
    a = ap.parse_args()
    out_cfg = a.out_config or f"{a.stage1}_grp"
    if not out_cfg.endswith("_grp"):
        out_cfg += "_grp"
    if a.keep_rates:
        KEEP_RATES = [float(k) for k in a.keep_rates.split(",")]
    HUB_DEGREE, MAX_LINKED_DAYS, S.LINK_DAYS = a.hub_degree, a.max_linked_days, a.link_days
    S.STAGE2_PARAMS.update({k: value(v) for k, v in (p.split("=", 1) for p in a.param)})
    settings = {"namer_params": S.STAGE2_PARAMS, "keep_rates": KEEP_RATES, "hub_degree": HUB_DEGREE,
                "max_linked_days": MAX_LINKED_DAYS, "link_days": S.LINK_DAYS, "features": a.features}
    print(f"[settings] {json.dumps(settings)}")
    t0 = time.time()

    res = S.load_table("txn_results", ds.field("config") == a.stage1)
    if res.empty:
        raise SystemExit(f"No txn_results for {a.stage1}")
    train_end = {k: pd.Timestamp(v).date() for k, v in json.loads(res["train_end_dates"].iloc[0]).items()}
    alerts = S.load_table("txn_alerts", ds.field("config") == a.stage1)
    alerts = alerts[alerts["split"] == "validation"].copy()
    alerts["date"] = pd.to_datetime(alerts["date"]).dt.date
    merge = ({"bipartite": "layering", "stack": "layering"}
             if (alerts["predicted_pattern"] == "layering").any() else {})
    classes = [p for p in S.PATTERNS if p not in merge] + sorted(set(merge.values())) + ["unclassified"]
    print(f"[run] stage 1 = {a.stage1}: {len(alerts)} validation alerts, train end "
          f"{ {k: str(v) for k, v in train_end.items()} }, classes {len(classes) - 1} + unclassified")

    # ---- training: true laundering of the training days, whole and as fragments
    smp = S.open_dataset(f"{S.CURATED}/{a.features}_sample").to_table(
        filter=ds.field("dataset").isin(list(train_end)) & (ds.field("label") == 1)).to_pandas()
    smp["date"] = pd.to_datetime(smp["date"]).dt.date
    smp = smp[[dt_ <= train_end[d] for d, dt_ in zip(smp["dataset"], smp["date"])]].reset_index(drop=True)
    feat = [c for c in smp.columns if c not in META]
    pays = S.payments_for(smp)
    copies = []
    for i, keep in enumerate(KEEP_RATES):
        part = smp if keep >= 1 else smp.sample(frac=keep, random_state=SEED + i)
        copies.append(with_groups(part, pays, f"train keep={keep:.0%}"))
    train = pd.concat(copies, ignore_index=True)
    gcols = [c for c in train.columns if c.startswith(("g_", "r_"))]
    y = S.true_class(train, merge).to_numpy()
    model = S.fit(train[feat + gcols].fillna(0.0).to_numpy(np.float32), y)
    print(f"[train] group namer on {len(train)} rows ({len(feat)} account-day + {len(gcols)} group "
          f"features), {time.time() - t0:.0f}s")

    # ---- validation rows: the alerts (to name) + all true laundering (recall denominators)
    full = S.open_dataset(f"{S.CURATED}/{a.features}")
    tables, confusion = [], []
    for d, al in alerts.groupby("dataset"):
        keys = set(zip(al["acct_bank"].astype(int), al["acct"], al["date"]))
        flt = (ds.field("dataset") == d) & (ds.field("date") > pa.scalar(train_end[d], pa.date32()))
        keep = []
        for b in full.scanner(columns=META[2:] + feat + ["dataset"], filter=flt, batch_size=S.BATCH_SIZE,
                              batch_readahead=1, fragment_readahead=1).to_batches():
            p = b.to_pandas()
            p["date"] = pd.to_datetime(p["date"]).dt.date
            p["flagged"] = [(int(x), y_, z) in keys for x, y_, z in zip(p["acct_bank"], p["acct"], p["date"])]
            p = p[p["flagged"] | (p["label"] == 1)]
            if len(p):
                keep.append(p)
        g = pd.concat(keep, ignore_index=True)
        g["true"] = np.where(g["label"] == 1, S.true_class(g, merge).to_numpy(), "not_laundering")
        total_true = g.loc[g["label"] == 1, "true"].value_counts().to_dict()
        flagged = g[g["flagged"]].copy()
        pays_v = S.payments_for(flagged)
        fg = with_groups(flagged, pays_v, "alerts")
        fg["pred_group"] = model.predict(fg[feat + gcols].fillna(0.0).to_numpy(np.float32))
        acc = al.set_index(["acct_bank", "acct", "date"])["predicted_pattern"]
        fg["pred_account"] = acc.reindex(pd.MultiIndex.from_arrays(
            [fg["acct_bank"].astype(al["acct_bank"].dtype), fg["acct"], fg["date"]])).to_numpy()
        print(f"[score] {d}: {len(al)} alerts, {len(fg)} matched, {int((fg['label'] == 1).sum())} truly laundering, "
              f"{len(g[g['label'] == 1])} true laundering account-days in total")
        for method in ("account", "group"):
            pred = fg[f"pred_{method}"].to_numpy()
            caught = fg["label"].to_numpy() == 1
            confusion.append(pd.DataFrame({"true_pattern": fg["true"].to_numpy(), "predicted_pattern": pred})
                             .fillna("none").value_counts().rename("n").reset_index()
                             .assign(dataset=d, method=method))
            for mode, t in [("end_to_end", class_table(fg["true"].to_numpy(), pred, classes, total_true)),
                            ("caught", class_table(fg["true"].to_numpy()[caught], pred[caught], classes))]:
                t["dataset"], t["method"], t["mode"] = d, method, mode
                tables.append(t)
        e_acc = [t for t in tables if (t["dataset"].iloc[0], t["method"].iloc[0], t["mode"].iloc[0]) == (d, "account", "end_to_end")][0]
        e_grp = [t for t in tables if (t["dataset"].iloc[0], t["method"].iloc[0], t["mode"].iloc[0]) == (d, "group", "end_to_end")][0]
        show = e_acc[["pattern", "true_cases"]].copy()
        show["account_P"], show["account_R"], show["account_F1"] = e_acc["precision"], e_acc["recall"], e_acc["f1"]
        show["group_P"], show["group_R"], show["group_F1"] = e_grp["precision"].to_numpy(), e_grp["recall"].to_numpy(), e_grp["f1"].to_numpy()
        show["change_F1"] = show["group_F1"] - show["account_F1"]
        print(f"-- {d} validation · END TO END (account = today's namer, group = + group shape)")
        print(show.round(3).to_string(index=False))

    out = pd.concat(tables, ignore_index=True)
    out["stage1_config"], out["run_at"] = a.stage1, S.RUN_AT
    out["settings_json"] = json.dumps(settings)
    S.write_results(out, "group_stage2", out_cfg)
    S.write_results(pd.concat(confusion, ignore_index=True), "group_stage2_confusion", out_cfg)
    imp = pd.Series(model.feature_importances_, index=feat + gcols).sort_values(ascending=False)
    print("\n[group namer] top features:", ", ".join(imp.head(12).index))
    print(f"Saved analytics/group_stage2 under config={out_cfg} ({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    main()
