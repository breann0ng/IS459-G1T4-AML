"""
group_stage2.py
Round 3, step 3: name the pattern from the GROUP's shape, fixed version of Task C.

Plain Python on the EMR primary node (needs scheme_stage2.py in the same folder):
    pip3 install --user pandas pyarrow scikit-learn lightgbm networkx
    python3 group_stage2.py txn_v3m                 stage 1 + namer = that train_txn.py run
    python3 group_stage2.py txn_v3m --param num_leaves=63 --out-config s2_leaves63
                                                    one namer / grouping setting changed
    python3 group_stage2.py txn_v3m --group-vote --min-confidence 0.5 --out-config s2_vote_c50
                                                    one name per group; unsure -> unclassified
    python3 group_stage2.py txn_v3m_xp --context --group-vote --baselines --out-config s2_xp_ctx_vote_bl
                                                    + logistic regression and shape-rule namers
    python3 group_stage2.py txn_v3m_xp --context --group-vote --timing --out-config s2_xp_ctx_vote_tim
                                                    + order of the payments inside each group
    Every run also saves the top-2 hit rate (mode "caught_top2").

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
      with --baselines, also
        logreg    logistic regression on the same features as "group", named the same way
                  (--class-scale / --group-vote / --min-confidence apply): only the model differs
        rules     no model: shape_rules.classify on the payments inside each group

Reads   analytics/txn_alerts, analytics/txn_results (the run), curated/<features>(_sample),
        curated/transactions (only payments between the accounts involved)
Writes  analytics/group_stage2/            per class P / R / F1 per dataset, method and mode
        analytics/group_stage2_confusion/  alerts per true x predicted pattern (dashboard)
"""

import argparse
import datetime as dt
import json
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import networkx as nx
from collections import defaultdict
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.utils.class_weight import compute_sample_weight

import scheme_stage2 as S
import shape_rules as R

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


# ============================================================== context: one step outside the group
ONWARD_DAYS = 2                  # how long after receiving an outside account may pass the money on


def _scan(dataset, start, end, mask, keep, full=None):
    """Payments of one dataset in [start, end] whose raw rows pass mask(batch) and keep(rows).
    Returns only src / dst / date (enough for the context features: keeps HI-Large in memory),
    plus, with full(rows), those rows with every column."""
    tx = S.open_dataset(f"{S.CURATED}/transactions")
    flt = ((ds.field("dataset") == dataset)
           & (ds.field("txn_date") >= pa.scalar(start, pa.date32()))
           & (ds.field("txn_date") <= pa.scalar(end, pa.date32())))
    cols = ["txn_time", "txn_date", "from_bank", "from_account", "to_bank", "to_account",
            "amount_paid", "payment_currency", "payment_format"]
    slim, wide = [], []
    for b in tx.scanner(columns=cols, filter=flt, batch_size=S.BATCH_SIZE,
                        batch_readahead=1, fragment_readahead=1).to_batches():
        b = b.filter(mask(b)) if b.num_rows else b
        if not b.num_rows:
            continue
        p = b.to_pandas()
        p["src"], p["dst"] = S.key(p["from_bank"], p["from_account"]), S.key(p["to_bank"], p["to_account"])
        p = p[(p["src"] != p["dst"]) & keep(p)].copy()
        p["date"] = pd.to_datetime(p["txn_date"]).dt.date
        p["amount_paid"] = p["amount_paid"].astype(float)
        slim.append(p[["src", "dst", "date"]])
        if full is not None:
            wide.append(p[full(p)])
    cat = lambda parts, c: pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=c)
    return cat(slim, ["src", "dst", "date"]), cat(wide, cols + ["src", "dst", "date"])


def context_for(rows):
    """Per dataset: (inner, hop1, hop2).
    inner = payments with BOTH ends among the rows' accounts (exactly S.payments_for: the groups
            are built from these, so they do not change);
    hop1  = every payment with at least one end among them (money entering / leaving the group);
    hop2  = onward payments of the outside accounts that received money from them
            (hop1 / hop2: src, dst, date only; hop2 without repeats)."""
    out = {}
    for d, r in rows.groupby("dataset"):
        accts = set(S.key(r["acct_bank"], r["acct"]))
        start, end = min(r["date"]), max(r["date"])
        names = pa.array(sorted({a.split("|", 1)[1] for a in accts}))
        hop1, inner = _scan(d, start, end,
                            lambda b: pc.or_(pc.is_in(b["from_account"], names), pc.is_in(b["to_account"], names)),
                            keep=lambda p: p["src"].isin(accts) | p["dst"].isin(accts),
                            full=lambda p: p["src"].isin(accts) & p["dst"].isin(accts))
        outside = set(hop1.loc[hop1["src"].isin(accts) & ~hop1["dst"].isin(accts), "dst"])
        names2 = pa.array(sorted({a.split("|", 1)[1] for a in outside}))
        hop2, _ = _scan(d, start, end + dt.timedelta(days=ONWARD_DAYS), lambda b: pc.is_in(b["from_account"], names2),
                        keep=lambda p: p["src"].isin(outside))
        hop2 = hop2.drop_duplicates()
        print(f"[context] {d}: {len(inner)} inner payments, {len(hop1) - len(inner)} in/out of the accounts, "
              f"{len(hop2)} onward payments of {len(outside)} outside receivers")
        out[d] = (inner, hop1, hop2)
    return out


def context_features(r, gid, hop1, hop2):
    """Group + role features from payments that cross the group's border (no labels used).
    c_bridge          outside accounts that receive from the group AND pay back into it
                      (scatter-gather / cycle with unflagged middle accounts)
    c_reconverge      largest share of the group's outside receivers that pay ONE common account
                      within ONWARD_DAYS (scatter-gather whose gather account was not flagged)
    c_passthrough     share of outside receivers that pass money on within ONWARD_DAYS (mules)
    c_ext_out/in_*    payments and counterparties outside the group; max per account
    rc_ext_out/in_cp  this account-day's outside counterparties"""
    gs = np.array([gid.get((s, d), -1) for s, d in zip(hop1["src"], hop1["date"])], dtype=np.int64)
    gd = np.array([gid.get((t, d), -1) for t, d in zip(hop1["dst"], hop1["date"])], dtype=np.int64)
    out_e = hop1[(gs >= 0) & (gd != gs)].assign(g=gs[(gs >= 0) & (gd != gs)])[["g", "src", "dst", "date"]]
    out_e.columns = ["g", "acct", "x", "date"]
    in_e = hop1[(gd >= 0) & (gs != gd)].assign(g=gd[(gd >= 0) & (gs != gd)])[["g", "dst", "src", "date"]]
    in_e.columns = ["g", "acct", "x", "date"]
    G = pd.DataFrame(index=pd.Index(sorted(set(gid.values())), name="g"))
    G["c_ext_out"] = out_e.groupby("g").size()
    G["c_ext_out_cp"] = out_e.groupby("g")["x"].nunique()
    G["c_ext_in"] = in_e.groupby("g").size()
    G["c_ext_in_cp"] = in_e.groupby("g")["x"].nunique()
    G["c_max_ext_out_cp"] = out_e.groupby(["g", "acct"])["x"].nunique().groupby("g").max()
    G["c_max_ext_in_cp"] = in_e.groupby(["g", "acct"])["x"].nunique().groupby("g").max()
    br = out_e[["g", "x"]].drop_duplicates().merge(in_e[["g", "x"]].drop_duplicates(), on=["g", "x"])
    G["c_bridge"] = br.groupby("g").size()
    # onward: outside receiver x got money from account a on day d1, then paid y on d1..d1+ONWARD_DAYS
    first = out_e.groupby(["g", "x"]).agg(d1=("date", "min"), a=("acct", "first")).reset_index()
    on = first.merge(hop2[["src", "dst", "date"]].rename(columns={"src": "x", "dst": "y"}), on="x")
    lag = (pd.to_datetime(on["date"]) - pd.to_datetime(on["d1"])).dt.days
    on = on[(lag >= 0) & (lag <= ONWARD_DAYS) & (on["y"] != on["a"])]
    G["c_passthrough"] = on.groupby("g")["x"].nunique()
    G["c_reconverge_n"] = on.groupby(["g", "y"])["x"].nunique().groupby("g").max()
    G = G.fillna(0.0)
    G["c_passthrough"] = G["c_passthrough"] / G["c_ext_out_cp"].clip(lower=1)
    G["c_reconverge"] = np.where(G["c_ext_out_cp"] >= 2, G["c_reconverge_n"] / G["c_ext_out_cp"].clip(lower=1), 0.0)
    G["c_bridge_share"] = G["c_bridge"] / G["c_ext_out_cp"].clip(lower=1)
    node_out = out_e.groupby(["acct", "date"])["x"].nunique()
    node_in = in_e.groupby(["acct", "date"])["x"].nunique()
    keys = pd.MultiIndex.from_arrays([r["k"], r["date"]])
    f = G.reindex([gid[(k, dt)] for k, dt in zip(r["k"], r["date"])]).reset_index(drop=True)
    f["rc_ext_out_cp"] = node_out.reindex(keys).fillna(0).to_numpy()
    f["rc_ext_in_cp"] = node_in.reindex(keys).fillna(0).to_numpy()
    f.index = r.index
    return f


# ============================================================== timing: order of payments in the group
TIMING = False                   # --timing


def _in_first(t_in, t_out):
    """Share of the incoming payments made before the first outgoing one (1 if nothing goes out)."""
    if not len(t_in):
        return 0.0
    return float((t_in < t_out.min()).mean()) if len(t_out) else 1.0


def timing_features(r, gid, edges):
    """Order of the payments inside each group: the shape features (S.group_features) ignore
    time, yet the patterns the namer confuses most differ mainly in it.
    h_hub_in_first    share of the hub's incoming payments before its first outgoing one
                      (gather-scatter: collects, then pays out)
    h_hub_out_first   share of the hub's outgoing payments before its first incoming one
    h_hub_gap_h       hours from the hub's last incoming to its first outgoing payment (0 if one is missing)
    h_two_hubs        the top sender and the top receiver are different accounts (scatter-gather)
    h_scatter_first   ... and the top sender pays out before the top receiver collects
    h_origin_returns  the account that made the group's first payment later receives money in it (cycle)
    rh_in_first       this account: share of its incoming payments before its first outgoing one
    (hub = the account with the most payments in and out of the group)"""
    e = edges.assign(g=[gid[(s, d)] for s, d in zip(edges["src"], edges["date"])],
                     t=pd.to_datetime(edges["txn_time"]))
    G, node = {}, {}
    for g, x in e.groupby("g"):
        n_in, n_out = x["dst"].value_counts(), x["src"].value_counts()
        hub = n_in.add(n_out, fill_value=0).idxmax()
        t_in, t_out = x.loc[x["dst"] == hub, "t"], x.loc[x["src"] == hub, "t"]
        top_s, top_r = n_out.idxmax(), n_in.idxmax()
        two = top_s != top_r and n_out.max() > 1 and n_in.max() > 1
        first = x.loc[x["t"].idxmin()]
        G[g] = {"h_hub_in_first": _in_first(t_in, t_out), "h_hub_out_first": _in_first(t_out, t_in),
                "h_hub_gap_h": (t_out.min() - t_in.max()).total_seconds() / 3600 if len(t_in) and len(t_out) else 0.0,
                "h_two_hubs": int(two),
                "h_scatter_first": int(two and x.loc[x["src"] == top_s, "t"].min() < x.loc[x["dst"] == top_r, "t"].min()),
                "h_origin_returns": int(((x["dst"] == first["src"]) & (x["t"] > first["t"])).any())}
        for a in set(x["src"]) | set(x["dst"]):
            node[(g, a)] = _in_first(x.loc[x["dst"] == a, "t"], x.loc[x["src"] == a, "t"])
    gids = [gid[(k, d)] for k, d in zip(r["k"], r["date"])]
    cols = ["h_hub_in_first", "h_hub_out_first", "h_hub_gap_h", "h_two_hubs", "h_scatter_first", "h_origin_returns"]
    f = pd.DataFrame([G.get(g, {}) for g in gids], columns=cols).fillna(0.0)    # no payments: all 0
    f["rh_in_first"] = [node.get((g, k), 0.0) for g, k in zip(gids, r["k"])]
    f.index = r.index
    return f


def with_groups(rows, payments_by_ds, label=""):
    parts = []
    for d, r in rows.groupby("dataset"):
        r = r.copy()
        r["k"] = S.key(r["acct_bank"], r["acct"])
        pay = payments_by_ds[d]
        ctx = isinstance(pay, tuple)                 # (inner, hop1, hop2) from context_for
        gid, edges, n_hub = build_groups(r, pay[0] if ctx else pay)
        gf = S.group_features(r, gid, edges)
        if ctx:
            gf = pd.concat([gf, context_features(r, gid, pay[1], pay[2])], axis=1)
        if TIMING:
            gf = pd.concat([gf, timing_features(r, gid, edges)], axis=1)
        gf["group_id"] = [f"{d}#{gid[(k, dt)]}" for k, dt in zip(r["k"], r["date"])]
        print(f"[groups] {label} {d}: {len(r)} account-days -> {len(set(gid.values()))} groups "
              f"({len(r) / max(len(set(gid.values())), 1):.1f} per group), {len(edges)} linking payments, "
              f"{n_hub} busy accounts kept apart, largest group {int(gf['g_nodes'].max())}")
        parts.append(pd.concat([r.drop(columns="k"), gf], axis=1))
    return pd.concat(parts)


def linear_X(X):
    """Logistic regression cannot take inf; the trees never minded."""
    return np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)


def fit_logreg(X, y):
    """Baseline: same features and class weights as the LightGBM namer, linear model."""
    model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000))
    model.fit(linear_X(X), y, logisticregression__sample_weight=compute_sample_weight("balanced", y))
    return model


def rule_names(rows, payments, merge):
    """Baseline: name each row's group with shape_rules (groups rebuilt as in with_groups)."""
    r = rows.assign(k=S.key(rows["acct_bank"], rows["acct"]))
    gid, edges, _ = build_groups(r, payments)
    e_gid = [gid[(s, d)] for s, d in zip(edges["src"], edges["date"])]
    names = {g: R.name_group(e["src"].to_numpy(), e["dst"].to_numpy(), merge)
             for g, e in edges.assign(_gid=e_gid).groupby("_gid")}
    return np.array([names.get(gid[(k, d)], "unclassified") for k, d in zip(r["k"], r["date"])])


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


def scale_classes(proba, scale):
    """Multiply some classes' probabilities (DataFrame, one column per class), then renormalise."""
    proba = proba.copy()
    for c, f in scale.items():
        if c in proba:
            proba[c] *= f
    return proba.div(proba.sum(axis=1), axis=0)


def value(text):
    for cast in (int, float):
        try:
            return cast(text)
        except ValueError:
            pass
    return text


def main():
    global KEEP_RATES, HUB_DEGREE, MAX_LINKED_DAYS, TIMING
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
    ap.add_argument("--group-vote", action="store_true",
                    help="average the namer's probabilities over each group: one name per group")
    ap.add_argument("--context", action="store_true",
                    help="add features from payments that cross the group border (one step outside)")
    ap.add_argument("--timing", action="store_true",
                    help="add features on the ORDER of the payments inside the group (h_*, rh_*)")
    ap.add_argument("--min-confidence", type=float, default=0.0,
                    help="name 'unclassified' (analyst decides) when the top probability is below this")
    ap.add_argument("--class-scale", action="append", default=[], metavar="CLASS=FACTOR",
                    help="multiply a class's probability before naming, e.g. layering=0.6 so the "
                         "catch-all class has to win more clearly (repeatable)")
    ap.add_argument("--baselines", action="store_true",
                    help="also grade a logistic regression namer and the shape rules (shape_rules.py)")
    a = ap.parse_args()
    methods = ["account", "group"] + (["logreg", "rules"] if a.baselines else [])
    class_scale = {k: float(v) for k, v in (p.split("=", 1) for p in a.class_scale)}
    out_cfg = a.out_config or f"{a.stage1}_grp"
    if not out_cfg.endswith("_grp"):
        out_cfg += "_grp"
    if a.keep_rates:
        KEEP_RATES = [float(k) for k in a.keep_rates.split(",")]
    HUB_DEGREE, MAX_LINKED_DAYS, S.LINK_DAYS, TIMING = a.hub_degree, a.max_linked_days, a.link_days, a.timing
    S.STAGE2_PARAMS.update({k: value(v) for k, v in (p.split("=", 1) for p in a.param)})
    settings = {"namer_params": S.STAGE2_PARAMS, "keep_rates": KEEP_RATES, "hub_degree": HUB_DEGREE,
                "max_linked_days": MAX_LINKED_DAYS, "link_days": S.LINK_DAYS, "features": a.features,
                "group_vote": a.group_vote, "min_confidence": a.min_confidence, "context": a.context,
                "timing": a.timing,
                "class_scale": class_scale, "baselines": a.baselines}
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
    feat = S.drop_features([c for c in smp.columns if c not in META])
    pays = context_for(smp) if a.context else S.payments_for(smp)
    copies = []
    for i, keep in enumerate(KEEP_RATES):
        part = smp if keep >= 1 else smp.sample(frac=keep, random_state=SEED + i)
        copies.append(with_groups(part, pays, f"train keep={keep:.0%}"))
    train = pd.concat(copies, ignore_index=True)
    gcols = [c for c in train.columns if c.startswith(("g_", "r_", "c_", "rc_", "h_", "rh_"))]
    y = S.true_class(train, merge).to_numpy()
    model = S.fit(train[feat + gcols].fillna(0.0).to_numpy(np.float32), y)
    print(f"[train] group namer on {len(train)} rows ({len(feat)} account-day + {len(gcols)} group "
          f"features), {time.time() - t0:.0f}s")
    if a.baselines:
        logreg = fit_logreg(train[feat + gcols].fillna(0.0).to_numpy(np.float32), y)
        print(f"[train] logistic regression baseline, {time.time() - t0:.0f}s")

    def name(proba, groups):
        """Class scaling, group vote and min confidence, as asked: the same for every model."""
        if class_scale:                       # make a catch-all class (layering) win more clearly
            proba = scale_classes(proba, class_scale)
        if a.group_vote:                      # one name per group: average its members' probabilities
            proba = proba.groupby(groups).transform("mean")
        pred = proba.idxmax(axis=1)
        if a.min_confidence:                  # not sure enough: leave it to the analyst
            pred[proba.max(axis=1) < a.min_confidence] = "unclassified"
        return proba, pred

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
        pays_v = context_for(flagged) if a.context else S.payments_for(flagged)
        fg = with_groups(flagged, pays_v, "alerts")
        X = fg[feat + gcols].fillna(0.0).to_numpy(np.float32)
        proba, fg["pred_group"] = name(pd.DataFrame(model.predict_proba(X), columns=model.classes_,
                                                    index=fg.index), fg["group_id"])
        if a.baselines:
            _, fg["pred_logreg"] = name(pd.DataFrame(logreg.predict_proba(linear_X(X)),
                                                     columns=logreg.classes_, index=fg.index), fg["group_id"])
            fg["pred_rules"] = rule_names(fg, pays_v[d][0] if a.context else pays_v[d], merge)
        top2 = np.asarray(model.classes_)[np.argsort(-proba.to_numpy(), axis=1)[:, :2]]
        fg["top2_hit"] = [t in row for t, row in zip(fg["true"], top2)]
        acc = al.set_index(["acct_bank", "acct", "date"])["predicted_pattern"]
        fg["pred_account"] = acc.reindex(pd.MultiIndex.from_arrays(
            [fg["acct_bank"].astype(al["acct_bank"].dtype), fg["acct"], fg["date"]])).to_numpy()
        print(f"[score] {d}: {len(al)} alerts, {len(fg)} matched, {int((fg['label'] == 1).sum())} truly laundering, "
              f"{len(g[g['label'] == 1])} true laundering account-days in total")
        for method in methods:
            pred = fg[f"pred_{method}"].to_numpy()
            caught = fg["label"].to_numpy() == 1
            confusion.append(pd.DataFrame({"true_pattern": fg["true"].to_numpy(), "predicted_pattern": pred})
                             .fillna("none").value_counts().rename("n").reset_index()
                             .assign(dataset=d, method=method))
            for mode, t in [("end_to_end", class_table(fg["true"].to_numpy(), pred, classes, total_true)),
                            ("caught", class_table(fg["true"].to_numpy()[caught], pred[caught], classes))]:
                t["dataset"], t["method"], t["mode"] = d, method, mode
                tables.append(t)
        # top-2: of the truly laundering, patterned alerts, how often the true pattern is in the
        # namer's two best guesses (what an analyst sees with "fan out or scatter gather")
        tp = fg[(fg["label"] == 1) & (fg["true"] != "unclassified")]
        t2 = (tp.groupby("true").agg(true_cases=("top2_hit", "size"), correct=("top2_hit", "sum"))
                .reset_index().rename(columns={"true": "pattern"}))
        t2["recall"] = t2["correct"] / t2["true_cases"]
        t2 = pd.concat([t2, pd.DataFrame([{"pattern": f"MACRO ({len(t2)} patterns)",
                                           "true_cases": int(t2["true_cases"].sum()),
                                           "correct": int(t2["correct"].sum()), "recall": t2["recall"].mean()}])],
                       ignore_index=True)
        t2["dataset"], t2["method"], t2["mode"] = d, "group", "caught_top2"
        tables.append(t2)
        print(f"-- {d} · top-2 hit rate on true patterned alerts: {t2['recall'].iloc[-1]:.3f} (macro)")
        e_acc = [t for t in tables if (t["dataset"].iloc[0], t["method"].iloc[0], t["mode"].iloc[0]) == (d, "account", "end_to_end")][0]
        e_grp = [t for t in tables if (t["dataset"].iloc[0], t["method"].iloc[0], t["mode"].iloc[0]) == (d, "group", "end_to_end")][0]
        show = e_acc[["pattern", "true_cases"]].copy()
        show["account_P"], show["account_R"], show["account_F1"] = e_acc["precision"], e_acc["recall"], e_acc["f1"]
        show["group_P"], show["group_R"], show["group_F1"] = e_grp["precision"].to_numpy(), e_grp["recall"].to_numpy(), e_grp["f1"].to_numpy()
        show["change_F1"] = show["group_F1"] - show["account_F1"]
        for m in methods[2:]:
            e_m = [t for t in tables if (t["dataset"].iloc[0], t["method"].iloc[0], t["mode"].iloc[0]) == (d, m, "end_to_end")][0]
            show[f"{m}_F1"] = e_m["f1"].to_numpy()
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
