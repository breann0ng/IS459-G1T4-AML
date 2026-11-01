"""
bq2_analysis_spark.py
BQ2: "Are criminals changing their tactics over time, and can we tell when our detection is
      falling behind?"

Run (EMR, after bq2_score_spark.py has scored every monitored week):
    spark-submit [S3 confs] [driver confs] bq2_analysis_spark.py HI-Large 2022-08-22 2022-10-30

Two scenarios are analysed side by side
    real        the data as it is: are tactics in HI-Large actually changing, and does the
                frozen BQ1 model keep up?
    new_tactic  controlled test: the HIDDEN patterns (default fan_in + fan_out) are removed from
                every week before INTRO_WEEK, so to the monitor they look like a brand-new tactic
                appearing in INTRO_WEEK. Two models are compared:
                  all    the BQ1 final model (has seen every pattern)
                  nofan  the same model trained without the hidden patterns (never seen them)
                If "nofan" catches the new tactic much less, detection is falling behind; the
                question is whether the monitor notices, and how early.

What is measured, per week (Monday-Sunday weeks, the pipeline's week numbers)
  BQ2a tactics (labels / answer key)
    mix of laundering payments by pattern, payment format, amount band, cross-currency,
    cross-bank; scheme size and duration. Each week vs the BASELINE weeks:
    PSI (0.1 = moderate shift, 0.2 = large shift) and a chi-square test.
  BQ2b detection (labels, which in a real bank arrive LABEL_DELAY weeks late)
    per model: precision, recall, alerts/day, top-1000/day precision, recall per pattern group,
    schemes caught and caught before they finish (end-of-day alerts).
    Alarm: a recall below the baseline's mean - 2 sd for 2 weeks in a row. It is only KNOWN
    LABEL_DELAY weeks later, because labels arrive late.
  Early warning (no labels needed, known the same week)
    alerts per day vs baseline (z-score), score distribution PSI, feature PSI (top features).
    Lead time = week the label-based alarm is known - week of the first early warning.

Writes  analytics/bq2_tactics, bq2_tactics_shift, bq2_scorecard, bq2_scheme_card, bq2_drift,
        bq2_alarms (Parquet, partitioned by dataset) and analytics/bq2/<dataset>.json
        (everything the dashboard needs; bq2_report.py prints it and copies it to app/data/)
Settings (env): BQ2_MODELS, BQ2_HIDE, BQ2_INTRO_WEEK, BQ2_BASELINE_WEEKS, BQ2_LABEL_DELAY
"""

import json
import math
import os
import sys

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "pw-aml-g1t4"
ROOT = os.environ.get("AML_ROOT", f"s3://{BUCKET}")
DATASET = sys.argv[1] if len(sys.argv) > 1 else "HI-Large"
MON_FROM = sys.argv[2] if len(sys.argv) > 2 else "2022-08-22"
MON_TO = sys.argv[3] if len(sys.argv) > 3 else "2022-10-30"
MODELS = dict(m.split("=", 1) for m in
              os.environ.get("BQ2_MODELS", "all=models/final,nofan=models/final_nofan").split(","))
HIDE = [x for x in os.environ.get("BQ2_HIDE", "fan_in,fan_out").split(",") if x]
BASELINE_WEEKS = int(os.environ.get("BQ2_BASELINE_WEEKS", "3"))
INTRO_WEEK = os.environ.get("BQ2_INTRO_WEEK")          # default: the 5th monitored week
LABEL_DELAY = int(os.environ.get("BQ2_LABEL_DELAY", "2"))
DRIFT_FEATURES = os.environ.get("BQ2_DRIFT_FEATURES",
    "t_fmt_ach,t_pair_age_d,r_age_days,s_n_cross_ccy,t_pair_n_7d,t_amt_rel,r_x_trend,s_age_days,"
    "r_x_new_cp_in,t_fmt_credit_card,s_n_out,r_cycles_today,s_cycles_today,s_cycles_7d,"
    "s_out_cp_max_senders").split(",")
DRIFT_SAMPLE = float(os.environ.get("BQ2_DRIFT_SAMPLE", "0.1"))   # share of payments used for feature drift
PATTERNS = ["fan_in", "fan_out", "gather_scatter", "scatter_gather", "cycle", "bipartite", "stack", "random"]
TOP_K = 1000
PSI_MODERATE, PSI_LARGE = 0.1, 0.2
CURATED, ANALYTICS = f"{ROOT}/curated", f"{ROOT}/analytics"

spark = SparkSession.builder.appName(f"BQ2Analysis-{DATASET}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", os.environ.get("AML_SHUFFLE", "200"))


def log(*a):
    print("[bq2]", *a, flush=True)


def text(path):
    return spark.sparkContext.wholeTextFiles(path).collect()[0][1]


def read(name, base=ANALYTICS):
    return spark.read.parquet(f"{base}/{name}").filter(F.col("dataset") == DATASET)


def put_text(uri, body):
    import pyarrow.fs as pafs
    if uri.startswith("s3://"):
        fs, path = pafs.S3FileSystem(region=os.environ.get("AWS_REGION", "us-east-1")), uri[5:]
    else:
        fs, path = pafs.LocalFileSystem(), os.path.abspath(uri)
    fs.create_dir(path.rsplit("/", 1)[0], recursive=True)
    with fs.open_output_stream(path) as f:
        f.write(body.encode())


def save(df, name):
    if df.empty:
        return
    out = df.copy()
    for c in out.columns:
        if out[c].dtype == object:
            out[c] = out[c].astype(str)
    spark.createDataFrame(out.assign(dataset=DATASET).replace({np.nan: None})) \
         .write.mode("overwrite").partitionBy("dataset").parquet(f"{ANALYTICS}/{name}")


def psi(cur, base, eps=1e-4):
    """Population stability index between two count vectors on the same bins."""
    c = np.asarray(cur, float); b = np.asarray(base, float)
    if c.sum() == 0 or b.sum() == 0:
        return None
    c = np.clip(c / c.sum(), eps, None); b = np.clip(b / b.sum(), eps, None)
    return float(((c - b) * np.log(c / b)).sum())


def chi2_p(cur, base):
    try:
        from scipy.stats import chi2_contingency
    except ImportError:
        return None
    t = np.array([cur, base], float)
    t = t[:, t.sum(axis=0) > 0]
    if t.shape[1] < 2 or (t.sum(axis=1) == 0).any():
        return None
    return float(chi2_contingency(t)[1])


def first_run(weeks, flags, n=2):
    """First week ending a run of n consecutive True flags (weeks in order)."""
    run = 0
    for w, f in zip(weeks, flags):
        run = run + 1 if f else 0
        if run >= n:
            return int(w)
    return None


def main():
    metas = {m: json.loads(text(f"{ROOT}/{d.strip('/')}/meta.json")) for m, d in MODELS.items()}
    thr = {m: float(metas[m]["threshold"]) for m in MODELS}
    names = list(MODELS)

    # ------------------------------------------------------------------ the monitored weeks
    totals = read("bq2_day_totals").filter(
        (F.col("date") >= F.lit(MON_FROM).cast("date")) & (F.col("date") <= F.lit(MON_TO).cast("date"))).toPandas()
    if totals.empty:
        raise SystemExit("no bq2_day_totals in the window: run bq2_score_spark.py first")
    totals["date"] = pd.to_datetime(totals["date"])
    wk = (totals.groupby("week").agg(start=("date", "min"), end=("date", "max"), days=("date", "nunique"))
                .reset_index().sort_values("week"))
    weeks = [int(w) for w in wk["week"]]
    base_weeks = weeks[:BASELINE_WEEKS]
    intro = int(INTRO_WEEK) if INTRO_WEEK else weeks[min(BASELINE_WEEKS + 1, len(weeks) - 1)]
    days_in = dict(zip(wk["week"].astype(int), wk["days"]))
    log(f"{DATASET}: weeks {weeks[0]}-{weeks[-1]} ({MON_FROM} to {MON_TO}), baseline {base_weeks}, "
        f"hidden {HIDE} introduced in week {intro}, label delay {LABEL_DELAY} weeks, models "
        + ", ".join(f"{m} thr {thr[m]:.4f}" for m in names))

    def hidden_pre_intro(df, wcol="week", pcol="pattern_label"):
        """new_tactic scenario: True for rows of the hidden patterns before the intro week."""
        return (df[wcol] < intro) & df[pcol].isin(HIDE)

    # ------------------------------------------------------------------ data check: daily profile
    prof = read("daily_profile", CURATED).select("week", "txn_date", "txns", "laundering_txns").toPandas()
    prof = prof.sort_values("txn_date")
    prof["laundering_rate"] = prof["laundering_txns"] / prof["txns"]

    # ================================================================== BQ2a: tactics
    tx = read("transactions", CURATED).select("txn_id", "week", "txn_date", "payment_format", "amount_paid",
                                              "is_cross_currency", "from_bank", "to_bank")
    lab = read("labels", CURATED).filter("is_laundering = 1").select("txn_id")
    st = read("scheme_txns", CURATED).select("txn_id", "pattern_type")
    pat = st.groupBy("txn_id").agg(F.min("pattern_type").alias("pattern"))
    amt_band = F.when(F.col("amount_paid") <= 0, F.lit("0")).otherwise(
        F.concat(F.lit("1e"), F.floor(F.log10(F.col("amount_paid").cast("double"))).cast("int").cast("string")))
    laund = (tx.join(lab, "txn_id").join(pat, "txn_id", "left")
               .withColumn("pattern", F.coalesce("pattern", F.lit("unclassified")))
               .groupBy("week", "pattern", "payment_format", amt_band.alias("amount_band"),
                        F.col("is_cross_currency").cast("string").alias("cross_currency"),
                        (F.col("from_bank") != F.col("to_bank")).cast("string").alias("cross_bank"))
               .agg(F.count(F.lit(1)).alias("n")).toPandas())
    laund["week"] = laund["week"].astype(int)
    dims = ["pattern", "payment_format", "amount_band", "cross_currency", "cross_bank"]

    tactics_rows, shift_rows = [], []
    for scen in ("real", "new_tactic"):
        L = laund if scen == "real" else laund[~hidden_pre_intro(laund, pcol="pattern")]
        for dim in dims:
            t = L.groupby(["week", dim])["n"].sum().unstack(fill_value=0).sort_index()
            share = t.div(t.sum(axis=1), axis=0)
            for w in t.index:
                for cat in t.columns:
                    tactics_rows.append({"scenario": scen, "dimension": dim, "week": int(w), "category": str(cat),
                                         "n": int(t.loc[w, cat]), "share": float(share.loc[w, cat])})
            base = t.loc[[w for w in base_weeks if w in t.index]].sum()
            for w in t.index:
                shift_rows.append({"scenario": scen, "dimension": dim, "week": int(w),
                                   "laundering_payments": int(t.loc[w].sum()),
                                   "psi": psi(t.loc[w].to_numpy(), base.to_numpy()),
                                   "chi2_p": chi2_p(t.loc[w].to_numpy(), base.to_numpy())})
    tactics, shift = pd.DataFrame(tactics_rows), pd.DataFrame(shift_rows)

    # scheme shape by start week (answer key)
    sch = read("schemes", CURATED).select("scheme_id", "pattern_type", "start_time", "end_time", "n_txns",
                                          "n_accounts", "duration_h", "first_week").toPandas()
    sch = sch.dropna(subset=["first_week", "start_time"]).copy()
    sch["first_week"] = sch["first_week"].astype(int)
    shape_rows = []
    for scen in ("real", "new_tactic"):
        S = sch if scen == "real" else sch[~hidden_pre_intro(sch, "first_week", "pattern_type")]
        for w, g in S.groupby("first_week"):
            shape_rows.append({"scenario": scen, "week": int(w), "schemes": len(g),
                               "median_accounts": float(g["n_accounts"].median()),
                               "median_payments": float(g["n_txns"].median()),
                               "median_duration_h": float(g["duration_h"].median())})
    shape = pd.DataFrame(shape_rows)

    # ================================================================== BQ2b: weekly scorecard
    ad = read("bq2_acct_days").filter(F.col("week").isin(weeks)).drop("dataset").toPandas()
    ad["week"] = ad["week"].astype(int)
    ad["date"] = pd.to_datetime(ad["date"])
    groups = {"all laundering": lambda d: d["label"] == 1,
              "hidden (" + "+".join(HIDE) + ")": lambda d: (d["label"] == 1) & d["pattern_label"].isin(HIDE),
              "other known patterns": lambda d: (d["label"] == 1) & d["pattern_label"].isin([p for p in PATTERNS if p not in HIDE]),
              "unclassified": lambda d: (d["label"] == 1) & ~d["pattern_label"].isin(PATTERNS)}
    for p in PATTERNS:
        groups[p] = (lambda p_: (lambda d: (d["label"] == 1) & (d["pattern_label"] == p_)))(p)
    HID = "hidden (" + "+".join(HIDE) + ")"

    card_rows = []
    for scen in ("real", "new_tactic"):
        A = ad if scen == "real" else ad[~hidden_pre_intro(ad)]
        for m in names:
            flag = A[f"score_{m}"] >= thr[m]
            top = A[f"rank_{m}"] <= TOP_K
            for w in weeks:
                inw = (A["week"] == w).to_numpy()
                aw, fw, tw = A[inw], flag[inw], top[inw]
                y = aw["label"] == 1
                n_alert, n_true = int(fw.sum()), int((fw & y).sum())
                row = {"scenario": scen, "model": m, "week": w, "group": "overall",
                       "alerts": n_alert, "true_alerts": n_true, "positives": int(y.sum()),
                       "precision": n_true / n_alert if n_alert else None,
                       "recall": n_true / int(y.sum()) if y.any() else None,
                       "alerts_per_day": n_alert / days_in[w],
                       "precision_top1000": float((tw & y).sum() / tw.sum()) if tw.any() else None,
                       "recall_top1000": float((tw & y).sum() / y.sum()) if y.any() else None}
                card_rows.append(row)
                for g, fn in groups.items():
                    pos = fn(aw)
                    n = int(pos.sum())
                    card_rows.append({"scenario": scen, "model": m, "week": w, "group": g, "positives": n,
                                      "true_alerts": int((pos & fw).sum()),
                                      "recall": float((pos & fw).sum() / n) if n else None})
    card = pd.DataFrame(card_rows)

    # ---- schemes: caught, and alerted (end of day) before the scheme's last payment
    stx = (read("scheme_txns", CURATED).filter("found_in_transactions")
           .select("scheme_id", "pattern_type", "txn_id", "txn_time",
                   F.xxhash64(F.col("from_bank").cast("string"), "from_account").alias("s_id"),
                   F.xxhash64(F.col("to_bank").cast("string"), "to_account").alias("r_id")).toPandas())
    fp = read("bq2_flagged_payments").select("txn_id", *[f"score_{m}" for m in names]).toPandas()
    sw = sch[(sch["start_time"] >= pd.Timestamp(MON_FROM)) &
             (sch["end_time"] < pd.Timestamp(MON_TO) + pd.Timedelta(days=1))].copy()
    date_week = {d.normalize(): int(w) for d, w in zip(totals["date"], totals["week"])}
    sw["week"] = sw["start_time"].dt.normalize().map(date_week)
    sw = sw.dropna(subset=["week"])
    sw["week"] = sw["week"].astype(int)
    stx = stx[stx["scheme_id"].isin(sw["scheme_id"])]
    stx["date"] = pd.to_datetime(stx["txn_time"]).dt.normalize()
    sacct = pd.concat([stx[["scheme_id", "s_id", "date"]].rename(columns={"s_id": "id"}),
                       stx[["scheme_id", "r_id", "date"]].rename(columns={"r_id": "id"})]).drop_duplicates()
    scheme_rows = []
    for m in names:
        flagged_ids = set(fp.loc[fp[f"score_{m}"] >= thr[m], "txn_id"])
        caught = stx[stx["txn_id"].isin(flagged_ids)].groupby("scheme_id").size()
        alerts = ad.loc[ad[f"score_{m}"] >= thr[m], ["id", "date"]]
        first_alert = sacct.merge(alerts, on=["id", "date"]).groupby("scheme_id")["date"].min()
        s = sw.assign(caught=sw["scheme_id"].map(caught).notna(),
                      first_alert_day=sw["scheme_id"].map(first_alert))
        s["alert_ready"] = s["first_alert_day"] + pd.Timedelta(days=1)       # list published after the day
        s["before_end"] = s["alert_ready"] <= s["end_time"]
        s["hours_to_alert"] = (s["alert_ready"] - s["start_time"]).dt.total_seconds() / 3600
        for scen in ("real", "new_tactic"):
            S = s if scen == "real" else s[~hidden_pre_intro(s, "week", "pattern_type")]
            for w, g in S.groupby("week"):
                for grp, gg in [("all schemes", g), (HID, g[g["pattern_type"].isin(HIDE)]),
                                ("other schemes", g[~g["pattern_type"].isin(HIDE)])]:
                    if len(gg):
                        scheme_rows.append({"scenario": scen, "model": m, "week": int(w), "group": grp,
                                            "schemes": len(gg), "caught": float(gg["caught"].mean()),
                                            "before_end": float(gg["before_end"].mean()),
                                            "median_hours_to_alert": float(gg["hours_to_alert"].median())
                                            if gg["hours_to_alert"].notna().any() else None})
    scheme_card = pd.DataFrame(scheme_rows)

    # ================================================================== early warning (no labels)
    hist = read("bq2_score_hist").filter(F.col("week").isin(weeks)).drop("dataset").toPandas()
    hist["week"] = hist["week"].astype(int)
    feats = [f for f in DRIFT_FEATURES if f in
             spark.read.parquet(f"{CURATED}/{metas[names[0]]['txn_table']}").columns]
    txf = (read(metas[names[0]]["txn_table"], CURATED).filter(F.col("week").isin(weeks))
           .select("week", "pattern_label", *feats))
    if DRIFT_SAMPLE < 1:
        txf = txf.sample(fraction=DRIFT_SAMPLE, seed=7)
    txf = txf.cache()
    basef = txf.filter(F.col("week").isin(base_weeks))
    edges = {}
    for f in feats:
        q = basef.approxQuantile(f, [i / 10 for i in range(1, 10)], 0.01)
        edges[f] = sorted(set(x for x in q if x is not None and not math.isnan(x)))
    def bin_expr(f):
        e = edges[f]
        b = sum((F.col(f) > F.lit(x)).cast("int") for x in e) if e else F.lit(0)
        return F.when(F.col(f).isNull(), F.lit(-1)).otherwise(b)
    fh = (txf.select("week", F.col("pattern_label").isin(HIDE).alias("hidden"),
                     F.explode(F.array(*[F.struct(F.lit(f).alias("feature"), bin_expr(f).alias("bin"))
                                         for f in feats])).alias("fb"))
             .groupBy("week", "hidden", "fb.feature", "fb.bin").agg(F.count(F.lit(1)).alias("n")).toPandas())
    txf.unpersist()
    fh["week"] = fh["week"].astype(int)

    overall = card[card["group"] == "overall"]
    drift_rows = []
    for scen in ("real", "new_tactic"):
        H = hist if scen == "real" else hist[~hidden_pre_intro(hist)]
        FH = fh if scen == "real" else fh[~((fh["week"] < intro) & fh["hidden"])]
        fpsi = {}
        for f, g in FH.groupby("feature"):
            t = g.groupby(["week", "bin"])["n"].sum().unstack(fill_value=0)
            b = t.loc[[w for w in base_weeks if w in t.index]].sum()
            for w in t.index:
                fpsi.setdefault(int(w), {})[f] = psi(t.loc[w].to_numpy(), b.to_numpy())
        for m in names:
            t = H[H["model"] == m].groupby(["week", "bin"])["n"].sum().unstack(fill_value=0)
            b = t.loc[[w for w in base_weeks if w in t.index]].sum()
            o = overall[(overall["scenario"] == scen) & (overall["model"] == m)].set_index("week")
            apd = o["alerts_per_day"]
            mu, sd = apd.loc[base_weeks].mean(), apd.loc[base_weeks].std(ddof=1)
            sd = max(sd if sd == sd else 0.0, 0.05 * mu, 1.0)
            for w in weeks:
                fp_w = fpsi.get(w, {})
                top_f = max(fp_w, key=lambda k: fp_w[k] or 0) if fp_w else None
                drift_rows.append({"scenario": scen, "model": m, "week": w,
                                   "score_psi": psi(t.loc[w].to_numpy(), b.to_numpy()) if w in t.index else None,
                                   "alerts_per_day": float(apd.get(w, np.nan)),
                                   "alerts_z": float((apd.get(w, np.nan) - mu) / sd),
                                   "feature_psi_max": fp_w.get(top_f) if top_f else None,
                                   "feature_psi_top": top_f,
                                   "feature_psi": json.dumps({k: (round(v, 4) if v is not None else None)
                                                              for k, v in fp_w.items()})})
    drift = pd.DataFrame(drift_rows)

    # ================================================================== alarms and lead time
    post = [w for w in weeks if w not in base_weeks]
    alarm_rows = []
    for scen in ("real", "new_tactic"):
        # BQ2a: did the tactics shift? (labels / answer key, so also known LABEL_DELAY weeks late)
        for dim in dims:
            s = shift[(shift["scenario"] == scen) & (shift["dimension"] == dim)].set_index("week")
            flags = [(s["psi"].get(w) or 0) >= PSI_LARGE for w in post]
            w_al = first_run(post, flags)
            alarm_rows.append({"scenario": scen, "model": None, "kind": "tactic shift", "signal": f"mix by {dim}",
                               "alarm_week": w_al, "known_week": w_al + LABEL_DELAY if w_al else None,
                               "detail": f"PSI >= {PSI_LARGE} two weeks in a row; max PSI "
                                         f"{s['psi'].reindex(post).max():.3f}" if len(s) else ""})
        for m in names:
            c = card[(card["scenario"] == scen) & (card["model"] == m)]
            ref = c[(c["group"] == "all laundering") & c["week"].isin(base_weeks)]["recall"].dropna()
            ref_lcl = ref.mean() - 2 * max(ref.std(ddof=1) if len(ref) > 1 else 0.0, 0.02) if len(ref) else None
            for g in ["all laundering", HID, "other known patterns", "unclassified"]:
                cg = c[c["group"] == g].set_index("week")
                base = cg["recall"].reindex(base_weeks).dropna()
                base = base[cg["positives"].reindex(base.index) > 0]
                if len(base):
                    lcl = base.mean() - 2 * max(base.std(ddof=1) if len(base) > 1 else 0.0, 0.02)
                    how = f"recall below baseline {base.mean():.1%} - 2 sd = {lcl:.1%}"
                else:                     # new group (no baseline): compare with overall baseline
                    lcl, how = ref_lcl, (f"new pattern group; recall below overall baseline limit {ref_lcl:.1%}"
                                         if ref_lcl is not None else "")
                if lcl is None:
                    continue
                flags = [(cg["positives"].get(w, 0) or 0) > 0 and (cg["recall"].get(w) is not None)
                         and cg["recall"].get(w) < lcl for w in post]
                w_al = first_run(post, flags)
                alarm_rows.append({"scenario": scen, "model": m, "kind": "falling behind",
                                   "signal": f"recall: {g}", "alarm_week": w_al,
                                   "known_week": w_al + LABEL_DELAY if w_al else None, "detail": how})
            d = drift[(drift["scenario"] == scen) & (drift["model"] == m)].set_index("week")
            for sig, flags, how in [
                ("alerts per day", [abs(d["alerts_z"].get(w, 0) or 0) >= 3 for w in post], "|z| >= 3 vs baseline"),
                ("score distribution", [(d["score_psi"].get(w) or 0) >= PSI_MODERATE for w in post],
                 f"PSI >= {PSI_MODERATE}"),
                ("feature distribution", [(d["feature_psi_max"].get(w) or 0) >= PSI_LARGE for w in post],
                 f"any top feature PSI >= {PSI_LARGE}")]:
                w_al = next((w for w, f in zip(post, flags) if f), None)
                alarm_rows.append({"scenario": scen, "model": m, "kind": "early warning", "signal": sig,
                                   "alarm_week": w_al, "known_week": w_al, "detail": how})
    alarms = pd.DataFrame(alarm_rows)

    # ---- headline: lead time of the early warning over the label-based alarm
    head = []
    for scen in ("real", "new_tactic"):
        for m in names:
            a = alarms[(alarms["scenario"] == scen) & (alarms["model"] == m)]
            fb = a[(a["kind"] == "falling behind") & a["known_week"].notna()].sort_values("known_week")
            ew = a[(a["kind"] == "early warning") & a["known_week"].notna()].sort_values("known_week")
            head.append({"scenario": scen, "model": m,
                         "falling_behind_week": int(fb["alarm_week"].iloc[0]) if len(fb) else None,
                         "falling_behind_known_week": int(fb["known_week"].iloc[0]) if len(fb) else None,
                         "falling_behind_signal": fb["signal"].iloc[0] if len(fb) else None,
                         "early_warning_week": int(ew["known_week"].iloc[0]) if len(ew) else None,
                         "early_warning_signal": ew["signal"].iloc[0] if len(ew) else None,
                         "lead_time_weeks": int(fb["known_week"].iloc[0] - ew["known_week"].iloc[0])
                         if len(fb) and len(ew) else None})
    headline = pd.DataFrame(head)
    for r in head:
        log(f"{r['scenario']:>10} / {r['model']:>5}: falling behind week {r['falling_behind_week']} "
            f"(known {r['falling_behind_known_week']}, {r['falling_behind_signal']}); first early warning "
            f"week {r['early_warning_week']} ({r['early_warning_signal']}); lead time {r['lead_time_weeks']}")

    # ================================================================== save
    for df, name in [(tactics, "bq2_tactics"), (shift, "bq2_tactics_shift"), (card, "bq2_scorecard"),
                     (scheme_card, "bq2_scheme_card"), (drift, "bq2_drift"), (alarms, "bq2_alarms")]:
        save(df, name)

    def rec(df):
        return json.loads(df.to_json(orient="records", date_format="iso"))
    out = {"dataset": DATASET, "window": [MON_FROM, MON_TO], "weeks": rec(wk.assign(
               start=wk["start"].dt.strftime("%Y-%m-%d"), end=wk["end"].dt.strftime("%Y-%m-%d"))),
           "baseline_weeks": base_weeks, "intro_week": intro, "hidden": HIDE, "label_delay_weeks": LABEL_DELAY,
           "models": {m: {"folder": MODELS[m], "threshold": thr[m],
                          "excluded_patterns": metas[m].get("excluded_patterns", [])} for m in names},
           "psi_levels": [PSI_MODERATE, PSI_LARGE], "top_k": TOP_K, "drift_features": feats,
           "daily_profile": rec(prof.assign(txn_date=prof["txn_date"].astype(str))),
           "tactics": rec(tactics), "tactics_shift": rec(shift), "scheme_shape": rec(shape),
           "scorecard": rec(card), "scheme_card": rec(scheme_card), "drift": rec(drift),
           "alarms": rec(alarms), "headline": rec(headline)}
    put_text(f"{ANALYTICS}/bq2/{DATASET}.json", json.dumps(out, default=str))
    log(f"saved analytics/bq2_* tables and analytics/bq2/{DATASET}.json")
    with pd.option_context("display.width", 220, "display.max_columns", 20):
        print(card[card["group"] == "overall"].drop(columns=["group"]).round(3).to_string(index=False))
        print(alarms.to_string(index=False))


if __name__ == "__main__":
    main()
    spark.stop()
