"""
bq2_part1_tactics_spark.py                                               (BQ2, part 1)
"Are criminals changing their tactics over time?" Uses ONLY the confirmed-case history (the
labels and pattern names), the way a bank's analytics team would; no model is involved.

Run on EMR, after clean_transactions_spark.py and parse_patterns_spark.py for the dataset:
    spark-submit [S3 confs] bq2_part1_tactics_spark.py <dataset> <baseline from> <baseline to> [tag|-] [as-of date]
    e.g. SAML-R 2022-10-03 2023-01-29     ramp: the training period is the baseline
         SAML-C 2022-10-03 2023-01-29     control: nothing changes, so any alarm is false
         HI-Large 2022-08-29 2022-09-11   IBM: weeks 5-6 (weeks 1-4 are simulator warm-up)
         SAML-R 2023-05-22 2023-07-02 _nb   round 2: NEW baseline = weeks 34-39, once the new tactics are
                                            fully in (tables bq2r_part1_*_nb; round 1 is kept)

Every week is profiled on six dimensions, for laundering and (as a control) honest payments:
    pattern     laundering typology (laundering only; "unclassified" = in no scheme)
    format      payment format
    amount      amount relative to the currency's median payment: <0.1, 0.1-0.5, 0.5-2, 2-10, >10
    cross_ccy   paid and received currency differ
    cross_bank  sender and receiver at different banks (SAML-D: different countries)
    hour        0-5, 6-11, 12-17, 18-23
and compared with the baseline weeks (pooled) by
    js         Jensen-Shannon distance, 0 = identical, 1 = no overlap (handles zero shares)
    psi        PSI with every share smoothed by EPS (plain PSI explodes when a share is ~0)
    p_value    chi-square test: is the difference more than chance at this sample size?
A week CHANGED when js > its threshold AND p_value < P_MAX (big AND real). The js threshold is
max(JS_MIN, the largest leave-one-out js among the baseline weeks), i.e. above normal noise.
LASTING = changed this week and the week before. TACTIC CHANGE = a lasting laundering change
while honest payments did NOT change on that dimension that week (else: calendar / business).
CUSUM per category share (after the baseline) dates WHEN a confirmed change started (CUSUM
alone is not proof: with many shares watched for many weeks some cross by chance, so alarms on a
dimension with no confirmed change are reported apart, as "unconfirmed"):
    z = (share - baseline mean) / sd;  S+ = max(0, S+ + z - K), S- = max(0, S- - z - K);
    alarm when S+ or S- > H. A category new since the baseline (mean 0) alarms within weeks.

Reads   curated/transactions, curated/labels, curated/scheme_txns    (dataset = argv[1])
Writes  analytics/bq2r_part1_weekly/   week x population x dimension: n, js, psi, p, flags
        analytics/bq2r_part1_shares/   week x population x dimension x category: share, CUSUM
        analytics/bq2r_part1_summary/  per dimension: threshold, first change, first CUSUM alarm
"""

import os
import sys

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
import math

# =============================================================================== SETTINGS
ROOT = os.environ["AML_ROOT"]      # s3://<bucket>; no default, so a job never writes to the wrong bucket
DATASET = sys.argv[1] if len(sys.argv) > 1 else "SAML-R"
BASE_FROM = sys.argv[2] if len(sys.argv) > 2 else "2022-10-03"
BASE_TO = sys.argv[3] if len(sys.argv) > 3 else "2023-01-29"
TAG = sys.argv[4] if len(sys.argv) > 4 else ""      # e.g. _nb: new-baseline run, written to separate tables
TAG = "" if TAG == "-" else TAG                     # "-" = no tag
AS_OF = sys.argv[5] if len(sys.argv) > 5 else ""    # weekly replay: only payments up to this date (YYYY-MM-DD)
# Optional new-tactic test on a dataset whose tactics do not change (HI-Large): the laundering of
# these patterns is left out of every week before HIDE_BEFORE, so to Part 1 they look brand new
# from that day (e.g. BQ2_HIDE=fan_in,fan_out BQ2_HIDE_BEFORE=2022-10-03, tag _nt). Unset = off.
HIDE = [x for x in os.environ.get("BQ2_HIDE", "").split(",") if x]
HIDE_BEFORE = os.environ.get("BQ2_HIDE_BEFORE", "")
JS_MIN = 0.10          # js below this is never a change, whatever the noise
P_MAX = 0.01           # chi-square p-value must be below this
EPS = 0.001            # PSI smoothing
MIN_N = 30             # weeks with fewer payments in a population are not judged
K, H = 0.5, 5.0        # CUSUM slack and alarm level (standard choices)
SD_FLOOR = 0.005       # CUSUM: smallest sd used for a share
DIMS = ["pattern", "format", "amount", "cross_ccy", "cross_bank", "hour"]
# =========================================================================================

CURATED, ANALYTICS = f"{ROOT}/curated", f"{ROOT}/analytics"
spark = SparkSession.builder.appName(f"BQ2Part1-{DATASET}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.shuffle.partitions", os.environ.get("AML_SHUFFLE", "200"))


def weekly_counts():
    rd = lambda name: spark.read.parquet(f"{CURATED}/{name}").filter(F.col("dataset") == DATASET)
    tx = rd("transactions").select("txn_id", "txn_time", "txn_date", "from_bank", "to_bank", "amount_paid",
                                   "payment_currency", "payment_format", "is_cross_currency")
    if AS_OF:
        tx = tx.filter(F.col("txn_date") <= F.lit(AS_OF).cast("date"))
    lab = rd("labels").select("txn_id", F.col("is_laundering").cast("int").alias("label"))
    first = Window.partitionBy("txn_id").orderBy("txn_time", "scheme_id")
    sch = (rd("scheme_txns").filter("found_in_transactions")
           .withColumn("_n", F.row_number().over(first)).filter("_n = 1")
           .select("txn_id", F.col("pattern_type").alias("scheme_pattern")))
    med = tx.groupBy("payment_currency").agg(
        F.expr("percentile_approx(cast(amount_paid as double), 0.5)").alias("med"))
    rel = F.col("amount_paid").cast("double") / F.col("med")
    hour = F.hour("txn_time")
    t = (tx.join(lab, "txn_id", "left").join(sch, "txn_id", "left").join(F.broadcast(med), "payment_currency")
           .withColumn("label", F.coalesce("label", F.lit(0)))
           .select(F.date_format(F.date_trunc("week", "txn_date"), "yyyy-MM-dd").alias("week_start"),
                   F.when(F.col("label") == 1, "laundering").otherwise("honest").alias("population"),
                   F.when(F.col("label") == 0, "none")
                    .otherwise(F.coalesce("scheme_pattern", F.lit("unclassified"))).alias("pattern"),
                   F.col("payment_format").alias("format"),
                   F.when(rel < 0.1, "a <0.1").when(rel < 0.5, "b 0.1-0.5").when(rel < 2, "c 0.5-2")
                    .when(rel < 10, "d 2-10").otherwise("e >10").alias("amount"),
                   F.col("is_cross_currency").cast("string").alias("cross_ccy"),
                   (F.col("from_bank") != F.col("to_bank")).cast("string").alias("cross_bank"),
                   F.when(hour < 6, "00-05").when(hour < 12, "06-11").when(hour < 18, "12-17")
                    .otherwise("18-23").alias("hour")))
    if HIDE and HIDE_BEFORE:
        t = t.filter(~((F.col("population") == "laundering") & F.col("pattern").isin(HIDE)
                       & (F.col("week_start") < F.lit(HIDE_BEFORE))))
        print(f"[part1] new-tactic test: laundering of {HIDE} left out before {HIDE_BEFORE}")
    c = t.groupBy("week_start", "population", *DIMS).count().toPandas()
    long = []
    for d in DIMS:
        if d == "pattern":
            sub = c[c["population"] == "laundering"]
        else:
            sub = c
        g = sub.groupby(["week_start", "population", d], as_index=False)["count"].sum()
        long.append(g.rename(columns={d: "category", "count": "n"}).assign(dimension=d))
    return pd.concat(long, ignore_index=True)


# ---- the two statistics, written out so the script needs only numpy (EMR's Spark Python has no
#      scipy). They give the same numbers as scipy's jensenshannon(base=2) and chi2_contingency.
def jensenshannon(p, q):
    m = (p + q) / 2
    kl = lambda a: float(np.sum(a[a > 0] * np.log2(a[a > 0] / m[a > 0])))
    return math.sqrt(max((kl(p) + kl(q)) / 2, 0.0))


def chi2_sf(x, k):
    """P(chi-square with k degrees of freedom > x) = regularised upper incomplete gamma Q(k/2, x/2)."""
    a, x = k / 2.0, x / 2.0
    if x <= 0:
        return 1.0
    lg = a * math.log(x) - x - math.lgamma(a)
    if x < a + 1:                          # series for P, then Q = 1 - P
        term = tot = 1.0 / a
        n = a
        for _ in range(1000):
            n += 1
            term *= x / n
            tot += term
            if abs(term) < abs(tot) * 1e-15:
                break
        return max(0.0, 1.0 - tot * math.exp(lg))
    b, c, d = x + 1 - a, 1e300, 1 / (x + 1 - a)   # continued fraction for Q (Lentz)
    h = d
    for i in range(1, 1000):
        an = -i * (i - a)
        b += 2
        d = an * d + b
        d = 1e-300 if abs(d) < 1e-300 else d
        c = b + an / c
        c = 1e-300 if abs(c) < 1e-300 else c
        d = 1 / d
        h *= d * c
        if abs(d * c - 1) < 1e-15:
            break
    return min(1.0, h * math.exp(lg))


def chi2_contingency(table):
    """Chi-square test of independence (Yates' correction when dof = 1, as scipy does)."""
    t = np.asarray(table, float)
    exp = np.outer(t.sum(axis=1), t.sum(axis=0)) / t.sum()
    dof = (t.shape[0] - 1) * (t.shape[1] - 1)
    diff = np.abs(t - exp)
    if dof == 1:
        diff = np.maximum(diff - 0.5, 0.0)
    stat = float(np.sum(diff ** 2 / exp))
    return stat, chi2_sf(stat, dof), dof


def compare(cw, cb):
    """cw, cb: counts over the same categories (this week, baseline). Returns js, psi, p."""
    keep = (cw + cb) > 0
    cw, cb = cw[keep], cb[keep]
    if cw.sum() == 0 or cb.sum() == 0:
        return np.nan, np.nan, np.nan
    pw, pb = cw / cw.sum(), cb / cb.sum()
    js = float(jensenshannon(pw, pb))
    sw, sb = (pw + EPS) / (1 + EPS * len(pw)), (pb + EPS) / (1 + EPS * len(pb))
    psi = float(np.sum((sw - sb) * np.log(sw / sb)))
    p = float(chi2_contingency(np.vstack([cw, cb]))[1]) if len(cw) > 1 else 1.0
    return js, psi, p


def analyse(long):
    weeks = sorted(long["week_start"].unique())
    base = [w for w in weeks if BASE_FROM <= w <= BASE_TO]
    if len(base) < 2:
        raise SystemExit(f"baseline {BASE_FROM}..{BASE_TO} holds {len(base)} weeks; need at least 2")
    rows, shares, summary = [], [], []
    for (pop, dim), g in long.groupby(["population", "dimension"]):
        m = g.pivot_table(index="week_start", columns="category", values="n", aggfunc="sum",
                          fill_value=0).reindex(weeks, fill_value=0)
        cb = m.loc[base].sum().to_numpy(float)
        # leave-one-out js of each baseline week: the noise level of a normal week
        loo = [compare(m.loc[b].to_numpy(float), cb - m.loc[b].to_numpy(float))[0] for b in base]
        thr = max(JS_MIN, float(np.nanmax(loo)) if np.isfinite(loo).any() else JS_MIN)
        for w in weeks:
            cw = m.loc[w].to_numpy(float)
            ref = cb - cw if w in base else cb
            js, psi, p = compare(cw, ref)
            n = int(cw.sum())
            judged = n >= MIN_N and np.isfinite(js)
            rows.append({"population": pop, "dimension": dim, "week_start": w, "n": n,
                         "in_baseline": w in base, "js": js, "psi": psi, "p_value": p, "js_threshold": thr,
                         "changed": bool(judged and js > thr and p < P_MAX)})
        # CUSUM on every category share, after the baseline
        tot = m.sum(axis=1).replace(0, np.nan)
        sh = m.div(tot, axis=0)
        for cat in m.columns:
            s_b = sh.loc[base, cat].dropna()
            mu = float(s_b.mean()) if len(s_b) else 0.0
            sd_b = float(s_b.std(ddof=1)) if len(s_b) > 1 else 0.0
            up = dn = 0.0
            for w in weeks:
                s, n = sh.loc[w, cat], tot.loc[w]
                z = alarm = np.nan
                if w > BASE_TO and np.isfinite(s) and n >= MIN_N:
                    sd = max(sd_b, np.sqrt(max(mu * (1 - mu), 0) / n), SD_FLOOR)
                    z = (s - mu) / sd
                    up, dn = max(0.0, up + z - K), max(0.0, dn - z - K)
                    alarm = up > H or dn > H
                shares.append({"population": pop, "dimension": dim, "category": str(cat), "week_start": w,
                               "n": int(m.loc[w, cat]), "share": s, "baseline_share": mu, "z": z,
                               "cusum_up": up if w > BASE_TO else np.nan,
                               "cusum_down": dn if w > BASE_TO else np.nan,
                               "alarm": bool(alarm) if alarm == alarm else False})
    wk = pd.DataFrame(rows).sort_values(["population", "dimension", "week_start"]).reset_index(drop=True)
    wk["lasting"] = wk.groupby(["population", "dimension"])["changed"].transform(
        lambda s: s & s.shift(1, fill_value=False))
    hon = wk[wk["population"] == "honest"].set_index(["dimension", "week_start"])["changed"]
    wk["honest_changed"] = [bool(hon.get((d, w), False)) for d, w in zip(wk["dimension"], wk["week_start"])]
    wk["tactic_change"] = (wk["population"] == "laundering") & wk["lasting"] & ~wk["honest_changed"]
    me = pd.to_datetime(wk["week_start"])
    wk["month_end_week"] = (me + pd.Timedelta(days=6)).dt.month != me.dt.month
    sh = pd.DataFrame(shares)

    after = wk["week_start"] > BASE_TO
    for (pop, dim), g in wk.groupby(["population", "dimension"]):
        a = g[after.loc[g.index]]
        tc = a[a["tactic_change"]] if pop == "laundering" else a[a["lasting"]]
        al = sh[(sh["population"] == pop) & (sh["dimension"] == dim) & sh["alarm"]]
        first_al = al.sort_values("week_start").groupby("category")["week_start"].first().sort_values()
        summary.append({"population": pop, "dimension": dim, "js_threshold": float(g["js_threshold"].iloc[0]),
                        "baseline_weeks": len(base), "weeks_after": int(len(a)),
                        "weeks_changed": int(a["changed"].sum()), "weeks_lasting": int(a["lasting"].sum()),
                        "weeks_tactic_change": int(a["tactic_change"].sum()),
                        "first_change": tc["week_start"].min() if len(tc) else "",
                        "max_js_after": float(a["js"].max()) if len(a) else np.nan,
                        # CUSUM dates a change the tests above confirmed; on its own it is noise-prone
                        # (many category shares are watched for many weeks), so it is listed apart
                        "change_started": (first_al.iloc[0] if len(first_al) and len(tc) else ""),
                        "cusum_alarms": ("; ".join(f"{c} from {w}" for c, w in first_al.head(6).items())
                                         if len(tc) else ""),
                        "cusum_unconfirmed": ("" if len(tc) else
                                              "; ".join(f"{c} from {w}" for c, w in first_al.head(6).items()))})
    return wk, sh, pd.DataFrame(summary)


def save(pdf, name):
    pdf = pdf.assign(dataset=DATASET)
    for c in pdf.columns:
        if pdf[c].dtype == object:
            pdf[c] = pdf[c].fillna("").astype(str)
    (spark.createDataFrame(pdf).repartition(1).write.mode("overwrite").partitionBy("dataset")
          .parquet(f"{ANALYTICS}/{name}"))


def main():
    long = weekly_counts()
    wk, sh, sm = analyse(long)
    save(wk, f"bq2r_part1_weekly{TAG}")
    save(sh, f"bq2r_part1_shares{TAG}")
    save(sm, f"bq2r_part1_summary{TAG}")
    with pd.option_context("display.width", 220, "display.max_columns", 20, "display.max_colwidth", 70):
        print(f"[part1] {DATASET}: baseline {BASE_FROM}..{BASE_TO}, {wk['week_start'].nunique()} weeks")
        print(sm.round(3).to_string(index=False))
        lw = wk[(wk["population"] == "laundering") & (wk["week_start"] > BASE_TO)]
        print("\n[part1] laundering js per week after the baseline (* = tactic change)")
        piv = lw.assign(v=[f"{j:.2f}{'*' if t else ''}" if j == j else "" for j, t in
                           zip(lw["js"], lw["tactic_change"])]).pivot(index="week_start", columns="dimension",
                                                                      values="v")
        print(piv.to_string())
    print(f"[part1] saved analytics/bq2r_part1_weekly, _shares, _summary (dataset={DATASET})")


if __name__ == "__main__":
    main()
    spark.stop()
