"""
build_features_spark.py
Step 3 of BQ1: clean transactions  ->  one row of graph features per account per active day.

Run (on the EMR master node, after clean_transactions_spark.py and parse_patterns_spark.py):
    spark-submit build_features_spark.py HI-Small                        whole dataset
    spark-submit build_features_spark.py HI-Large 2022-08-01 2022-08-14  one chunk (HI-Large)

Chunks: a big dataset can be built a few weeks at a time. A chunk reads its own dates PLUS
LOOKBACK_DAYS of history before them (so every 3/7-day window is exact) and writes only its
own dates. Start chunks on a Monday and end them on a Sunday, so each week folder is written
whole by exactly one chunk. Each finished chunk is saved, so a crash only costs that chunk.

Reads
    curated/transactions/    clean payments
    curated/labels/          is_laundering per payment (label column only, never a feature)
    curated/scheme_txns/     pattern type per payment (pattern_label only, never a feature)
Writes (Parquet, partitioned by dataset and week; re-running a dataset replaces it)
    curated/<FEATURES_TABLE>/          every account-day (scored by train_ml.py)
    curated/<FEATURES_TABLE>_sample/   every laundering account-day + NEG_SAMPLE_RATE of the rest
                                       (what train_ml.py trains on)
    FEATURES_TABLE = "features_v2" (this version); train with  train_ml.py --features features_v2

One row = one account on one day it sent or received money. Every feature uses only that
day and earlier days, so the model never sees the future.

Columns that are NOT features
    dataset, week, acct_bank, acct, date
    label                1 = the account had a laundering payment that day
    first_laundering_ts  first laundering payment that day (to measure detection delay)
    pattern_label        fan_in, fan_out, gather_scatter, scatter_gather, cycle, bipartite,
                         stack, random; "unclassified" = laundering in no listed scheme;
                         "none" = normal. Several schemes that day -> the earliest payment wins.

Features
    Same day      n_in, n_out, n_senders, n_receivers, n_formats, n_currencies, n_cross_ccy,
                  n_self, night_share, round_share, amt_in_rel, amt_out_rel, amt_max_rel,
                  n_pass_24h, min_in_out_gap_h
                  (amounts are relative to that currency's median payment, so Yen and US Dollar
                  amounts are comparable without exchange rates)
    Rolling       n_in_Kd, n_out_Kd, n_senders_Kd, n_receivers_Kd, n_pass_24h_Kd (K = 3, 7, 14),
                  active_days_7d, active_days_14d, age_days
    Amount match  n_match_24h: payments sent within 24 h of receiving money, for about the same
                  amount (within AMOUNT_MATCH) as that last receipt (money in = money out)
                  min_amt_gap: smallest relative difference between such a payment and the last
                  receipt (9999 = no payment out within 24 h)
                  io_ratio: money out / money in that day (relative amounts; ~1 = pass-through)
    Fan-in/out    fanin_max_Wd, fanout_max_Wd: the most DISTINCT senders / receivers within any
                  W-day window ending that day (exact sliding count, W in WINDOWS_DAYS)
    Gather-scat.  gs_max_Wd: the most counterparties that are BOTH distinct senders and
                  distinct receivers at the same time, i.e. min(fan-in, fan-out) in a W window
    Scatter-gath. sg_src_max, sg_dst_max: most distinct middle accounts linking this account
                  (as source / as destination) to one other account within SG_WINDOW_DAYS
    Two hops      hop2_out: distinct accounts reached in two hops (this -> M -> X) within
                  SG_WINDOW_DAYS; hop2_in: distinct accounts reaching this one in two hops;
                  mid_paths: distinct (sender, receiver) pairs routed THROUGH this account
    Cycles        cycles_today, cycles_7d: distinct time-ordered loops (2..CYCLE_MAX_HOPS hops)
                  through this account that closed today / in the last 7 days
    One hop       in_cp_avg_senders, in_cp_avg_receivers, in_cp_max_receivers,
                  out_cp_avg_senders, out_cp_avg_receivers, out_cp_max_senders:
                  what this account's counterparties did the same day

Scatter-gather and cycles skip "hub" accounts with more than HUB_DEGREE_LIMIT different
counterparties in the data read (the whole dataset, or a chunk plus its lookback) (busy accounts are rarely mules and make the multi-hop
joins explode). Report the limit as an assumption.
"""

import os
import re
import sys
import time

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException


# =============================================================================== SETTINGS
ROOT = os.environ["AML_ROOT"]      # s3://<bucket>; no default, so a job never writes to the wrong bucket

DATASET = sys.argv[1] if len(sys.argv) > 1 else "HI-Small"

FEATURES_TABLE = os.environ.get("AML_FEATURES_TABLE", "features_v2")  # writes curated/<this> and curated/<this>_sample
WINDOWS_DAYS = [3, 7, 14]      # fan-in / fan-out / gather-scatter windows
ROLLING_DAYS = [3, 7, 14]      # rolling totals
AMOUNT_MATCH = 0.10            # "same amount" = within 10% (pass-through matching)
USE_SCATTER_GATHER = True
SG_WINDOW_DAYS = 3
USE_CYCLES = True
CYCLE_MAX_HOPS = 3             # 2 or 3; 4 is much heavier
CYCLE_WINDOW_DAYS = 7
HUB_DEGREE_LIMIT = 50

NEG_SAMPLE_RATE = 0.05         # share of normal account-days kept in features_sample
RANDOM_SEED = 42

# Optional chunk, from the command line: OUTPUT_FROM and OUTPUT_TO ("YYYY-MM-DD", inclusive)
OUTPUT_FROM = sys.argv[2] if len(sys.argv) > 2 else None
OUTPUT_TO = sys.argv[3] if len(sys.argv) > 3 else None
# Daily mode (Step Functions aml-daily): with no chunk given, build the week of AML_RUN_DATE so far
# (Monday to that day). The chunk still reads its look-back history and rewrites only that week.
if os.environ.get("AML_RUN_DATE") and not OUTPUT_FROM:
    import datetime as _dt
    _d = _dt.date.fromisoformat(os.environ["AML_RUN_DATE"])
    OUTPUT_FROM, OUTPUT_TO = (_d - _dt.timedelta(days=_d.weekday())).isoformat(), _d.isoformat()
LOOKBACK_DAYS = 21             # history read before OUTPUT_FROM (> the longest window, 14 days)
SHUFFLE_PARTITIONS = int(os.environ.get("AML_SHUFFLE", 400))   # env override: local tests only
# =========================================================================================

CURATED = f"{ROOT}/curated"
DAY = 86400
DISK = StorageLevel.MEMORY_AND_DISK

spark = SparkSession.builder.appName(f"BuildFeatures-{DATASET}").getOrCreate()
# Every feature family is computed and checkpointed in its own labelled Spark job before the final
# join, instead of all of them inside the one write action. Same values (the checkpoint only keeps
# rows); the Spark UI shows each family's time / memory / spill, and the families no longer share one
# plan. Without this the final join planned every family's whole history at once, and that plan filled
# the driver heap (HI-Medium week 2022-09-12: driver killed at 10 and 20 GB; with it, peak 2.3 GB).


def materialise(df, name):
    t0 = time.time()
    spark.sparkContext.setJobDescription(f"v2 {name}")
    # localCheckpoint (eager) keeps the same rows on the executors (memory + disk) but cuts the
    # plan: the final join then plans 13 small leaves instead of every family's whole history,
    # which is what filled the driver heap (re-planned after each stage, kept for the SQL UI).
    df = df.localCheckpoint(eager=True)
    n = df.count()
    spark.sparkContext.setJobDescription(None)
    print(f"[v2] {name}: {n} rows in {time.time() - t0:.0f}s", flush=True)
    return df


def rebalance(df, *cols):
    """REBALANCE hint in SQL form: works on Spark 3.2+ (EMR 7, local 3.5) and 4.x (EMR Serverless 8).
    df.hint("rebalance", "col", ...) only works on Spark 4 (3.5 passes the names as strings and fails)."""
    view = f"_rebalance_{id(df)}"
    df.createOrReplaceTempView(view)
    return spark.sql(f"SELECT /*+ REBALANCE({', '.join(cols)}) */ * FROM {view}")
spark.sparkContext.setLogLevel("WARN")      # keep logs small: warnings and errors only
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))


def node(bank, account):
    """Internal account key '<bank>|<account>'; split back into two columns at the end."""
    return F.concat_ws("|", F.col(bank).cast("string"), F.col(account))


# =========================================================================== load inputs
def load_transactions():
    all_txns = spark.read.parquet(f"{CURATED}/transactions").filter(F.col("dataset") == DATASET)
    # Median payment per currency: whole dataset; a chunk uses only payments up to its last day
    # (no future weeks, even when later weeks are already cleaned).
    med_base = all_txns
    if OUTPUT_TO:
        med_base = med_base.filter(F.col("txn_date") <= F.lit(OUTPUT_TO).cast("date"))
    med_paid = med_base.groupBy("payment_currency").agg(
        F.expr("percentile_approx(amount_paid, 0.5)").cast("double").alias("med_paid"))
    med_recv = med_base.groupBy("receiving_currency").agg(
        F.expr("percentile_approx(amount_received, 0.5)").cast("double").alias("med_recv"))
    txns = all_txns
    if OUTPUT_FROM:
        start = F.date_sub(F.lit(OUTPUT_FROM).cast("date"), LOOKBACK_DAYS)
        txns = txns.filter(F.col("txn_date") >= start)
    if OUTPUT_TO:
        txns = txns.filter(F.col("txn_date") <= F.lit(OUTPUT_TO).cast("date"))
    labels = (spark.read.parquet(f"{CURATED}/labels")
                   .filter(F.col("dataset") == DATASET).select("txn_id", "is_laundering"))
    t = txns.join(labels, "txn_id", "left")

    try:   # pattern type per payment; a payment in several schemes keeps the first scheme
        st = (spark.read.parquet(f"{CURATED}/scheme_txns")
                   .filter((F.col("dataset") == DATASET) & F.col("found_in_transactions")))
        first = Window.partitionBy("txn_id").orderBy("scheme_id")
        st = (st.withColumn("_n", F.row_number().over(first)).filter("_n = 1")
                .select("txn_id", "pattern_type"))
        t = t.join(F.broadcast(st), "txn_id", "left")
    except AnalysisException:
        print("[features] no scheme_txns: laundering rows get pattern_label 'unclassified'")
        t = t.withColumn("pattern_type", F.lit(None).cast("string"))

    # Amounts relative to the median payment in the same currency (no exchange rates needed).
    return (t.join(F.broadcast(med_paid), "payment_currency")
             .join(F.broadcast(med_recv), "receiving_currency")
             .withColumn("is_laundering", F.coalesce("is_laundering", F.lit(0)))
             .withColumn("ts", F.unix_timestamp("txn_time"))
             .withColumn("night", (F.hour("txn_time") < 6).cast("int"))
             .withColumn("round_amt", (F.col("amount_paid") % 100 == 0).cast("int"))
             .withColumn("rel_paid", F.col("amount_paid").cast("double") / F.col("med_paid"))
             .withColumn("rel_recv", F.col("amount_received").cast("double") / F.col("med_recv"))
             .withColumn("src", node("from_bank", "from_account"))
             .withColumn("dst", node("to_bank", "to_account")))


# =========================================================== account events + same-day stats
def account_events(t):
    """Each payment twice: once for the sender ('out'), once for the receiver ('in')."""
    common = ["ts", "txn_date", "week", "night", "round_amt", "payment_format",
              F.col("is_cross_currency").cast("int").alias("cross"),
              F.col("is_self_transfer").cast("int").alias("self"),
              "is_laundering", "pattern_type"]
    out = t.select(F.col("src").alias("acct"), F.col("dst").alias("cp"), F.lit("out").alias("dir"),
                   F.col("payment_currency").alias("ccy"), F.col("rel_paid").alias("rel_amt"),
                   F.col("amount_paid").cast("double").alias("amt"), *common)
    inn = t.select(F.col("dst").alias("acct"), F.col("src").alias("cp"), F.lit("in").alias("dir"),
                   F.col("receiving_currency").alias("ccy"), F.col("rel_recv").alias("rel_amt"),
                   F.col("amount_received").cast("double").alias("amt"), *common)
    ev = out.unionByName(inn)
    # Pass-through: for each payment sent, the time and amount of the last money RECEIVED.
    by_time = (Window.partitionBy("acct").orderBy("ts", "dir")            # "in" before "out"
                     .rowsBetween(Window.unboundedPreceding, 0))
    last_in = lambda c: F.last(F.when(F.col("dir") == "in", F.col(c)), ignorenulls=True).over(by_time)
    is_out = F.col("dir") == "out"
    ev = (ev.withColumn("in_out_gap", F.when(is_out, F.col("ts") - last_in("ts")))
            .withColumn("_in_amt", last_in("amt"))
            .withColumn("_in_rel", last_in("rel_amt"))
            .withColumn("_in_ccy", last_in("ccy")))
    # Same currency: compare real amounts. Different currency: compare relative amounts.
    same_ccy = F.col("_in_ccy") == F.col("ccy")
    gap = F.when(same_ccy, F.abs(F.col("amt") - F.col("_in_amt")) / F.col("_in_amt")) \
           .otherwise(F.abs(F.col("rel_amt") - F.col("_in_rel")) / F.col("_in_rel"))
    return (ev.withColumn("amt_gap", F.when(is_out & (F.col("in_out_gap") <= DAY), gap))
              .drop("_in_amt", "_in_rel", "_in_ccy"))


def same_day_features(ev):
    is_in, is_out = F.col("dir") == "in", F.col("dir") == "out"
    other = F.col("self") == 0
    return (ev.groupBy("acct", "txn_date")
        .agg(F.max("week").alias("week"),
             F.sum(is_in.cast("int")).alias("n_in"),
             F.sum(is_out.cast("int")).alias("n_out"),
             F.countDistinct(F.when(is_in & other, F.col("cp"))).alias("n_senders"),
             F.countDistinct(F.when(is_out & other, F.col("cp"))).alias("n_receivers"),
             F.countDistinct("payment_format").alias("n_formats"),
             F.countDistinct("ccy").alias("n_currencies"),
             F.sum("cross").alias("n_cross_ccy"),
             F.sum("self").alias("n_self"),
             F.avg("night").alias("night_share"),
             F.avg("round_amt").alias("round_share"),
             F.sum(F.when(is_in, F.col("rel_amt"))).alias("amt_in_rel"),
             F.sum(F.when(is_out, F.col("rel_amt"))).alias("amt_out_rel"),
             F.max("rel_amt").alias("amt_max_rel"),
             F.sum((is_out & (F.col("in_out_gap") <= DAY)).cast("int")).alias("n_pass_24h"),
             (F.min("in_out_gap") / 3600.0).alias("min_in_out_gap_h"),
             F.sum((F.col("amt_gap") <= AMOUNT_MATCH).cast("int")).alias("n_match_24h"),
             F.min("amt_gap").alias("min_amt_gap"),
             F.max("is_laundering").alias("label"),
             F.min(F.when(F.col("is_laundering") == 1, F.col("ts"))).alias("first_laundering_ts"),
             F.min(F.when(F.col("pattern_type").isNotNull(),
                          F.struct("ts", "pattern_type"))).alias("_first_pattern"))
        .withColumn("pattern_label",
                    F.when(F.col("label") == 0, "none")
                     .when(F.col("_first_pattern").isNull(), "unclassified")
                     .otherwise(F.col("_first_pattern.pattern_type")))
        .drop("_first_pattern")
        .withColumnRenamed("txn_date", "date"))


def rolling_features(daily):
    # Range windows and age depend on day differences, not the choice of origin.
    # A fixed origin avoids a full aggregation + driver round-trip for min(date).
    daily = daily.withColumn("day_idx", F.datediff("date", F.lit("1970-01-01")))
    by_day = Window.partitionBy("acct").orderBy("day_idx")
    last = lambda k: by_day.rangeBetween(-(k - 1), 0)
    for k in ROLLING_DAYS:
        for c in ("n_in", "n_out", "n_senders", "n_receivers", "n_pass_24h"):
            daily = daily.withColumn(f"{c}_{k}d", F.sum(c).over(last(k)))
    return (daily
        .withColumn("io_ratio", F.when(F.col("amt_in_rel") > 0, F.col("amt_out_rel") / F.col("amt_in_rel")))
        .withColumn("active_days_7d", F.count(F.lit(1)).over(last(7)))
        .withColumn("active_days_14d", F.count(F.lit(1)).over(last(14)))
        .withColumn("age_days", F.col("day_idx") - F.min("day_idx").over(
            by_day.rowsBetween(Window.unboundedPreceding, 0)))
        .drop("day_idx"))


# ============================================================= exact sliding distinct counts
def active_runs(edges, key_cols, cp_col, W):
    """Merge each key-counterparty's payments into runs separated by more than W seconds."""
    pair = Window.partitionBy(*key_cols, cp_col).orderBy("ts")
    return (edges
        .withColumn("_new", (F.lag("ts").over(pair).isNull()
                             | (F.col("ts") - F.lag("ts").over(pair) > W)).cast("int"))
        .withColumn("_run", F.sum("_new").over(pair.rowsBetween(Window.unboundedPreceding, 0)))
        .groupBy(*key_cols, cp_col, "_run")
        .agg(F.min("ts").alias("start"), F.max("ts").alias("last")))


def run_events(runs, key_cols, W):
    """Emit both endpoints in one pass, so the run-building plan is not duplicated."""
    return (runs.select(*key_cols, F.explode(F.array(
                F.struct(F.col("start").alias("t"), F.lit(1).alias("delta")),
                F.struct((F.col("last") + W + 1).alias("t"), F.lit(-1).alias("delta"))
            )).alias("_event"))
            .select(*key_cols, "_event.*"))


def directional_edges(edges):
    """One input scan yields incoming and outgoing counterparty edges."""
    return (edges.select("ts", F.explode(F.array(
                F.struct(F.col("dst").alias("acct"), F.col("src").alias("cp"), F.lit("in").alias("dir")),
                F.struct(F.col("src").alias("acct"), F.col("dst").alias("cp"), F.lit("out").alias("dir"))
            )).alias("_edge"))
            .select("ts", "_edge.*"))


def sliding_distinct_max(edges, key_cols, cp_col, W, out_name, runs=None):
    """For each key and day: the most distinct counterparties seen within any W-second window
    ending during that day. Exact and O(n log n): each key-counterparty pair is merged into
    'active runs' (payments less than W apart); a run counts from its first payment until W
    after its last. A running sum of +1 (run starts) / -1 (run expires) gives the live count,
    and a 0-event at every payment reads it."""
    if runs is None:
        runs = active_runs(edges, key_cols, cp_col, W)
    ev = (run_events(runs, key_cols, W)
          .unionByName(edges.select(*key_cols, F.col("ts").alias("t"), F.lit(0).alias("delta"))))
    live = (Window.partitionBy(*key_cols).orderBy("t", F.col("delta").desc())
                  .rowsBetween(Window.unboundedPreceding, 0))
    return (ev.withColumn("n", F.sum("delta").over(live))
              .filter(F.col("delta") >= 0)
              .groupBy(*key_cols, F.to_date(F.from_unixtime("t")).alias("date"))
              .agg(F.max("n").alias(out_name)))


def gather_scatter_max(edges, W, out_name, runs=None):
    """Like sliding_distinct_max, but tracks distinct senders AND receivers together and
    reports the most min(senders, receivers) reached during the day."""
    if runs is None:
        runs = active_runs(directional_edges(edges), ["acct", "dir"], "cp", W)
    ev = (run_events(runs, ["acct", "dir"], W)
          .withColumn("d_in", F.when(F.col("dir") == "in", F.col("delta")).otherwise(0))
          .withColumn("d_out", F.when(F.col("dir") == "out", F.col("delta")).otherwise(0)))
    live = (Window.partitionBy("acct").orderBy("t", F.col("delta").desc())
                  .rowsBetween(Window.unboundedPreceding, 0))
    return (ev.withColumn("m", F.least(F.sum("d_in").over(live), F.sum("d_out").over(live)))
              .filter(F.col("delta") >= 0)
              .groupBy("acct", F.to_date(F.from_unixtime("t")).alias("date"))
              .agg(F.max("m").alias(out_name)))


# ============================================================================ multi-hop
def since(df, days):
    """Chunk mode: keep only payments from `days` before OUTPUT_FROM. Each heavy step gets the
    history its OUTPUT days need and no more, so the chunk's own rows are unchanged:
      fan-in / fan-out / gather-scatter over w days   -> w days
      scatter-gather (2 hops within 3 days, then a 3-day sliding max)  -> 2 x 3 = 6 days
      cycles (closed within 7 days, then cycles_7d sums 7 days)        -> 7 + 6 = 13 days
    The hub list is still built from the full look-back (hubs depend on how much is read).
    Whole-dataset runs (no OUTPUT_FROM) are untouched."""
    if not OUTPUT_FROM:
        return df
    cut = F.unix_timestamp(F.to_timestamp(F.lit(OUTPUT_FROM))) - days * DAY
    return df.filter(F.col("ts") >= cut)


def non_hub_edges(edges):
    degree = (edges.select(F.col("src").alias("n"), F.col("dst").alias("cp"))
                   .unionByName(edges.select(F.col("dst").alias("n"), F.col("src").alias("cp")))
                   .distinct().groupBy("n").count())
    hubs = degree.filter(F.col("count") > HUB_DEGREE_LIMIT).select("n")
    return (edges.join(hubs.withColumnRenamed("n", "src"), "src", "left_anti")
                 .join(hubs.withColumnRenamed("n", "dst"), "dst", "left_anti"))


def scatter_gather_features(small):
    """Paths S -> M -> D (D != S, second hop within the window): the most distinct middle
    accounts M linking S to the same D, reported for S (as source) and D (as destination)."""
    W = SG_WINDOW_DAYS * DAY
    e1 = small.select(F.col("src").alias("S"), F.col("dst").alias("M"), F.col("ts").alias("t1"))
    e2 = small.select(F.col("src").alias("M2"), F.col("dst").alias("D"), F.col("ts").alias("t2"))
    paths = (e1.join(e2, (F.col("M") == F.col("M2")) & (F.col("t2") >= F.col("t1"))
                         & (F.col("t2") <= F.col("t1") + W))
               .filter(F.col("D") != F.col("S"))
               .select("S", "M", "D", F.col("t2").alias("ts"))
               .persist(DISK))
    per_pair = sliding_distinct_max(paths, ["S", "D"], "M", W, "n_mid")
    as_src = (per_pair.groupBy(F.col("S").alias("acct"), "date")
                      .agg(F.max("n_mid").alias("sg_src_max")))
    as_dst = (per_pair.groupBy(F.col("D").alias("acct"), "date")
                      .agg(F.max("n_mid").alias("sg_dst_max")))
    # Two hops, by the day the second payment happens.
    p = paths.withColumn("date", F.to_date(F.from_unixtime("ts")))
    hop_out = p.groupBy(F.col("S").alias("acct"), "date").agg(F.countDistinct("D").alias("hop2_out"))
    hop_in = p.groupBy(F.col("D").alias("acct"), "date").agg(F.countDistinct("S").alias("hop2_in"))
    through = (p.groupBy(F.col("M").alias("acct"), "date")
                .agg(F.countDistinct(F.concat_ws(">", "S", "D")).alias("mid_paths")))
    return (as_src.join(as_dst, ["acct", "date"], "full")
                  .join(hop_out, ["acct", "date"], "full")
                  .join(hop_in, ["acct", "date"], "full")
                  .join(through, ["acct", "date"], "full"))


def cycle_features(small):
    """Time-ordered loops of 2..CYCLE_MAX_HOPS hops within CYCLE_WINDOW_DAYS. Each loop counts
    for every account on it, on the day the loop closes. For each start payment and reachable
    account only the earliest-arriving path is kept, which keeps the search small."""
    W = CYCLE_WINDOW_DAYS * DAY
    E = small.select("src", "dst", "ts", "txn_id").persist(DISK)
    P = E.select(F.col("src").alias("start"), F.col("dst").alias("cur"),
                 F.col("ts").alias("first_ts"), F.col("ts").alias("last_ts"),
                 F.array("src", "dst").alias("nodes"), F.array("txn_id").alias("txns"))
    loops = None
    for hops in range(2, CYCLE_MAX_HOPS + 1):
        j = P.alias("p").join(E.alias("e"),
                              (F.col("p.cur") == F.col("e.src"))
                              & (F.col("e.ts") >= F.col("p.last_ts"))
                              & (F.col("e.ts") <= F.col("p.first_ts") + W))
        closed = (j.filter(F.col("e.dst") == F.col("p.start"))
                   .select(F.col("p.nodes").alias("nodes"), F.col("e.ts").alias("close_ts"),
                           F.concat("p.txns", F.array("e.txn_id")).alias("txns")))
        loops = closed if loops is None else loops.unionByName(closed)
        if hops < CYCLE_MAX_HOPS:
            P = (j.filter((F.col("e.dst") != F.col("p.start"))
                          & ~F.array_contains("p.nodes", F.col("e.dst")))
                  .select(F.col("p.start").alias("start"), F.col("e.dst").alias("cur"),
                          F.col("p.first_ts").alias("first_ts"), F.col("e.ts").alias("last_ts"),
                          F.concat("p.nodes", F.array("e.dst")).alias("nodes"),
                          F.concat("p.txns", F.array("e.txn_id")).alias("txns")))
            # ties (same minute) broken by the last payment's id, so the result never depends on
            # row order / partition layout: paths that also tie on it end with the same payment,
            # so they visit the same accounts (all the loop count uses). One id sorts far cheaper
            # than all of them joined (that spilled ~1.3 GB more on HI-Small).
            earliest = (Window.partitionBy("start", "first_ts", "cur")
                              .orderBy("last_ts", F.element_at("txns", -1)))
            P = P.withColumn("_n", F.row_number().over(earliest)).filter("_n = 1").drop("_n")
    # Repeat payments between the same accounts form many copies of one loop: count each
    # distinct set of accounts once per day.
    loops = (loops.withColumn("date", F.to_date(F.from_unixtime("close_ts")))
                  .withColumn("_key", F.concat_ws(",", F.array_sort("nodes")))
                  .dropDuplicates(["_key", "date"]))
    return (loops.select(F.explode("nodes").alias("acct"), "date")
                 .groupBy("acct", "date").agg(F.count(F.lit(1)).alias("cycles_today")))


def one_hop_features(ev, daily):
    """What this account's counterparties did the same day (their own sender/receiver counts)."""
    cp_deg = daily.select(F.col("acct").alias("cp"), F.col("date").alias("txn_date"),
                          F.col("n_senders").alias("cp_senders"),
                          F.col("n_receivers").alias("cp_receivers"))
    links = (ev.filter(F.col("self") == 0).select("acct", "cp", "dir", "txn_date").distinct()
               .join(cp_deg, ["cp", "txn_date"], "left"))
    is_in, is_out = F.col("dir") == "in", F.col("dir") == "out"
    return (links.groupBy("acct", F.col("txn_date").alias("date"))
        .agg(F.avg(F.when(is_in, F.col("cp_senders"))).alias("in_cp_avg_senders"),
             F.avg(F.when(is_in, F.col("cp_receivers"))).alias("in_cp_avg_receivers"),
             F.max(F.when(is_in, F.col("cp_receivers"))).alias("in_cp_max_receivers"),
             F.avg(F.when(is_out, F.col("cp_senders"))).alias("out_cp_avg_senders"),
             F.avg(F.when(is_out, F.col("cp_receivers"))).alias("out_cp_avg_receivers"),
             F.max(F.when(is_out, F.col("cp_senders"))).alias("out_cp_max_senders")))


# ================================================================================= main
def fan_windows_needed():
    """The 9 fan-in / fan-out / gather-scatter window features cost ~9 min a run. Compute them only
    while a model still uses them: the BQ1 champion (detector or namer), the BQ1 recipe BQ2 trains
    with (models/final/meta.json), or any BQ2 model version (models/bq2r/<dataset>/<version>/).
    AML_FAN_WINDOWS=1 / 0 forces it. If the champion or the recipe cannot be read, compute them
    (safe default)."""
    force = os.environ.get("AML_FAN_WINDOWS", "").strip()
    if force in ("0", "1"):
        return force == "1"
    import json
    rx = r"(^|_)(fanin|fanout|gs)_max_\d+d$"
    uses = lambda meta: [c for c in meta.get("stage1_features", []) + meta.get("namer_features", [])
                         if re.search(rx, c)]
    try:
        read = lambda path: json.loads("\n".join(r.value for r in spark.read.text(path).collect()))
        run = read(f"{ROOT}/models/bq1_champion.json")["run"]
        metas = {f"champion {run}": read(f"{ROOT}/models/{run}/meta.json"),
                 "BQ2 recipe models/final": read(f"{ROOT}/models/final/meta.json")}
    except Exception as e:                       # no champion yet, unreadable meta, ...
        print(f"[v2] could not read a model's meta ({e}); computing fan/gs windows", flush=True)
        return True
    try:                                         # BQ2 versions (none yet in a fresh bucket)
        for path, text in spark.sparkContext.wholeTextFiles(f"{ROOT}/models/bq2r/*/*/meta.json").collect():
            metas["BQ2 " + "/".join(path.split("/")[-3:-1])] = json.loads(text)
    except Exception:
        pass
    need = False
    for who, meta in metas.items():
        used = uses(meta)
        print(f"[v2] {who} uses {len(used)} fan/gs window features", flush=True)
        need = need or bool(used)
    return need


def main():
    t = load_transactions().persist(DISK)
    edges = t.filter(~F.col("is_self_transfer")).select("src", "dst", "ts", "txn_id").persist(DISK)

    ev = account_events(t).persist(DISK)
    daily = same_day_features(ev).persist(DISK)
    feats = rolling_features(daily)
    feats = feats.join(materialise(one_hop_features(ev, daily), "one_hop"), ["acct", "date"], "left")
    feats = materialise(feats, "rolling+one_hop")

    fan_cols = []
    for w in (WINDOWS_DAYS if fan_windows_needed() else []):
        W = w * DAY
        e_w = since(edges, w)          # chunk mode: only the w days of history this window needs
        # All three features use the same runs. Cache the narrow run table once per window;
        # release it once the three results have been eagerly checkpointed.
        runs = active_runs(directional_edges(e_w), ["acct", "dir"], "cp", W).persist(DISK)
        fin = sliding_distinct_max(e_w.select(F.col("dst").alias("acct"), F.col("src").alias("cp"), "ts"),
                                   ["acct"], "cp", W, f"fanin_max_{w}d", runs=runs.filter("dir = 'in'"))
        fout = sliding_distinct_max(e_w.select(F.col("src").alias("acct"), F.col("dst").alias("cp"), "ts"),
                                    ["acct"], "cp", W, f"fanout_max_{w}d", runs=runs.filter("dir = 'out'"))
        gs = gather_scatter_max(e_w, W, f"gs_max_{w}d", runs=runs)
        try:
            fin = materialise(fin, f"fanin_{w}d")
            fout = materialise(fout, f"fanout_{w}d")
            gs = materialise(gs, f"gather_scatter_{w}d")
        finally:
            runs.unpersist()
        feats = (feats.join(fin, ["acct", "date"], "left")
                      .join(fout, ["acct", "date"], "left")
                      .join(gs, ["acct", "date"], "left"))
        fan_cols += [f"fanin_max_{w}d", f"fanout_max_{w}d", f"gs_max_{w}d"]

    graph_cols = []
    if USE_SCATTER_GATHER or USE_CYCLES:
        small = non_hub_edges(edges).persist(DISK)
        if USE_SCATTER_GATHER:
            sg = materialise(scatter_gather_features(since(small, 2 * SG_WINDOW_DAYS)), "scatter_gather")
            feats = feats.join(sg, ["acct", "date"], "left")
            graph_cols += ["sg_src_max", "sg_dst_max", "hop2_out", "hop2_in", "mid_paths"]
        if USE_CYCLES:
            cyc = materialise(cycle_features(since(small, CYCLE_WINDOW_DAYS + 6)), "cycles")
            feats = feats.join(cyc, ["acct", "date"], "left")
            feats = feats.fillna(0, subset=["cycles_today"])
            last7 = (Window.partitionBy("acct").orderBy(F.datediff("date", F.lit("1970-01-01")))
                           .rangeBetween(-6, 0))
            feats = feats.withColumn("cycles_7d", F.sum("cycles_today").over(last7))
            graph_cols += ["cycles_today", "cycles_7d"]

    feature_cols = (
        ["n_in", "n_out", "n_senders", "n_receivers", "n_formats", "n_currencies", "n_cross_ccy",
         "n_self", "night_share", "round_share", "amt_in_rel", "amt_out_rel", "amt_max_rel",
         "n_pass_24h", "min_in_out_gap_h", "n_match_24h", "min_amt_gap", "io_ratio"]
        + [f"{c}_{k}d" for k in ROLLING_DAYS for c in ("n_in", "n_out", "n_senders", "n_receivers", "n_pass_24h")]
        + ["active_days_7d", "active_days_14d", "age_days"]
        + fan_cols + graph_cols
        + ["in_cp_avg_senders", "in_cp_avg_receivers", "in_cp_max_receivers",
           "out_cp_avg_senders", "out_cp_avg_receivers", "out_cp_max_senders"])

    acct_parts = F.split("acct", r"\|")
    features = feats.select(
        F.lit(DATASET).alias("dataset"), "week",
        acct_parts.getItem(0).cast("int").alias("acct_bank"),
        acct_parts.getItem(1).alias("acct"),
        "date", F.col("label").cast("int").alias("label"), "first_laundering_ts", "pattern_label",
        *[F.coalesce(F.col(c).cast("double"),
                     F.lit(9999.0 if c in ("min_in_out_gap_h", "min_amt_gap") else 0.0)).alias(c)
          for c in feature_cols])
    if OUTPUT_FROM:                    # drop the history days: they belong to the previous chunk
        features = features.filter(F.col("date") >= F.lit(OUTPUT_FROM).cast("date"))
    spark.sparkContext.setJobDescription("v2 final join + write")
    features = features.persist(DISK)

    path = f"{CURATED}/{FEATURES_TABLE}"
    # rebalance, not repartition: by week alone one task wrote each week while the other cores
    # idled; rebalance lets AQE split big weeks across tasks and merge small ones
    rebalance(features, "dataset", "week").write.mode("overwrite") \
            .partitionBy("dataset", "week").parquet(path)
    sample = features.filter((F.col("label") == 1) | (F.rand(RANDOM_SEED) < NEG_SAMPLE_RATE))
    rebalance(sample, "dataset", "week").write.mode("overwrite") \
          .partitionBy("dataset", "week").parquet(f"{CURATED}/{FEATURES_TABLE}_sample")

    by_label = {r["label"]: r["count"] for r in features.groupBy("label").count().collect()}
    by_pattern = {r["pattern_label"]: r["count"] for r in
                  features.filter("label = 1").groupBy("pattern_label").count().collect()}
    chunk = f" {OUTPUT_FROM or 'start'} to {OUTPUT_TO or 'end'}" if (OUTPUT_FROM or OUTPUT_TO) else ""
    print(f"[features] {DATASET}{chunk}: account-days by label {by_label}")
    print(f"[features] laundering account-days by pattern: {dict(sorted(by_pattern.items()))}")
    print(f"[features] {len(feature_cols)} features: {feature_cols}")


if __name__ == "__main__":
    main()
    spark.stop()
