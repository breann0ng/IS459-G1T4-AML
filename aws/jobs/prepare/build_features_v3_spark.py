"""
build_features_v3_spark.py
Round 3, step 2: add "unusual for this account" and owner features ON TOP of the existing
tables, without rebuilding the heavy graph features.

Run (EMR, after build_features_spark.py and build_txn_features_spark.py for the dataset):
    spark-submit [S3 confs] build_features_v3_spark.py HI-Small
    spark-submit [S3 confs] build_features_v3_spark.py HI-Medium
    spark-submit [S3 confs] build_features_v3_spark.py HI-Large 2022-08-01 2022-08-14   one chunk
    (a chunk reads LOOKBACK_DAYS of history before it and writes only its own dates; start
     chunks on a Monday and end them on a Sunday so each week folder is written by one chunk)

Reads    curated/transactions, curated/features_v2 (+ _sample), curated/txn_features (+ _sample)
         raw/accounts/set=<DATASET>/*.csv    (Kaggle *_accounts.csv: Bank Name, Bank ID,
                                              Account Number, Entity ID, Entity Name)
Writes   curated/features_v3 (+ _sample)       = features_v2 + the x_ / o_ columns below
         curated/txn_features_v3 (+ _sample)   = txn_features + t_ extras + s_ / r_ copies of x_ / o_
         Same rows as v2 (the samples keep exactly the same rows), so v2 vs v3 is a fair test.
Train:   python3 train_txn.py --txn-table txn_features_v3 --stage2-features features_v3 ...

Why: the unclassified profile showed laundering outside the named schemes looks like an
ordinary one-off payment (one sender, one receiver, no graph shape), slightly larger than usual,
often to a new counterparty, from a quieter account, and from accounts that are never in a
scheme. Graph-shape features cannot see that; "unusual for THIS account" features might.

Account-day features (x_ = lead-up / unusual for this account, o_ = owner from accounts.csv)
    x_out_spike, x_in_spike     payments out / in today ÷ the account's average per active day
                                over the previous 14 days (-1 = no history)
    x_amt_out_vs_own            largest payment out today vs the account's usual payment out
                                (difference of log relative amounts; 0 = no history)
    x_amt_in_vs_own             same for money in
    x_days_since_prev           days since the account was last active (99 = first time seen)
    x_active_prev14             active days in the previous 14 days (today excluded)
    x_trend                     activity over the last 3 days ÷ the 11 days before (per day)
    x_new_cp_out, x_new_cp_in   share of today's receivers / senders never paid / paid by before
    o_type_*                    owner type: corporation, partnership, sole_prop, individual,
                                country, direct
    o_n_accounts, o_n_banks     accounts / banks the owner has
    o_bank_size                 accounts held at this account's bank
    o_crypto_bank               the account's bank is a crypto bank
    o_owner_active_accts        how many of the owner's accounts were active today
Payment features (t_)
    t_same_owner                sender and receiver accounts belong to the same owner
                                (different accounts): moving money through one's own accounts
    t_cross_country             sender's and receiver's banks are in different countries
    t_amt_vs_sender             this payment vs the sender's usual payment out in the previous
                                14 days (log difference; 0 = no history)
    t_amt_vs_receiver           this payment vs the receiver's usual money in, previous 14 days
    t_sender_new                sender has no payment out in the previous 14 days
"""

import os
import sys
from datetime import date as _date, timedelta as _timedelta

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

ROOT = os.environ["AML_ROOT"]      # s3://<bucket>; no default, so a job never writes to the wrong bucket
DATASET = sys.argv[1] if len(sys.argv) > 1 else "HI-Small"
CHUNK_FROM = sys.argv[2] if len(sys.argv) > 2 else None    # optional chunk, inclusive dates
CHUNK_TO = sys.argv[3] if len(sys.argv) > 3 else None      # (HI-Large: two weeks at a time)
# Daily mode (Step Functions aml-daily): with no chunk given, build the week of AML_RUN_DATE so far
# (Monday to that day). The chunk still reads its look-back history and rewrites only that week.
if os.environ.get("AML_RUN_DATE") and not CHUNK_FROM:
    import datetime as _dt
    _d = _dt.date.fromisoformat(os.environ["AML_RUN_DATE"])
    CHUNK_FROM, CHUNK_TO = (_d - _dt.timedelta(days=_d.weekday())).isoformat(), _d.isoformat()
LOOKBACK_DAYS = 21                                         # history read before a chunk
SRC_FEATURES, SRC_TXN = "features_v2", "txn_features"
OUT_FEATURES, OUT_TXN = "features_v3", "txn_features_v3"
HISTORY_DAYS = 14
SHUFFLE_PARTITIONS = int(os.environ.get("AML_SHUFFLE", 400))
# Send the (narrow) accounts table to every executor instead of shuffling the big tables to it:
# saves a shuffle of the account-days and two of the payments. ~2.1M accounts on HI-Medium /
# HI-Large; set AML_BROADCAST_ACCOUNTS=0 if executors run out of memory.
BROADCAST_ACCOUNTS = os.environ.get("AML_BROADCAST_ACCOUNTS", "1") == "1"

CURATED = f"{ROOT}/curated"
DAY = 86400
DISK = StorageLevel.MEMORY_AND_DISK
OWNER_TYPES = {"corporation": "Corporation", "partnership": "Partnership",
               "sole_prop": "Sole Proprietorship", "individual": "Individual",
               "country": "Country", "direct": "Direct"}

spark = SparkSession.builder.appName(f"BuildFeaturesV3-{DATASET}").getOrCreate()


def rebalance(df, *cols):
    """REBALANCE hint in SQL form: works on Spark 3.2+ (EMR 7, local 3.5) and 4.x (EMR Serverless 8).
    df.hint("rebalance", "col", ...) only works on Spark 4 (3.5 passes the names as strings and fails)."""
    view = f"_rebalance_{id(df)}"
    df.createOrReplaceTempView(view)
    return spark.sql(f"SELECT /*+ REBALANCE({', '.join(cols)}) */ * FROM {view}")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.ansi.enabled", "false")   # Spark 4 default is strict; ratios are guarded anyway
spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))


def node(bank, account):
    return F.concat_ws("|", F.col(bank).cast("string"), F.upper(F.trim(F.col(account))))


def load_accounts():
    """One row per account: owner, owner type, bank country; plus owner / bank sizes."""
    a = (spark.read.option("header", True).option("recursiveFileLookup", True)
         .option("pathGlobFilter", "*.csv").csv(f"{ROOT}/raw/accounts/set={DATASET}/")
         .select(F.col("Bank ID").cast("int").alias("bank"),
                 F.upper(F.trim("Account Number")).alias("account"),
                 F.trim("Entity ID").alias("owner"),
                 F.regexp_replace(F.trim("Entity Name"), r"\s*#\d+$", "").alias("owner_type"),
                 F.regexp_replace(F.trim("Bank Name"), r"\s*#\d+$", "").alias("bank_country"))
         .dropDuplicates(["bank", "account"]))
    n = a.count()
    if n == 0:
        raise SystemExit(f"No accounts file under raw/accounts/set={DATASET}/")
    own = a.groupBy("owner").agg(F.countDistinct("account").alias("o_n_accounts"),
                                 F.countDistinct("bank").alias("o_n_banks"))
    bank = a.groupBy("bank").agg(F.count(F.lit(1)).alias("o_bank_size"))
    a = (a.join(own, "owner").join(bank, "bank")
          .withColumn("acct", node("bank", "account"))
          .withColumn("o_crypto_bank", F.lower("bank_country").rlike("crypt|crytp").cast("int")))
    for k, v in OWNER_TYPES.items():
        a = a.withColumn(f"o_type_{k}", (F.col("owner_type") == v).cast("int"))
    print(f"[v3] {DATASET}: {n} accounts, owner types "
          f"{ {r['owner_type']: r['count'] for r in a.groupBy('owner_type').count().collect()} }")
    return a


def week_folders(tx):
    """Chunk mode: date -> week folder number, so reads open only the week folders they need.
    Every table's `week` comes from clean_transactions_spark.py: Monday-to-Sunday weeks numbered in
    order, so one (date, week) pair from transactions fixes the number of any date. Filtering on
    `date` alone does not prune folders (the tables are partitioned by week): Spark lists and opens
    every week's files of these wide tables. None outside chunk mode (whole dataset: read everything)."""
    if not CHUNK_FROM:
        return None
    anchor = tx.filter(F.col("week") > 0).select("txn_date", "week").first()
    if anchor is None:
        return None
    monday = lambda d: d - _timedelta(days=d.weekday())
    return lambda day: anchor["week"] + (monday(_date.fromisoformat(day) if isinstance(day, str) else day)
                                         - monday(anchor["txn_date"])).days // 7


def in_weeks(df, week_of, lo, hi):
    """Keep only the week folders holding dates lo..hi (the date filters stay, so rows are unchanged)."""
    if week_of is None:
        return df
    return df.filter(F.col("week").between(week_of(lo), week_of(hi)))


def main():
    tx = spark.read.parquet(f"{CURATED}/transactions").filter(F.col("dataset") == DATASET)
    week_of = week_folders(tx)
    med_base = tx           # median per currency: a chunk uses only payments up to its last day
    if CHUNK_TO:
        med_base = med_base.filter(F.col("txn_date") <= F.lit(CHUNK_TO).cast("date"))
        if week_of is not None:     # no later week folders
            med_base = med_base.filter(F.col("week") <= week_of(CHUNK_TO))
    # Both medians in ONE scan of the dataset (was two scans of every week so far: this grows each
    # week), collected to the driver (a few currencies).
    sides = med_base.select(F.explode(F.array(
        F.struct(F.lit("paid").alias("side"), F.col("payment_currency").alias("ccy"),
                 F.col("amount_paid").cast("double").alias("amt")),
        F.struct(F.lit("recv").alias("side"), F.col("receiving_currency").alias("ccy"),
                 F.col("amount_received").cast("double").alias("amt")))).alias("_s")).select("_s.*")
    m = (sides.groupBy("side", "ccy")
              .agg(F.expr("percentile_approx(amt, 0.5)").alias("med")).collect())
    med = spark.createDataFrame([(r["ccy"], r["med"]) for r in m if r["side"] == "paid"],
                                "payment_currency string, med_paid double")
    medr = spark.createDataFrame([(r["ccy"], r["med"]) for r in m if r["side"] == "recv"],
                                 "receiving_currency string, med_recv double")
    if CHUNK_FROM:
        lookback_from = _date.fromisoformat(CHUNK_FROM) - _timedelta(days=LOOKBACK_DAYS)
        tx = in_weeks(tx, week_of, lookback_from, CHUNK_TO)
        tx = tx.filter((F.col("txn_date") >= F.date_sub(F.lit(CHUNK_FROM).cast("date"), LOOKBACK_DAYS))
                       & (F.col("txn_date") <= F.lit(CHUNK_TO).cast("date")))
    t = (tx.join(F.broadcast(med), "payment_currency").join(F.broadcast(medr), "receiving_currency")
           .withColumn("ts", F.unix_timestamp("txn_time"))
           .withColumn("lp", F.log1p(F.col("amount_paid").cast("double") / F.col("med_paid")))
           .withColumn("lr", F.log1p(F.col("amount_received").cast("double") / F.col("med_recv")))
           .withColumn("src", node("from_bank", "from_account"))
           .withColumn("dst", node("to_bank", "to_account"))
           .select("txn_id", "txn_date", "ts", "lp", "lr", "src", "dst", "is_self_transfer")
           .persist(DISK))
    acc = load_accounts().persist(DISK)
    first_date = t.select(F.min("txn_date")).first()[0]

    # ---------------------------------------------------------------- account-day x_ features
    ev = (t.select(F.col("src").alias("acct"), F.col("dst").alias("cp"), F.lit("out").alias("dir"),
                   "txn_date", "ts", F.col("lp").alias("la"))
           .unionByName(t.select(F.col("dst").alias("acct"), F.col("src").alias("cp"), F.lit("in").alias("dir"),
                                 "txn_date", "ts", F.col("lr").alias("la"))))
    pair = Window.partitionBy("acct", "cp", "dir")
    ev = ev.withColumn("is_new", (F.col("txn_date") == F.min("txn_date").over(pair)).cast("int"))
    is_out, is_in = F.col("dir") == "out", F.col("dir") == "in"
    d = (ev.groupBy("acct", F.col("txn_date").alias("date"))
           .agg(F.sum(is_out.cast("int")).alias("n_out"), F.sum(is_in.cast("int")).alias("n_in"),
                F.max(F.when(is_out, F.col("la"))).alias("max_out"),
                F.max(F.when(is_in, F.col("la"))).alias("max_in"),
                F.sum(F.when(is_out, F.col("la"))).alias("sum_out"),
                F.sum(F.when(is_in, F.col("la"))).alias("sum_in"),
                F.avg(F.when(is_out, F.col("is_new"))).alias("x_new_cp_out"),
                F.avg(F.when(is_in, F.col("is_new"))).alias("x_new_cp_in"))
           .withColumn("di", F.datediff("date", F.lit(first_date))))
    w = Window.partitionBy("acct").orderBy("di")
    prev = w.rangeBetween(-HISTORY_DAYS, -1)
    s = lambda c: F.sum(c).over(prev)
    d = (d.withColumn("p_days", F.count(F.lit(1)).over(prev))
          .withColumn("p_out", s("n_out")).withColumn("p_in", s("n_in"))
          .withColumn("p_sum_out", s("sum_out")).withColumn("p_sum_in", s("sum_in"))
          # every ratio is guarded: an account may have history but no payments OUT (or IN) in it
          .withColumn("x_out_spike", F.when(F.col("p_out") > 0, F.col("n_out") * F.col("p_days") / F.col("p_out"))
                                      .when(F.col("p_days") > 0, F.col("n_out") + 1.0))
          .withColumn("x_in_spike", F.when(F.col("p_in") > 0, F.col("n_in") * F.col("p_days") / F.col("p_in"))
                                     .when(F.col("p_days") > 0, F.col("n_in") + 1.0))
          .withColumn("x_amt_out_vs_own", F.when(F.col("p_out") > 0, F.col("max_out") - F.col("p_sum_out") / F.col("p_out")))
          .withColumn("x_amt_in_vs_own", F.when(F.col("p_in") > 0, F.col("max_in") - F.col("p_sum_in") / F.col("p_in")))
          .withColumn("x_days_since_prev", F.col("di") - F.lag("di").over(w))
          .withColumn("x_active_prev14", F.col("p_days"))
          .withColumn("_a3", F.sum(F.col("n_out") + F.col("n_in")).over(w.rangeBetween(-2, 0)))
          .withColumn("_a11", F.sum(F.col("n_out") + F.col("n_in")).over(w.rangeBetween(-13, -3)))
          .withColumn("x_trend", (F.col("_a3") / 3) / ((F.coalesce("_a11", F.lit(0)) + 1) / 11)))
    # owner columns in ONE join (was two), then: how many of the owner's accounts were active today
    small = F.broadcast if BROADCAST_ACCOUNTS else (lambda df: df)
    o_cols = ["o_n_accounts", "o_n_banks", "o_bank_size", "o_crypto_bank"] + [f"o_type_{k}" for k in OWNER_TYPES]
    d = d.join(small(acc.select("acct", "owner", *o_cols)), "acct", "left")
    own_day = d.filter(F.col("owner").isNotNull()).groupBy("owner", "date").agg(
        F.countDistinct("acct").alias("o_owner_active_accts"))
    d = d.join(own_day, ["owner", "date"], "left")
    fill = {"x_out_spike": -1.0, "x_in_spike": -1.0, "x_amt_out_vs_own": 0.0, "x_amt_in_vs_own": 0.0,
            "x_days_since_prev": 99.0, "x_active_prev14": 0.0, "x_trend": 0.0,
            "x_new_cp_out": -1.0, "x_new_cp_in": -1.0, "o_owner_active_accts": 0.0}
    fill.update({c: 0.0 for c in o_cols})
    extra_cols = list(fill)
    parts = F.split("acct", r"\|")
    extras = d.select(parts.getItem(0).cast("int").alias("acct_bank"), parts.getItem(1).alias("acct"),
                      "date", *[F.coalesce(F.col(c).cast("double"), F.lit(v)).alias(c) for c, v in fill.items()])
    in_chunk = lambda df, c="date": (df.filter((F.col(c) >= F.lit(CHUNK_FROM).cast("date"))
                                               & (F.col(c) <= F.lit(CHUNK_TO).cast("date"))) if CHUNK_FROM else df)
    extras = in_chunk(extras).persist(DISK)
    # the stored wide tables: only the chunk's week folders (+ the exact date filter)
    read_chunk = lambda name: in_chunk(in_weeks(
        spark.read.parquet(f"{CURATED}/{name}").filter(F.col("dataset") == DATASET),
        week_of, CHUNK_FROM, CHUNK_TO))

    for src_t, out_t in [(SRC_FEATURES, OUT_FEATURES), (f"{SRC_FEATURES}_sample", f"{OUT_FEATURES}_sample")]:
        f = read_chunk(src_t)
        f = f.join(extras, ["acct_bank", "acct", "date"], "left").fillna(
            {c: v for c, v in fill.items()})
        # rebalance: AQE splits big weeks across tasks and merges small ones
        rebalance(f, "dataset", "week").write.mode("overwrite").partitionBy("dataset", "week") \
         .parquet(f"{CURATED}/{out_t}")
    chunk = f" {CHUNK_FROM} to {CHUNK_TO}" if CHUNK_FROM else ""
    print(f"[v3] {DATASET}{chunk}: wrote {OUT_FEATURES} (+_sample): {SRC_FEATURES} + {len(extra_cols)} columns")

    # ---------------------------------------------------------------- payment t_ features
    sw = Window.partitionBy("src").orderBy("ts").rangeBetween(-HISTORY_DAYS * DAY, -1)
    rw = Window.partitionBy("dst").orderBy("ts").rangeBetween(-HISTORY_DAYS * DAY, -1)
    a_s = acc.select(F.col("acct").alias("src"), F.col("owner").alias("s_owner"), F.col("bank_country").alias("s_ctry"))
    a_r = acc.select(F.col("acct").alias("dst"), F.col("owner").alias("r_owner"), F.col("bank_country").alias("r_ctry"))
    # Chunk mode: the two 14-day windows need only HISTORY_DAYS of history (not all LOOKBACK_DAYS), and
    # only the chunk's payments are ever looked up (txn_features holds the chunk only): sort less,
    # then cache and join only the chunk's rows. Same values for every chunk payment.
    pt = t
    if CHUNK_FROM:
        pt = pt.filter(F.col("txn_date") >= F.date_sub(F.lit(CHUNK_FROM).cast("date"), HISTORY_DAYS))
    pt = (pt.withColumn("_s_avg", F.avg("lp").over(sw)).withColumn("_s_n", F.count(F.lit(1)).over(sw))
            .withColumn("_r_avg", F.avg("lr").over(rw)))
    p = (in_chunk(pt, "txn_date")
          .join(small(a_s), "src", "left").join(small(a_r), "dst", "left")
          .select("txn_id",
                  ((F.col("s_owner") == F.col("r_owner")) & ~F.col("is_self_transfer")).cast("float").alias("t_same_owner"),
                  (F.col("s_ctry") != F.col("r_ctry")).cast("float").alias("t_cross_country"),
                  F.coalesce(F.col("lp") - F.col("_s_avg"), F.lit(0.0)).cast("float").alias("t_amt_vs_sender"),
                  F.coalesce(F.col("lr") - F.col("_r_avg"), F.lit(0.0)).cast("float").alias("t_amt_vs_receiver"),
                  (F.col("_s_n") == 0).cast("float").alias("t_sender_new"))
          .fillna(0.0).persist(DISK))
    # sender / receiver copies of the account-day extras, keyed like txn_features (s_id, r_id)
    ex_id = extras.select(F.xxhash64(F.col("acct_bank").cast("string"), "acct").alias("_id"), "date",
                          *[F.col(c).cast("float").alias(c) for c in extra_cols])
    sx = ex_id.select(F.col("_id").alias("s_id"), "date", *[F.col(c).alias("s_" + c) for c in extra_cols])
    rx = ex_id.select(F.col("_id").alias("r_id"), "date", *[F.col(c).alias("r_" + c) for c in extra_cols])
    sfill = {("s_" + c): float(v) for c, v in fill.items()}
    sfill.update({("r_" + c): float(v) for c, v in fill.items()})
    for src_t, out_t in [(SRC_TXN, OUT_TXN), (f"{SRC_TXN}_sample", f"{OUT_TXN}_sample")]:
        q = read_chunk(src_t)
        attach = lambda df: (df.join(p, "txn_id", "left").join(sx, ["s_id", "date"], "left")
                               .join(rx, ["r_id", "date"], "left"))
        # The three joins have different keys, so done on q directly each one shuffled the ~140-column
        # payment rows. Do them on a 4-column key table instead, then join the wide rows ONCE by
        # txn_id (one row per payment). `cols` is only analysed, never run: it keeps the old column order.
        cols = attach(q).columns
        add = attach(q.select("txn_id", "s_id", "r_id", "date")).drop("s_id", "r_id", "date")
        q = (q.join(add, "txn_id", "left").select(*cols)
              .fillna({**sfill, "t_same_owner": 0.0, "t_cross_country": 0.0, "t_amt_vs_sender": 0.0,
                       "t_amt_vs_receiver": 0.0, "t_sender_new": 0.0}))
        rebalance(q, "dataset", "week").write.mode("overwrite").partitionBy("dataset", "week") \
         .parquet(f"{CURATED}/{out_t}")
    n_same = p.filter("t_same_owner = 1").count()
    print(f"[v3] {DATASET}{chunk}: wrote {OUT_TXN} (+_sample): {SRC_TXN} + 5 payment + {2 * len(extra_cols)} "
          f"sender/receiver columns; same-owner payments: {n_same}")


if __name__ == "__main__":
    main()
    spark.stop()
