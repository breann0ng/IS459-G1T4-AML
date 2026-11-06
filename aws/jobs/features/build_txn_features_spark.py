"""
build_txn_features_spark.py
Stage D of BQ1: one row per PAYMENT (not per account-day), for a transaction-level model.
Published AML results (IBM's GFP + XGBoost, Multi-FraudGT) are per payment, so this lets the
project report a number on the same footing. It is kept ALONGSIDE the account-day model.

Run (EMR master node, after build_features_spark.py for the same dataset):
    spark-submit build_txn_features_spark.py HI-Small                  uses features_v2
    spark-submit build_txn_features_spark.py HI-Small features         uses the old 43 features
    spark-submit build_txn_features_spark.py HI-Large features_v2 2022-08-01 2022-08-14   one chunk

Each payment gets
    its own features   amount (relative to the currency median, and log), hour of day, round
                       amount, cross-currency, self-transfer, same bank, payment format,
                       pair history (payments between the same two accounts in the previous
                       7 days, days since they first paid each other), position of the payment
                       in the sender's / receiver's day
    the sender's account-day features   prefixed s_   (from curated/<features table>)
    the receiver's account-day features prefixed r_
Account-day features describe the whole day, so this matches the account-day model's
"end of day" view (a payment is scored when its day closes).

Writes (Parquet, partitioned by dataset and week)
    curated/txn_features_sample/   every laundering payment + NEG_SAMPLE_RATE of the rest, all
                                   dates (what train_txn.py trains on)
    curated/txn_features/          EVERY payment from FULL_FROM on (what train_txn.py scores:
                                   validation / test days only, to keep the table small)
Columns that are NOT features: dataset, week, txn_id, date, s_id, r_id, label, pattern_label
"""

import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# =============================================================================== SETTINGS
BUCKET = "pw-aml-g1t4"
ROOT = os.environ.get("AML_ROOT", f"s3://{BUCKET}")      # AML_ROOT is only for local testing

DATASET = sys.argv[1] if len(sys.argv) > 1 else "HI-Small"
FEATURES_TABLE = sys.argv[2] if len(sys.argv) > 2 else "features_v2"
CHUNK_FROM = sys.argv[3] if len(sys.argv) > 3 else None    # optional chunk (inclusive dates)
CHUNK_TO = sys.argv[4] if len(sys.argv) > 4 else None

# Full rows are written only from this day on (validation + test). None = every day (HI-Large).
# These match train_ml.py's cross_dataset split (last 25% of days = validation).
FULL_FROM = {"HI-Small": "2022-09-09", "HI-Medium": "2022-09-13", "HI-Large": None}

NEG_SAMPLE_RATE = 0.02     # share of normal payments kept in txn_features_sample
PAIR_DAYS = 7              # pair history window
RANDOM_SEED = 42
SHUFFLE_PARTITIONS = int(os.environ.get("AML_SHUFFLE", 400))   # env override: local tests only
# =========================================================================================

CURATED = f"{ROOT}/curated"
META = ["dataset", "week", "acct_bank", "acct", "date", "label", "first_laundering_ts",
        "pattern_label"]
FORMATS = ["ACH", "Bitcoin", "Cash", "Cheque", "Credit Card", "Reinvestment", "Wire"]

spark = SparkSession.builder.appName(f"BuildTxnFeatures-{DATASET}").getOrCreate()


def rebalance(df, *cols):
    """REBALANCE hint in SQL form: works on Spark 3.2+ (EMR 7, local 3.5) and 4.x (EMR Serverless 8).
    df.hint("rebalance", "col", ...) only works on Spark 4 (3.5 passes the names as strings and fails)."""
    view = f"_rebalance_{id(df)}"
    df.createOrReplaceTempView(view)
    return spark.sql(f"SELECT /*+ REBALANCE({', '.join(cols)}) */ * FROM {view}")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
spark.conf.set("spark.sql.ansi.enabled", "false")


def main():
    all_tx = spark.read.parquet(f"{CURATED}/transactions").filter(F.col("dataset") == DATASET)
    med = all_tx.groupBy("payment_currency").agg(
        F.expr("percentile_approx(amount_paid, 0.5)").cast("double").alias("med_paid"))
    tx = all_tx
    if CHUNK_FROM:     # read PAIR_DAYS of history before the chunk, write only the chunk
        tx = tx.filter((F.col("txn_date") >= F.date_sub(F.lit(CHUNK_FROM).cast("date"), PAIR_DAYS))
                       & (F.col("txn_date") <= F.lit(CHUNK_TO).cast("date")))

    labels = spark.read.parquet(f"{CURATED}/labels").filter(F.col("dataset") == DATASET)
    if CHUNK_FROM:     # only the weeks this chunk touches (partition pruning: much less to read)
        weeks = [r["week"] for r in tx.select("week").distinct().collect()]
        labels = labels.filter(F.col("week").isin(weeks))
    labels = labels \
                  .select("txn_id", F.col("is_laundering").alias("label"))
    first_scheme = Window.partitionBy("txn_id").orderBy("txn_time", "scheme_id")
    schemes = (spark.read.parquet(f"{CURATED}/scheme_txns").filter(F.col("dataset") == DATASET)
               .filter("found_in_transactions")
               .withColumn("_n", F.row_number().over(first_scheme)).filter("_n = 1")
               .select("txn_id", F.col("pattern_type").alias("scheme_pattern")))

    ts = F.col("txn_time").cast("long")
    pair = Window.partitionBy("from_bank", "from_account", "to_bank", "to_account")
    s_day = Window.partitionBy("from_bank", "from_account", "txn_date").orderBy("txn_time", "txn_id")
    r_day = Window.partitionBy("to_bank", "to_account", "txn_date").orderBy("txn_time", "txn_id")
    amt = F.col("amount_paid").cast("double")
    t = (tx.join(F.broadcast(med), "payment_currency", "left")
           .withColumn("t_amt_rel", amt / F.col("med_paid"))
           .withColumn("t_log_amt", F.log1p(amt))
           .withColumn("t_hour", F.hour("txn_time").cast("double"))
           .withColumn("t_round", ((amt % 100) == 0).cast("double"))
           .withColumn("t_cross_ccy", F.col("is_cross_currency").cast("double"))
           .withColumn("t_self", F.col("is_self_transfer").cast("double"))
           .withColumn("t_same_bank", (F.col("from_bank") == F.col("to_bank")).cast("double"))
           .withColumn("t_pair_n_7d", F.count(F.lit(1)).over(
               pair.orderBy(ts).rangeBetween(-PAIR_DAYS * 86400, -1)).cast("double"))
           .withColumn("t_pair_age_d", ((ts - F.min(ts).over(pair)) / 86400.0))
           .withColumn("t_s_seq", F.row_number().over(s_day).cast("double"))
           .withColumn("t_r_seq", F.row_number().over(r_day).cast("double")))
    for f in FORMATS:
        t = t.withColumn("t_fmt_" + f.lower().replace(" ", "_"),
                         (F.col("payment_format") == f).cast("double"))
    if CHUNK_FROM:
        t = t.filter(F.col("txn_date") >= F.lit(CHUNK_FROM).cast("date"))

    feats = spark.read.parquet(f"{CURATED}/{FEATURES_TABLE}").filter(F.col("dataset") == DATASET)
    if CHUNK_FROM:
        feats = feats.filter((F.col("date") >= F.lit(CHUNK_FROM).cast("date"))
                             & (F.col("date") <= F.lit(CHUNK_TO).cast("date")))
    fcols = [c for c in feats.columns if c not in META]
    side = lambda p, b, a: feats.select(
        F.col("acct_bank").alias(b), F.col("acct").alias(a), F.col("date").alias("txn_date"),
        *[F.col(c).cast("float").alias(p + c) for c in fcols])

    t = (t.join(side("s_", "from_bank", "from_account"), ["from_bank", "from_account", "txn_date"], "left")
          .join(side("r_", "to_bank", "to_account"), ["to_bank", "to_account", "txn_date"], "left")
          .join(labels, "txn_id", "left")
          .join(schemes, "txn_id", "left")
          .withColumn("label", F.coalesce("label", F.lit(0)))
          .withColumn("pattern_label", F.when(F.col("label") == 0, "none")
                                        .otherwise(F.coalesce("scheme_pattern", F.lit("unclassified"))))
          .withColumn("s_id", F.xxhash64(F.col("from_bank").cast("string"), "from_account"))
          .withColumn("r_id", F.xxhash64(F.col("to_bank").cast("string"), "to_account"))
          .withColumnRenamed("txn_date", "date"))
    tcols = [c for c in t.columns if c.startswith("t_")]
    out = t.select("dataset", "week", "txn_id", "date", "s_id", "r_id", "label", "pattern_label",
                   *[F.col(c).cast("float").alias(c) for c in tcols],
                   *[F.col(c) for c in t.columns if c.startswith(("s_", "r_")) and c not in ("s_id", "r_id")])
    out = out.fillna(0.0).persist()

    sample = out.filter((F.col("label") == 1) | (F.rand(RANDOM_SEED) < NEG_SAMPLE_RATE))
    # rebalance, not repartition: by week alone one task wrote each week while the other cores
    # idled; rebalance lets AQE split big weeks across tasks and merge small ones
    rebalance(sample, "dataset", "week").write.mode("overwrite") \
          .partitionBy("dataset", "week").parquet(f"{CURATED}/txn_features_sample")
    full_from = FULL_FROM.get(DATASET)
    full = out.filter(F.col("date") >= F.lit(full_from).cast("date")) if full_from else out
    rebalance(full, "dataset", "week").write.mode("overwrite") \
        .partitionBy("dataset", "week").parquet(f"{CURATED}/txn_features")

    n = out.count()
    n_pos = out.filter("label = 1").count()
    print(f"[txn] {DATASET}: payments={n} laundering={n_pos} features={len(tcols) + 2 * len(fcols)} "
          f"(own {len(tcols)} + sender/receiver {len(fcols)} each); full rows from {full_from or 'start'}")


if __name__ == "__main__":
    main()
    spark.stop()
