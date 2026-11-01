"""
clean_transactions_spark.py
Step 1 of BQ1: raw transaction CSV  ->  clean, typed Parquet.

Run (on the EMR master node):
    spark-submit clean_transactions_spark.py HI-Small

Reads
    raw/transactions/set=<DATASET>/*.csv          the Kaggle *_Trans.csv file
Writes (Parquet, partitioned by dataset and week; re-running a dataset replaces it)
    curated/transactions/   clean rows the features are built from (no label)
    curated/labels/         txn_id + is_laundering, used only for training and grading
    curated/quarantine/     rejected rows + the reason
    curated/dq_report/      row counts per week (raw, quarantined, duplicates, clean)
    curated/daily_profile/  transactions and laundering per day (to spot the data "tail")

Notes
    * Every column is read as text first, so account IDs can never be turned into numbers.
    * Bank IDs are cast to integers ("010" -> 10). Bank and account stay separate columns.
    * Amounts keep 8 decimal places, so small Bitcoin payments are not rounded to 0.
    * Weeks are Monday-to-Sunday; week 1 is the week of the earliest transaction.
    * txn_id = SHA-256 of the cleaned values. parse_patterns_spark.py uses the same recipe.
"""

import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# =============================================================================== SETTINGS
BUCKET = "pw-aml-g1t4"
ROOT = os.environ.get("AML_ROOT", f"s3://{BUCKET}")      # AML_ROOT is only for local testing

DATASET = sys.argv[1] if len(sys.argv) > 1 else "HI-Small"

# Drop the end of a dataset where normal activity collapses (see curated/daily_profile).
# Format "YYYY-MM-DD HH:MM"; None keeps everything. Set it per dataset after the first run.
MAX_TIMESTAMP = {
    "HI-Small": "2022-09-10 23:59",     # after 10 Sep: ~1% of normal volume, ~60% laundering
    "HI-Medium": "2022-09-16 23:59",    # after 16 Sep: tail, ~60% laundering
    "HI-Large": "2022-11-05 23:59",     # after 5 Nov: tail, ~60% laundering
}

ALLOWED_FORMATS = ["ACH", "Bitcoin", "Cash", "Cheque", "Credit Card", "Reinvestment", "Wire"]
# =========================================================================================

RAW_PATH = f"{ROOT}/raw/transactions/set={DATASET}/"
OUT = f"{ROOT}/curated"
AMOUNT_TYPE = "decimal(28,8)"   # 8 decimals: Bitcoin amounts are tiny (e.g. 0.00843)
RUN_AT = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# The Kaggle file has two columns both called "Account", so columns are named by position.
RAW_COLS = ["ts_raw", "from_bank_raw", "from_account", "to_bank_raw", "to_account",
            "amount_received_raw", "receiving_currency", "amount_paid_raw",
            "payment_currency", "payment_format", "is_laundering_raw"]

spark = SparkSession.builder.appName(f"CleanTransactions-{DATASET}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")      # keep logs small: warnings and errors only
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


def txn_id_col():
    """Stable transaction ID. Must match parse_patterns_spark.py exactly."""
    return F.sha2(F.concat_ws(
        "|",
        F.date_format("txn_time", "yyyy-MM-dd HH:mm"),
        F.col("from_bank").cast("string"), F.col("from_account"),
        F.col("to_bank").cast("string"), F.col("to_account"),
        F.col("amount_paid").cast("string"), F.col("payment_currency"),
        F.col("amount_received").cast("string"), F.col("receiving_currency"),
        F.col("payment_format")), 256)


def read_raw():
    schema = ", ".join(f"{c} string" for c in RAW_COLS)
    df = (spark.read
          .option("header", True)
          .option("recursiveFileLookup", True)
          .option("pathGlobFilter", "*.csv")
          .schema(schema)
          .csv(RAW_PATH))
    for c in RAW_COLS:
        df = df.withColumn(c, F.trim(c))
    return df


def parse(df):
    return (df
        .withColumn("txn_time", F.to_timestamp("ts_raw", "yyyy/MM/dd HH:mm"))
        .withColumn("txn_date", F.to_date("txn_time"))
        .withColumn("from_bank", F.col("from_bank_raw").cast("int"))
        .withColumn("to_bank", F.col("to_bank_raw").cast("int"))
        .withColumn("from_account", F.upper("from_account"))
        .withColumn("to_account", F.upper("to_account"))
        .withColumn("amount_received", F.col("amount_received_raw").cast(AMOUNT_TYPE))
        .withColumn("amount_paid", F.col("amount_paid_raw").cast(AMOUNT_TYPE))
        .withColumn("is_laundering", F.col("is_laundering_raw").cast("int")))


def add_week(df):
    week_start = F.to_date(F.date_trunc("week", "txn_time"))
    first_week = df.select(F.min(week_start)).first()[0]
    week = F.floor(F.datediff(week_start, F.lit(first_week)) / 7) + 1
    return df.withColumn("week", F.coalesce(week.cast("int"), F.lit(0)))


def reject_reason():
    """First failing rule wins. None = the row is clean."""
    blank = lambda c: F.col(c).isNull() | (F.col(c) == "")
    excel_damage = r"E\+|\."                                # e.g. 8.14E+08
    reason = (
        F.when(F.col("txn_time").isNull(), "BAD_TIMESTAMP")
         .when(F.col("from_bank").isNull() | F.col("to_bank").isNull(), "BAD_BANK_ID")
         .when(blank("from_account") | blank("to_account"), "BAD_ACCOUNT")
         .when(F.col("from_account").rlike(excel_damage) | F.col("to_account").rlike(excel_damage),
               "CORRUPT_ACCOUNT_ID")
         .when(~(F.col("amount_paid") > 0) | ~(F.col("amount_received") > 0)
               | F.col("amount_paid").isNull() | F.col("amount_received").isNull(), "BAD_AMOUNT")
         .when(blank("payment_currency") | blank("receiving_currency"), "MISSING_CURRENCY")
         .when(~F.col("payment_format").isin(ALLOWED_FORMATS), "UNKNOWN_FORMAT")
         .when(~F.coalesce(F.col("is_laundering").isin(0, 1), F.lit(False)), "BAD_LABEL"))
    cutoff = MAX_TIMESTAMP.get(DATASET)
    if cutoff:
        after = F.col("txn_time") > F.to_timestamp(F.lit(cutoff), "yyyy-MM-dd HH:mm")
        reason = F.when(F.col("txn_time").isNotNull() & after, "AFTER_CUTOFF").otherwise(reason)
    return reason


def write(df, name, partitions=("dataset", "week")):
    df = df.repartition(*partitions)
    if "txn_time" in df.columns:
        df = df.sortWithinPartitions("txn_time")
    df.write.mode("overwrite").partitionBy(*partitions).parquet(f"{OUT}/{name}")


def main():
    df = (add_week(parse(read_raw()))
          .withColumn("dataset", F.lit(DATASET))
          .withColumn("reject_reason", reject_reason())
          .cache())

    # ---- quarantine: rejected rows keep their original text values
    quarantine = (df.filter(F.col("reject_reason").isNotNull())
                    .select(*RAW_COLS, "reject_reason", "dataset", "week",
                            F.lit(RUN_AT).alias("loaded_at")))

    # ---- clean rows: drop exact duplicates, add txn_id and flags
    key_cols = ["txn_time", "from_bank", "from_account", "to_bank", "to_account",
                "amount_received", "receiving_currency", "amount_paid",
                "payment_currency", "payment_format", "is_laundering"]
    good = df.filter(F.col("reject_reason").isNull())
    clean = good.dropDuplicates(key_cols).withColumn("txn_id", txn_id_col())

    # Two rows identical except for the label would share an id: suffix -2, -3 ...
    same_id = Window.partitionBy("txn_id").orderBy(F.col("is_laundering").desc())
    clean = (clean.withColumn("_n", F.row_number().over(same_id))
                  .withColumn("txn_id", F.when(F.col("_n") == 1, F.col("txn_id"))
                                         .otherwise(F.concat_ws("-", "txn_id", F.col("_n").cast("string"))))
                  .drop("_n")
                  .withColumn("is_self_transfer", (F.col("from_bank") == F.col("to_bank"))
                                                  & (F.col("from_account") == F.col("to_account")))
                  .withColumn("is_cross_currency", F.col("payment_currency") != F.col("receiving_currency"))
                  .cache())

    transactions = clean.select(
        "txn_id", "txn_time", "txn_date",
        "from_bank", "from_account", "to_bank", "to_account",
        "amount_received", "receiving_currency", "amount_paid", "payment_currency",
        "payment_format", "is_self_transfer", "is_cross_currency", "dataset", "week")
    labels = clean.select("txn_id", "is_laundering", "dataset", "week")

    # ---- data-quality report per week
    raw_wk = df.groupBy("dataset", "week").agg(
        F.count(F.lit(1)).alias("rows_raw"),
        F.sum(F.col("reject_reason").isNotNull().cast("int")).alias("rows_quarantined"))
    reasons =(df.filter(F.col("reject_reason").isNotNull())
                 .groupBy("dataset", "week", "reject_reason").count()
                 .groupBy("dataset", "week")
                 .agg(F.to_json(F.map_from_entries(F.collect_list(F.struct("reject_reason", "count"))))
                       .alias("quarantine_by_reason")))
    good_wk = good.groupBy("dataset", "week").agg(F.count(F.lit(1)).alias("rows_valid"))
    clean_wk = clean.groupBy("dataset", "week").agg(
        F.count(F.lit(1)).alias("rows_clean"),
        F.sum(F.col("is_self_transfer").cast("int")).alias("self_transfers"),
        F.sum(F.col("is_cross_currency").cast("int")).alias("cross_currency"),
        F.sum("is_laundering").alias("laundering_rows"),
        F.min("txn_time").cast("string").alias("min_time"),
        F.max("txn_time").cast("string").alias("max_time"))
    dq_report = (raw_wk.join(reasons, ["dataset", "week"], "left")
                       .join(good_wk, ["dataset", "week"], "left")
                       .join(clean_wk, ["dataset", "week"], "left")
                       .fillna(0, subset=["rows_valid", "rows_clean", "self_transfers",
                                          "cross_currency", "laundering_rows"])
                       .withColumn("duplicates_removed", F.col("rows_valid") - F.col("rows_clean"))
                       .drop("rows_valid")
                       .withColumn("run_at", F.lit(RUN_AT)))

    # ---- daily profile: find where normal activity stops (the "tail")
    daily_profile = (clean.groupBy("dataset", "week", "txn_date")
                          .agg(F.count(F.lit(1)).alias("txns"),
                               F.sum("is_laundering").alias("laundering_txns"))
                          .withColumn("laundering_rate", F.col("laundering_txns") / F.col("txns")))

    write(transactions, "transactions")
    write(labels, "labels")
    write(quarantine, "quarantine")
    write(dq_report, "dq_report")
    write(daily_profile, "daily_profile")

    n_raw, n_q = df.count(), quarantine.count()
    n_clean = clean.count()
    print(f"[clean] {DATASET}: raw={n_raw} quarantined={n_q} "
          f"duplicates_removed={n_raw - n_q - n_clean} clean={n_clean}")
    print("[clean] daily profile (look for the day normal volume collapses):")
    daily_profile.orderBy("txn_date").show(60, truncate=False)


if __name__ == "__main__":
    main()
    spark.stop()
