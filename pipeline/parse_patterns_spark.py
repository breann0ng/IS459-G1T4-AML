"""
parse_patterns_spark.py
Step 2 of BQ1: *_Patterns.txt (the answer key)  ->  scheme tables.

Run (on the EMR master node, after clean_transactions_spark.py):
    spark-submit parse_patterns_spark.py HI-Small

Reads
    raw/patterns/set=<DATASET>/*Patterns*.txt
    curated/transactions/        to link each scheme payment to its txn_id
Writes (Parquet, partitioned by dataset; re-running a dataset replaces it)
    curated/schemes/        one row per laundering scheme: type, size, start, end, duration
    curated/scheme_txns/    one row per scheme payment: scheme_id, pattern_type, txn_id
    curated/patterns_dq/    how many scheme payments were found in the clean data

The file is blocks like:
    BEGIN LAUNDERING ATTEMPT - FAN-OUT:  Max 16-degree Fan-Out
    2022/09/01 00:06,021174,800737690,012,80011F990,2848.96,Euro,2848.96,Euro,ACH,1
    END LAUNDERING ATTEMPT - FAN-OUT
Payment lines have the same 11 columns as *_Trans.csv, so txn_id uses the same recipe as
clean_transactions_spark.py. Used ONLY as labels, never as model input.

pattern_type: fan_in, fan_out, gather_scatter, scatter_gather, cycle, bipartite, stack, random
"""

import os
import re
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# =============================================================================== SETTINGS
BUCKET = "pw-aml-g1t4"
ROOT = os.environ.get("AML_ROOT", f"s3://{BUCKET}")      # AML_ROOT is only for local testing

DATASET = sys.argv[1] if len(sys.argv) > 1 else "HI-Small"
# =========================================================================================

PATTERNS_PATH = f"{ROOT}/raw/patterns/set={DATASET}/*Patterns*.txt"
OUT = f"{ROOT}/curated"
AMOUNT_TYPE = "decimal(28,8)"   # 8 decimals: Bitcoin amounts are tiny (e.g. 0.00843)
RUN_AT = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

BEGIN = re.compile(r"^BEGIN LAUNDERING ATTEMPT\s*-\s*([A-Z\-]+)\s*:?\s*(.*)$")
END = re.compile(r"^END LAUNDERING ATTEMPT")
RAW_COLS = ["ts_raw", "from_bank_raw", "from_account", "to_bank_raw", "to_account",
            "amount_received_raw", "receiving_currency", "amount_paid_raw",
            "payment_currency", "payment_format", "is_laundering_raw"]

spark = SparkSession.builder.appName(f"ParsePatterns-{DATASET}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")      # keep logs small: warnings and errors only
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


def parse_file(path_and_text):
    """Walk the file top to bottom; yield one row per payment line inside a scheme.
    Schemes are numbered in file order, so scheme_id is stable across re-runs."""
    _, text = path_and_text
    seq, ptype, detail = 0, None, None
    for line_no, line in enumerate(text.splitlines(), start=1):
        line = line.strip()
        begin = BEGIN.match(line)
        if begin:
            seq += 1
            ptype = begin.group(1).lower().replace("-", "_")     # FAN-OUT -> fan_out
            detail = begin.group(2).strip() or None              # "Max 16-degree Fan-Out"
        elif END.match(line):
            ptype = None
        elif ptype and line:
            parts = [p.strip() for p in line.split(",")]
            if len(parts) == 11:
                yield (seq, ptype, detail, line_no, *parts)


def txn_id_col():
    """Stable transaction ID. Must match clean_transactions_spark.py exactly."""
    return F.sha2(F.concat_ws(
        "|",
        F.date_format("txn_time", "yyyy-MM-dd HH:mm"),
        F.col("from_bank").cast("string"), F.col("from_account"),
        F.col("to_bank").cast("string"), F.col("to_account"),
        F.col("amount_paid").cast("string"), F.col("payment_currency"),
        F.col("amount_received").cast("string"), F.col("receiving_currency"),
        F.col("payment_format")), 256)


def main():
    schema = ("scheme_seq int, pattern_type string, pattern_detail string, line_no int, "
              + ", ".join(f"{c} string" for c in RAW_COLS))
    rows = spark.sparkContext.wholeTextFiles(PATTERNS_PATH).flatMap(parse_file)
    p = (spark.createDataFrame(rows, schema)
         .withColumn("dataset", F.lit(DATASET))
         .withColumn("scheme_id", F.format_string("%s-%06d", F.lit(DATASET), F.col("scheme_seq")))
         .withColumn("txn_time", F.to_timestamp("ts_raw", "yyyy/MM/dd HH:mm"))
         .withColumn("from_bank", F.col("from_bank_raw").cast("int"))
         .withColumn("to_bank", F.col("to_bank_raw").cast("int"))
         .withColumn("from_account", F.upper("from_account"))
         .withColumn("to_account", F.upper("to_account"))
         .withColumn("amount_received", F.col("amount_received_raw").cast(AMOUNT_TYPE))
         .withColumn("amount_paid", F.col("amount_paid_raw").cast(AMOUNT_TYPE)))
    p = p.withColumn("txn_id", txn_id_col()).cache()
    n_lines = p.count()
    if n_lines == 0:
        raise SystemExit(f"No scheme payments found at {PATTERNS_PATH}")

    # ---- link to clean transactions. Cleaning may suffix an id with -2/-3, so match on the
    # part before "-". The scheme ids are small, so they are broadcast (no big shuffle).
    txns = (spark.read.parquet(f"{OUT}/transactions")
                 .filter(F.col("dataset") == DATASET)
                 .select(F.split("txn_id", "-").getItem(0).alias("txn_id"),
                         F.col("txn_id").alias("clean_txn_id"), "week"))
    matched = txns.join(F.broadcast(p.select("txn_id").distinct()), "txn_id")
    first = Window.partitionBy("txn_id").orderBy("clean_txn_id")
    matched = matched.withColumn("_n", F.row_number().over(first)).filter("_n = 1").drop("_n")
    linked = (p.join(matched, "txn_id", "left")
               .withColumn("found_in_transactions", F.col("clean_txn_id").isNotNull()))

    # A payment can belong to several schemes (accounts are reused): keep every pair.
    scheme_txns = linked.select(
        "dataset", "scheme_id", "pattern_type",
        F.coalesce("clean_txn_id", "txn_id").alias("txn_id"),
        "txn_time", "from_bank", "from_account", "to_bank", "to_account",
        "amount_paid", "payment_currency", "amount_received", "receiving_currency",
        "payment_format", "week", "found_in_transactions", "line_no")

    acct = lambda b, a: F.concat_ws("|", F.col(b).cast("string"), F.col(a))
    accounts = (p.select("scheme_id", acct("from_bank", "from_account").alias("a"))
                 .unionByName(p.select("scheme_id", acct("to_bank", "to_account").alias("a")))
                 .groupBy("scheme_id").agg(F.countDistinct("a").alias("n_accounts")))
    schemes = (linked.groupBy("dataset", "scheme_id", "pattern_type", "pattern_detail")
        .agg(F.count(F.lit(1)).alias("n_txns"),
             F.sum(F.col("found_in_transactions").cast("int")).alias("n_txns_found"),
             F.min("txn_time").alias("start_time"),
             F.max("txn_time").alias("end_time"),
             F.countDistinct("payment_currency").alias("n_currencies"),
             F.min("week").alias("first_week"))
        .join(accounts, "scheme_id", "left")
        .withColumn("duration_h",
                    (F.unix_timestamp("end_time") - F.unix_timestamp("start_time")) / 3600.0))

    n_found = linked.filter("found_in_transactions").count()
    by_type = {r["pattern_type"]: r["count"] for r in schemes.groupBy("pattern_type").count().collect()}
    patterns_dq = spark.createDataFrame(
        [(DATASET, schemes.count(), n_lines, n_found, n_found / n_lines,
          str(dict(sorted(by_type.items()))), RUN_AT)],
        "dataset string, n_schemes long, n_scheme_txns long, n_found long, "
        "match_rate double, schemes_by_type string, run_at string")

    schemes.coalesce(1).write.mode("overwrite").partitionBy("dataset").parquet(f"{OUT}/schemes")
    (scheme_txns.coalesce(1).sortWithinPartitions("scheme_id", "line_no")
                .write.mode("overwrite").partitionBy("dataset").parquet(f"{OUT}/scheme_txns"))
    patterns_dq.write.mode("overwrite").partitionBy("dataset").parquet(f"{OUT}/patterns_dq")

    print(f"[patterns] {DATASET}: schemes={sum(by_type.values())} scheme_txns={n_lines} "
          f"found_in_transactions={n_found} ({n_found / n_lines:.2%})")
    print(f"[patterns] schemes by type: {dict(sorted(by_type.items()))}")
    if n_found / n_lines < 0.99:
        print("[patterns] WARNING: under 99% matched. Check that the clean job ran on this "
              "dataset, and look at curated/quarantine.")


if __name__ == "__main__":
    main()
    spark.stop()
