"""
split_weeks_spark.py                                  (one-off setup for the weekly replay)
Splits one Kaggle *_Trans.csv into one CSV folder per Monday-to-Sunday week, so replay_week.py can
play "the bank" and drop one weekly file at a time into raw/ (which starts aml_bq1_weekly).

Run once (EMR Serverless, or local Spark with AML_ROOT=<local folder>):
    spark-submit split_weeks_spark.py HI-Large HI-Large-live [2022-08-22 2022-09-18]

Reads   raw/transactions/set=<SRC>/*.csv                     (never changed)
Writes  staging/transactions/set=<OUT>/week=<YYYY-MM-DD>/     one folder per week (Monday date),
                                                              same 11 columns and text as the original

Notes
    * Every value is read and written as text, so bank IDs keep their leading zeros ("020") and
      clean_transactions_spark.py sees exactly what it would see in the original file.
    * The header has the original column names (the second "Account" becomes "Account.1"; clean
      reads columns by position, so names do not matter).
    * Optional FROM / TO (Mondays, inclusive) keep only those weeks: the demo needs 3 weeks of
      history plus the test weeks, not the whole file.
    * Rows whose timestamp cannot be parsed go to week=unknown, so nothing is silently dropped
      (clean quarantines them as BAD_TIMESTAMP if that folder is ever replayed).
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "is459-g1t4-aml"
ROOT = os.environ["AML_ROOT"]      # s3://<bucket>; no default, so a job never writes to the wrong bucket
SRC = sys.argv[1] if len(sys.argv) > 1 else "HI-Large"
OUT = sys.argv[2] if len(sys.argv) > 2 else f"{SRC}-live"
FROM = sys.argv[3] if len(sys.argv) > 3 else None
TO = sys.argv[4] if len(sys.argv) > 4 else None
FILES_PER_WEEK = int(os.environ.get("AML_FILES_PER_WEEK", 4))   # parallel writes; clean reads them all

COLS = ["Timestamp", "From Bank", "Account", "To Bank", "Account.1", "Amount Received",
        "Receiving Currency", "Amount Paid", "Payment Currency", "Payment Format", "Is Laundering"]

spark = SparkSession.builder.appName(f"SplitWeeks-{SRC}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")   # re-running replaces only its weeks


def main():
    src = f"{ROOT}/raw/transactions/set={SRC}/"
    out = f"{ROOT}/staging/transactions/set={OUT}"
    df = (spark.read.option("header", True).option("recursiveFileLookup", True)
          .option("pathGlobFilter", "*.csv")
          .schema(", ".join(f"`{c}` string" for c in COLS)).csv(src))
    ts = F.to_timestamp(F.trim("Timestamp"), "yyyy/MM/dd HH:mm")
    df = df.withColumn("week", F.coalesce(F.date_format(F.date_trunc("week", ts), "yyyy-MM-dd"),
                                          F.lit("unknown")))
    if FROM:
        df = df.filter((F.col("week") >= FROM) & (F.col("week") <= (TO or "9999-12-31")))

    # deterministic file number (hash, not rand): a retried task writes exactly the same rows
    part = F.pmod(F.xxhash64(*[F.col(f"`{c}`") for c in COLS]), F.lit(FILES_PER_WEEK))
    (df.repartition(FILES_PER_WEEK * 20, "week", part)
       .write.mode("overwrite").partitionBy("week").option("header", True)
       .csv(out))

    counts = (spark.read.option("header", True).csv(out)
              .groupBy("week").count().orderBy("week").collect())
    print(f"[split] {SRC} -> staging/transactions/set={OUT}/ ({FROM or 'start'} to {TO or 'end'})")
    for r in counts:
        print(f"[split]   week={r['week']}: {r['count']} rows")


if __name__ == "__main__":
    main()
    spark.stop()
