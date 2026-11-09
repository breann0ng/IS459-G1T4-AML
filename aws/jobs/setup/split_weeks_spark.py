"""
split_weeks_spark.py                                  (one-off setup for the weekly replay)
Splits one whole transaction file (IBM layout) into one CSV folder per Monday-to-Sunday week, so
replay_week.py can play "the bank" and drop one weekly file at a time into raw/ (which starts
aml_prepare). Every dataset except the training sets HI-Small / HI-Medium is fed this way.

Run once per replay dataset (EMR Serverless, or local Spark with AML_ROOT=<local folder>):
    spark-submit split_weeks_spark.py HI-Large HI-Large-live [2022-08-22 2022-09-18]
    spark-submit split_weeks_spark.py SAML-R SAML-R-demo        (after saml_ramp_spark.py converted SAML-D)

Reads   raw/transactions/set=<SRC>/*.csv                     (never changed)
        raw/patterns/set=<SRC>/, raw/accounts/set=<SRC>/     answer key and accounts file
Writes  staging/transactions/set=<OUT>/week=<YYYY-MM-DD>/     one folder per week (Monday date),
                                                              same 11 columns and text as the original
        raw/patterns/set=<OUT>/, raw/accounts/set=<OUT>/     copies, so ParsePatterns and FeaturesV3
                                                              find them under the replay's own name

Notes
    * Every value is read and written as text, so bank IDs keep their leading zeros ("020") and
      clean_transactions_spark.py sees exactly what it would see in the original file.
    * The header has the original column names (the second "Account" becomes "Account.1"; clean
      reads columns by position, so names do not matter).
    * Optional FROM / TO (Mondays, inclusive) keep only those weeks: the demo needs 3 weeks of
      history plus the test weeks, not the whole file.
    * Rows whose timestamp cannot be parsed go to week=unknown, so nothing is silently dropped
      (clean quarantines them as BAD_TIMESTAMP if that folder is ever replayed).
    * OUT must differ from SRC: replayed weeks land in raw/transactions/set=<OUT>/, and this job reads
      raw/transactions/set=<SRC>/ recursively, so the same name would read replayed weeks back in.
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

if OUT == SRC:
    raise SystemExit(f"OUT must differ from SRC ({SRC}): e.g. {SRC}-live or {SRC}-demo")

spark = SparkSession.builder.appName(f"SplitWeeks-{SRC}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")   # re-running replaces only its weeks


def copy_side_files():
    """Answer key and accounts file under the replay's name (replaces any older copy)."""
    jvm, conf = spark._jvm, spark._jsc.hadoopConfiguration()
    for kind in ("patterns", "accounts"):
        src = jvm.org.apache.hadoop.fs.Path(f"{ROOT}/raw/{kind}/set={SRC}")
        dst = jvm.org.apache.hadoop.fs.Path(f"{ROOT}/raw/{kind}/set={OUT}")
        fs = src.getFileSystem(conf)
        if not fs.exists(src):
            print(f"[split] no raw/{kind}/set={SRC}/: not copied")
            continue
        if fs.exists(dst):
            fs.delete(dst, True)
        jvm.org.apache.hadoop.fs.FileUtil.copy(fs, src, fs, dst, False, conf)
        print(f"[split] copied raw/{kind}/set={SRC}/ -> raw/{kind}/set={OUT}/")


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
    copy_side_files()
    spark.stop()
