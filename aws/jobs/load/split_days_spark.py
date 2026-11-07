"""
split_days_spark.py                              (one-off setup for the daily simulation)
Cut a dataset's Kaggle transaction file into one CSV per day in staging/, so
local/simulate/replay_day.py can drop one day at a time into raw/ and start aml-daily.

    spark-submit split_days_spark.py HI-Large 2022-09-12 2022-09-25

Reads    raw/transactions/set=<DATASET>/*.csv          the Kaggle *_Trans.csv (top-level files only,
                                                       so replayed day folders are never re-read)
Writes   staging/transactions/set=<DATASET>/date=YYYY-MM-DD/part-*.csv   one file per day, original
                                                       text values and column order, with a header
Only the dates asked for are (re)written; other staged days are kept.
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "pw-aml-g1t4"
ROOT = os.environ.get("AML_ROOT", f"s3://{BUCKET}")
DATASET, DATE_FROM, DATE_TO = sys.argv[1], sys.argv[2], sys.argv[3]

# Kaggle column order (both account columns are called "Account" in the file; .1 marks the receiver)
COLS = ["Timestamp", "From Bank", "Account", "To Bank", "Account.1", "Amount Received", "Receiving Currency",
        "Amount Paid", "Payment Currency", "Payment Format", "Is Laundering"]

spark = SparkSession.builder.appName(f"SplitDays-{DATASET}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

raw = (spark.read.option("header", True)
       .schema(", ".join(f"`{c}` string" for c in COLS))
       .csv(f"{ROOT}/raw/transactions/set={DATASET}/*.csv"))
day = F.regexp_replace(F.substring(F.trim("Timestamp"), 1, 10), "/", "-")        # 2022/09/12 -> 2022-09-12
days = raw.withColumn("date", day).filter((F.col("date") >= DATE_FROM) & (F.col("date") <= DATE_TO))

(days.repartition("date")                          # one file per day
     .write.mode("overwrite").option("header", True)
     .partitionBy("date").csv(f"{ROOT}/staging/transactions/set={DATASET}/"))

counts = days.groupBy("date").count().orderBy("date").collect()
print(f"[split] {DATASET}: {len(counts)} days staged, {sum(r['count'] for r in counts):,} rows")
for r in counts:
    print(f"    {r['date']}  {r['count']:,}")
spark.stop()
