"""
bq2_score_spark.py
BQ2 step 1: score every payment of a date window with SEVERAL frozen stage 1 models in one pass
(normally: "all" = the BQ1 final model, "nofan" = the same model trained blind to fan-in/fan-out),
and keep what BQ2 needs per week. Safe to run in chunks: each run replaces only its own weeks.

Run (EMR, after the HI-Large features exist for the window; start on a Monday, end on a Sunday):
    spark-submit [S3 confs] [driver confs] --conf spark.pyspark.python=/usr/bin/python3 \
        bq2_score_spark.py HI-Large 2022-08-22 2022-09-04
Models: env BQ2_MODELS, default "all=models/final,nofan=models/final_nofan"
        (each folder holds stage1.txt + meta.json with its own frozen threshold)

Reads   models/<each>/, curated/txn_features_v3, curated/features_v3
Writes  (Parquet, partitioned by dataset and week; only the weeks of this window are replaced)
  analytics/bq2_acct_days/        account-days needed for grading: every laundering account-day,
                                  every alert of any model, and each model's top KEEP_RANK per day.
                                  Columns: id, acct_bank, acct, date, label, pattern_label,
                                  score_<model>, rank_<model>
  analytics/bq2_day_totals/       per day: account-days, payments, and per model alerts / true alerts
  analytics/bq2_score_hist/       per week, model and pattern_label: account-days per score bin
                                  (score drift without labels; pattern_label lets BQ2 remove the
                                  "new tactic" before it is introduced)
  analytics/bq2_flagged_payments/ payments flagged by any model (scheme timing)
"""

import json
import os
import sys

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

BUCKET = "pw-aml-g1t4"
ROOT = os.environ.get("AML_ROOT", f"s3://{BUCKET}")
DATASET = sys.argv[1] if len(sys.argv) > 1 else "HI-Large"
DATE_FROM = sys.argv[2] if len(sys.argv) > 2 else None
DATE_TO = sys.argv[3] if len(sys.argv) > 3 else None
MODELS = dict(m.split("=", 1) for m in
              os.environ.get("BQ2_MODELS", "all=models/final,nofan=models/final_nofan").split(","))
KEEP_RANK = 5000                    # each model's top K account-days per day are kept (budget views)
DISK = StorageLevel.MEMORY_AND_DISK
CURATED, ANALYTICS = f"{ROOT}/curated", f"{ROOT}/analytics"

spark = SparkSession.builder.appName(f"BQ2Score-{DATASET}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", os.environ.get("AML_SHUFFLE", "400"))
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "20000")


def text(path):
    return spark.sparkContext.wholeTextFiles(path).collect()[0][1]


def in_dates(df, col="date"):
    if not DATE_FROM:
        return df
    c = F.to_date(F.col(col))
    return df.filter((c >= F.lit(DATE_FROM).cast("date")) & (c <= F.lit(DATE_TO).cast("date")))


def write(df, name):
    (df.withColumn("dataset", F.lit(DATASET)).repartition("dataset", "week").write.mode("overwrite")
       .partitionBy("dataset", "week").parquet(f"{ANALYTICS}/{name}"))


def score_bin(c):
    """Log-scale bin: 4 bins per factor of 10, from 1e-7 (bin -28) up to 1 (bin 0)."""
    return F.floor(F.log10(F.greatest(F.col(c), F.lit(1e-7))) * 4).cast("int")


def main():
    metas = {m: json.loads(text(f"{ROOT}/{d.strip('/')}/meta.json")) for m, d in MODELS.items()}
    boosters = {m: text(f"{ROOT}/{d.strip('/')}/stage1.txt") for m, d in MODELS.items()}
    thr = {m: float(metas[m]["threshold"]) for m in MODELS}
    feats = metas[next(iter(MODELS))]["stage1_features"]
    for m in MODELS:
        if metas[m]["stage1_features"] != feats:
            raise SystemExit(f"model {m} uses different features: all models must share one feature table")
    bc = spark.sparkContext.broadcast(boosters)
    names = list(MODELS)
    print(f"[bq2-score] {DATASET} {DATE_FROM or 'all'} to {DATE_TO or 'all'}; models "
          + ", ".join(f"{m} ({MODELS[m]}, threshold {thr[m]:.4f}, excluded {metas[m].get('excluded_patterns', [])})"
                      for m in names))

    def score_part(it):
        import lightgbm as lgb
        import numpy as np
        bs = {m: lgb.Booster(model_str=s) for m, s in bc.value.items()}
        for pdf in it:
            x = pdf[feats].to_numpy(np.float32)
            out = pdf[["txn_id", "s_id", "r_id", "date", "week", "label", "pattern_label"]].copy()
            for m in names:
                out[f"score_{m}"] = bs[m].predict(x).astype("float32")
            yield out

    txn = (in_dates(spark.read.parquet(f"{CURATED}/{metas[names[0]]['txn_table']}").filter(F.col("dataset") == DATASET))
           .select("txn_id", "s_id", "r_id", "date", "week", "label", "pattern_label", *feats))
    schema = ("txn_id string, s_id long, r_id long, date date, week int, label int, pattern_label string, "
              + ", ".join(f"score_{m} float" for m in names))
    sc = txn.mapInPandas(score_part, schema).persist(DISK)
    n_pay = sc.count()
    print(f"[bq2-score] scored {n_pay} payments")

    # ---- flagged payments (any model) for scheme timing
    any_flag = None
    for m in names:
        c = F.col(f"score_{m}") >= thr[m]
        any_flag = c if any_flag is None else (any_flag | c)
    write(sc.filter(any_flag), "bq2_flagged_payments")

    # ---- account-day: score = its riskiest payment (sent or received), as in BQ1
    ad = (sc.select(F.col("s_id").alias("id"), "date", *[f"score_{m}" for m in names])
            .unionByName(sc.select(F.col("r_id").alias("id"), "date", *[f"score_{m}" for m in names]))
            .groupBy("id", "date").agg(*[F.max(f"score_{m}").alias(f"score_{m}") for m in names]))
    acct = (in_dates(spark.read.parquet(f"{CURATED}/{metas[names[0]]['acct_table']}").filter(F.col("dataset") == DATASET))
            .withColumn("id", F.xxhash64(F.col("acct_bank").cast("string"), "acct"))
            .select("id", "date", "week", "acct_bank", "acct", "label", "pattern_label"))
    adj = ad.join(acct, ["id", "date"], "inner")
    for m in names:
        w = Window.partitionBy("date").orderBy(F.col(f"score_{m}").desc(), F.col("id"))
        adj = adj.withColumn(f"rank_{m}", F.row_number().over(w))
    adj = adj.persist(DISK)
    keep = F.col("label") == 1
    for m in names:
        keep = keep | (F.col(f"rank_{m}") <= KEEP_RANK) | (F.col(f"score_{m}") >= thr[m])
    write(adj.filter(keep), "bq2_acct_days")

    # ---- per day totals (alerts at each model's frozen threshold)
    aggs = [F.count(F.lit(1)).alias("account_days"), F.sum("label").alias("laundering_account_days")]
    for m in names:
        f = (F.col(f"score_{m}") >= thr[m]).cast("int")
        aggs += [F.sum(f).alias(f"alerts_{m}"), F.sum(f * F.col("label")).alias(f"true_alerts_{m}")]
    days = adj.groupBy("date", "week").agg(*aggs)
    pays = sc.groupBy("date").agg(F.count(F.lit(1)).alias("payments"))
    write(days.join(pays, "date", "left"), "bq2_day_totals")

    # ---- score histograms (label-free drift), by pattern_label so BQ2 can hide a pattern
    hist = None
    for m in names:
        h = (adj.groupBy("week", "pattern_label", score_bin(f"score_{m}").alias("bin"))
                .agg(F.count(F.lit(1)).alias("n")).withColumn("model", F.lit(m)))
        hist = h if hist is None else hist.unionByName(h)
    write(hist, "bq2_score_hist")

    for r in days.orderBy("date").collect():
        print(f"[bq2-score] {r['date']} week {r['week']}: {r['account_days']} account-days, "
              f"{r['laundering_account_days']} laundering; "
              + "; ".join(f"{m} {r[f'alerts_{m}']} alerts ({r[f'true_alerts_{m}']} true)" for m in names))
    print("[bq2-score] saved analytics/bq2_acct_days, bq2_day_totals, bq2_score_hist, bq2_flagged_payments")


if __name__ == "__main__":
    main()
    spark.stop()
