"""
bq2_ibm_retrain_sample_spark.py                                          (BQ2, HI-Large retraining)
The training sample a BQ2 retrain adds: the HI-Large payments whose labels are confirmed by the alert,
with BQ1's sampling rule (every laundering payment + 2% of honest ones, seed 42; build_txn_features_spark.py).
final_train.py then trains on BQ1's HI-Small + HI-Medium sample PLUS this (env AML_EXTRA_SAMPLE), the way the
SAML pipeline retrains on every case confirmed up to the alert.

Run (EMR Serverless, from aml_bq2_weekly_ibm's RetrainSample step):
    bq2_ibm_retrain_sample_spark.py HI-Large-live 2022-09-12 <retrain_to = the alert week's Sunday>
Reads   <AML_DATA_ROOT>/curated/txn_features_v3   (BQ1's weekly features)
Writes  <AML_ROOT>/curated/txn_features_v3_sample/dataset=<dataset>/   (replaced on every run)
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = os.environ["AML_ROOT"]      # s3://<bucket>; no default, so a job never writes to the wrong bucket
DATA_ROOT = os.environ.get("AML_DATA_ROOT", "").rstrip("/") or ROOT
DATASET, DATE_FROM, DATE_TO = sys.argv[1], sys.argv[2], sys.argv[3]
TABLE = os.environ.get("BQ2_TXN_TABLE", "txn_features_v3")
NEG_SAMPLE_RATE = 0.02             # as build_txn_features_spark.py
RANDOM_SEED = 42
# The retrain belongs to the new-tactic test (HI-Large-NT), where the hidden patterns do not exist before INTRO:
# their laundering before that day is left out here too, so the retrain learns only the fan-in / fan-out cases
# that appeared from Week 10 and were confirmed by the alert.
HIDE = [x for x in os.environ.get("BQ2_HIDE", "fan_in,fan_out").split(",") if x]
INTRO = os.environ.get("BQ2_HIDE_BEFORE", "2022-10-03")

spark = SparkSession.builder.appName(f"BQ2RetrainSample-{DATASET}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.ansi.enabled", "false")


def main():
    d = F.to_date(F.col("date"))
    src = (spark.read.parquet(f"{DATA_ROOT}/curated/{TABLE}").filter(F.col("dataset") == DATASET)
           .filter((d >= F.lit(DATE_FROM).cast("date")) & (d <= F.lit(DATE_TO).cast("date"))))
    sample = src.filter((F.col("label") == 1) | (F.rand(RANDOM_SEED) < NEG_SAMPLE_RATE)).drop("dataset")
    if HIDE:
        sample = sample.filter(~((F.col("label") == 1) & F.col("pattern_label").isin(HIDE)
                                 & (d < F.lit(INTRO).cast("date"))))
    out = f"{ROOT}/curated/{TABLE}_sample/dataset={DATASET}"
    sample.repartition("week").write.mode("overwrite").partitionBy("week").parquet(out)
    n = spark.read.parquet(out).groupBy("label").count().collect()
    counts = {int(r["label"]): int(r["count"]) for r in n}
    if not counts.get(1):
        raise SystemExit(f"[retrain-sample] no laundering payments for {DATASET} {DATE_FROM}..{DATE_TO}")
    fan = spark.read.parquet(out).filter((F.col("label") == 1) & F.col("pattern_label").isin(HIDE)).count() if HIDE else 0
    print(f"[retrain-sample] new-tactic test: {HIDE} laundering before {INTRO} left out; {fan} confirmed from {INTRO} kept")
    print(f"[retrain-sample] {DATASET} {DATE_FROM}..{DATE_TO}: {counts.get(1, 0)} laundering + "
          f"{counts.get(0, 0)} honest payments ({NEG_SAMPLE_RATE:.0%}) -> {out}")


if __name__ == "__main__":
    main()
    spark.stop()
