"""
score_final_spark.py
Final run, step 2 of 3: score EVERY payment of the test dataset in Spark, in parallel on all
core nodes (box 6 of the pipeline diagram). The model is copied to each executor and each one
scores its own share of the rows (mapInPandas). All metrics are computed inside Spark too, so
nothing big ever goes through a single machine.

Run (EMR, after final_train.py; core nodes need lightgbm: see bootstrap_py.sh):
    spark-submit [S3 confs] [driver confs] score_final_spark.py HI-Large

Reads   models/final/ (stage1.txt, meta.json), curated/txn_features_v3, curated/features_v3,
        curated/scheme_txns, curated/schemes                                 (dataset = argv[1])
Writes  analytics/final_results/     stage 1 metrics: payment and account-day level, at the frozen
                                     threshold and as top-K per day (analyst budgets)
        analytics/final_daily/       per day: rows, laundering, alerts, correct alerts
        analytics/final_budget/      precision / recall if analysts review the top K per day
        analytics/final_patterns/    per pattern: laundering caught (payment + account-day level)
        analytics/final_schemes/     per scheme: caught?, when, share of its payments still to come
        analytics/final_alert_rows/  every flagged account-day + its 78 account features (stage 2 input)
        analytics/final_truth/       true laundering account-days per pattern (stage 2 recall base)
        analytics/final_flagged_payments/  flagged payments (for drill-down)
"""

import json
import os
import sys

import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

BUCKET = "pw-aml-g1t4"
ROOT = os.environ.get("AML_ROOT", f"s3://{BUCKET}")
DATASET = sys.argv[1] if len(sys.argv) > 1 else "HI-Large"
DATE_FROM = sys.argv[2] if len(sys.argv) > 2 else None    # optional: test on these dates only
DATE_TO = sys.argv[3] if len(sys.argv) > 3 else None      # (inclusive), e.g. to fit a budget
MODEL_DIR = f"{ROOT}/models/final"
TOP_K = [100, 250, 500, 1000, 2000, 5000]
PATTERNS = ["fan_in", "fan_out", "gather_scatter", "scatter_gather", "cycle", "bipartite", "stack", "random"]
BINS = 20000                                 # score histogram for PR-AUC
DISK = StorageLevel.MEMORY_AND_DISK
CURATED, ANALYTICS = f"{ROOT}/curated", f"{ROOT}/analytics"

spark = SparkSession.builder.appName(f"ScoreFinal-{DATASET}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", os.environ.get("AML_SHUFFLE", "400"))
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "20000")


def text(path):
    return spark.sparkContext.wholeTextFiles(path).collect()[0][1]


def write(df, name, parts=("dataset",)):
    df.withColumn("dataset", F.lit(DATASET)).repartition(*parts).write.mode("overwrite") \
      .partitionBy(*parts).parquet(f"{ANALYTICS}/{name}")


def prf(tp, alerts, pos):
    p = tp / alerts if alerts else 0.0
    r = tp / pos if pos else 0.0
    return p, r, (2 * p * r / (p + r) if p + r else 0.0)


def pr_auc(df, score="score", label="label"):
    """Average precision from a fine score histogram (exact up to ties within a bin)."""
    h = (df.groupBy(F.least(F.floor(F.col(score) * BINS), F.lit(BINS - 1)).alias("b"))
           .agg(F.sum(label).alias("pos"), F.count(F.lit(1)).alias("n")).toPandas()
           .sort_values("b", ascending=False))
    total = h["pos"].sum()
    if not total:
        return None
    tp = h["pos"].cumsum(); n = h["n"].cumsum()
    return float(((h["pos"] / total) * (tp / n)).sum())


def in_dates(df, col="date"):
    if not DATE_FROM:
        return df
    c = F.to_date(F.col(col))
    return df.filter((c >= F.lit(DATE_FROM).cast("date")) & (c <= F.lit(DATE_TO).cast("date")))


def main():
    meta = json.loads(text(f"{MODEL_DIR}/meta.json"))
    model_str = text(f"{MODEL_DIR}/stage1.txt")
    feats, thr = meta["stage1_features"], float(meta["threshold"])
    bc = spark.sparkContext.broadcast(model_str)
    print(f"[score] {DATASET} dates {DATE_FROM or 'all'} to {DATE_TO or 'all'}: model from {meta['validated_run']}, {len(feats)} features, threshold {thr:.4f}")

    def score_part(it):
        import lightgbm as lgb
        import numpy as np
        booster = lgb.Booster(model_str=bc.value)
        for pdf in it:
            s = booster.predict(pdf[feats].to_numpy(np.float32)).astype("float32")
            yield pdf[["txn_id", "s_id", "r_id", "date", "label", "pattern_label"]].assign(score=s)

    txn = (in_dates(spark.read.parquet(f"{CURATED}/{meta['txn_table']}").filter(F.col("dataset") == DATASET))
           .select("txn_id", "s_id", "r_id", "date", "label", "pattern_label", *feats))
    schema = "txn_id string, s_id long, r_id long, date date, label int, pattern_label string, score float"
    sc = txn.mapInPandas(score_part, schema).persist(DISK)
    n_pay = sc.count()
    days = sc.select("date").distinct().count()
    print(f"[score] scored {n_pay} payments over {days} days")

    res, daily_parts, budget = [], [], []

    def evaluate(level, df, unit):
        """df: one row per unit with score + label (+ pattern_label)."""
        pos = df.filter("label = 1").count()
        flag = (F.col("score") >= thr).cast("int")
        tot = df.agg(F.sum(flag).alias("a"), F.sum(flag * F.col("label")).alias("tp")).first()
        p, r, f = prf(tot["tp"], tot["a"], pos)
        scheme = F.col("pattern_label").isin(PATTERNS)
        rp = df.agg(F.sum(flag * F.col("label") * scheme.cast("int")).alias("c"),
                    F.sum(F.col("label") * scheme.cast("int")).alias("n"),
                    F.sum(flag * F.col("label") * (~scheme).cast("int")).alias("cu"),
                    F.sum(F.col("label") * (~scheme).cast("int")).alias("nu")).first()
        auc = pr_auc(df)
        res.append({"level": level, "cutoff": "frozen_threshold", "threshold": thr, "units": unit,
                    "alerts": int(tot["a"]), "true_alerts": int(tot["tp"]), "positives": pos,
                    "precision": p, "recall": r, "f1": f, "alerts_per_day": tot["a"] / days, "pr_auc": auc,
                    "recall_patterns": rp["c"] / rp["n"] if rp["n"] else None,
                    "recall_unclassified": rp["cu"] / rp["nu"] if rp["nu"] else None})
        daily_parts.append(df.groupBy("date").agg(
            F.count(F.lit(1)).alias("rows"), F.sum("label").alias("laundering"),
            F.sum(flag).alias("alerts"), F.sum(flag * F.col("label")).alias("true_alerts"))
            .withColumn("level", F.lit(level)))
        rank = F.row_number().over(Window.partitionBy("date").orderBy(F.col("score").desc()))
        rk = df.select("date", "label", rank.alias("rk")).filter(F.col("rk") <= max(TOP_K)).persist(DISK)
        for k in TOP_K:
            tk = rk.filter(F.col("rk") <= k).agg(F.count(F.lit(1)).alias("a"), F.sum("label").alias("tp")).first()
            p, r, f = prf(tk["tp"], tk["a"], pos)
            budget.append({"level": level, "k_per_day": k, "alerts": int(tk["a"]), "true_alerts": int(tk["tp"] or 0),
                           "precision": p, "recall": r, "f1": f})
            res.append({"level": level, "cutoff": f"top{k}/day", "threshold": None, "units": unit,
                        "alerts": int(tk["a"]), "true_alerts": int(tk["tp"] or 0), "positives": pos,
                        "precision": p, "recall": r, "f1": f, "alerts_per_day": tk["a"] / days, "pr_auc": auc,
                        "recall_patterns": None, "recall_unclassified": None})
        rk.unpersist()
        print(f"[score] {level}: P {res[-len(TOP_K) - 1]['precision']:.3f} R {res[-len(TOP_K) - 1]['recall']:.3f} "
              f"F1 {res[-len(TOP_K) - 1]['f1']:.3f} alerts/day {res[-len(TOP_K) - 1]['alerts_per_day']:.0f} PR-AUC {auc}")

    # ---------------- payment level
    evaluate("payment", sc, "payments")
    pat_pay = (sc.filter("label = 1").groupBy("pattern_label")
                 .agg(F.count(F.lit(1)).alias("positives"),
                      F.sum((F.col("score") >= thr).cast("int")).alias("caught"))
                 .withColumn("level", F.lit("payment")))

    # ---------------- account-day level: an account-day's score = its riskiest payment
    ad = (sc.select(F.col("s_id").alias("id"), "date", "score")
            .unionByName(sc.select(F.col("r_id").alias("id"), "date", "score"))
            .groupBy("id", "date").agg(F.max("score").alias("score")))
    acct = (in_dates(spark.read.parquet(f"{CURATED}/{meta['acct_table']}").filter(F.col("dataset") == DATASET))
            .withColumn("id", F.xxhash64(F.col("acct_bank").cast("string"), "acct")))
    adj = (ad.join(acct.select("id", "date", "acct_bank", "acct", "label", "pattern_label", "first_laundering_ts"),
                   ["id", "date"], "inner").persist(DISK))
    evaluate("account_day", adj, "account-days")
    pat_ad = (adj.filter("label = 1").groupBy("pattern_label")
                .agg(F.count(F.lit(1)).alias("positives"),
                     F.sum((F.col("score") >= thr).cast("int")).alias("caught"))
                .withColumn("level", F.lit("account_day")))

    # ---------------- stage 2 inputs: alerts with their account features; truth per pattern
    alerts = adj.filter(F.col("score") >= thr).select("id", "date", "score")
    nf = meta["namer_account_features"]
    alert_rows = (alerts.join(acct.select("id", "date", "acct_bank", "acct", "label", "pattern_label",
                                          "first_laundering_ts", *nf), ["id", "date"]))
    write(alert_rows.drop("id"), "final_alert_rows")
    write(acct.filter("label = 1").groupBy("pattern_label").count(), "final_truth")

    # ---------------- schemes: caught? how early?
    st = (spark.read.parquet(f"{CURATED}/scheme_txns").filter((F.col("dataset") == DATASET) & F.col("found_in_transactions"))
          .select("scheme_id", "pattern_type", "txn_id", "txn_time"))
    if DATE_FROM:   # only schemes that START inside the test window (whole scheme kept for timing)
        starts = st.groupBy("scheme_id").agg(F.min("txn_time").alias("_s"))
        st = st.join(in_dates(starts, "_s").select("scheme_id"), "scheme_id")
    stp = st.join(sc.select("txn_id", "score", "s_id", "r_id", "date"), "txn_id", "inner")
    first_flag = F.min(F.when(F.col("score") >= thr, F.col("txn_time")))
    sch = stp.groupBy("scheme_id", "pattern_type").agg(
        F.count(F.lit(1)).alias("n_payments"), F.min("txn_time").alias("start_time"),
        F.max("txn_time").alias("end_time"), F.sum((F.col("score") >= thr).cast("int")).alias("n_flagged"),
        first_flag.alias("first_flag_time"))
    after = (stp.join(sch.select("scheme_id", "first_flag_time"), "scheme_id")
                .groupBy("scheme_id").agg(F.sum((F.col("txn_time") >= F.col("first_flag_time")).cast("int")).alias("n_from_first_flag")))
    # end-of-day (batch) view: the first day any of its account-days is an alert
    sad = (stp.select("scheme_id", F.col("s_id").alias("id"), "date")
              .unionByName(stp.select("scheme_id", F.col("r_id").alias("id"), "date")).distinct()
              .join(alerts.select("id", "date"), ["id", "date"]))
    first_alert_day = sad.groupBy("scheme_id").agg(F.min("date").alias("first_alert_day"))
    sch = (sch.join(after, "scheme_id", "left").join(first_alert_day, "scheme_id", "left")
              .withColumn("caught", F.col("first_flag_time").isNotNull())
              .withColumn("duration_h", (F.unix_timestamp("end_time") - F.unix_timestamp("start_time")) / 3600)
              .withColumn("hours_to_first_flag", (F.unix_timestamp("first_flag_time") - F.unix_timestamp("start_time")) / 3600)
              .withColumn("share_from_first_flag", F.col("n_from_first_flag") / F.col("n_payments"))
              .withColumn("alert_ready_time", F.to_timestamp(F.date_add("first_alert_day", 1)))
              .withColumn("caught_before_end_eod", F.col("alert_ready_time") <= F.col("end_time")))
    write(sch, "final_schemes")
    s = sch.agg(F.count(F.lit(1)).alias("n"), F.sum(F.col("caught").cast("int")).alias("caught"),
                F.expr("percentile_approx(hours_to_first_flag, 0.5)").alias("med_h"),
                F.avg("share_from_first_flag").alias("share"),
                F.sum(F.col("caught_before_end_eod").cast("int")).alias("eod")).first()
    print(f"[score] schemes: {s['caught']}/{s['n']} caught; median {s['med_h']} h from start to first flag; "
          f"avg share of a caught scheme's payments from first flag on {s['share']}; "
          f"{s['eod']} alerted (end of day) before the scheme ended")

    # ---------------- save the small tables
    write(pat_pay.unionByName(pat_ad), "final_patterns")
    d = daily_parts[0]
    for x in daily_parts[1:]:
        d = d.unionByName(x)
    write(d, "final_daily")
    write(spark.createDataFrame(pd.DataFrame(budget)), "final_budget")
    r = pd.DataFrame(res).assign(days=days, payments=n_pay, validated_run=meta["validated_run"],
                                 date_from=DATE_FROM or "all", date_to=DATE_TO or "all")
    for c in ("threshold", "pr_auc", "recall_patterns", "recall_unclassified", "precision", "recall", "f1", "alerts_per_day"):
        r[c] = pd.to_numeric(r[c], errors="coerce").astype("float64")
    write(spark.createDataFrame(r), "final_results")
    write(sc.filter(F.col("score") >= thr), "final_flagged_payments")
    print("[score] saved analytics/final_*")


if __name__ == "__main__":
    main()
    spark.stop()
