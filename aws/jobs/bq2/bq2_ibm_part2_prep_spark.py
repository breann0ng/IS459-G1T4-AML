"""
bq2_ibm_part2_prep_spark.py                                              (BQ2, HI-Large new-tactic test)
Gets HI-Large ready for Joey's bq2_part2_eval.py, so both BQ2 tabs are graded by the same rules.

Two "datasets" for the evaluation, both from the same HI-Large scores (bq2_ibm_score_spark.py):
    HI-Large     the data as it is.                     v0 = the BQ1 final model (models/final)
    HI-Large-NT  new-tactic test: laundering of the HIDDEN patterns (default fan_in, fan_out) is left
                 out before INTRO, so they appear as a brand-new tactic that day.
                                                        v0 = the same model trained WITHOUT them
                                                             (models/final_nofan: the bank's model
                                                             before the tactic existed)
                                                        v1 = the BQ1 final model (has seen them: what
                                                             retraining with the confirmed cases gives)
For each version it writes what bq2_part2_train.py would have written for SAML:
    models/bq2r/<dataset>/<version>/meta.json   threshold, validation recall / precision overall and
        per pattern (scored here on the SAME HI-Small + HI-Medium validation days the threshold was set
        on; the hidden patterns are left out for the model that never saw them), train_patterns
    analytics/bq2r_scores/dataset=<dataset>/version=<version>/
        every laundering payment of the window + every payment either model flagged:
        txn_id, date, week, label, pattern_label, score, alert. Honest payments that no model flagged
        are not stored (HI-Large has ~13M a week); precision and recall are exact, the evaluation's
        "honest_flag_rate" column is not meaningful for HI-Large.

Run (EMR, after bq2_ibm_score_spark.py has scored every week of the window):
    spark-submit [S3 confs] bq2_ibm_part2_prep_spark.py HI-Large 2022-08-29 2022-10-30
Then:  python3 bq2_part2_eval.py HI-Large ;  BQ2_EMERGING=fan_in,fan_out BQ2_FIRST_DOSE=2022-10-03 \
       python3 bq2_part2_eval.py HI-Large-NT
Settings (env): BQ2_MODELS (all=models/final,nofan=models/final_nofan), BQ2_HIDE (fan_in,fan_out),
                BQ2_HIDE_BEFORE (2022-10-03 = Week 10)
"""

import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = os.environ["AML_ROOT"]      # s3://<bucket>; no default, so a job never writes to the wrong bucket
DATASET = sys.argv[1] if len(sys.argv) > 1 else "HI-Large"
MON_FROM = sys.argv[2] if len(sys.argv) > 2 else "2022-08-29"
MON_TO = sys.argv[3] if len(sys.argv) > 3 else "2022-10-30"
MODELS = dict(m.split("=", 1) for m in
              os.environ.get("BQ2_MODELS", "all=models/final,nofan=models/final_nofan").split(","))
HIDE = [x for x in os.environ.get("BQ2_HIDE", "fan_in,fan_out").split(",") if x]
INTRO = os.environ.get("BQ2_HIDE_BEFORE", "2022-10-03")
NT = f"{DATASET}-NT"
# evaluation dataset -> {version: model}
PLAN = {DATASET: {"v0": "all"}, NT: {"v0": "nofan", "v1": "all"}}
PATTERNS = ["fan_in", "fan_out", "gather_scatter", "scatter_gather", "cycle", "bipartite", "stack", "random",
            "unclassified"]
# Input data can come from another root / dataset name than where BQ2 writes (e.g. BQ1's shared data):
#   AML_DATA_ROOT=s3://<bucket>  (curated/ is read there; default AML_ROOT)
#   AML_SRC_DATASET=HI-Large-live (the dataset partition read; default = the dataset argument, which still
#                                  names everything BQ2 writes, so the dashboard and decisions keep "HI-Large")
DATA_ROOT = os.environ.get("AML_DATA_ROOT", "").rstrip("/") or ROOT
SRC = os.environ.get("AML_SRC_DATASET", "") or DATASET
CURATED, ANALYTICS = f"{DATA_ROOT}/curated", f"{ROOT}/analytics"

spark = SparkSession.builder.appName(f"BQ2IbmPrep-{DATASET}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", os.environ.get("AML_SHUFFLE", "200"))
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "20000")


def model_path(d):
    """A BQ2_MODELS folder: absolute s3:// path, or relative to AML_ROOT."""
    return d.rstrip("/") if d.startswith("s3://") else f"{ROOT}/{d.strip('/')}"


def text(path):
    return spark.sparkContext.wholeTextFiles(path).collect()[0][1]


def put_text(uri, body):
    import pyarrow.fs as pafs
    if uri.startswith("s3://"):
        fs, path = pafs.S3FileSystem(region=os.environ.get("AWS_REGION", "us-east-1")), uri[5:]
    else:
        fs, path = pafs.LocalFileSystem(), os.path.abspath(uri)
    fs.create_dir(path.rsplit("/", 1)[0], recursive=True)
    with fs.open_output_stream(path) as f:
        f.write(body.encode())


def drop_missing_models():
    """BQ2_MODELS entries ending in "?" are optional (e.g. the retrained challenger before any retrain): skipped
    while their folder has no meta.json."""
    for m, d in list(MODELS.items()):
        if d.endswith("?"):
            MODELS[m] = d = d[:-1]
            try:
                text(f"{model_path(d)}/meta.json")
            except Exception:
                print(f"[bq2] optional model {m} ({d}) not trained yet: skipped")
                del MODELS[m]


def validation(m, meta, booster):
    """Score the HI-Small / HI-Medium validation days (after each training end) with model m."""
    feats, thr = meta["stage1_features"], float(meta["threshold"])
    excl = list(meta.get("excluded_patterns") or [])
    cond = None
    for d, end in meta["train_end"].items():
        if d == SRC:        # a retrain that added HI-Large weeks: validation stays HI-Small + HI-Medium
            continue
        c = (F.col("dataset") == d) & (F.col("date") > F.lit(end).cast("date"))
        cond = c if cond is None else (cond | c)
    v = spark.read.parquet(f"{CURATED}/{meta['txn_table']}").filter(cond).select("label", "pattern_label", *feats)
    if excl:
        v = v.filter(~F.col("pattern_label").isin(excl))
    bc = spark.sparkContext.broadcast(booster)

    def score(it):
        import lightgbm as lgb
        import numpy as np
        b = lgb.Booster(model_str=bc.value)
        for pdf in it:
            out = pdf[["label", "pattern_label"]].copy()
            out["alert"] = (b.predict(pdf[feats].to_numpy(np.float32)) >= thr).astype("int32")
            yield out

    s = v.mapInPandas(score, "label int, pattern_label string, alert int")
    tot = s.agg(F.sum("alert").alias("alerts"), F.sum(F.col("alert") * F.col("label")).alias("true_alerts"),
                F.sum("label").alias("positives")).collect()[0]
    per = (s.filter("label = 1").groupBy("pattern_label")
            .agg(F.count(F.lit(1)).alias("cases"), F.sum("alert").alias("caught")).collect())
    alerts, tp, pos = int(tot["alerts"] or 0), int(tot["true_alerts"] or 0), int(tot["positives"] or 0)
    val = {"alerts": alerts, "true_alerts": tp, "positives": pos,
           "precision": tp / alerts if alerts else None, "recall": tp / pos if pos else None}
    by = {r["pattern_label"]: {"cases": int(r["cases"]), "recall": int(r["caught"]) / int(r["cases"])}
          for r in per if r["cases"]}
    print(f"[ibm-prep] {m}: validation on {meta['train_end']} -> precision {val['precision']:.3f}, recall "
          f"{val['recall']:.3f} ({pos} laundering payments); excluded {excl or 'nothing'}")
    return val, by


def main():
    drop_missing_models()
    # the new-tactic test's v1 (the model a retrain gives): BQ2_NT_V1 once it exists (e.g. "retrained"), else BQ1's model
    v1 = os.environ.get("BQ2_NT_V1", "all")
    PLAN[NT]["v1"] = v1 if v1 in MODELS else "all"
    print(f"[ibm-prep] models {MODELS}; plan {PLAN}")
    metas = {m: json.loads(text(f"{model_path(d)}/meta.json")) for m, d in MODELS.items()}
    boosters = {m: text(f"{model_path(d)}/stage1.txt") for m, d in MODELS.items()}
    thr = {m: float(metas[m]["threshold"]) for m in MODELS}

    # ---- 1. meta.json per evaluation dataset x version (validation scored once per model).
    # The validation days never change, so once every meta.json exists it is reused (BQ2_REUSE_META=0 forces a
    # rescore): weekly runs then skip rescoring HI-Small / HI-Medium, which need not even be in the bucket.
    def existing(ds_name, ver):
        try:
            return json.loads(text(f"{ROOT}/models/bq2r/{ds_name}/{ver}/meta.json"))
        except Exception:
            return None
    have = {(d, v): existing(d, v) for d, vs in PLAN.items() for v in vs}
    # reused only if each meta.json was made from the model folder in use now (a model swap rescores)
    reuse = (os.environ.get("BQ2_REUSE_META", "1") == "1" and all(have.values())
             and all(have[(d, v)].get("model_folder") == MODELS[m] for d, vs in PLAN.items() for v, m in vs.items()))
    if reuse:
        print("[ibm-prep] validation already scored: reusing models/bq2r/*/meta.json")
    vals = {} if reuse else {m: validation(m, metas[m], boosters[m]) for m in MODELS}
    for ds_name, versions in PLAN.items():
        for ver, m in versions.items():
            if reuse:
                continue
            excl = list(metas[m].get("excluded_patterns") or [])
            val, by = vals[m]
            meta = {"dataset": ds_name, "version": ver, "model_folder": MODELS[m], "model": m,
                    "threshold": thr[m], "train_end": metas[m]["train_end"], "excluded_patterns": excl,
                    "train_patterns": [p for p in PATTERNS if p not in excl],
                    "validation": val, "validation_by_pattern": by,
                    "note": "BQ1 final stage 1 (final_train.py), validation = HI-Small + HI-Medium validation days"}
            put_text(f"{ROOT}/models/bq2r/{ds_name}/{ver}/meta.json", json.dumps(meta, indent=1))

    # ---- 2. analytics/bq2r_scores: every laundering payment of the window + every flagged payment
    win = (F.col("date") >= F.lit(MON_FROM).cast("date")) & (F.col("date") <= F.lit(MON_TO).cast("date"))
    laund = (spark.read.parquet(f"{CURATED}/{metas['all']['txn_table']}")
             .filter((F.col("dataset") == SRC) & win & (F.col("label") == 1))
             .select("txn_id", "date", "week", "label", "pattern_label"))
    flag = (spark.read.option("mergeSchema", "true").parquet(f"{ANALYTICS}/bq2_flagged_payments").filter((F.col("dataset") == DATASET) & win)
            .select("txn_id", "date", "week", "label", "pattern_label", *[f"score_{m}" for m in MODELS]))
    base = (laund.join(flag.select("txn_id", *[f"score_{m}" for m in MODELS]), "txn_id", "left")
                 .unionByName(flag.filter("label = 0"))).cache()
    n_l, n_f = laund.count(), flag.count()
    out = None
    for ds_name, versions in PLAN.items():
        for ver, m in versions.items():
            x = base
            if ds_name == NT:
                x = x.filter(~((F.col("label") == 1) & F.col("pattern_label").isin(HIDE)
                               & (F.col("date") < F.lit(INTRO).cast("date"))))
            x = (x.select("txn_id", "date", "week", "label", "pattern_label",
                          F.col(f"score_{m}").alias("score"),
                          F.coalesce((F.col(f"score_{m}") >= thr[m]).cast("int"), F.lit(0)).alias("alert"))
                   .withColumn("dataset", F.lit(ds_name)).withColumn("version", F.lit(ver)))
            out = x if out is None else out.unionByName(x)
    (out.repartition("dataset", "version").write.mode("overwrite").partitionBy("dataset", "version")
        .parquet(f"{ANALYTICS}/bq2r_scores"))
    summ = (out.groupBy("dataset", "version").agg(
        F.sum("label").alias("laundering"), F.sum("alert").alias("alerts"),
        F.sum(F.col("alert") * F.col("label")).alias("caught")).orderBy("dataset", "version").collect())
    print(f"[ibm-prep] {DATASET} {MON_FROM}..{MON_TO}: {n_l} laundering payments, {n_f} flagged payments; "
          f"new-tactic test: {HIDE} appear on {INTRO}")
    for r in summ:
        print(f"[ibm-prep] {r['dataset']:>12} {r['version']} ({PLAN[r['dataset']][r['version']]}): "
              f"{r['caught']}/{r['laundering']} laundering caught ({r['caught'] / max(r['laundering'], 1):.1%}), "
              f"{r['alerts']} alerts, precision {r['caught'] / max(r['alerts'], 1):.1%}")
    print("[ibm-prep] saved models/bq2r/{HI-Large,HI-Large-NT}/*/meta.json and analytics/bq2r_scores")


if __name__ == "__main__":
    main()
    spark.stop()
