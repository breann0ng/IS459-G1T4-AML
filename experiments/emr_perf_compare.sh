#!/bin/bash
# Old vs new2 Spark scripts on EMR, for the write-up. Run on the primary node AFTER round4_prep.sh
# (same cluster: EMR keeps Spark's event logs in HDFS, which is gone once the cluster terminates).
#   1. old = s3://<bucket>/scripts_old/ (main's 5 Spark scripts) runs the 5 Spark steps of
#      round4_prep.sh on HI-Medium, into its OWN data root s3://<bucket>/perf_old/ (raw files
#      copied from raw/), so new2's curated/ that round 4 uses is never touched
#   2. collects the event logs: new2 = the HI-Medium Spark steps round4_prep.sh ran (EMR logs every
#      app to hdfs:///var/log/spark/apps), old = step 1; matched to steps by Spark app name
#   3. copies them + index.tsv / versions.tsv to s3://<bucket>/perf/eventlogs/HI-Medium-emr/
#
#   export AML_BUCKET=<bucket>
#   nohup bash emr_perf_compare.sh > emr_perf_compare.log 2>&1 &
#   COLLECT_ONLY=1 bash emr_perf_compare.sh          steps 2-3 only (old already ran)
# Laptop, afterwards:
#   aws s3 cp --recursive s3://<bucket>/perf/eventlogs/HI-Medium-emr data/perf/eventlogs/HI-Medium-emr
#   python experiments/spark_perf_report.py HI-Medium-emr > results/experiments/perf_comparison_emr.txt
# Then delete s3://<bucket>/perf_old/ (old's data, only needed for this comparison).

BUCKET="${AML_BUCKET:?set AML_BUCKET first, e.g. export AML_BUCKET=pw-aml-g1t4-b}"
DATASET=${DATASET:-HI-Medium}
ROOT="s3://$BUCKET"
OLD_ROOT="$ROOT/perf_old"
OUT="$ROOT/perf/eventlogs/$DATASET-emr"
OLD_EV=hdfs:///perf/eventlogs/old
NEW_EV=${NEW_EV:-hdfs:///var/log/spark/apps}      # EMR's default spark.eventLog.dir
# same spark-submit settings as round4_prep.sh, so old and new2 run alike
S3CONF="--conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider,software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider"
DRV="--driver-memory 6g --conf spark.driver.maxResultSize=2g --conf spark.sql.autoBroadcastJoinThreshold=-1"
EVCONF="--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=$OLD_EV --conf spark.eventLog.compress=false"
mkdir -p logs
set -o pipefail

# ------------------------------------------------------------------ 1. old's 5 Spark steps
if [ -z "$COLLECT_ONLY" ]; then
  rm -rf perf_old && mkdir -p perf_old
  aws s3 cp --recursive "$ROOT/scripts_old/" perf_old/ > /dev/null || exit 1
  for k in transactions patterns accounts; do
    aws s3 ls "$ROOT/raw/$k/set=$DATASET/" > /dev/null \
      || { echo "no $ROOT/raw/$k/set=$DATASET/: run round4_prep.sh first"; exit 1; }
    aws s3 cp --recursive "$ROOT/raw/$k/set=$DATASET/" "$OLD_ROOT/raw/$k/set=$DATASET/" > /dev/null || exit 1
  done
  hdfs dfs -mkdir -p "$OLD_EV"
  for s in "clean_transactions_spark.py $DATASET" "parse_patterns_spark.py $DATASET" \
           "build_features_spark.py $DATASET" "build_txn_features_spark.py $DATASET features_v2" \
           "build_features_v3_spark.py $DATASET"; do
    name=${s%%.py*}
    echo "=== $(date '+%m-%d %H:%M') start old $name"
    ( cd perf_old && AML_ROOT="$OLD_ROOT" spark-submit $S3CONF $DRV $EVCONF $s ) > "logs/old_$name.log" 2>&1; ok=$?
    aws s3 cp "logs/old_$name.log" "$ROOT/logs/emr_perf/old_$name.log" > /dev/null
    if [ $ok -ne 0 ]; then echo "=== FAILED old $name (see logs/old_$name.log); stopping"; exit 1; fi
    echo "=== $(date '+%m-%d %H:%M') done  old $name"
  done
fi

# ------------------------------------------------------------------ 2. collect the event logs
rm -rf ev && mkdir -p ev/old ev/new2
hdfs dfs -get "$OLD_EV/*" ev/old/ || echo "no old event logs in $OLD_EV"
hdfs dfs -get "$NEW_EV/*" ev/new2/ || echo "no new2 event logs in $NEW_EV (is spark.eventLog.dir set elsewhere?)"
# EMR compresses its default event logs (events_<n>_<app>.zstd): unpack them so both reports read plain JSON lines
command -v zstd > /dev/null || sudo dnf install -y -q zstd
find ev -name '*.zstd' -exec zstd -dq --rm {} \;
python3 - "$DATASET" "$BUCKET" <<'EOF'
"""Keep the 5 Spark steps of DATASET, write index.tsv (run = attempt, by start time; only the
last attempt per step is 'ok': round4_prep.sh only re-runs a step after it failed) + versions.tsv."""
import json, os, shutil, sys
dataset, bucket = sys.argv[1], sys.argv[2]
STEP = {"CleanTransactions": "clean", "ParsePatterns": "patterns", "BuildFeatures": "features_v2",
        "BuildTxnFeatures": "txn_features", "BuildFeaturesV3": "features_v3"}
def first_file(p):
    if os.path.isdir(p):
        parts = sorted((f for f in os.listdir(p) if f.startswith("events_")), key=lambda f: int(f.split("_")[1]))
        return os.path.join(p, parts[0]) if parts else None
    return p
rows = []
for v in ["old", "new2"]:
    for app in sorted(os.listdir(f"ev/{v}")):
        path, keep = f"ev/{v}/{app}", None
        if app.endswith((".lz4", ".lzf", ".snappy", ".zstd")):
            print(f"skipped {path}: compressed event log (set spark.eventLog.compress=false)")
        elif not app.endswith(".inprogress") and first_file(path):
            with open(first_file(path)) as fh:
                for line in fh:
                    e = json.loads(line)
                    if e["Event"] == "SparkListenerApplicationStart":
                        app_name = e["App Name"]                    # e.g. BuildFeatures-HI-Medium
                        name = app_name[:-len(dataset) - 1]
                        if app_name.endswith("-" + dataset) and name in STEP:
                            keep = (STEP[name], e["Timestamp"])
                        break
        if keep:
            rows.append((v, keep[0], keep[1], app))
        elif os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
with open("ev/index.tsv", "w") as f:
    f.write("version\tstep\trun\tapp\tstatus\tsubmit_s\n")
    for v, s in sorted({(v, s) for v, s, _, _ in rows}):
        tries = sorted((t, a) for vv, ss, t, a in rows if (vv, ss) == (v, s))
        for i, (t, a) in enumerate(tries, 1):
            f.write(f"{v}\t{s}\t{i}\t{a}\t{'ok' if i == len(tries) else 'retried'}\t\n")
with open("ev/versions.tsv", "w") as f:
    f.write(f"old\tmain (s3://{bucket}/scripts_old/) on EMR\n")
    f.write(f"new2\ts3://{bucket}/scripts/ (round4_prep.sh) on EMR\n")
print(f"kept {len(rows)} event logs:", sorted((v, s) for v, s, _, _ in rows))
EOF

# ------------------------------------------------------------------ 3. to S3 (kept after the cluster)
aws s3 cp --recursive ev/ "$OUT/" > /dev/null && echo "=== event logs + index.tsv in $OUT/"
echo "=== $(date '+%m-%d %H:%M') ALL DONE: terminate the cluster when round 4 is finished too"
