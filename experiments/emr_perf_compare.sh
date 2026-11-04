#!/bin/bash
# Old (main) vs new2 (optimised) Spark scripts on EMR, HI-Medium, for the write-up.
# Runs the 5 Spark steps of BOTH versions on the same cluster, alternating old / new2 per step
# (so a slow node or a busy S3 hits both alike), each version in its own data root
# s3://<bucket>/perf/<version>/ (raw copied in; curated/ that round 4 uses is never touched).
# Nothing lives only on the cluster: after EVERY step the step's event log, index.tsv and this
# script's log are copied to s3://<bucket>/perf/eventlogs/<dataset>-emr/, so a cluster that dies
# or auto-terminates keeps everything up to its last finished step. Does not need round4_prep.sh.
#
# On the primary node (Java 17 active, scripts/ and scripts_old/ in S3):
#   export AML_BUCKET=<bucket> AWS_REGION=<bucket region>
#   nohup bash emr_perf_compare.sh > emr_perf_compare.log 2>&1 &
#   STEPS="clean features_v2" REPEATS=2 ...      only some steps / more runs per version
# Laptop, afterwards (or any time, for the steps finished so far):
#   aws s3 cp --recursive s3://<bucket>/perf/eventlogs/HI-Medium-emr data/perf/eventlogs/HI-Medium-emr
#   python experiments/spark_perf_report.py HI-Medium-emr > results/experiments/perf_comparison_emr.txt
# Then delete s3://<bucket>/perf/old/ and perf/new2/ (only needed for this comparison).

BUCKET="${AML_BUCKET:?set AML_BUCKET first, e.g. export AML_BUCKET=is459-aml}"
: "${AWS_REGION:?set AWS_REGION to the bucket region first, e.g. export AWS_REGION=ap-southeast-2}"
DATASET=${DATASET:-HI-Medium}
STEPS=${STEPS:-"clean patterns features_v2 txn_features features_v3"}
REPEATS=${REPEATS:-1}
ROOT="s3://$BUCKET"
OUT="$ROOT/perf/eventlogs/$DATASET-emr"
LOG=$(pwd)/emr_perf_compare.log
# same spark-submit settings as round4_prep.sh, so both versions run alike
S3CONF="--conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider,software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider"
DRV="--driver-memory 6g --conf spark.driver.maxResultSize=2g --conf spark.sql.autoBroadcastJoinThreshold=-1"
declare -A SCRIPT=([clean]="clean_transactions_spark.py $DATASET" [patterns]="parse_patterns_spark.py $DATASET"
                   [features_v2]="build_features_spark.py $DATASET"
                   [txn_features]="build_txn_features_spark.py $DATASET features_v2"
                   [features_v3]="build_features_v3_spark.py $DATASET")
declare -A CODE=([old]=scripts_old [new2]=scripts)
mkdir -p logs ev
set -o pipefail

java -version 2>&1 | grep -q '"17' || { echo "Java 17 is not active: export JAVA_HOME=/usr/lib/jvm/jre-17"; exit 1; }
save () {   # index, versions and this log to S3 (called after every step)
  aws s3 cp ev/index.tsv "$OUT/index.tsv" > /dev/null
  aws s3 cp ev/versions.tsv "$OUT/versions.tsv" > /dev/null
  aws s3 cp "$LOG" "$OUT/emr_perf_compare.log" > /dev/null 2>&1
}

# ------------------------------------------------------------------ setup: code + data root per version
for v in old new2; do
  rm -rf "code_$v" && mkdir -p "code_$v"
  aws s3 cp --recursive "$ROOT/${CODE[$v]}/" "code_$v/" > /dev/null || { echo "cannot read $ROOT/${CODE[$v]}/"; exit 1; }
  for k in transactions patterns accounts; do
    aws s3 ls "$ROOT/raw/$k/set=$DATASET/" > /dev/null || { echo "no $ROOT/raw/$k/set=$DATASET/"; exit 1; }
    aws s3 sync "$ROOT/raw/$k/set=$DATASET/" "$ROOT/perf/$v/raw/$k/set=$DATASET/" > /dev/null || exit 1
  done
  hdfs dfs -mkdir -p "/perf/eventlogs/$v"
done
[ -f ev/index.tsv ] || printf "version\tstep\trun\tapp\tstatus\tsubmit_s\n" > ev/index.tsv
printf "old\tmain (s3://%s/scripts_old/) on EMR\nnew2\toptimised (s3://%s/scripts/) on EMR\n" "$BUCKET" "$BUCKET" > ev/versions.tsv
save

# ------------------------------------------------------------------ the steps, old then new2 each time
for r in $(seq 1 "$REPEATS"); do
  for s in $STEPS; do
    for v in old new2; do
      EV="/perf/eventlogs/$v"
      before=$(hdfs dfs -ls -C "$EV" 2>/dev/null | sort)
      echo "=== $(date '+%m-%d %H:%M') start $v $s (run $r)"
      t0=$(date +%s)
      ( cd "code_$v" && AML_ROOT="$ROOT/perf/$v" spark-submit $S3CONF $DRV \
          --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="hdfs://$EV" \
          --conf spark.eventLog.compress=false ${SCRIPT[$s]} ) > "logs/${v}_${s}_$r.log" 2>&1; ok=$?
      secs=$(( $(date +%s) - t0 ))
      aws s3 cp "logs/${v}_${s}_$r.log" "$OUT/logs/${v}_${s}_$r.log" > /dev/null
      app=$(comm -13 <(echo "$before") <(hdfs dfs -ls -C "$EV" | sort) | head -1)
      app=${app##*/}
      if [ -n "$app" ]; then              # this step's event log, straight to S3
        rm -rf "ev/$v/$app" && mkdir -p "ev/$v" && hdfs dfs -get "$EV/$app" "ev/$v/" \
          && aws s3 cp --recursive "ev/$v/$app" "$OUT/$v/$app" > /dev/null
        [ -d "ev/$v/$app" ] || aws s3 cp "ev/$v/$app" "$OUT/$v/$app" > /dev/null   # single-file log
      fi
      status=$([ $ok -eq 0 ] && echo ok || echo FAILED)
      printf "%s\t%s\t%s\t%s\t%s\t%s\n" "$v" "$s" "$r" "${app:-none}" "$status" "$secs" >> ev/index.tsv
      save
      if [ $ok -ne 0 ]; then
        echo "=== FAILED $v $s after ${secs}s (logs/${v}_${s}_$r.log, also in $OUT/logs/); stopping"; exit 1
      fi
      echo "=== $(date '+%m-%d %H:%M') done  $v $s in ${secs}s"
    done
  done
done
echo "=== $(date '+%m-%d %H:%M') ALL DONE: everything is in $OUT/ ; terminate the cluster"
