#!/bin/bash
# BQ2 · HI-Large tab: Joey's BQ2 method (Part 1 tactics, Part 2 rules R1-R3) on IBM HI-Large, plus the
# new-tactic test (fan-in / fan-out appear as a brand-new tactic on 3 Oct = Week 10).
# RESUMABLE: every finished step leaves a marker in progress/bq2/ (the same markers as rebuild_bq2.sh,
# so HI-Large weeks that were already built and scored are skipped).
#
# Needs in the bucket: curated/ for HI-Small, HI-Medium, HI-Large (clean, answer key, features of the
# training sets) and the two frozen models: models/final (BQ1 final stage 1, final_train.py txn_v3m)
# and models/final_nofan (same recipe, fan-in / fan-out left out: final_train.py txn_v3m_nofan
# --exclude-patterns fan_in,fan_out --model-dir models/final_nofan --stage1-only).
#
# On the EMR primary node, all scripts in ONE folder (aws/jobs/*/* + local/legacy-emr/* flattened):
#   export AML_BUCKET=<bucket>
#   setsid nohup bash run_bq2_ibm.sh > run_bq2_ibm.log 2>&1 < /dev/null &
#   tail -f run_bq2_ibm.log
# Local test (no AWS): AML_LOCAL=/abs/path/lake SUBMIT="spark-submit --master local[2]" bash run_bq2_ibm.sh

MON_FROM=${MON_FROM:-2022-08-29}   # Week 5 (Mon): weeks 1-4 are the simulator warm-up
MON_TO=${MON_TO:-2022-10-30}       # Week 13 (Sun): 9 weeks
CHUNK_DAYS=${CHUNK_DAYS:-7}        # HI-Large is built and scored 1 week at a time (disk / memory)
BASE_FROM=${BASE_FROM:-2022-08-29} # Part 1 baseline = Weeks 5-6, as in Joey's HI-Large run
BASE_TO=${BASE_TO:-2022-09-11}
HIDE=${HIDE:-fan_in,fan_out}       # the new tactic of the test ...
INTRO=${INTRO:-2022-10-03}         # ... appears on this day (Monday of Week 10)

if [ -n "$AML_LOCAL" ]; then
  export AML_ROOT="$AML_LOCAL"; P="$AML_LOCAL/progress/bq2"; mkdir -p "$P"
  SUBMIT=${SUBMIT:?local test: set SUBMIT, e.g. "spark-submit --master local[2]"}
  SCORE_SUBMIT="$SUBMIT"; PY=${PY:-python3}
  done_marker () { [ -f "$P/$1.done" ]; }
  mark_done () { echo done > "$P/$1.done"; }
  have () { [ -e "$AML_ROOT/$1" ]; }
else
  BUCKET="${AML_BUCKET:?set AML_BUCKET first, e.g. export AML_BUCKET=is459-g1t4-aml-tasha}"
  export AML_ROOT="s3://$BUCKET"; P="$AML_ROOT/progress/bq2"; PY=python3
  S3CONF="--conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider,software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider"
  DRV="--driver-memory 6g --conf spark.driver.maxResultSize=2g --conf spark.sql.autoBroadcastJoinThreshold=-1"
  export AML_SHUFFLE=${AML_SHUFFLE:-1000}
  SPARK_EXTRA=${SPARK_EXTRA:---conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.executor.memoryOverhead=2g}
  SUBMIT="spark-submit $S3CONF $DRV $SPARK_EXTRA"
  SCORE_SUBMIT="$SUBMIT --conf spark.pyspark.python=/usr/bin/python3 --conf spark.pyspark.driver.python=/usr/bin/python3 --conf spark.executor.memory=5g --conf spark.executor.memoryOverhead=4g"
  done_marker () { aws s3 ls "$P/$1.done" > /dev/null 2>&1; }
  mark_done () { echo done | aws s3 cp - "$P/$1.done" > /dev/null; }
  have () { aws s3 ls "$AML_ROOT/$1" > /dev/null 2>&1; }
fi
mkdir -p logs
set -o pipefail

step () {   # step <name> <command ...>: skipped if already done, stops the run if it fails
  if done_marker "$1"; then echo "=== skip $1 (already done)"; return 0; fi
  echo "=== $(date '+%m-%d %H:%M') start $1"
  if "${@:2}" > "logs/$1.log" 2>&1; then
    mark_done "$1"; echo "=== $(date '+%m-%d %H:%M') done  $1"
  else
    echo "=== $(date '+%m-%d %H:%M') FAILED $1 (see logs/$1.log); stopping"; tail -20 "logs/$1.log"; exit 1
  fi
}

chunks () {
  python3 - "$MON_FROM" "$MON_TO" "$CHUNK_DAYS" <<'EOF'
import sys, datetime as dt
a, b, n = dt.date.fromisoformat(sys.argv[1]), dt.date.fromisoformat(sys.argv[2]), int(sys.argv[3])
while a <= b:
    e = min(a + dt.timedelta(days=n - 1), b)
    print(a, e); a = e + dt.timedelta(days=1)
EOF
}

echo "=== $(date '+%m-%d %H:%M') root $AML_ROOT, HI-Large $MON_FROM to $MON_TO, baseline $BASE_FROM..$BASE_TO, new tactic $HIDE from $INTRO"
for m in models/final/stage1.txt models/final_nofan/stage1.txt; do
  have "$m" || { echo "missing $m: train it first (see the header)"; exit 1; }
done
if [ -z "$AML_LOCAL" ]; then
  java -version 2>&1 | grep -q '"17' || { echo "Java 17 is not active: run check_cluster.sh, then source ~/.bashrc"; exit 1; }
  pip3 install --user -q pandas pyarrow scikit-learn scipy lightgbm networkx xxhash > logs/pip.log 2>&1 \
    || { echo "pip install failed (see logs/pip.log)"; exit 1; }
fi

# 1. HI-Large features + scores (both models), one week at a time
while read -r F T; do
  step feat_large_$F   $SUBMIT build_features_spark.py HI-Large $F $T
  step txn_large_$F    $SUBMIT build_txn_features_spark.py HI-Large features_v2 $F $T
  step v3_large_$F     $SUBMIT build_features_v3_spark.py HI-Large $F $T
  step bq2_score_$F    $SCORE_SUBMIT bq2_ibm_score_spark.py HI-Large $F $T
done < <(chunks)

# 2. Part 1 (Joey): tactics from confirmed cases only. Real data, then the new-tactic test (tag _nt)
step bq2i_part1     $SUBMIT bq2_part1_tactics_spark.py HI-Large $BASE_FROM $BASE_TO
step bq2i_part1_nt  env BQ2_HIDE=$HIDE BQ2_HIDE_BEFORE=$INTRO $SUBMIT bq2_part1_tactics_spark.py HI-Large $BASE_FROM $BASE_TO _nt

# 3. Part 2 (Joey's rules): scores + validation baselines in her format, then her evaluation, unchanged
step bq2i_prep      env BQ2_HIDE=$HIDE BQ2_HIDE_BEFORE=$INTRO $SCORE_SUBMIT bq2_ibm_part2_prep_spark.py HI-Large $MON_FROM $MON_TO
step bq2i_eval      env BQ2_EMERGING=$HIDE BQ2_FIRST_DOSE=$INTRO $PY -u bq2_part2_eval.py HI-Large
step bq2i_eval_nt   env BQ2_EMERGING=$HIDE BQ2_FIRST_DOSE=$INTRO $PY -u bq2_part2_eval.py HI-Large-NT
echo "=== $(date '+%m-%d %H:%M') ALL DONE: terminate the cluster now"
# Laptop:  export AML_ROOT=s3://<bucket>   then   python3 local/reports/bq2_ibm_report.py
