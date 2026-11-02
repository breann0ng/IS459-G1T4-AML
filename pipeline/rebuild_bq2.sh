#!/bin/bash
# BQ2 pipeline: rebuild the BQ1 final model, train a second model that has never seen fan-in /
# fan-out, score 8 weeks of HI-Large with both, and analyse tactics + detection week by week.
# RESUMABLE: every finished step leaves a marker; after a lab timeout start a new cluster, do the
# first-login setup, and run this again (finished steps are skipped).
#
#   export AML_BUCKET=<your bucket name>
#   nohup bash rebuild_bq2.sh > rebuild_bq2.log 2>&1 &
#   tail -f rebuild_bq2.log
#
# Needs: ~/.kaggle/kaggle.json (legacy token), Java 17 on PATH, all pipeline/ scripts in this
#        folder, and the cluster created with the bootstrap action s3://<bucket>/scripts/install_libs.sh
#
# Local test (no AWS): AML_LOCAL=/path/to/lake SUBMIT="spark-submit ..." bash rebuild_bq2.sh

MON_FROM=${MON_FROM:-2022-09-05}   # first monitored day (Monday, week 6; 5 weeks after HI-Large starts,
                                   # so every 14-day feature window has full history)
MON_TO=${MON_TO:-2022-10-30}       # last monitored day (Sunday, week 13) -> 8 weeks in 4 chunks:
                                   # baseline weeks 6-8, the "new tactic" appears in week 10 (3 Oct).
                                   # More weeks (more budget): MON_FROM=2022-08-29 MON_TO=2022-11-06
CHUNK_DAYS=${CHUNK_DAYS:-14}       # HI-Large is built and scored 2 weeks at a time
HIDE=${HIDE:-fan_in,fan_out}       # the "new tactic" the second model never sees
KAGGLE=https://www.kaggle.com/api/v1/datasets/download/ealtman2019/ibm-transactions-for-anti-money-laundering-aml

if [ -n "$AML_LOCAL" ]; then       # local test: paths on disk, markers on disk, no downloads
  export AML_ROOT="$AML_LOCAL"; P="$AML_LOCAL/progress/bq2"; mkdir -p "$P"
  SUBMIT=${SUBMIT:?local test: set SUBMIT, e.g. "spark-submit --master local[4]"}
  SCORE_SUBMIT="$SUBMIT"; PY=${PY:-python3}
  done_marker () { [ -f "$P/$1.done" ]; }
  mark_done () { echo done > "$P/$1.done"; }
  have () { [ -e "$AML_ROOT/$1" ]; }
else
  BUCKET="${AML_BUCKET:?set AML_BUCKET first, e.g. export AML_BUCKET=is459-g1t4-aml-tasha}"
  export AML_ROOT="s3://$BUCKET"; P="$AML_ROOT/progress/bq2"; PY=python3
  S3CONF="--conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider,software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider"
  DRV="--driver-memory 6g --conf spark.driver.maxResultSize=2g --conf spark.sql.autoBroadcastJoinThreshold=-1"
  SUBMIT="spark-submit $S3CONF $DRV"
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

reuse () {      # reuse <step> <output path> ...: the step counts as done if ALL its outputs exist
                # (e.g. after copying the team's pipeline data into this bucket with aws s3 sync)
  done_marker "$1" && return 0
  local s=$1; shift
  for p in "$@"; do have "$p" || return 0; done
  mark_done "$s"; echo "=== reuse $s (output already in the bucket)"
}

large_weeks () {   # HI-Large week numbers of a chunk (week 1 = Mon 1 Aug 2022)
  python3 -c "import sys,datetime as d;a,b=[d.date.fromisoformat(x) for x in sys.argv[1:]];s=d.date(2022,8,1);print(*range((a-s).days//7+1,(b-s).days//7+2))" "$1" "$2"
}

download () {   # download <kaggle file> <s3 target>: streamed straight into S3, nothing on disk
  U=$(python3 -c "import json;print(json.load(open('$HOME/.kaggle/kaggle.json'))['username'])") || return 1
  K=$(python3 -c "import json;print(json.load(open('$HOME/.kaggle/kaggle.json'))['key'])") || return 1
  curl -sfL -u "$U:$K" "$KAGGLE/$1" | aws s3 cp - "$2" --expected-size 30000000000 || return 1
  head=$(aws s3 cp "$2" - 2>/dev/null | head -c 60)
  echo "first bytes of $1: $head"
  case "$head" in Timestamp*|BEGIN*|Bank\ Name*) return 0 ;; *) echo "unexpected content (zip or error page?)"; return 1 ;; esac
}

chunks () {     # 2-week chunks from MON_FROM to MON_TO: "from to" per line
  python3 - "$MON_FROM" "$MON_TO" "$CHUNK_DAYS" <<'EOF'
import sys, datetime as dt
a, b, n = dt.date.fromisoformat(sys.argv[1]), dt.date.fromisoformat(sys.argv[2]), int(sys.argv[3])
while a <= b:
    e = min(a + dt.timedelta(days=n - 1), b)
    print(a, e); a = e + dt.timedelta(days=1)
EOF
}

echo "=== $(date '+%m-%d %H:%M') root $AML_ROOT, HI-Large monitored $MON_FROM to $MON_TO, hidden pattern(s): $HIDE"
if [ -z "$AML_LOCAL" ]; then
  java -version 2>&1 | grep -q '"17' || { echo "Java 17 is not active: run the first-login setup"; exit 1; }
  pip3 install --user -q pandas pyarrow scikit-learn scipy lightgbm networkx xxhash > logs/pip.log 2>&1 \
    || { echo "pip install failed (see logs/pip.log)"; exit 1; }
  # 1. raw data from Kaggle
  for D in HI-Small HI-Medium HI-Large; do
    reuse dl_trans_$D curated/transactions/dataset=$D/
    reuse dl_patterns_$D curated/schemes/dataset=$D/
    reuse dl_accounts_$D raw/accounts/set=$D/        # small; HI-Large's is needed for the new weeks' features
    step dl_trans_$D     download "${D}_Trans.csv"    "$AML_ROOT/raw/transactions/set=$D/${D}_Trans.csv"
    step dl_patterns_$D  download "${D}_Patterns.txt" "$AML_ROOT/raw/patterns/set=$D/${D}_Patterns.txt"
    step dl_accounts_$D  download "${D}_accounts.csv" "$AML_ROOT/raw/accounts/set=$D/${D}_accounts.csv"
  done
fi

# 2. clean + answer key (whole datasets; HI-Large's tail after 5 Nov is cut by the clean step)
for D in HI-Small HI-Medium HI-Large; do
  reuse clean_$D curated/transactions/dataset=$D/ curated/labels/dataset=$D/ curated/daily_profile/dataset=$D/
  reuse patterns_$D curated/schemes/dataset=$D/ curated/scheme_txns/dataset=$D/
  step clean_$D     $SUBMIT clean_transactions_spark.py $D
  step patterns_$D  $SUBMIT parse_patterns_spark.py $D
done
# 3. training data: HI-Small + HI-Medium (exactly as rebuild_final.sh)
for D in HI-Small HI-Medium; do
  reuse feat_$D curated/features_v2_sample/dataset=$D/
  reuse txn_$D curated/txn_features_sample/dataset=$D/
  reuse v3_$D curated/txn_features_v3_sample/dataset=$D/ curated/txn_features_v3/dataset=$D/ curated/features_v3_sample/dataset=$D/
  step feat_$D  $SUBMIT build_features_spark.py $D
  step txn_$D   $SUBMIT build_txn_features_spark.py $D features_v2
  step v3_$D    $SUBMIT build_features_v3_spark.py $D
done
# 4. model "all": the BQ1 final model (validated run txn_v3m -> models/final)
reuse validate models/final/meta.json      # a finished final model implies its validation run
reuse final_train models/final/stage1.txt models/final/meta.json
step validate     $PY -u train_txn.py --txn-table txn_features_v3 --stage2-features features_v3 --preset p3 --merge-layering --config txn_v3m
step final_train  $PY -u final_train.py txn_v3m
# 5. model "nofan": identical, but fan-in / fan-out laundering never seen (own validation threshold)
step validate_nofan     $PY -u train_txn.py --txn-table txn_features_v3 --stage2-features features_v3 --preset p3 --no-stage2 --exclude-patterns $HIDE --config txn_v3m_nofan
step final_train_nofan  $PY -u final_train.py txn_v3m_nofan --exclude-patterns $HIDE --model-dir models/final_nofan --stage1-only
# 6. HI-Large features + scores, 2 weeks at a time (each chunk reads 21 days of history before it)
while read -r F T; do
  W=$(large_weeks $F $T)
  reuse feat_large_$F $(for w in $W; do echo curated/features_v2/dataset=HI-Large/week=$w/; done)
  reuse txn_large_$F  $(for w in $W; do echo curated/txn_features/dataset=HI-Large/week=$w/; done)
  reuse v3_large_$F   $(for w in $W; do echo curated/txn_features_v3/dataset=HI-Large/week=$w/ curated/features_v3/dataset=HI-Large/week=$w/; done)
  step feat_large_$F   $SUBMIT build_features_spark.py HI-Large $F $T
  step txn_large_$F    $SUBMIT build_txn_features_spark.py HI-Large features_v2 $F $T
  step v3_large_$F     $SUBMIT build_features_v3_spark.py HI-Large $F $T
  step bq2_score_$F    $SCORE_SUBMIT bq2_score_spark.py HI-Large $F $T
done < <(chunks)
# 7. BQ2 analysis: tactics, weekly scorecards, alarms, early warning -> analytics/bq2/HI-Large.json
step bq2_analysis  $SUBMIT bq2_analysis_spark.py HI-Large $MON_FROM $MON_TO
echo "=== $(date '+%m-%d %H:%M') ALL DONE: terminate the cluster now"
# Laptop:  export AML_ROOT=s3://<bucket>   then   python pipeline/bq2_report.py HI-Large
