#!/bin/bash
# BQ1 final pipeline, rebuilt from scratch in a NEW AWS account, then tested once on a
# 2-week window of HI-Large. RESUMABLE: every finished step leaves a marker in S3; after a
# lab timeout, start a new cluster, do the first-login setup, and run this again.
#
#   export AML_BUCKET=<your bucket name>
#   nohup bash rebuild_final.sh > rebuild.log 2>&1 &
#   TRAIN_ONLY=1 nohup bash rebuild_final.sh > rebuild.log 2>&1 &     experiments: no HI-Large
#
# Needs: ~/.kaggle/kaggle.json (legacy token), Java 17 on PATH, all scripts in this folder,
#        and the cluster created with the bootstrap action s3://<bucket>/scripts/install_libs.sh

BUCKET="${AML_BUCKET:?set AML_BUCKET first, e.g. export AML_BUCKET=pw-aml-g1t4-b}"
export AML_ROOT="s3://$BUCKET"
P="$AML_ROOT/progress/rebuild"
WINDOW_FROM=2022-09-12          # HI-Large test window (Monday to Sunday, two weeks)
WINDOW_TO=2022-09-25
KAGGLE=https://www.kaggle.com/api/v1/datasets/download/ealtman2019/ibm-transactions-for-anti-money-laundering-aml
S3CONF="--conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider,software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider"
DRV="--driver-memory 6g --conf spark.driver.maxResultSize=2g --conf spark.sql.autoBroadcastJoinThreshold=-1"
SUBMIT="spark-submit $S3CONF $DRV"
mkdir -p logs
set -o pipefail

step () {   # step <name> <command ...>: skipped if already done, stops the run if it fails
  if aws s3 ls "$P/$1.done" > /dev/null 2>&1; then echo "=== skip $1 (already done)"; return 0; fi
  echo "=== $(date '+%m-%d %H:%M') start $1"
  if "${@:2}" > "logs/$1.log" 2>&1; then
    echo done | aws s3 cp - "$P/$1.done" > /dev/null
    echo "=== $(date '+%m-%d %H:%M') done  $1"
  else
    echo "=== $(date '+%m-%d %H:%M') FAILED $1 (see logs/$1.log); stopping"; exit 1
  fi
}

download () {   # download <kaggle file> <s3 target>: streamed straight into S3, nothing on disk
  U=$(python3 -c "import json;print(json.load(open('$HOME/.kaggle/kaggle.json'))['username'])") || return 1
  K=$(python3 -c "import json;print(json.load(open('$HOME/.kaggle/kaggle.json'))['key'])") || return 1
  curl -sfL -u "$U:$K" "$KAGGLE/$1" | aws s3 cp - "$2" --expected-size 30000000000 || return 1
  head=$(aws s3 cp "$2" - 2>/dev/null | head -c 60)
  echo "first bytes of $1: $head"
  case "$head" in Timestamp*|BEGIN*|Bank\ Name*) return 0 ;; *) echo "unexpected content (zip or error page?)"; return 1 ;; esac
}

echo "=== $(date '+%m-%d %H:%M') bucket $AML_ROOT, HI-Large test window $WINDOW_FROM to $WINDOW_TO"
java -version 2>&1 | grep -q '"17' || { echo "Java 17 is not active: run the first-login setup"; exit 1; }
pip3 install --user -q pandas pyarrow scikit-learn lightgbm networkx xxhash > logs/pip.log 2>&1 \
  || { echo "pip install failed (see logs/pip.log)"; exit 1; }

# TRAIN_ONLY=1: build HI-Small + HI-Medium, run the stage 1 and stage 2 baselines, stop
# before anything touches HI-Large (for experiments)
SETS="HI-Small HI-Medium HI-Large"
[ -n "$TRAIN_ONLY" ] && SETS="HI-Small HI-Medium"
# 1. raw data from Kaggle
for D in $SETS; do
  step dl_trans_$D     download "${D}_Trans.csv"    "$AML_ROOT/raw/transactions/set=$D/${D}_Trans.csv"
  step dl_patterns_$D  download "${D}_Patterns.txt" "$AML_ROOT/raw/patterns/set=$D/${D}_Patterns.txt"
  step dl_accounts_$D  download "${D}_accounts.csv" "$AML_ROOT/raw/accounts/set=$D/${D}_accounts.csv"
done
# 2. clean + answer key
for D in $SETS; do
  step clean_$D     $SUBMIT clean_transactions_spark.py $D
  step patterns_$D  $SUBMIT parse_patterns_spark.py $D
done
# 3. training data: HI-Small + HI-Medium, whole datasets
for D in HI-Small HI-Medium; do
  step feat_$D  $SUBMIT build_features_spark.py $D
  step txn_$D   $SUBMIT build_txn_features_spark.py $D features_v2
  step v3_$D    $SUBMIT build_features_v3_spark.py $D
done
# 4. validation run (threshold + check against round 3), 5. frozen models
step validate     python3 -u train_txn.py --txn-table txn_features_v3 --stage2-features features_v3 --preset p3 --merge-layering --config txn_v3m
if [ -n "$TRAIN_ONLY" ]; then
  step grp_baseline  python3 -u group_stage2.py txn_v3m
  echo "=== $(date '+%m-%d %H:%M') TRAIN_ONLY done: baselines txn_v3m + txn_v3m_grp saved, HI-Large not touched"
  exit 0
fi
step final_train  python3 -u final_train.py txn_v3m
# 6. HI-Large test window (each chunk reads 21 days of history before it)
step feat_large   $SUBMIT build_features_spark.py HI-Large $WINDOW_FROM $WINDOW_TO
step txn_large    $SUBMIT build_txn_features_spark.py HI-Large features_v2 $WINDOW_FROM $WINDOW_TO
step v3_large     $SUBMIT build_features_v3_spark.py HI-Large $WINDOW_FROM $WINDOW_TO
# 7. score in Spark: LightGBM / pyarrow are installed on every node by the bootstrap action
#    (install_libs.sh), so the executors use the normal python3
step score  $SUBMIT --conf spark.pyspark.python=/usr/bin/python3 --conf spark.pyspark.driver.python=/usr/bin/python3 --conf spark.executor.memory=5g --conf spark.executor.memoryOverhead=4g \
            score_final_spark.py HI-Large $WINDOW_FROM $WINDOW_TO
step name   python3 -u final_group_namer.py HI-Large
echo "=== $(date '+%m-%d %H:%M') ALL DONE: terminate the cluster now"
# Laptop:  set AML_ROOT=s3://<bucket>   then   python final_report.py HI-Large > final_report.txt
