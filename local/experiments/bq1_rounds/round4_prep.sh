#!/bin/bash
# Round 4 in a NEW AWS account: only the part of rebuild_final.sh that round 4 needs
# (HI-Small + HI-Medium ETL and the txn_v3m validation run; no HI-Large, no final model),
# then the three round 4 runs of round4_grouping.sh. RESUMABLE like rebuild_final.sh, with the
# SAME progress markers: a later rebuild_final.sh skips every step finished here.
#
#   export AML_BUCKET=<your bucket name>
#   nohup bash round4_prep.sh > round4_prep.log 2>&1 &
#
# Needs (as rebuild_final.sh): ~/.kaggle/kaggle.json (legacy token), Java 17 on PATH, every
# pipeline/ script + shape_rules.py in this folder, and the cluster created with the bootstrap
# action s3://<bucket>/scripts/install_libs.sh (1 primary + 4 core m5.xlarge for the Spark steps)

BUCKET="${AML_BUCKET:?set AML_BUCKET first, e.g. export AML_BUCKET=pw-aml-g1t4-b}"
export AML_ROOT="s3://$BUCKET"
P="$AML_ROOT/progress/rebuild"
KAGGLE=https://www.kaggle.com/api/v1/datasets/download/ealtman2019/ibm-transactions-for-anti-money-laundering-aml
S3CONF="--conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider,software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider"
DRV="--driver-memory 6g --conf spark.driver.maxResultSize=2g --conf spark.sql.autoBroadcastJoinThreshold=-1"
SUBMIT="spark-submit $S3CONF $DRV"
mkdir -p logs
set -o pipefail

step () {   # step <name> <command ...>: skipped if already done, stops the run if it fails
  if aws s3 ls "$P/$1.done" > /dev/null 2>&1; then echo "=== skip $1 (already done)"; return 0; fi
  echo "=== $(date '+%m-%d %H:%M') start $1"
  "${@:2}" > "logs/$1.log" 2>&1; ok=$?
  aws s3 cp "logs/$1.log" "$AML_ROOT/logs/round4_prep/$1.log" > /dev/null   # keep logs after the cluster is gone
  if [ $ok -eq 0 ]; then
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

echo "=== $(date '+%m-%d %H:%M') bucket $AML_ROOT, round 4 prep (HI-Small + HI-Medium only)"
java -version 2>&1 | grep -q '"17' || { echo "Java 17 is not active: run the first-login setup"; exit 1; }
[ -f shape_rules.py ] || { echo "shape_rules.py missing: copy it next to group_stage2.py"; exit 1; }
pip3 install --user -q pandas pyarrow scikit-learn lightgbm networkx xxhash > logs/pip.log 2>&1 \
  || { echo "pip install failed (see logs/pip.log)"; exit 1; }

# 1. raw data (accounts.csv is needed by the v3 owner features)
for D in HI-Small HI-Medium; do
  step dl_trans_$D     download "${D}_Trans.csv"    "$AML_ROOT/raw/transactions/set=$D/${D}_Trans.csv"
  step dl_patterns_$D  download "${D}_Patterns.txt" "$AML_ROOT/raw/patterns/set=$D/${D}_Patterns.txt"
  step dl_accounts_$D  download "${D}_accounts.csv" "$AML_ROOT/raw/accounts/set=$D/${D}_accounts.csv"
done
# 2. clean + answer key, 3. features (whole datasets)
for D in HI-Small HI-Medium; do
  step clean_$D     $SUBMIT clean_transactions_spark.py $D
  step patterns_$D  $SUBMIT parse_patterns_spark.py $D
  step feat_$D      $SUBMIT build_features_spark.py $D
  step txn_$D       $SUBMIT build_txn_features_spark.py $D features_v2
  step v3_$D        $SUBMIT build_features_v3_spark.py $D
done
# 4. validation run txn_v3m: stage 1 alerts + threshold that round 4 groups and names
step validate  python3 -u train_txn.py --txn-table txn_features_v3 --stage2-features features_v3 --preset p3 --merge-layering --config txn_v3m
# 5. round 4 (same runs as round4_grouping.sh): today's linking vs longer, each with the baselines
step r4_mld3   python3 -u group_stage2.py txn_v3m --baselines --out-config txn_v3m_mld3_grp
step r4_mld10  python3 -u group_stage2.py txn_v3m --baselines --max-linked-days 10 --out-config txn_v3m_mld10_grp
step r4_mld20  python3 -u group_stage2.py txn_v3m --baselines --max-linked-days 20 --out-config txn_v3m_mld20_grp
echo "=== $(date '+%m-%d %H:%M') ALL DONE: terminate the cluster now"
# Laptop: python pipeline/txn_report.py txn_v3m_mld3_grp txn_v3m_mld10_grp txn_v3m_mld20_grp > results/experiments/round4_grouping_results.txt
