#!/bin/bash
# BQ2 (redone): "Are criminals changing their tactics over time, and given the changes, is our
# laundering detection rate affected?" RESUMABLE: every finished step leaves a marker in S3;
# after a lab timeout, start a new cluster, do the first-login setup, and run this again.
#
#   export AML_BUCKET=<your bucket name>
#   nohup bash run_bq2_ramp.sh > bq2r.log 2>&1 &
#
# Needs: SAML-D.csv at s3://<bucket>/raw/saml/, HI-Large cleaned (BQ1's rebuild_final.sh did it),
#        models/final/meta.json (BQ1's frozen recipe), Java 17, all pipeline/ and bq2/ scripts in
#        this folder, and the cluster created with the bootstrap action install_libs.sh

BUCKET="${AML_BUCKET:?set AML_BUCKET first, e.g. export AML_BUCKET=pw-aml-g1t4-d}"
export AML_ROOT="s3://$BUCKET"
P="$AML_ROOT/progress/bq2r"
S3CONF="--conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider,software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider"
DRV="--driver-memory 6g --conf spark.driver.maxResultSize=2g --conf spark.sql.autoBroadcastJoinThreshold=-1"
SUBMIT="spark-submit $S3CONF $DRV"
R=SAML-R; C=SAML-C
BASE_FROM=2022-10-03; BASE_TO=2023-01-29          # SAML training period = part 1 baseline
mkdir -p logs
set -o pipefail

step () {   # step <name> <command ...>: skipped if already done, stops the run if it fails
  if aws s3 ls "$P/$1.done" > /dev/null 2>&1; then echo "=== skip $1 (already done)"; return 0; fi
  echo "=== $(date '+%m-%d %H:%M') start $1"
  if "${@:2}" > "logs/$1.log" 2>&1; then
    echo done | aws s3 cp - "$P/$1.done" > /dev/null
    aws s3 cp "logs/$1.log" "$P/logs/$1.log" > /dev/null
    echo "=== $(date '+%m-%d %H:%M') done  $1"
  else
    aws s3 cp "logs/$1.log" "$P/logs/$1.FAILED.log" > /dev/null
    echo "=== $(date '+%m-%d %H:%M') FAILED $1 (see logs/$1.log); stopping"; exit 1
  fi
}
decision () {   # decision <field>: read models/bq2r/SAML-R/decision.json
  aws s3 cp "$AML_ROOT/models/bq2r/$R/decision.json" - 2>/dev/null | python3 -c "import json,sys;print(json.load(sys.stdin).get('$1',''))"
}

# copy the logs to S3 every 5 minutes, so a lab timeout loses at most 5 minutes of log
( while true; do aws s3 sync logs "$P/logs/live/" --quiet; aws s3 cp bq2r.log "$P/bq2r.log" --quiet 2>/dev/null; sleep 300; done ) &
SYNC=$!; trap 'kill $SYNC 2>/dev/null' EXIT

echo "=== $(date '+%m-%d %H:%M') bucket $AML_ROOT"
java -version 2>&1 | grep -q '"17' || { echo "Java 17 is not active: run the first-login setup"; exit 1; }
aws s3 ls "$AML_ROOT/raw/saml/SAML-D.csv" > /dev/null || { echo "upload SAML-D.csv to $AML_ROOT/raw/saml/ first"; exit 1; }
aws s3 ls "$AML_ROOT/models/final/meta.json" > /dev/null || echo "note: no models/final/meta.json, using train_txn.py's preset p3"
pip3 install --user -q pandas pyarrow scikit-learn scipy lightgbm networkx xxhash > logs/pip.log 2>&1 \
  || { echo "pip install failed (see logs/pip.log)"; exit 1; }

# 1. the test world: SAML-D with emerging tactics switched on gradually (SAML-R) + control (SAML-C)
step ramp_build    $SUBMIT saml_ramp_spark.py
# 2. the UNCHANGED BQ1 steps: clean + answer key for both, features for the ramp (~20 min)
for D in $R $C; do
  step clean_$D     $SUBMIT clean_transactions_spark.py $D
  step patterns_$D  $SUBMIT parse_patterns_spark.py $D
done
step feat_$R  $SUBMIT build_features_spark.py $R
step txn_$R   $SUBMIT build_txn_features_spark.py $R features_v2
step v3_$R    $SUBMIT build_features_v3_spark.py $R
# 3. PART 1 · are tactics changing? (confirmed cases only, no model)
step part1_$R        $SUBMIT bq2_part1_tactics_spark.py $R $BASE_FROM $BASE_TO
step part1_$C        $SUBMIT bq2_part1_tactics_spark.py $C $BASE_FROM $BASE_TO
step part1_HI-Large  $SUBMIT bq2_part1_tactics_spark.py HI-Large 2022-08-29 2022-09-11
# 4. PART 2 · is detection affected? v0 = BQ1 recipe trained before any emerging tactic
step train_v0  python3 -u bq2_part2_train.py $R v0 $BASE_TO
step score_v0  python3 -u bq2_part2_score.py $R v0 2023-01-30
step eval_v0   python3 -u bq2_part2_eval.py $R
# 5. the alert -> retrain with the cases confirmed so far (v1) -> score from deployment -> compare
if [ "$(decision alert)" = "True" ]; then
  CUT=$(decision retrain_to); DEPLOY=$(decision deploy_from)
  echo "=== alert in week $(decision alert_week): retrain with cases up to $CUT, deploy v1 from $DEPLOY"
  [[ "$DEPLOY" > "2023-08-13" ]] && { echo "=== v1 would deploy after the data ends (23 Aug 2023): move DOSES earlier in bq2_common.py"; exit 0; }
  step train_v1  python3 -u bq2_part2_train.py $R v1 $CUT
  step score_v1  python3 -u bq2_part2_score.py $R v1 $DEPLOY
  step eval_v1   python3 -u bq2_part2_eval.py $R
else
  echo "=== no alert from the v0 rules: nothing to retrain (see logs/eval_v0.log)"
fi
echo "=== $(date '+%m-%d %H:%M') BQ2 ALL DONE: terminate the cluster now"
# Laptop:  set AML_ROOT=s3://<bucket>   then   python bq2\bq2_ramp_report.py > results\bq2\bq2_ramp_report.txt
