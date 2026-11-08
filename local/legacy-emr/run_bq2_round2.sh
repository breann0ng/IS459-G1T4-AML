#!/bin/bash
# BQ2 round 2: after retraining, move the baselines to the new normal and keep monitoring.
#   Part 1  new data baseline = Weeks 34-39 (all four new tactics fully in); any later change is
#           measured against that (tables bq2r_part1_*_nb)
#   Part 2  monitor v1 against v1's OWN validation (decision_v1.json); if an alert is raised,
#           retrain v2 on every case confirmed by then, score it, and compare v1 with v2
# Runs after run_bq2_ramp.sh (needs its features, v1 and v1's scores in S3). RESUMABLE like it.
#
#   export AML_BUCKET=<your bucket name>
#   nohup bash run_bq2_round2.sh > bq2r2.log 2>&1 &

BUCKET="${AML_BUCKET:?set AML_BUCKET first, e.g. export AML_BUCKET=pw-aml-g1t4-d}"
export AML_ROOT="s3://$BUCKET"
P="$AML_ROOT/progress/bq2r"
S3CONF="--conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider,software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider"
DRV="--driver-memory 6g --conf spark.driver.maxResultSize=2g --conf spark.sql.autoBroadcastJoinThreshold=-1"
SUBMIT="spark-submit $S3CONF $DRV"
R=SAML-R
NB_FROM=2023-05-22; NB_TO=2023-07-02             # new data baseline: Weeks 34-39
LAST_DEPLOY=2023-08-07                            # a v2 deployed later leaves under 3 weeks to test
mkdir -p logs
set -o pipefail

step () {
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
decision () {   # decision <field>: read models/bq2r/SAML-R/decision_v1.json (stops if it is missing)
  local j; j=$(aws s3 cp "$AML_ROOT/models/bq2r/$R/decision_v1.json" - 2>/dev/null)
  [ -z "$j" ] && { echo "decision_v1.json missing: the v1 check did not run" >&2; exit 1; }
  echo "$j" | python3 -c "import json,sys;print(json.load(sys.stdin).get('$1',''))"
}

( while true; do aws s3 sync logs "$P/logs/live/" --quiet; aws s3 cp bq2r2.log "$P/bq2r2.log" --quiet 2>/dev/null; sleep 300; done ) &
SYNC=$!; trap 'kill $SYNC 2>/dev/null' EXIT

echo "=== $(date '+%m-%d %H:%M') bucket $AML_ROOT · round 2"
java -version 2>&1 | grep -q '"17' || { echo "Java 17 is not active: run the first-login setup"; exit 1; }
aws s3 ls "$P/score_v1.done" > /dev/null || { echo "round 1 is not finished (no score_v1): run run_bq2_ramp.sh first"; exit 1; }
pip3 install --user -q pandas pyarrow scikit-learn lightgbm networkx xxhash > logs/pip.log 2>&1 \
  || { echo "pip install failed (see logs/pip.log)"; exit 1; }

# Part 1 with the new baseline
step r2_part1_nb_$R $SUBMIT bq2_part1_tactics_spark.py $R $NB_FROM $NB_TO _nb
# Part 2: monitor v1 against its own validation
step r2_eval_v1    python3 -u bq2_part2_eval.py $R v1
ALERT=$(decision alert) || exit 1
if [ "$ALERT" = "True" ]; then
  CUT=$(decision retrain_to) || exit 1; DEPLOY=$(decision deploy_from) || exit 1
  echo "=== alert on v1 in week $(decision alert_week): retrain v2 with cases up to $CUT, deploy from $DEPLOY"
  [[ "$DEPLOY" > "$LAST_DEPLOY" ]] && { echo "=== v2 would deploy after $LAST_DEPLOY: too few weeks left in the data to test it"; exit 0; }
  step r2_train_v2 python3 -u bq2_part2_train.py $R v2 $CUT
  step r2_score_v2 python3 -u bq2_part2_score.py $R v2 $DEPLOY
  step r2_eval_v1v2 python3 -u bq2_part2_eval.py $R v1
else
  echo "=== no alert on v1: the retrained model holds up, no second retraining needed"
fi
echo "=== $(date '+%m-%d %H:%M') BQ2 ROUND 2 DONE: terminate the cluster now"
