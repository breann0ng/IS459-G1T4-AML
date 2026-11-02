#!/bin/bash
# Stage 2 round 2: one name per group, "unsure" option, top-2 metric. Validation only, no HI-Large.
# Every run = the round 1 winner (s2_combo1) + one change. On the EMR primary node, in ~/pipeline:
#   export AML_ROOT=s3://<bucket>
#   setsid nohup bash stage2_round2.sh > s2_round2.log 2>&1 < /dev/null &
set -e
R="python3 -u group_stage2.py txn_v3m --link-days 3 --param n_estimators=800 --param learning_rate=0.03 --keep-rates 1,0.7,0.5,0.3"
$R                                   --out-config s2_combo1      # rerun: same scores, now with top-2
$R --group-vote                      --out-config s2_vote
$R --min-confidence 0.4              --out-config s2_conf40
$R --min-confidence 0.5              --out-config s2_conf50
$R --min-confidence 0.6              --out-config s2_conf60
$R --group-vote --min-confidence 0.5 --out-config s2_vote_conf50
echo "=== round 2 done"
