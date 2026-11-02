#!/bin/bash
# En Rui's track (different from round 4 grouping on breann-test-precision):
#   stage 1  alert expansion: follow the money from confident alerts (train_txn.py --expand)
#   stage 2  context features: payments that cross the group border (group_stage2.py --context)
#            + one name per group (--group-vote), "unsure" (--min-confidence), top-2 metric
# Validation days only (HI-Small 9-10 Sep, HI-Medium 13-16 Sep); HI-Large untouched.
# Every stage 2 run uses the round 1 winner settings (s2_combo1) + the change in its name.
#
# On the EMR primary node, in ~/pipeline (needs txn_v3m from rebuild_final.sh TRAIN_ONLY=1):
#   export AML_ROOT=s3://<bucket>
#   setsid nohup bash er_round3.sh > er_round3.log 2>&1 < /dev/null &
#   grep -E "start|done|FAILED" er_round3.log            progress (about 70 minutes)
: "${AML_ROOT:?set AML_ROOT first, e.g. export AML_ROOT=s3://4am-bucket1}"
run () {   # run <name> <command ...>: logs to <name>.log (copied to S3); a failure does not stop the rest
  echo "=== $(date '+%H:%M') start $1"
  if "${@:2}" > "$1.log" 2>&1; then echo "=== $(date '+%H:%M') done  $1"
  else echo "=== $(date '+%H:%M') FAILED $1 (tail $1.log)"; fi
  aws s3 cp "$1.log" "$AML_ROOT/logs/er_round3/$1.log" > /dev/null
}
C="--link-days 3 --param n_estimators=800 --param learning_rate=0.03 --keep-rates 1,0.7,0.5,0.3"

# ---- stage 1: same model as txn_v3m + alert expansion (LOW chosen by validation F1)
run s1_expand  python3 -u train_txn.py --txn-table txn_features_v3 --stage2-features features_v3 \
                 --preset p3 --merge-layering --expand auto --config txn_v3m_xp

# ---- stage 2 on today's stage 1 alerts (one change each vs s2_combo1)
run s2_combo1      python3 -u group_stage2.py txn_v3m $C --out-config s2_combo1        # + top-2
run s2_vote        python3 -u group_stage2.py txn_v3m $C --group-vote --out-config s2_vote
run s2_conf50      python3 -u group_stage2.py txn_v3m $C --min-confidence 0.5 --out-config s2_conf50
run s2_ctx         python3 -u group_stage2.py txn_v3m $C --context --out-config s2_ctx
run s2_ctx_vote    python3 -u group_stage2.py txn_v3m $C --context --group-vote --out-config s2_ctx_vote

# ---- stage 2 on the expanded alerts
run s2_xp          python3 -u group_stage2.py txn_v3m_xp $C --out-config s2_xp
run s2_xp_ctx      python3 -u group_stage2.py txn_v3m_xp $C --context --out-config s2_xp_ctx
run s2_xp_ctx_vote python3 -u group_stage2.py txn_v3m_xp $C --context --group-vote --out-config s2_xp_ctx_vote

# ---- reports (also copied to S3 for the laptop)
python3 stage2_report.py > er_round3_summary.txt 2>&1
python3 txn_report.py txn_v3m txn_v3m_xp s2_combo1_grp s2_vote_grp s2_conf50_grp s2_ctx_grp s2_ctx_vote_grp \
    s2_xp_grp s2_xp_ctx_grp s2_xp_ctx_vote_grp > er_round3_results.txt 2>&1
aws s3 cp er_round3_summary.txt "$AML_ROOT/results/er_round3_summary.txt"
aws s3 cp er_round3_results.txt "$AML_ROOT/results/er_round3_results.txt"
aws s3 cp ~/app/data/stage2_data.json "$AML_ROOT/results/stage2_data.json"
echo "=== $(date '+%H:%M') ALL DONE: terminate the cluster"
