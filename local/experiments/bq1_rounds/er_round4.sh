#!/bin/bash
# En Rui round 4: combine round 3's winner (s2_xp_ctx_vote: alert expansion + context + group vote)
# with Breann's longer cross-day linking (round4_grouping.sh on breann-test-precision:
# MAX_LINKED_DAYS 10 / 20 instead of 3). Validation days only; HI-Large untouched.
#
# On the EMR primary node, in ~/pipeline (needs txn_v3m_xp from er_round3.sh):
#   export AML_ROOT=s3://<bucket>
#   setsid nohup bash er_round4.sh > er_round4.log 2>&1 < /dev/null &
#   grep -E "start|done|FAILED" er_round4.log            progress (about 25 minutes)
: "${AML_ROOT:?set AML_ROOT first, e.g. export AML_ROOT=s3://4am-bucket1}"
run () {   # run <name> <command ...>: logs to <name>.log (copied to S3); a failure does not stop the rest
  echo "=== $(date '+%H:%M') start $1"
  if "${@:2}" > "$1.log" 2>&1; then echo "=== $(date '+%H:%M') done  $1"
  else echo "=== $(date '+%H:%M') FAILED $1 (tail $1.log)"; fi
  aws s3 cp "$1.log" "$AML_ROOT/logs/er_round4/$1.log" > /dev/null
}
C="--link-days 3 --param n_estimators=800 --param learning_rate=0.03 --keep-rates 1,0.7,0.5,0.3"
X="$C --context --group-vote"

run enrui_breann_mld10  python3 -u group_stage2.py txn_v3m_xp $X --max-linked-days 10 --out-config enrui_breann_mld10
run enrui_breann_mld20  python3 -u group_stage2.py txn_v3m_xp $X --max-linked-days 20 --out-config enrui_breann_mld20

python3 stage2_report.py > enrui_breann_summary.txt 2>&1
python3 txn_report.py s2_xp_ctx_vote_grp enrui_breann_mld10_grp enrui_breann_mld20_grp > enrui_breann_results.txt 2>&1
aws s3 cp enrui_breann_summary.txt "$AML_ROOT/results/enrui_breann_summary.txt"
aws s3 cp enrui_breann_results.txt "$AML_ROOT/results/enrui_breann_results.txt"
aws s3 cp ~/app/data/stage2_data.json "$AML_ROOT/results/stage2_data.json"
echo "=== $(date '+%H:%M') ALL DONE"
