#!/bin/bash
# En Rui round 5: stop "layering" being the catch-all name. On HI-Large (enrui_breann_mld20) the
# namer called 13,553 alerts layering for 9,713 true ones: about half of the cycles and random
# schemes, and ~30% of fan-in / fan-out, were named layering. --class-scale layering=F multiplies
# the layering probability by F before naming, so layering has to win more clearly.
# Same setup as enrui_breann_mld20 otherwise. Validation days only; HI-Large untouched.
#
# On the EMR primary node, in ~/pipeline (needs txn_v3m_xp from er_round3.sh):
#   export AML_ROOT=s3://<bucket>
#   setsid nohup bash er_round5.sh > er_round5.log 2>&1 < /dev/null &
#   grep -E "start|done|FAILED" er_round5.log            progress (about 50 minutes)
: "${AML_ROOT:?set AML_ROOT first, e.g. export AML_ROOT=s3://4am-bucket1}"
run () {   # run <name> <command ...>: logs to <name>.log (copied to S3); a failure does not stop the rest
  echo "=== $(date '+%H:%M') start $1"
  if "${@:2}" > "$1.log" 2>&1; then echo "=== $(date '+%H:%M') done  $1"
  else echo "=== $(date '+%H:%M') FAILED $1 (tail $1.log)"; fi
  aws s3 cp "$1.log" "$AML_ROOT/logs/er_round5/$1.log" > /dev/null
}
C="--link-days 3 --param n_estimators=800 --param learning_rate=0.03 --keep-rates 1,0.7,0.5,0.3"
X="$C --context --group-vote --max-linked-days 20"

for f in 80 65 50; do
  run enrui_breann_lay$f  python3 -u group_stage2.py txn_v3m_xp $X --class-scale layering=0.$f \
                            --out-config enrui_breann_lay$f
done

python3 stage2_report.py > er_round5_summary.txt 2>&1
python3 txn_report.py enrui_breann_mld20_grp enrui_breann_lay80_grp enrui_breann_lay65_grp \
    enrui_breann_lay50_grp > er_round5_results.txt 2>&1
aws s3 cp er_round5_summary.txt "$AML_ROOT/results/er_round5_summary.txt"
aws s3 cp er_round5_results.txt "$AML_ROOT/results/er_round5_results.txt"
aws s3 cp ~/app/data/stage2_data.json "$AML_ROOT/results/stage2_data.json"
echo "=== $(date '+%H:%M') ALL DONE"
