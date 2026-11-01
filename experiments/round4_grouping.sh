#!/bin/bash
# Round 4: longer cross-day linking for the stage 2 groups + baseline namers, graded end to end
# on the validation alerts of txn_v3m (HI-Small 9-10 Sep, HI-Medium 13-16 Sep). HI-Large untouched.
# Why: analysis/typology_attribution.ipynb §13 - with MAX_LINKED_DAYS = 3, 59% of HI-Medium
# schemes are split across groups; 10-20 keeps most of them whole without lowering purity.
#
# Python only, on the primary node (1 core node is enough). Needs group_stage2.py,
# scheme_stage2.py and shape_rules.py next to it, and txn_v3m in analytics/ (rebuild_final.sh
# step "validate"). Start with:
#   export AML_ROOT=s3://<bucket>
#   nohup bash round4_grouping.sh > round4_grouping.log 2>&1 &
: "${AML_ROOT:?set AML_ROOT first, e.g. export AML_ROOT=s3://pw-aml-g1t4-b}"
run () {
  echo "=== $(date '+%H:%M') start $1"
  "${@:2}" > "$1.log" 2>&1 && echo "=== $(date '+%H:%M') done  $1" || echo "=== $(date '+%H:%M') FAILED $1 (see $1.log)"
  aws s3 cp "$1.log" "$AML_ROOT/logs/round4/$1.log" > /dev/null    # keep the log after the cluster is gone
}
run grp_mld3   python3 -u group_stage2.py txn_v3m --baselines --out-config txn_v3m_mld3_grp    # today's setting + baselines
run grp_mld10  python3 -u group_stage2.py txn_v3m --baselines --max-linked-days 10 --out-config txn_v3m_mld10_grp
run grp_mld20  python3 -u group_stage2.py txn_v3m --baselines --max-linked-days 20 --out-config txn_v3m_mld20_grp
echo "=== $(date '+%H:%M') ALL DONE: terminate the cluster now"
# Laptop: python pipeline/txn_report.py txn_v3m_mld3_grp txn_v3m_mld10_grp txn_v3m_mld20_grp > results/experiments/round4_grouping_results.txt
