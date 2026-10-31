#!/bin/bash
# Round 3, step 3: group-level pattern naming, fixed and graded end to end.
# Python only, on the primary node (1 core node is enough). Needs scheme_stage2.py next to it.
# Start with:   nohup bash round3_step3.sh > round3_step3.log 2>&1 &
run () {
  echo "=== $(date '+%H:%M') start $1"
  "${@:2}" > "$1.log" 2>&1 && echo "=== $(date '+%H:%M') done  $1" || echo "=== $(date '+%H:%M') FAILED $1 (see $1.log)"
}
run grp_v3m  python3 -u group_stage2.py txn_v3m        # stage 1 = best so far (payment model p3, v3, 7 classes)
echo "=== $(date '+%H:%M') ALL DONE"
# Laptop: python txn_report.py txn_v3m_grp > step3_results.txt
