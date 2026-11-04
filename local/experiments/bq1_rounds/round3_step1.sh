#!/bin/bash
# Round 2 baseline (end to end) + round 3 step 1, one after another, on the EMR primary node.
# Start with:   nohup bash round3_step1.sh > round3_step1.log 2>&1 &
# Each run writes its own log; a failed run does not stop the next one.

run () {   # run <log name> <command ...>
  echo "=== $(date '+%H:%M') start $1"
  "${@:2}" > "$1.log" 2>&1 && echo "=== $(date '+%H:%M') done  $1" || echo "=== $(date '+%H:%M') FAILED $1 (see $1.log)"
}

# Step 0: round 2 baseline, stage 2 graded end to end
run txn_e2e       python3 -u train_txn.py --config txn_e2e
# Step 1a: tune the payment model
run txn_p1        python3 -u train_txn.py --preset p1 --config txn_p1
run txn_p2        python3 -u train_txn.py --preset p2 --config txn_p2
run txn_p3        python3 -u train_txn.py --preset p3 --config txn_p3
# Step 1b: how much does it rely on payment format (ACH)?
run txn_noformat  python3 -u train_txn.py --drop-format --config txn_noformat
# Step 1c: bipartite + stack -> layering, on the new features
run txn_merge     python3 -u train_txn.py --merge-layering --config txn_merge
# Step 1d: what does unclassified laundering look like?
run unclassified  python3 -u unclassified_profile.py
# Round 1 graded the same way (end to end), for the comparison
run round1_e2e    python3 -u stage_report.py lgbm_p2

echo "=== $(date '+%H:%M') ALL DONE"
