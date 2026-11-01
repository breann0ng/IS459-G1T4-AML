#!/bin/bash
# Round 3, step 2b: which of the three new feature groups did the work? (add-one test)
# Python only, on the primary node; uses the v3 tables already in S3 (no Spark).
# Start with:   nohup bash round3_step2b.sh > round3_step2b.log 2>&1 &
# Baseline (v2) = txn_p3m and all groups = txn_v3m already exist.

run () {
  echo "=== $(date '+%H:%M') start $1"
  "${@:2}" > "$1.log" 2>&1 && echo "=== $(date '+%H:%M') done  $1" || echo "=== $(date '+%H:%M') FAILED $1 (see $1.log)"
}
V3="--txn-table txn_features_v3 --stage2-features features_v3 --preset p3 --merge-layering"

run txn_v3m_x  python3 -u train_txn.py $V3 --new-groups x --config txn_v3m_x   # + unusual for this account
run txn_v3m_o  python3 -u train_txn.py $V3 --new-groups o --config txn_v3m_o   # + owner (accounts.csv)
run txn_v3m_t  python3 -u train_txn.py $V3 --new-groups t --config txn_v3m_t   # + new payment features

echo "=== $(date '+%H:%M') ALL DONE"
# Laptop: python txn_report.py txn_p3m txn_v3m_x txn_v3m_o txn_v3m_t txn_v3m > step2b_results.txt
