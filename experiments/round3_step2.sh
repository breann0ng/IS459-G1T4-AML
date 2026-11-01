#!/bin/bash
# Round 3, step 2: add the "unusual for this account" + owner features, then compare v2 vs v3.
# Needs: raw/accounts/set=HI-Small/ and set=HI-Medium/ in S3, Java 17 set up.
# Start with:   nohup bash round3_step2.sh > round3_step2.log 2>&1 &

S3CONF="--conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider,software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider"
DRV="--driver-memory 6g --conf spark.driver.maxResultSize=2g --conf spark.sql.autoBroadcastJoinThreshold=-1"

run () {   # run <log name> <command ...>
  echo "=== $(date '+%H:%M') start $1"
  "${@:2}" > "$1.log" 2>&1 && echo "=== $(date '+%H:%M') done  $1" || echo "=== $(date '+%H:%M') FAILED $1 (see $1.log)"
}

# Spark: build features_v3 and txn_features_v3 (stop here if either fails)
for D in HI-Small HI-Medium; do
  run v3_$D spark-submit $S3CONF $DRV build_features_v3_spark.py $D
  grep -q "wrote txn_features_v3" v3_$D.log || { echo "=== v3 build failed for $D, stopping"; exit 1; }
done

# Train + grade end to end. Same stage 1 setting (p3) everywhere; only the features differ.
run txn_v3   python3 -u train_txn.py --txn-table txn_features_v3 --stage2-features features_v3 --preset p3 --config txn_v3
run txn_p3m  python3 -u train_txn.py --preset p3 --merge-layering --config txn_p3m
run txn_v3m  python3 -u train_txn.py --txn-table txn_features_v3 --stage2-features features_v3 --preset p3 --merge-layering --config txn_v3m

echo "=== $(date '+%H:%M') ALL DONE"
# Compare (laptop is fine):  python txn_report.py txn_p3 txn_v3 txn_p3m txn_v3m > step2_results.txt
