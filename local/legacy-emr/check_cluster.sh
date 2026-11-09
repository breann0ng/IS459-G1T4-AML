#!/bin/bash
# Run on the EMR primary node before rebuild_bq2.sh: checks everything the run needs.
#   export AML_BUCKET=<your bucket>; bash check_cluster.sh
ok=1
say () { printf '%-42s %s\n' "$1" "$2"; }
if java -version 2>&1 | grep -q '"17'; then say "Java 17" OK; else
  J=$(ls -d /usr/lib/jvm/*17* 2>/dev/null | head -1)
  if [ -n "$J" ]; then
    echo "export JAVA_HOME=$J; export PATH=\$JAVA_HOME/bin:\$PATH" >> ~/.bashrc
    say "Java 17" "FIXED: added $J to ~/.bashrc -> run: source ~/.bashrc, then this check again"; ok=0
  else say "Java 17" "MISSING: create the cluster with release emr-7.x"; ok=0; fi
fi
command -v spark-submit > /dev/null && say "spark-submit" OK || { say "spark-submit" "MISSING: choose the Spark application"; ok=0; }
python3 -c "import lightgbm, pyarrow, pandas" 2>/dev/null && say "lightgbm / pyarrow / pandas" OK \
  || { say "lightgbm / pyarrow / pandas" "MISSING: bootstrap action install_libs.sh did not run"; ok=0; }
[ -n "$AML_BUCKET" ] && aws s3 ls "s3://$AML_BUCKET/" > /dev/null 2>&1 && say "bucket s3://$AML_BUCKET" OK \
  || { say "bucket" "NOT REACHABLE: export AML_BUCKET=<name> (no s3://)"; ok=0; }
python3 -c "import json,os;k=json.load(open(os.path.expanduser('~/.kaggle/kaggle.json')));assert k['username'] and k['key']" 2>/dev/null \
  && say "~/.kaggle/kaggle.json" OK || { say "~/.kaggle/kaggle.json" "missing (only needed for Kaggle downloads; fine to ignore here)"; }
for f in run_bq2_ibm.sh build_features_spark.py train_txn.py bq2_ibm_score_spark.py bq2_ibm_part2_prep_spark.py bq2_part1_tactics_spark.py bq2_part2_eval.py bq2_common.py; do
  [ -f "$f" ] || { say "$f" "MISSING in $(pwd): copy the scripts from S3"; ok=0; }
done
[ $ok = 1 ] && echo "All good: setsid nohup bash run_bq2_ibm.sh > run_bq2_ibm.log 2>&1 < /dev/null &" || echo "Fix the lines above first."
