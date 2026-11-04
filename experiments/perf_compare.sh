#!/bin/bash
# Before vs after the Spark optimisations, on a laptop (Spark local mode, no cluster):
# runs the five Spark steps of round4_prep.sh with the OLD pipeline/ (from git, default main)
# and the NEW one (this working tree, uncommitted edits included), each into its own folder
# with Spark event logs on. Then, from the repo root:
#   python experiments/spark_perf_report.py > results/experiments/perf_comparison.txt
#
#   bash experiments/perf_compare.sh                              HI-Small, 1 run each
#   DATASET=HI-Medium REPEATS=2 KEEP_OUTPUT=0 bash experiments/perf_compare.sh
#   STEPS="clean features_v2" bash experiments/perf_compare.sh    only some steps (the ones
#                                                                 before must have run once)
#   VERSIONS="new2" NOTE="rebalance writes" bash experiments/perf_compare.sh
#                       a further round of changes: the working tree is saved as version new2,
#                       next to the old / new runs already saved (the report shows them all)
# Settings (env): DATASET, REPEATS (runs alternate between VERSIONS), VERSIONS (default
# "old new"; "old" = BASE from git, any other name = the working tree now), NOTE (shown with
# the version in the report), BASE (git ref of the old code),
# CORES, DRIVER_MEM, AML_SHUFFLE (shuffle partitions, same for both), KEEP_OUTPUT (0 = delete
# a version's output after each full run: HI-Medium needs ~10+ GB per version).
#
# Browse a version's runs in the Spark UI (History Server, http://localhost:18080):
#   SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file://$PWD/data/perf/eventlogs/HI-Small/new" \
#     "$(python3 -c 'import pyspark,os;print(os.path.dirname(pyspark.__file__))')/sbin/start-history-server.sh"
# (stop-history-server.sh in the same folder; restart it to switch to .../old)

set -o pipefail
REPO=$(git rev-parse --show-toplevel) || exit 1
DATASET=${DATASET:-HI-Small}
REPEATS=${REPEATS:-1}
BASE=${BASE:-main}
CORES=${CORES:-$(sysctl -n hw.ncpu 2>/dev/null || nproc)}
DRIVER_MEM=${DRIVER_MEM:-5g}
export AML_SHUFFLE=${AML_SHUFFLE:-16}         # EMR uses 400; far too many tasks for one laptop
KEEP_OUTPUT=${KEEP_OUTPUT:-1}
STEPS=${STEPS:-"clean patterns features_v2 txn_features features_v3"}
VERSIONS=${VERSIONS:-"old new"}
NOTE=${NOTE:-}
PERF=$REPO/data/perf                           # data/ is not committed
EV=$PERF/eventlogs/$DATASET

script_of () {   # step name -> script + arguments (as round4_prep.sh)
  case $1 in
    clean)        echo "clean_transactions_spark.py $DATASET" ;;
    patterns)     echo "parse_patterns_spark.py $DATASET" ;;
    features_v2)  echo "build_features_spark.py $DATASET" ;;
    txn_features) echo "build_txn_features_spark.py $DATASET features_v2" ;;
    features_v3)  echo "build_features_v3_spark.py $DATASET" ;;
    *) echo "unknown step $1" >&2; return 1 ;;
  esac
}

java -version > /dev/null 2>&1 || { echo "java not found: check JAVA_HOME ($JAVA_HOME)"; exit 1; }
command -v spark-submit > /dev/null || { echo "spark-submit not found: activate .venv"; exit 1; }
for f in Trans.csv Patterns.txt accounts.csv; do
  [ -f "$REPO/data/${DATASET}_$f" ] || { echo "missing data/${DATASET}_$f"; exit 1; }
done

# 1. code snapshots of the versions run now: old = BASE from git, any other = the working tree
#    now (later edits don't leak in). Other versions' code and runs are left as they are.
mkdir -p "$EV"; touch "$EV/versions.tsv"
dirty=$(git -C "$REPO" status --porcelain -- pipeline | grep -q . && echo " + uncommitted changes")
for v in $VERSIONS; do
  rm -rf "$PERF/code/$v"; mkdir -p "$PERF/code/$v"
  if [ "$v" = old ]; then
    git -C "$REPO" archive "$BASE" pipeline | tar -x -C "$PERF/code/old" || exit 1
    label="$BASE @ $(git -C "$REPO" rev-parse --short "$BASE")"
  else
    cp -R "$REPO/pipeline" "$PERF/code/$v/"
    label="working tree @ $(git -C "$REPO" rev-parse --short HEAD)$dirty${NOTE:+ · $NOTE}"
  fi
  # versions.tsv keeps one line per version, in the order versions were first run
  if grep -q "^$v	" "$EV/versions.tsv"; then
    awk -F'\t' -v OFS='\t' -v v="$v" -v l="$label" '$1 == v { $2 = l } 1' "$EV/versions.tsv" > "$EV/versions.tmp"
    mv "$EV/versions.tmp" "$EV/versions.tsv"
  else
    printf "%s\t%s\n" "$v" "$label" >> "$EV/versions.tsv"
  fi
done

# 2. one data root per version, raw files linked (not copied) in the layout the scripts expect
for v in $VERSIONS; do
  for k in transactions:Trans.csv patterns:Patterns.txt accounts:accounts.csv; do
    mkdir -p "$PERF/$v/raw/${k%%:*}/set=$DATASET"
    ln -sf "$REPO/data/${DATASET}_${k#*:}" "$PERF/$v/raw/${k%%:*}/set=$DATASET/"
  done
  mkdir -p "$EV/$v" "$PERF/logs"
done
[ -f "$EV/index.tsv" ] || printf "version\tstep\trun\tapp\tstatus\tsubmit_s\n" > "$EV/index.tsv"
last=$(awk -F'\t' 'NR > 1 && $3 > m { m = $3 } END { print m + 0 }' "$EV/index.tsv")   # runs already saved

echo "=== $DATASET · versions $VERSIONS · local[$CORES] · AML_SHUFFLE=$AML_SHUFFLE"
for r in $(seq $((last + 1)) $((last + REPEATS))); do
  for v in $VERSIONS; do
    for s in $STEPS; do
      args=$(script_of "$s") || exit 1
      before=$(ls "$EV/$v")
      t0=$(date +%s)
      echo "=== $(date '+%H:%M') run $r · $v · $s"
      ( cd "$PERF/code/$v/pipeline" && AML_ROOT="$PERF/$v" spark-submit \
          --master "local[$CORES]" --driver-memory "$DRIVER_MEM" \
          --conf spark.eventLog.enabled=true --conf "spark.eventLog.dir=file://$EV/$v" \
          --conf spark.eventLog.compress=false --conf spark.ui.showConsoleProgress=false \
          $args ) > "$PERF/logs/$DATASET-$v-$s-r$r.log" 2>&1
      ok=$?
      app=$(comm -13 <(echo "$before") <(ls "$EV/$v") | head -1)
      status=$([ $ok -eq 0 ] && echo ok || echo FAILED)
      printf "%s\t%s\t%s\t%s\t%s\t%s\n" "$v" "$s" "$r" "$app" "$status" $(( $(date +%s) - t0 )) >> "$EV/index.tsv"
      if [ $ok -ne 0 ]; then
        echo "=== FAILED $v $s (see data/perf/logs/$DATASET-$v-$s-r$r.log); skipping $v's later steps"
        break
      fi
    done
    [ "$KEEP_OUTPUT" = 0 ] && rm -rf "$PERF/$v/curated"
  done
done
echo "=== $(date '+%H:%M') done: python experiments/spark_perf_report.py > results/experiments/perf_comparison.txt"
