"""
build_week_features.py                                        (BQ1, one Step Functions step)
Builds all of a week's features in ONE Spark application instead of three EMR jobs:
    features_v2 (account-day graph features)
 -> txn_features (per payment, joins features_v2)
 -> features_v3 (+ x_/o_/t_ extras, reads both)

    spark-submit build_week_features.py <dataset> <week_start> <week_end>   one week (scoring)
    spark-submit build_week_features.py <dataset>                          whole dataset (training)
    e.g. build_week_features.py HI-Medium-joey 2022-09-12 2022-09-18

The three builders are run unchanged (same code, same arguments as the three separate steps), so the
179 model features come out identical. What changes:
  * one Spark start-up and one venv unpack instead of three (about 1.2 min each on EMR Serverless)
  * one EMR job to watch, retry or fail in Step Functions
  * the per-step timings are printed and written to the run log by the state machine
Between builders the Spark cache is cleared, so the next builder gets the executors' full memory.
"""
import importlib
import json
import sys
import time

STEPS = [
    ("features_v2", "build_features_spark", lambda ds, dates: [ds, *dates]),
    ("txn_features", "build_txn_features_spark", lambda ds, dates: [ds, "features_v2", *dates]),
    ("features_v3", "build_features_v3_spark", lambda ds, dates: [ds, *dates]),
]


def run_step(module_name, argv):
    """Each builder reads sys.argv when it is imported, so set it first, then import and run."""
    sys.argv = [f"{module_name}.py", *argv]
    module = importlib.import_module(module_name)
    module.main()
    module.spark.catalog.clearCache()          # unpersist this builder's cached tables
    return module.spark


def main():
    if len(sys.argv) not in (2, 4):
        raise SystemExit("usage: build_week_features.py <dataset> [<week_start> <week_end>]")
    dataset, dates = sys.argv[1], sys.argv[2:4]          # no dates = the whole dataset
    week_start, week_end = (dates + [None, None])[:2]
    seconds, spark = {}, None
    for name, module_name, args in STEPS:
        t0 = time.time()
        spark = run_step(module_name, args(dataset, dates))
        seconds[name] = round(time.time() - t0)
        print(f"[week-features] {name} done in {seconds[name]}s")
    print("[week-features] " + json.dumps({"dataset": dataset, "week_start": week_start,
                                            "week_end": week_end, "seconds": seconds}))
    spark.stop()


if __name__ == "__main__":
    main()
