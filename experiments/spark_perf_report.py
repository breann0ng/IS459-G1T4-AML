"""
spark_perf_report.py
Spark script versions compared (old = main, new, new2 ...), from the event logs experiments/perf_compare.sh saved
(data/perf/eventlogs/<dataset>/<version>/ + index.tsv, versions.tsv). No Spark needed: event logs are JSON lines.

    python experiments/spark_perf_report.py > results/experiments/perf_comparison.txt
    python experiments/spark_perf_report.py HI-Small HI-Medium        only these datasets

Per step and version (median over repeats when perf_compare.sh ran with REPEATS > 1):
    wall_s          Spark application start to end
    actions         queries Spark ran (each write / count / first ...): an extra action re-runs its plan
    stages          stages that ran (stages Spark skipped because the data was already there not counted)
    shuffles        stages that wrote shuffle data = shuffle exchanges actually executed
    shuffle_mb      shuffle bytes written
    file_read_mb    size of the files each scan opened, every scan counted (parsing the CSV twice =
                    2x its size; parquet counted whole, before Spark skips unused columns)
    spill_mb        disk spill (memory ran out)
    core_use        task time / (wall x cores): how busy the cores were (1.00 = all cores, all the time)
Then CHANGE (each version vs old), every run's time (to see the noise), the actions each step ran, and each step's slowest stages
(ids as in the Spark UI, to look them up in the History Server; a stage's input_mb includes
reads from Spark's cache).
"""

import json
import os
import re
import sys
from collections import Counter, defaultdict

import pandas as pd

EVENTLOGS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "perf", "eventlogs")
STEPS = ["clean", "patterns", "features_v2", "txn_features", "features_v3"]
MB = 1024 * 1024
SLOWEST = 3


def events(app_dir):
    """All events of one application: a folder of events_<n>_<app id> files (Spark 4, rolling
    logs) or one file (Spark 3 / EMR)."""
    if os.path.isdir(app_dir):
        parts = [os.path.join(app_dir, f) for f in sorted(
            (f for f in os.listdir(app_dir) if f.startswith("events_")), key=lambda f: int(f.split("_")[1]))]
    else:
        parts = [app_dir]
    for f in parts:
        with open(f) as fh:
            for line in fh:
                yield json.loads(line)


def action_label(e):
    """What a SQL execution did: 'write <table>' for a parquet write, else the action ('count', ...)."""
    m = re.search(r"Execute InsertIntoHadoopFsRelationCommand\n(?:.*\n)*?Arguments: (\S+?),",
                  e.get("physicalPlanDescription", ""))
    if m:
        path = m.group(1).rstrip("/")
        return "write " + (path.split("/curated/", 1)[1] if "/curated/" in path else path.rsplit("/", 1)[-1])
    return (e.get("description") or "query").split(" at ")[0]


def plan_metrics(node, out):
    """accumulator id -> metric name, for every node of a SQL plan (sparkPlanInfo)."""
    for m in node.get("metrics", []):
        out[m["accumulatorId"]] = m["name"]
    for c in node.get("children", []):
        plan_metrics(c, out)


def parse_app(app_dir):
    """Totals for one Spark application + one row per stage."""
    start = end = None
    cores = os.cpu_count()
    live, peak_cores = {}, 0          # executor id -> cores; most cores running at once (YARN scales them)
    sql, job_sql, stage_job, stages = {}, {}, {}, []
    metric_names, driver_acc = {}, {}
    for e in events(app_dir):
        kind = e["Event"]
        if "sparkPlanInfo" in e:                    # SQL execution start + every adaptive re-plan
            plan_metrics(e["sparkPlanInfo"], metric_names)
        if kind.endswith("SparkListenerDriverAccumUpdates"):
            for acc_id, value in e["accumUpdates"]:
                driver_acc[acc_id] = max(driver_acc.get(acc_id, 0), value)
        if kind == "SparkListenerApplicationStart":
            start = e["Timestamp"]
        elif kind == "SparkListenerApplicationEnd":
            end = e["Timestamp"]
        elif kind == "SparkListenerEnvironmentUpdate":
            m = re.match(r"local\[(\d+)\]", e["Spark Properties"].get("spark.master", ""))
            cores = int(m.group(1)) if m else cores
        elif kind == "SparkListenerExecutorAdded":
            live[e["Executor ID"]] = e["Executor Info"].get("Total Cores", 0)
            peak_cores = max(peak_cores, sum(live.values()))
        elif kind == "SparkListenerExecutorRemoved":
            live.pop(e["Executor ID"], None)
        elif kind.endswith("SparkListenerSQLExecutionStart"):
            root = e.get("rootExecutionId", e["executionId"])
            if root == e["executionId"]:            # nested executions belong to their root's action
                sql[e["executionId"]] = action_label(e)
        elif kind == "SparkListenerJobStart":
            sid = e.get("Properties", {}).get("spark.sql.execution.id")
            job_sql[e["Job ID"]] = int(sid) if sid is not None else None
            for s in e["Stage IDs"]:
                stage_job[s] = e["Job ID"]
        elif kind == "SparkListenerStageCompleted":
            si = e["Stage Info"]
            if "Failure Reason" in si:
                continue
            acc = defaultdict(float)
            for a in si.get("Accumulables", []):
                if a.get("Name", "").startswith("internal.metrics.") and "Value" in a:
                    try:
                        acc[a["Name"][len("internal.metrics."):]] += float(a["Value"])
                    except (TypeError, ValueError):
                        pass
            job = stage_job.get(si["Stage ID"])
            stages.append({
                "stage": si["Stage ID"], "job": job, "sql": job_sql.get(job),
                "tasks": si["Number of Tasks"],
                "duration_s": (si.get("Completion Time", 0) - si.get("Submission Time", 0)) / 1000,
                "shuffle_mb": acc["shuffle.write.bytesWritten"] / MB,
                "shuffle_read_mb": (acc["shuffle.read.localBytesRead"] + acc["shuffle.read.remoteBytesRead"]) / MB,
                "input_mb": acc["input.bytesRead"] / MB,
                "spill_mb": acc["diskBytesSpilled"] / MB,
                "task_s": acc["executorRunTime"] / 1000})
    st = pd.DataFrame(stages)
    if start is None or end is None or st.empty:
        return None, st
    st["action"] = [sql.get(q, "-") if q is not None else "-" for q in st["sql"]]
    wall = (end - start) / 1000
    # files scanned, from each scan's "size of files read" (stage input bytes also count cache reads)
    file_read = sum(v for a, v in driver_acc.items() if metric_names.get(a) == "size of files read")
    return {"wall_s": wall, "actions": len(sql), "stages": len(st),
            "shuffles": int((st["shuffle_mb"] > 0).sum()), "shuffle_mb": st["shuffle_mb"].sum(),
            "file_read_mb": file_read / MB, "spill_mb": st["spill_mb"].sum(),
            "core_use": st["task_s"].sum() / (wall * (peak_cores or cores)),
            "_actions": Counter(sql.values())}, st


def report(dataset):
    base = os.path.join(EVENTLOGS, dataset)
    index = pd.read_csv(os.path.join(base, "index.tsv"), sep="\t", dtype=str)
    labels = dict(pd.read_csv(os.path.join(base, "versions.tsv"), sep="\t", header=None).values)
    runs, slow = [], []
    for _, r in index[index["status"] == "ok"].iterrows():
        totals, st = parse_app(os.path.join(base, r["version"], r["app"]))
        if totals is None:
            continue
        runs.append({"version": r["version"], "step": r["step"], "run": int(r["run"]), **totals})
        if len(st):
            slow.append(st.assign(version=r["version"], step=r["step"], run=int(r["run"])))
    print(f"\n=================== {dataset}")
    if not runs:
        print("  (no finished runs)")
        return
    runs = pd.DataFrame(runs)
    versions = [v for v in labels if v in set(runs["version"])]     # in the order first run
    for v in versions:
        print(f"  {v:6s} = {labels[v]}")
    n = runs.groupby(["version", "step"]).size()
    print(f"runs per step: {n.min()}{'' if n.min() == n.max() else f' to {n.max()}'} (median shown); "
          f"runs left out: {index.loc[index['status'] != 'ok', 'status'].value_counts().to_dict() or 0} "
          f"(FAILED, or a status set by hand in index.tsv, e.g. slept)")

    cols = ["wall_s", "actions", "stages", "shuffles", "shuffle_mb", "file_read_mb", "spill_mb", "core_use"]
    steps = [s for s in STEPS if s in set(runs["step"])]
    med = runs.groupby(["step", "version"])[cols].median()
    t = med.unstack("version").reindex(steps)
    t = t.reindex(columns=[(c, v) for c in cols for v in versions])
    total = t.dropna().sum()
    total[[("core_use", v) for v in versions]] = float("nan")
    t.loc["TOTAL (steps every version ran)"] = total
    print(f"\nPER STEP ({' | '.join(versions)})")
    print(t.round(2).to_string())

    base_v = "old" if "old" in versions else versions[0]
    for v in [v for v in versions if v != base_v]:
        ch = pd.DataFrame(index=t.index)
        for c in ["wall_s", "shuffle_mb", "file_read_mb", "spill_mb"]:
            o, w = t[(c, base_v)], t[(c, v)]
            ch[c + " %"] = ((w - o) / o.where(o > 0) * 100).round(1)
        for c in ["actions", "stages", "shuffles"]:
            ch[c] = (t[(c, v)] - t[(c, base_v)]).round(1)
        ch["core_use"] = (t[("core_use", v)] - t[("core_use", base_v)]).round(2)
        print(f"\nCHANGE {v} vs {base_v} (% for sizes and time, difference for counts; negative = {v} does less)")
        print(ch.to_string())

    print("\nEVERY RUN · wall_s (spread between runs of the same code = timing noise)")
    w = runs.pivot_table(index="step", columns=["version", "run"], values="wall_s").reindex(steps)
    w = w.reindex(columns=[c for v in versions for c in w.columns if c[0] == v])
    print(w.round(0).to_string(na_rep="-"))

    print("\nACTIONS per step (from each version's first run)")
    first = runs[runs["run"] == runs.groupby(["version", "step"])["run"].transform("min")]
    for s in steps:
        for v in versions:
            a = first[(first["step"] == s) & (first["version"] == v)]["_actions"]
            if len(a):
                print(f"  {s:13s}{v:5s}: " + ", ".join(f"{k} x{c}" if c > 1 else k for k, c in sorted(a.iloc[0].items())))

    print(f"\nSLOWEST {SLOWEST} STAGES per step (each version's first run; stage / job / sql ids as in the Spark UI)")
    sl = pd.concat(slow)
    sl = sl[sl["run"] == sl.groupby(["version", "step"])["run"].transform("min")]
    show = ["stage", "job", "sql", "action", "tasks", "duration_s", "shuffle_mb", "input_mb", "spill_mb"]
    for s in steps:
        for v in versions:
            x = sl[(sl["step"] == s) & (sl["version"] == v)].nlargest(SLOWEST, "duration_s")
            if len(x):
                print(f"-- {s} · {v}")
                print(x[show].round(2).to_string(index=False))


def main(datasets):
    pd.set_option("display.width", 240)
    if not os.path.isdir(EVENTLOGS):
        raise SystemExit("No data/perf/eventlogs yet: run experiments/perf_compare.sh first.")
    for d in datasets or sorted(os.listdir(EVENTLOGS)):
        if os.path.exists(os.path.join(EVENTLOGS, d, "index.tsv")):
            report(d)


if __name__ == "__main__":
    main(sys.argv[1:])
