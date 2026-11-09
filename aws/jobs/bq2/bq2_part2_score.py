"""
bq2_part2_score.py                                                       (BQ2, part 2)
Scores every payment of a period with one BQ2 model version (the model never sees labels; they
are kept only to grade it afterwards).

Plain Python on the EMR primary node (SAML-R is small: ~200k payments a week):
    python3 bq2_part2_score.py SAML-R v0 2023-01-30               v0 from the end of training
    python3 bq2_part2_score.py SAML-R v1 2023-07-24               v1 from the day it is deployed
    python3 bq2_part2_score.py SAML-R v0 2023-01-30 2023-08-31    optional end date

Reads   models/bq2r/<dataset>/<version>/, curated/txn_features_v3
Writes  analytics/bq2r_scores/dataset=<dataset>/version=<version>/
        txn_id, date, week, s_id, r_id, label, pattern_label, score, alert (score >= threshold)
"""

import hashlib
import json
import os
import sys
import time
from datetime import date, timedelta
import lightgbm as lgb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

import bq2_common as C
import train_txn as T

DATASET = sys.argv[1] if len(sys.argv) > 1 else C.RAMP
VERSION = sys.argv[2] if len(sys.argv) > 2 else "v0"          # "deployed" = whatever deployed.json points to
if VERSION == "deployed":
    _dep = C.deployed_info(DATASET)
    VERSION = _dep["version"]
FROM = date.fromisoformat(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] != "-" else None   # "-" = default start
if FROM is None and sys.argv[2:3] == ["deployed"] and _dep.get("deploy_from"):
    FROM = date.fromisoformat(_dep["deploy_from"])                # a deployed model is scored from its deploy date
TO = date.fromisoformat(sys.argv[4]) if len(sys.argv) > 4 else None
KEEP = ["txn_id", "date", "week", "s_id", "r_id", "label", "pattern_label"]
FULL = "--full" in sys.argv or os.environ.get("BQ2_SCORE_FULL") == "1"   # force a rescore from the start
OVERLAP_DAYS = 7                                                        # re-score the last week: it may have been partial


def incremental_base(fs, vdir, mdir, sha, start):
    """Reuse stored scores: returns (rows to keep, date to score from). (None, start) = score everything.
    Only reused when the same model produced them and they begin at the requested start."""
    try:
        prev = C.get_json(f"{mdir}/scored.json")
    except Exception:
        prev = None
    if not prev or prev.get("model_md5") != sha:
        return None, start
    try:
        old = ds.dataset(vdir, filesystem=fs, format="parquet").to_table().to_pandas()
    except Exception:
        return None, start
    if old.empty:
        return None, start
    old["date"] = pd.to_datetime(old["date"]).dt.date
    if old["date"].min() > start:
        return None, start
    top = old["date"].max() if not TO else min(old["date"].max(), TO)
    new_start = max(start, top - timedelta(days=OVERLAP_DAYS))
    kept = old[old["date"] < new_start].drop(columns=["dataset", "version"], errors="ignore")
    return kept, new_start


def main():
    t0 = time.time()
    mdir = f"{T.ROOT}/models/bq2r/{DATASET}/{VERSION}"
    meta = C.get_json(f"{mdir}/meta.json")
    booster = lgb.Booster(model_str=C.get_text(f"{mdir}/stage1.txt"))
    feat, thr = meta["stage1_features"], float(meta["threshold"])
    start = FROM or date.fromisoformat(meta["train_to"]) + timedelta(days=1)   # never score the training days
    fs, path = T.fs_path(f"{T.ANALYTICS}/bq2r_scores")
    vdir = f"{path}/dataset={DATASET}/version={VERSION}"
    kept, sha = None, hashlib.md5(C.get_text(f"{mdir}/stage1.txt").encode()).hexdigest()
    if not FULL:
        kept, start = incremental_base(fs, vdir, mdir, sha, start)
    f = (ds.field("dataset") == DATASET) & (ds.field("date") >= start)
    if TO:
        f = f & (ds.field("date") <= TO)
    src = T.open_dataset(f"{T.CURATED}/txn_features_v3")
    parts, n = [], 0
    for b in src.to_batches(columns=KEEP + feat, filter=f, batch_size=T.BATCH_SIZE):
        if b.num_rows == 0:
            continue
        x = np.column_stack([b.column(c).to_numpy(zero_copy_only=False) for c in feat]).astype(np.float32)
        s = booster.predict(x).astype(np.float32)
        out = b.select(KEEP).to_pandas()
        out["score"], out["alert"] = s, s >= thr
        parts.append(out)
        n += b.num_rows
    if not parts and kept is None:
        raise SystemExit(f"no rows of {DATASET} from {start}: check the dates and the feature steps")
    df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=KEEP + ["score", "alert"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["week"] = df["week"].astype("int32")
    if kept is not None and len(kept):
        df = pd.concat([kept, df], ignore_index=True)
    df["week"] = df["week"].astype("int32")
    df["dataset"], df["version"] = DATASET, VERSION
    part = ds.partitioning(pa.schema([("dataset", pa.string()), ("version", pa.string())]), flavor="hive")
    ds.write_dataset(pa.Table.from_pandas(df, preserve_index=False), path, filesystem=fs, format="parquet",
                     partitioning=part, basename_template=f"part-{T.RUN_ID}-{{i}}.parquet",
                     existing_data_behavior="delete_matching")
    C.put_text(f"{mdir}/scored.json", json.dumps({"model_md5": sha, "from": str(df["date"].min())}))
    y, a = df["label"].to_numpy(bool), df["alert"].to_numpy(bool)
    print(f"[score] {DATASET} {VERSION}: {n} newly scored of {len(df)} payments "
          f"{'(incremental, kept ' + str(len(kept)) + ')' if kept is not None else '(full)'} "
          f"{df['date'].min()}..{df['date'].max()}, "
          f"{int(y.sum())} laundering, {int(a.sum())} alerts, recall {(a & y).sum() / max(y.sum(), 1):.3f}, "
          f"precision {(a & y).sum() / max(a.sum(), 1):.3f}, threshold {thr:.4f} ({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    main()
