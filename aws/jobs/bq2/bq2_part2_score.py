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


def main():
    t0 = time.time()
    mdir = f"{T.ROOT}/models/bq2r/{DATASET}/{VERSION}"
    meta = C.get_json(f"{mdir}/meta.json")
    booster = lgb.Booster(model_str=C.get_text(f"{mdir}/stage1.txt"))
    feat, thr = meta["stage1_features"], float(meta["threshold"])
    start = FROM or date.fromisoformat(meta["train_to"]) + timedelta(days=1)   # never score the training days
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
    if not parts:
        raise SystemExit(f"no rows of {DATASET} from {start}: check the dates and the feature steps")
    df = pd.concat(parts, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["week"] = df["week"].astype("int32")
    df["dataset"], df["version"] = DATASET, VERSION
    fs, path = T.fs_path(f"{T.ANALYTICS}/bq2r_scores")
    part = ds.partitioning(pa.schema([("dataset", pa.string()), ("version", pa.string())]), flavor="hive")
    ds.write_dataset(pa.Table.from_pandas(df, preserve_index=False), path, filesystem=fs, format="parquet",
                     partitioning=part, basename_template=f"part-{T.RUN_ID}-{{i}}.parquet",
                     existing_data_behavior="delete_matching")
    y, a = df["label"].to_numpy(bool), df["alert"].to_numpy(bool)
    print(f"[score] {DATASET} {VERSION}: {n} payments {df['date'].min()}..{df['date'].max()}, "
          f"{int(y.sum())} laundering, {int(a.sum())} alerts, recall {(a & y).sum() / max(y.sum(), 1):.3f}, "
          f"precision {(a & y).sum() / max(a.sum(), 1):.3f}, threshold {thr:.4f} ({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    main()
