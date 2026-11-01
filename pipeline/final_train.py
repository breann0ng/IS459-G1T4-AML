"""
final_train.py
Final run, step 1 of 3: retrain the FROZEN setup and save it to S3 (nothing is tuned here).

Plain Python on the EMR primary node (needs train_txn.py, group_stage2.py, scheme_stage2.py):
    python3 final_train.py                       uses the validated run txn_v3m

Frozen setup (chosen on HI-Small + HI-Medium validation, never on HI-Large)
    stage 1   payment model: LightGBM preset p3, 179 features (txn_features_v3),
              trained on the training days of HI-Small + HI-Medium (same split as validation)
    threshold the validation best-F1 threshold of txn_v3m (applied unchanged to HI-Large)
    stage 2   group namer: 78 account-day features (features_v3) + 35 group / role features,
              7 classes (bipartite + stack = layering), trained on whole + 70% + 50% schemes
Writes  models/final/stage1.txt, models/final/namer_group.txt, models/final/meta.json
Next    spark-submit [confs] score_final_spark.py HI-Large        (scores every payment)
        python3 final_group_namer.py HI-Large                      (names the alerts)
"""

import json
import sys
import math
import time

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
from lightgbm import LGBMClassifier

import train_txn as T
import group_stage2 as G
import scheme_stage2 as S

VALIDATED_RUN = sys.argv[1] if len(sys.argv) > 1 else "txn_v3m"
TXN_TABLE, ACCT_TABLE = "txn_features_v3", "features_v3"
MERGE = {"bipartite": "layering", "stack": "layering"}
MODEL_DIR = f"{T.ROOT}/models/final"


def put_text(path, text):
    fs, p = T.fs_path(path)
    fs.create_dir(p.rsplit("/", 1)[0], recursive=True)
    with fs.open_output_stream(p) as f:
        f.write(text.encode())


def main():
    t0 = time.time()
    # ---- the validated run: its threshold and split
    res = S.load_table("txn_results", ds.field("config") == VALIDATED_RUN)
    thr = float(res["threshold_val_best_f1"].iloc[0])
    train_end = {k: pd.Timestamp(v).date() for k, v in json.loads(res["train_end_dates"].iloc[0]).items()}
    print(f"[final] frozen from {VALIDATED_RUN}: threshold {thr:.4f}, train end "
          f"{ {k: str(v) for k, v in train_end.items()} }")

    # ---- stage 1: payment model (same data, features, settings and seed as the validated run)
    sd = T.open_dataset(f"{T.CURATED}/{TXN_TABLE}_sample")
    keep = [c for c in sd.schema.names if c not in ("txn_id", "s_id", "r_id", "week", "pattern_label")]
    smp = sd.to_table(columns=keep, filter=ds.field("dataset").isin(T.TRAIN_DATASETS)).to_pandas()
    smp["date"] = pd.to_datetime(smp["date"]).dt.date
    feat = [c for c in smp.columns if c not in T.META]
    m = np.zeros(len(smp), dtype=bool)
    for d, end in train_end.items():
        m |= (smp["dataset"] == d).to_numpy() & (smp["date"] <= end).to_numpy()
    tr = smp[m]
    params = {**T.PARAMS, **T.PRESETS["p3"]}
    n_pos, n_neg = int(tr["label"].sum()), int((tr["label"] == 0).sum())
    if params.get("scale_pos_weight") == "auto":
        params["scale_pos_weight"] = n_neg / max(n_pos, 1)
    stage1 = LGBMClassifier(**params).fit(tr[feat].to_numpy(np.float32), tr["label"].to_numpy())
    put_text(f"{MODEL_DIR}/stage1.txt", stage1.booster_.model_to_string())
    print(f"[final] stage 1 trained on {len(tr)} payments ({n_pos} laundering), {len(feat)} features")
    del smp, tr

    # ---- stage 2: group namer (same recipe as group_stage2.py)
    a = S.open_dataset(f"{S.CURATED}/{ACCT_TABLE}_sample").to_table(
        filter=ds.field("dataset").isin(list(train_end)) & (ds.field("label") == 1)).to_pandas()
    a["date"] = pd.to_datetime(a["date"]).dt.date
    a = a[[dt_ <= train_end[d] for d, dt_ in zip(a["dataset"], a["date"])]].reset_index(drop=True)
    f2 = [c for c in a.columns if c not in S.META]
    pays = S.payments_for(a)
    copies = []
    for i, k in enumerate(G.KEEP_RATES):
        part = a if k >= 1 else a.sample(frac=k, random_state=G.SEED + i)
        copies.append(G.with_groups(part, pays, f"train keep={k:.0%}"))
    tr2 = pd.concat(copies, ignore_index=True)
    gcols = [c for c in tr2.columns if c.startswith(("g_", "r_"))]
    y = S.true_class(tr2, MERGE).to_numpy()
    namer = S.fit(tr2[f2 + gcols].fillna(0.0).to_numpy(np.float32), y)
    put_text(f"{MODEL_DIR}/namer_group.txt", namer.booster_.model_to_string())
    print(f"[final] group namer trained on {len(tr2)} rows, {len(f2)} + {len(gcols)} features, "
          f"classes {list(namer.classes_)}")

    meta = {"validated_run": VALIDATED_RUN, "threshold": thr,
            "train_end": {k: str(v) for k, v in train_end.items()},
            "txn_table": TXN_TABLE, "acct_table": ACCT_TABLE,
            "stage1_features": feat, "stage1_params": {k: v for k, v in params.items()},
            "namer_features": f2 + gcols, "namer_account_features": f2, "namer_group_features": gcols,
            "namer_classes": [str(c) for c in namer.classes_], "merge": MERGE,
            "keep_rates": G.KEEP_RATES, "hub_degree": G.HUB_DEGREE,
            "max_linked_days": G.MAX_LINKED_DAYS, "trained_at": T.RUN_AT}
    put_text(f"{MODEL_DIR}/meta.json", json.dumps(meta, indent=1, default=str))
    print(f"[final] saved models/final/ (stage1.txt, namer_group.txt, meta.json) in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
