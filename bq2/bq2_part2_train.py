"""
bq2_part2_train.py                                                       (BQ2, part 2)
Trains the BQ1 stage 1 model ON SAML-R, with the BQ1 recipe: the same features, LightGBM
settings and threshold rule as the frozen BQ1 model (read from models/final/meta.json, so a
teammate's improved BQ1 is picked up by rerunning this step). It is a new trained model, not a
new kind of model: "the BQ1 pipeline applied to SAML-D".

Plain Python on the EMR primary node, after the BQ1 feature steps for the dataset:
    python3 bq2_part2_train.py SAML-R v0 2023-01-29     the bank's model: trained before any
                                                        emerging tactic existed
    python3 bq2_part2_train.py SAML-R v1 2023-06-25     retrained with every case confirmed
                                                        up to that day (emerging ones included)

Split (BQ1's rule): every day from the dataset's start to <train to>; the first 75% of those days
train the model (on curated/txn_features_v3_sample: all laundering + 2% of honest payments), the
last 25% are validation (every payment, curated/txn_features_v3). Threshold = best payment F1 on
validation.

Writes  models/bq2r/<dataset>/<version>/stage1.txt and meta.json (threshold, days, validation
        precision / recall / F1 / PR-AUC overall and per typology, typologies seen in training)
"""

import json
import math
import sys
import time
from datetime import date

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
from lightgbm import LGBMClassifier
from sklearn.metrics import average_precision_score

import bq2_common as C
import train_txn as T

DATASET = sys.argv[1] if len(sys.argv) > 1 else C.RAMP
VERSION = sys.argv[2] if len(sys.argv) > 2 else "v0"
TRAIN_TO = date.fromisoformat(sys.argv[3] if len(sys.argv) > 3 else C.TRAIN_TO)
TXN_TABLE = "txn_features_v3"
MODEL_DIR = f"{T.ROOT}/models/bq2r/{DATASET}/{VERSION}"


def recipe():
    """Stage 1 features and LightGBM settings of the frozen BQ1 model."""
    try:
        meta = C.get_json(f"{T.ROOT}/models/final/meta.json")
        return meta["stage1_features"], dict(meta["stage1_params"]), "models/final/meta.json"
    except (OSError, KeyError, FileNotFoundError):
        cols = T.open_dataset(f"{T.CURATED}/{TXN_TABLE}_sample").schema.names
        return ([c for c in cols if c not in T.META],
                {**T.PARAMS, **T.PRESETS["p3"]}, "train_txn.py PARAMS + preset p3 (no meta.json)")


def main():
    t0 = time.time()
    feat, params, src = recipe()
    in_ds = ds.field("dataset") == DATASET
    sd = T.open_dataset(f"{T.CURATED}/{TXN_TABLE}_sample")
    smp = sd.to_table(columns=["date", "label", "pattern_label", *feat], filter=in_ds).to_pandas()
    smp["date"] = pd.to_datetime(smp["date"]).dt.date
    smp = smp[smp["date"] <= TRAIN_TO]
    days = sorted(smp["date"].unique())
    if len(days) < 8:
        raise SystemExit(f"only {len(days)} days of {DATASET} up to {TRAIN_TO}: run the feature steps first")
    n_val = math.ceil(len(days) * T.VALIDATION_SHARE)
    val_from, train_end = days[-n_val], days[-n_val - 1]
    tr = smp[smp["date"] <= train_end]
    print(f"[train] {DATASET} {VERSION}: recipe from {src}, {len(feat)} features")
    print(f"[train] train {days[0]}..{train_end} ({len(days) - n_val} days), "
          f"validation {val_from}..{TRAIN_TO} ({n_val} days)")

    n_pos, n_neg = int(tr["label"].sum()), int((tr["label"] == 0).sum())
    if params.get("scale_pos_weight") == "auto":
        params["scale_pos_weight"] = n_neg / max(n_pos, 1)
    model = LGBMClassifier(**params).fit(tr[feat].to_numpy(np.float32), tr["label"].to_numpy())
    train_patterns = sorted(p for p in tr.loc[tr["label"] == 1, "pattern_label"].unique())
    print(f"[train] trained on {len(tr)} payments ({n_pos} laundering), typologies {train_patterns}")
    del smp, tr

    va = T.open_dataset(f"{T.CURATED}/{TXN_TABLE}").to_table(
        columns=["date", "label", "pattern_label", *feat],
        filter=in_ds & (ds.field("date") >= val_from) & (ds.field("date") <= TRAIN_TO)).to_pandas()
    s = model.predict_proba(va[feat].to_numpy(np.float32))[:, 1]
    y = va["label"].to_numpy().astype(bool)
    thr = T.best_f1_threshold(y, s)
    m = T.metrics(y, s >= thr, n_val)
    m["pr_auc"] = float(average_precision_score(y, s)) if y.any() else None
    per = (pd.DataFrame({"p": va["pattern_label"], "y": y, "a": s >= thr})[y]
           .groupby("p").agg(cases=("y", "size"), caught=("a", "sum")))
    per["recall"] = per["caught"] / per["cases"]
    print(f"[train] validation: {len(va)} payments, {int(y.sum())} laundering; threshold {thr:.4f}; "
          f"P {m['precision']:.3f} R {m['recall']:.3f} F1 {m['f1']:.3f} PR-AUC {m['pr_auc'] or 0:.3f}")
    print(per.round(3).to_string())

    refit = VERSION != "v0"
    if refit:
        del va
        smp = sd.to_table(columns=["date", "label", *feat], filter=in_ds).to_pandas()
        smp = smp[pd.to_datetime(smp["date"]).dt.date <= TRAIN_TO]
        model = LGBMClassifier(**params).fit(smp[feat].to_numpy(np.float32), smp["label"].to_numpy())
        print(f"[train] refit on all {len(smp)} sample payments up to {TRAIN_TO} "
              f"({int(smp['label'].sum())} laundering), threshold kept at {thr:.4f}")
        train_patterns = sorted(set(train_patterns) | set(per.index))
        del smp
    C.put_text(f"{MODEL_DIR}/stage1.txt", model.booster_.model_to_string())
    meta = {"dataset": DATASET, "version": VERSION, "recipe_from": src, "threshold": thr,
            "train_from": str(days[0]), "train_end": str(train_end), "val_from": str(val_from),
            "train_to": str(TRAIN_TO), "refit_on_all_days": refit, "train_laundering": n_pos, "train_payments": int(n_pos + n_neg),
            "train_patterns": train_patterns, "validation": {k: (float(v) if v is not None else None)
                                                             for k, v in m.items()},
            "validation_by_pattern": {p: {"cases": int(r.cases), "recall": float(r.recall)}
                                      for p, r in per.iterrows()},
            "stage1_features": feat, "stage1_params": params, "trained_at": T.RUN_AT}
    C.put_text(f"{MODEL_DIR}/meta.json", json.dumps(meta, indent=1, default=str))
    print(f"[train] saved models/bq2r/{DATASET}/{VERSION}/ in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
