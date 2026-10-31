"""
final_group_namer.py
Final run, step 3 of 3: name the pattern of every alert from stage 1, using the FROZEN group
namer, and grade it end to end (same definitions as validation).

Plain Python on the EMR primary node (needs group_stage2.py, scheme_stage2.py, train_txn.py):
    python3 final_group_namer.py HI-Large

Reads   models/final/ (namer_group.txt, meta.json), analytics/final_alert_rows,
        analytics/final_truth, curated/transactions (payments between alerted accounts only)
Writes  analytics/final_stage2/          per pattern: label precision / recall / F1, end to end
        analytics/final_alerts_named/    the analyst's alert list: account, day, risk score,
                                         predicted pattern + confidence, group id and size,
                                         truth (for the report only)
"""

import json
import sys
import time

import lightgbm as lgb
import numpy as np
import pandas as pd
import pyarrow.dataset as ds

import group_stage2 as G
import scheme_stage2 as S
import train_txn as T

DATASET = sys.argv[1] if len(sys.argv) > 1 else "HI-Large"
MODEL_DIR = f"{T.ROOT}/models/final"


def get_text(path):
    fs, p = T.fs_path(path)
    with fs.open_input_stream(p) as f:
        return f.read().decode()


def main():
    t0 = time.time()
    meta = json.loads(get_text(f"{MODEL_DIR}/meta.json"))
    booster = lgb.Booster(model_str=get_text(f"{MODEL_DIR}/namer_group.txt"))
    classes = meta["namer_classes"]
    merge = meta["merge"]
    f2, gcols = meta["namer_account_features"], meta["namer_group_features"]
    G.HUB_DEGREE, G.MAX_LINKED_DAYS = meta["hub_degree"], meta["max_linked_days"]

    al = S.load_table("final_alert_rows", ds.field("dataset") == DATASET)
    al["date"] = pd.to_datetime(al["date"]).dt.date
    al = al.reset_index(drop=True)
    print(f"[name] {DATASET}: {len(al)} alerts (account-days) over {al['date'].nunique()} days, "
          f"{int(al['label'].sum())} truly laundering")

    pays = S.payments_for(al)
    g = G.with_groups(al, pays, "alerts")
    proba = booster.predict(g[f2 + gcols].fillna(0.0).to_numpy(np.float32))
    g["predicted_pattern"] = np.array(classes)[proba.argmax(axis=1)]
    g["pattern_confidence"] = proba.max(axis=1)
    g["true_pattern"] = np.where(g["label"] == 1, S.true_class(g, merge).to_numpy(), "not_laundering")

    truth = S.load_table("final_truth", ds.field("dataset") == DATASET)
    tt = truth.assign(p=S.true_class(truth.rename(columns={"pattern_label": "pattern_label"}), merge))
    total_true = tt.groupby("p")["count"].sum().to_dict()
    report = [p for p in classes if p != "unclassified"] + ["unclassified"]
    e2e = G.class_table(g["true_pattern"].to_numpy(), g["predicted_pattern"].to_numpy(), report, total_true)
    caught = g["label"].to_numpy() == 1
    cau = G.class_table(g["true_pattern"].to_numpy()[caught], g["predicted_pattern"].to_numpy()[caught], report)
    e2e["mode"], cau["mode"] = "end_to_end", "caught"
    out = pd.concat([e2e, cau], ignore_index=True)
    out["dataset"], out["run_at"] = DATASET, S.RUN_AT
    S.write_results(out, "final_stage2", meta["validated_run"])
    show = e2e[["pattern", "true_cases", "named_as", "correct", "precision", "recall", "f1"]].rename(
        columns={"precision": "label_precision", "recall": "e2e_recall", "f1": "e2e_f1"})
    show["caught_f1"] = cau["f1"].to_numpy()
    print(f"-- {DATASET} · STAGE 2 END TO END (group namer)")
    print(show.round(3).to_string(index=False))

    # the analyst's alert list (group id = connected group of alerts, as built above)
    g["k"] = S.key(g["acct_bank"], g["acct"])
    gid, _, _ = G.build_groups(g, pays[DATASET])
    g["group_id"] = [gid[(k, d)] for k, d in zip(g["k"], g["date"])]
    cols = ["acct_bank", "acct", "date", "score", "predicted_pattern", "pattern_confidence", "group_id",
            "g_accounts", "g_nodes", "g_payments", "g_days", "g_max_in", "g_max_out", "g_has_cycle",
            "g_longest", "r_role", "label", "true_pattern", "first_laundering_ts"]
    named = g[cols].copy()
    named["dataset"] = DATASET
    S.write_results(named, "final_alerts_named", meta["validated_run"])
    print(f"[name] saved analytics/final_stage2 and final_alerts_named ({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    main()
