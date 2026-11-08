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
import os
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
RUN = os.environ.get("AML_FINAL_RUN", "final")     # a named final run keeps its own models + tables
MODEL_DIR = f"{T.ROOT}/models/{RUN}"
# Daily mode (Step Functions aml-daily): name one day's alerts. Groups are still linked across days, so
# the alerts of the previous max_linked_days days are read too; only this day's rows are graded and saved,
# in the live_* tables (one partition per day). Unset = the one-off evaluation run, as before.
RUN_DATE = os.environ.get("AML_RUN_DATE")
# Weekly mode (aml_bq1_weekly): AML_RUN_DATE = the week's Monday (its run_date partition) and
# AML_RUN_TO = its Sunday, so the whole week is named. Unset = one day, as in daily mode.
RUN_TO = os.environ.get("AML_RUN_TO") or RUN_DATE
EXTRA = ("run_date",) if RUN_DATE else ()


def table(name):
    """final_stage2 -> final_<RUN>_stage2 for a named run (FINAL_NAME in rebuild_final.sh);
    live_stage2 in daily mode."""
    if RUN_DATE:
        return f"live_{name[len('final_'):]}"
    return name if RUN == "final" else f"final_{RUN}_{name[len('final_'):]}"


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
    S.LINK_DAYS = meta.get("link_days", S.LINK_DAYS)
    ctx, vote = meta.get("context", False), meta.get("group_vote", False)
    print(f"[name] settings from {meta.get('group_run') or 'defaults'}: link_days {S.LINK_DAYS}, "
          f"max_linked_days {G.MAX_LINKED_DAYS}, hub {G.HUB_DEGREE}, context {ctx}, group vote {vote}, "
          f"class scale {meta.get('class_scale') or 'none'}")

    flt = ds.field("dataset") == DATASET
    if RUN_DATE:                              # this day + the days its groups can link back to
        since = pd.Timestamp(RUN_DATE) - pd.Timedelta(days=int(G.MAX_LINKED_DAYS))
        span = pd.Timestamp(RUN_TO) - pd.Timestamp(RUN_DATE)        # 0 days (daily) or 6 (weekly)
        # a run_date partition holds RUN_DATE .. RUN_DATE + span, so start one span earlier
        flt = (flt & (ds.field("run_date") >= (since - span).strftime("%Y-%m-%d"))
                   & (ds.field("run_date") <= RUN_DATE))
    al = S.load_table(table("final_alert_rows"), flt)
    al["date"] = pd.to_datetime(al["date"]).dt.date
    if RUN_DATE:                              # exactly max_linked_days of context before the period
        al = al[al["date"] >= since.date()]
    al = al.reset_index(drop=True)
    print(f"[name] {DATASET}: {len(al)} alerts (account-days) over {al['date'].nunique()} days, "
          f"{int(al['label'].sum())} truly laundering")

    pays = G.context_for(al) if ctx else S.payments_for(al)
    g = G.with_groups(al, pays, "alerts")
    proba = booster.predict(g[f2 + gcols].fillna(0.0).to_numpy(np.float32))
    if meta.get("class_scale"):               # same class scaling as the validated run
        proba = G.scale_classes(pd.DataFrame(proba, columns=classes, index=g.index), meta["class_scale"]).to_numpy()
    if vote:                                  # one name per group: average its members' probabilities
        proba = pd.DataFrame(proba, index=g.index).groupby(g["group_id"]).transform("mean").to_numpy()
    g["predicted_pattern"] = np.array(classes)[proba.argmax(axis=1)]
    g["pattern_confidence"] = proba.max(axis=1)
    second = np.argsort(-proba, axis=1)[:, 1]           # the analyst sees two guesses: "fan out, or scatter gather"
    g["second_pattern"] = np.array(classes)[second]
    g["second_confidence"] = proba[np.arange(len(proba)), second]
    g["true_pattern"] = np.where(g["label"] == 1, S.true_class(g, merge).to_numpy(), "not_laundering")

    truth_flt = ds.field("dataset") == DATASET
    if RUN_DATE:                              # grade and save this day only (earlier days gave context)
        g = g[(g["date"] >= pd.Timestamp(RUN_DATE).date()) & (g["date"] <= pd.Timestamp(RUN_TO).date())].copy()
        truth_flt = truth_flt & (ds.field("run_date") == RUN_DATE)
        print(f"[name] {len(g)} alerts from {RUN_DATE} to {RUN_TO}")
    truth = S.load_table(table("final_truth"), truth_flt)
    tt = truth.assign(p=S.true_class(truth.rename(columns={"pattern_label": "pattern_label"}), merge))
    total_true = tt.groupby("p")["count"].sum().to_dict()
    report = [p for p in classes if p != "unclassified"] + ["unclassified"]
    e2e = G.class_table(g["true_pattern"].to_numpy(), g["predicted_pattern"].to_numpy(), report, total_true)
    caught = g["label"].to_numpy() == 1
    cau = G.class_table(g["true_pattern"].to_numpy()[caught], g["predicted_pattern"].to_numpy()[caught], report)
    e2e["mode"], cau["mode"] = "end_to_end", "caught"
    out = pd.concat([e2e, cau], ignore_index=True)
    out["dataset"], out["run_at"] = DATASET, S.RUN_AT
    if RUN_DATE:
        out["run_date"] = RUN_DATE
    S.write_results(out, table("final_stage2"), meta["validated_run"], EXTRA)
    show = e2e[["pattern", "true_cases", "named_as", "correct", "precision", "recall", "f1"]].rename(
        columns={"precision": "label_precision", "recall": "e2e_recall", "f1": "e2e_f1"})
    show["caught_f1"] = cau["f1"].to_numpy()
    print(f"-- {DATASET} · STAGE 2 END TO END (group namer)")
    print(show.round(3).to_string(index=False))

    # the analyst's alert list (group id = connected group of alerts, as built above)
    cols = ["acct_bank", "acct", "date", "score", "predicted_pattern", "pattern_confidence",
            "second_pattern", "second_confidence", "group_id",
            "g_accounts", "g_nodes", "g_payments", "g_days", "g_max_in", "g_max_out", "g_has_cycle",
            "g_longest", "r_role", "label", "true_pattern", "first_laundering_ts"]
    named = g[cols].copy()
    named["dataset"] = DATASET
    if RUN_DATE:
        named["run_date"] = RUN_DATE
    S.write_results(named, table("final_alerts_named"), meta["validated_run"], EXTRA)
    print(f"[name] saved analytics/final_stage2 and final_alerts_named ({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    main()
