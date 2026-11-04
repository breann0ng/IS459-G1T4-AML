"""
bq2_part2_eval.py                                                        (BQ2, part 2)
"Given the changes, is our detection rate affected?" Grades each model version week by week
with the confirmed cases (labels), applies the alert rules to the deployed model (v0), and,
once the retrained model (v1) has been scored, compares the two on the same weeks.

Plain Python on the EMR primary node, after bq2_part2_score.py:
    python3 bq2_part2_eval.py SAML-R            round 1: monitor v0 (writes decision.json)
    python3 bq2_part2_eval.py SAML-R v1         round 2: monitor v1 against v1's OWN validation (the new
                                                baseline; writes decision_v1.json), and compare v1 with
                                                v2 once v2 has been trained and scored

Alert rules (bq2_common.py), checked every week on v0 with that week's confirmed cases:
    R1  recall below v0's validation recall minus RECALL_DROP          (weeks with >= MIN_WEEK_CASES)
    R2  a typology with no validation baseline (never trained on, or under MIN_TYPOLOGY_CASES cases
        in validation): >= NEW_TYPOLOGY_MIN confirmed cases so far, and the model caught under
        NEW_TYPOLOGY_RECALL of them
    R3  a known typology's recall below its validation recall minus TYPOLOGY_DROP (>= MIN_TYPOLOGY_CASES)
The alert is raised when any rule holds CONSECUTIVE weeks in a row. Cases are confirmed
LABEL_LAG_DAYS after the week, so the bank KNOWS on alert-week end + lag; it then retrains with
every case confirmed up to the alert week (v1 train_to) and deploys v1 the next Monday.

Reads   analytics/bq2r_scores (all versions), models/bq2r/<dataset>/<version>/meta.json
Writes  analytics/bq2r_part2_weekly/    version x week x group: cases, caught, recall, alerts, precision
        analytics/bq2r_part2_typology/  version x period x typology: cases, caught, recall
        analytics/bq2r_part2_rules/     v0: each week's rule checks
        models/bq2r/<dataset>/decision.json   alert week, rules, retrain_to, deploy_from
"""

import json
import sys
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

import bq2_common as C
import train_txn as T

DATASET = sys.argv[1] if len(sys.argv) > 1 else C.RAMP
VERSION = sys.argv[2] if len(sys.argv) > 2 else "v0"          # the model being monitored
NEXT = f"v{int(VERSION[1:]) + 1}"                              # the model its alert would train
SUFFIX = "" if VERSION == "v0" else f"_{VERSION}"              # round-1 tables keep their names


def load_scores():
    try:
        sc = T.open_dataset(f"{T.ANALYTICS}/bq2r_scores").to_table(
            columns=["version", "date", "label", "pattern_label", "alert"],
            filter=ds.field("dataset") == DATASET).to_pandas()
    except (FileNotFoundError, OSError):
        raise SystemExit("no analytics/bq2r_scores yet: run bq2_part2_score.py first")
    sc["date"] = pd.to_datetime(sc["date"])
    sc["week_start"] = (sc["date"] - pd.to_timedelta(sc["date"].dt.weekday, unit="D")).dt.strftime("%Y-%m-%d")
    sc["group"] = [C.group_of(p) for p in sc["pattern_label"]]
    sc["label"], sc["alert"] = sc["label"].astype(bool), sc["alert"].astype(bool)
    return sc


def weekly(sc):
    rows = []
    for (v, w), g in sc.groupby(["version", "week_start"]):
        alerts, tp = int(g["alert"].sum()), int((g["alert"] & g["label"]).sum())
        honest = ~g["label"]
        base = {"version": v, "week_start": w, "payments": len(g), "alerts": alerts,
                "precision": tp / alerts if alerts else np.nan,
                "honest_flag_rate": float((g["alert"] & honest).sum() / max(honest.sum(), 1))}
        for grp, m in (("all", g["label"]), ("established", g["group"] == "established"),
                       ("emerging", g["group"] == "emerging")):
            n, c = int(m.sum()), int((g["alert"] & m).sum())
            rows.append({**base, "group": grp, "cases": n, "caught": c, "recall": c / n if n else np.nan})
    return pd.DataFrame(rows)


def rules(sc, meta):
    v0 = sc[sc["version"] == VERSION]
    if v0.empty:
        raise SystemExit(f"{VERSION} has not been scored")
    base_r = meta["validation"]["recall"]
    base_typ = {p: d["recall"] for p, d in meta["validation_by_pattern"].items()
                if d["cases"] >= C.MIN_TYPOLOGY_CASES}
    seen = set(meta["train_patterns"])
    lab = v0[v0["label"]]
    rows, cum = [], {}
    for w in sorted(v0["week_start"].unique()):
        g = lab[lab["week_start"] == w]
        n, c = len(g), int(g["alert"].sum())
        r1 = n >= C.MIN_WEEK_CASES and c / n < base_r - C.RECALL_DROP
        per = g.groupby("pattern_label")["alert"].agg(["size", "sum"])
        r2, r3 = [], []
        for p, (cases, caught) in per.iterrows():
            if p not in seen or p not in base_typ:          # no validation baseline: absolute bar
                a, b = cum.get(p, (0, 0))
                cum[p] = (a + int(cases), b + int(caught))
            elif p in base_typ and cases >= C.MIN_TYPOLOGY_CASES and caught / cases < base_typ[p] - C.TYPOLOGY_DROP:
                r3.append(f"{p} {caught / cases:.2f} (val {base_typ[p]:.2f})")
        for p, (cases, caught) in cum.items():
            if cases >= C.NEW_TYPOLOGY_MIN and caught / cases < C.NEW_TYPOLOGY_RECALL:
                r2.append(f"{p} {caught}/{cases}")
        rows.append({"week_start": w, "cases": n, "recall": c / n if n else np.nan, "baseline_recall": base_r,
                     "r1_recall_drop": bool(r1), "r2_new_typology": "; ".join(r2), "r3_typology_drop": "; ".join(r3),
                     "any_rule": bool(r1 or r2 or r3)})
    rw = pd.DataFrame(rows)
    run = rw["any_rule"].astype(int).groupby((~rw["any_rule"]).cumsum()).cumsum()
    rw["alert"] = run >= C.CONSECUTIVE
    return rw


def decide(rw, meta):
    hit = rw[rw["alert"]]
    if hit.empty:
        return {"dataset": DATASET, "monitored": VERSION, "alert": False,
                "note": f"no rule held long enough on {VERSION}: no retraining needed"}
    w = hit.iloc[0]
    week_end = date.fromisoformat(w["week_start"]) + timedelta(days=6)
    known = week_end + timedelta(days=C.LABEL_LAG_DAYS)
    deploy = known + timedelta(days=(7 - known.weekday()) % 7 or 7)       # the next Monday
    why = [x for x, on in (("R1 recall drop", w["r1_recall_drop"]), ("R2 new typology " + w["r2_new_typology"],
                           bool(w["r2_new_typology"])), ("R3 typology drop " + w["r3_typology_drop"],
                           bool(w["r3_typology_drop"]))) if on]
    first_dose = date.fromisoformat(C.FIRST_DOSE)
    return {"dataset": DATASET, "monitored": VERSION, "retrain": NEXT, "alert": True,
            "alert_week": w["week_start"], "rules": why,
            "known_on": str(known), "retrain_to": str(week_end), "deploy_from": str(deploy),
            "weeks_from_first_dose_to_alert_week": round((week_end - first_dose).days / 7, 1),
            "weeks_from_first_dose_to_known": round((known - first_dose).days / 7, 1),
            f"{VERSION}_validation_recall": meta["validation"]["recall"]}


def typology_table(sc, dec):
    sc = sc[sc["label"]].copy()
    dep = dec.get("deploy_from")
    sc["period"] = "after deploy" if dep is None else np.where(sc["date"] >= pd.Timestamp(dep),
                                                                "after deploy", "before deploy")
    t = sc.groupby(["version", "period", "group", "pattern_label"])["alert"].agg(["size", "sum"]).reset_index()
    t.columns = ["version", "period", "group", "typology", "cases", "caught"]
    t["recall"] = t["caught"] / t["cases"]
    return t


def save(pdf, name):
    fs, path = T.fs_path(f"{T.ANALYTICS}/{name}")
    part = ds.partitioning(pa.schema([("dataset", pa.string())]), flavor="hive")
    ds.write_dataset(pa.Table.from_pandas(pdf.assign(dataset=DATASET), preserve_index=False), path,
                     filesystem=fs, format="parquet", partitioning=part,
                     basename_template=f"part-{T.RUN_ID}-{{i}}.parquet", existing_data_behavior="delete_matching")


def main():
    mroot = f"{T.ROOT}/models/bq2r/{DATASET}"
    meta = C.get_json(f"{mroot}/{VERSION}/meta.json")
    sc = load_scores()
    wk, rw = weekly(sc), rules(sc, meta)
    dec = decide(rw, meta)
    C.put_text(f"{mroot}/decision{SUFFIX}.json", json.dumps(dec, indent=1))
    ty = typology_table(sc[sc["version"].isin([VERSION, NEXT])], dec)
    save(wk, "bq2r_part2_weekly")
    save(rw.assign(version=VERSION), f"bq2r_part2_rules{SUFFIX}")
    save(ty, f"bq2r_part2_typology{SUFFIX}")

    with pd.option_context("display.width", 220, "display.max_rows", 200, "display.max_colwidth", 60):
        print(f"[eval] {DATASET}: monitoring {VERSION} · its validation recall {meta['validation']['recall']:.3f}, "
              f"precision {meta['validation']['precision']:.3f}, threshold {meta['threshold']:.4f} (the baseline)")
        p = wk[wk["version"] == VERSION].pivot(index="week_start", columns="group", values="recall")
        p["alerts"] = wk[(wk["version"] == VERSION) & (wk["group"] == "all")].set_index("week_start")["alerts"]
        p["rule"] = rw.set_index("week_start")["any_rule"].map({True: "x", False: ""})
        p["ALERT"] = rw.set_index("week_start")["alert"].map({True: "ALERT", False: ""})
        print(f"\n[eval] {VERSION} week by week: recall (all / established / emerging), alerts, rule hit, alert")
        print(p.round(3).to_string())
        print(rw[rw["any_rule"]][["week_start", "r1_recall_drop", "r2_new_typology", "r3_typology_drop"]]
              .to_string(index=False))
        print(f"\n[eval] decision: {json.dumps(dec)}")
        if NEXT in set(sc["version"]) and dec.get("deploy_from"):
            d = dec["deploy_from"]
            both = sc[(sc["date"] >= pd.Timestamp(d)) & sc["version"].isin([VERSION, NEXT])]
            cmp = (both[both["label"]].groupby(["version", "group"])["alert"].mean().unstack("group"))
            cmp["precision"] = both.groupby("version").apply(
                lambda g: (g["alert"] & g["label"]).sum() / max(g["alert"].sum(), 1))
            print(f"\n[eval] recovery: {VERSION} vs {NEXT} on the same weeks, from {d}")
            print(cmp.round(3).to_string())
            print(ty[ty["period"] == "after deploy"].pivot_table(index=["group", "typology"], columns="version",
                                                                 values="recall").round(3).to_string())
    print(f"[eval] saved analytics/bq2r_part2_weekly, _rules{SUFFIX}, _typology{SUFFIX} and "
          f"models/bq2r/{DATASET}/decision{SUFFIX}.json")


if __name__ == "__main__":
    main()
