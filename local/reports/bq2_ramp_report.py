"""
bq2_ramp_report.py                                                       (BQ2, report)
Laptop (lab on), after run_bq2_ramp.sh. Reads only the small result tables:
    set AML_ROOT=s3://<bucket>
    python bq2\\bq2_ramp_report.py > results\\bq2\\bq2_ramp_report.txt
Also writes app/data/bq2_data.json (for a BQ2 dashboard page).
"""

import json
import os
import sys

import pandas as pd

import os as _os, sys as _sys                # pipeline modules live in aws/jobs/<folder>/
_JOBS = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "..", "..", "aws", "jobs")
_sys.path[:0] = [_os.path.join(_JOBS, d) for d in ("bq2", "bq1", "features", "clean", "load")]
import bq2_common as C
T = C          # storage helpers live in bq2_common (no train_txn.py needed on the laptop)

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "app", "data", "bq2_data.json")


def load(name):
    try:
        return T.open_dataset(f"{T.ANALYTICS}/{name}").to_table().to_pandas()
    except (FileNotFoundError, OSError):
        return pd.DataFrame()


def records(df):
    return json.loads(df.to_json(orient="records", date_format="iso")) if len(df) else []


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")      # Windows '>' would otherwise save cp1252
    except AttributeError:
        pass
    pd.set_option("display.width", 220)
    pd.set_option("display.max_rows", 300)
    pd.set_option("display.max_colwidth", 80)
    out = {}
    print("BQ2 · Are criminals changing their laundering tactics over time, and when they do,")
    print("      does our detection rate fall, and can retraining recover it?\n")
    print(f"Test world: SAML-D (Oztas et al. 2023). Emerging tactics {C.EMERGING} are absent until")
    print(f"{C.FIRST_DOSE}, then switched on gradually {C.DOSES}. SAML-C = control, nothing changes.")

    dz = load("bq2r_design")
    if len(dz):
        g = dz.groupby("group")[["payments_saml_d", "payments_ramp", "payments_control"]].sum()
        print("\nLaundering payments by group (SAML-D / ramp / control)")
        print(g.to_string())
        out["design"] = records(dz)

    # ---------------- PART 1
    sm, wk = load("bq2r_part1_summary"), load("bq2r_part1_weekly")
    if len(sm):
        print("\n" + "=" * 100 + "\nPART 1 · Are tactics changing? (confirmed cases only, no model)")
        print("changed = js above the noise threshold AND chi-square p < 0.01; tactic change = lasting "
              "(2 weeks) and honest payments unchanged\n")
        for d in ["SAML-R", "SAML-C", "HI-Large"]:
            s = sm[(sm["dataset"] == d) & (sm["population"] == "laundering")]
            if s.empty:
                continue
            print(f"--- {d}: laundering")
            print(s[["dimension", "js_threshold", "weeks_after", "weeks_changed", "weeks_tactic_change",
                     "first_change", "change_started", "max_js_after"]].round(3).to_string(index=False))
            for _, x in s.iterrows():
                if x["cusum_alarms"]:
                    print(f"    {x['dimension']}: CUSUM dates the shift: {x['cusum_alarms']}")
                if x["cusum_unconfirmed"]:
                    print(f"    {x['dimension']}: unconfirmed CUSUM alarms (no lasting change, likely noise): "
                          f"{x['cusum_unconfirmed']}")
            h = sm[(sm["dataset"] == d) & (sm["population"] == "honest")]
            print(f"    honest payments: lasting changes per dimension "
                  f"{dict(zip(h['dimension'], h['weeks_lasting']))} (should be 0: the control)")
        r = wk[(wk["dataset"] == "SAML-R") & (wk["population"] == "laundering")
               & (wk["dimension"] == "pattern") & (wk["week_start"] > C.TRAIN_TO)]
        if len(r):
            print("\nSAML-R pattern mix, week by week after the baseline (js, p, tactic change)")
            print(r[["week_start", "n", "js", "psi", "p_value", "changed", "tactic_change"]].round(4)
                  .to_string(index=False))
        out.update(part1_summary=records(sm), part1_weekly=records(wk))

    # ---------------- PART 2
    w2, rules, ty = load("bq2r_part2_weekly"), load("bq2r_part2_rules"), load("bq2r_part2_typology")
    if len(w2):
        print("\n" + "=" * 100 + "\nPART 2 · Is detection affected? (BQ1 recipe trained on SAML-R's first 17 weeks)")
        metas = {}
        for v in sorted(w2["version"].unique()):
            try:
                metas[v] = C.get_json(f"{T.ROOT}/models/bq2r/SAML-R/{v}/meta.json")
                m = metas[v]
                print(f"{v}: trained {m['train_from']}..{m['train_to']} ({m['train_laundering']} laundering), "
                      f"threshold {m['threshold']:.4f}, validation P {m['validation']['precision']:.3f} "
                      f"R {m['validation']['recall']:.3f} F1 {m['validation']['f1']:.3f}")
            except (OSError, FileNotFoundError):
                pass
        dec = C.get_json(f"{T.ROOT}/models/bq2r/SAML-R/decision.json", default={})
        p = w2.pivot_table(index="week_start", columns=["version", "group"], values="recall")
        p.columns = [f"{v} {g}" for v, g in p.columns]
        a = w2[(w2["group"] == "all")].pivot_table(index="week_start", columns="version", values="alerts")
        p = p.join(a.add_suffix(" alerts"))
        if len(rules):
            p = p.join(rules.set_index("week_start")[["any_rule", "alert"]])
        print("\nrecall week by week (all / established / emerging), alerts, v0 rule checks")
        print(p.round(3).to_string())
        print(f"\nDecision: {json.dumps(dec, indent=1)}")
        if len(ty):
            print("\nrecall per typology")
            print(ty.pivot_table(index=["group", "typology"], columns=["version", "period"],
                                 values="recall").round(3).to_string())
        out.update(part2_weekly=records(w2), part2_rules=records(rules), part2_typology=records(ty),
                   decision=dec, models={v: {k: m[k] for k in ("threshold", "train_from", "train_to",
                                                              "validation", "train_laundering")}
                                         for v, m in metas.items()})
    # ---------------- ROUND 2: new baselines, monitor v1, v2
    sn, r1, t1 = load("bq2r_part1_summary_nb"), load("bq2r_part2_rules_v1"), load("bq2r_part2_typology_v1")
    dec1 = C.get_json(f"{T.ROOT}/models/bq2r/SAML-R/decision_v1.json", default={})
    if len(sn) or dec1:
        print("\n" + "=" * 100 + "\nROUND 2 · after retraining: new baselines, keep monitoring")
        if len(sn):
            s2 = sn[(sn["dataset"] == "SAML-R") & (sn["population"] == "laundering")]
            print("Part 1 with the new data baseline (weeks with the new tactics fully in): lasting changes after it")
            print(s2[["dimension", "js_threshold", "baseline_weeks", "weeks_after", "weeks_tactic_change"]]
                  .round(3).to_string(index=False))
        if len(r1):
            print("\nPart 2 · v1 checked against v1's own validation")
            print(r1[r1["any_rule"]][["week_start", "recall", "r1_recall_drop", "r2_new_typology", "r3_typology_drop",
                                      "alert"]].round(3).to_string(index=False))
        print(f"\nDecision on v1: {json.dumps(dec1, indent=1)}")
        if len(t1):
            print("\nrecall per typology after v2 went live (v1 vs v2, same weeks)")
            print(t1[t1["period"] == "after deploy"].pivot_table(index=["group", "typology"], columns="version",
                                                                 values="recall").round(3).to_string())
        out.update(part1_summary_nb=records(sn), part1_weekly_nb=records(load("bq2r_part1_weekly_nb")),
                   part2_rules_v1=records(r1), part2_typology_v1=records(t1), decision_v1=dec1)
        try:
            m2 = C.get_json(f"{T.ROOT}/models/bq2r/SAML-R/v2/meta.json")
            out.setdefault("models", {})["v2"] = {k: m2[k] for k in ("threshold", "train_from", "train_to",
                                                                     "validation", "train_laundering")}
        except (OSError, FileNotFoundError):
            pass
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(out, f, indent=1, default=str)
    print(f"\nwrote {os.path.normpath(OUT)}", file=sys.stderr)


if __name__ == "__main__":
    main()
