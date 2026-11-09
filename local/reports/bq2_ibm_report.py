"""
bq2_ibm_report.py                                                        (BQ2, HI-Large tab report)
Laptop (lab on), after run_bq2_ibm.sh. Reads only the small result tables:
    export AML_ROOT=s3://<bucket>   (Windows: set AML_ROOT=s3://<bucket>)
    python3 local/reports/bq2_ibm_report.py > results/bq2/bq2_ibm_report.txt
Writes app/data/bq2_ibm_data.json for the HI-Large tab of app/app_bq2.py:
    part1      Joey's Part 1 on HI-Large: real data, and the new-tactic test (tag _nt)
    part2      Joey's Part 2 rules (bq2_part2_eval.py) on HI-Large (v0 = BQ1 model) and on
               HI-Large-NT (v0 = model that never saw the new tactic, v1 = BQ1 model)
"""
import json
import os
import sys

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))                     # local/reports/
_REPO = os.path.join(_HERE, "..", "..")
sys.path[:0] = [os.path.join(_REPO, "aws", "jobs", d) for d in ("bq2", "bq1", "prepare")]
import bq2_common as C   # noqa: E402  (storage helpers; no train_txn.py needed on the laptop)

DS, NT = "HI-Large", "HI-Large-NT"
OUT = os.path.join(_REPO, "app", "data", "bq2_ibm_data.json")


def load(name, ds_):
    try:
        t = C.open_dataset(f"{C.ANALYTICS}/{name}").to_table().to_pandas()
    except (FileNotFoundError, OSError):
        return pd.DataFrame()
    t["dataset"] = t["dataset"].astype(str)
    return t[t["dataset"] == ds_].reset_index(drop=True)


def records(df):
    return json.loads(df.to_json(orient="records", date_format="iso")) if len(df) else []


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except AttributeError:
        pass
    pd.set_option("display.width", 220)
    pd.set_option("display.max_rows", 300)
    pd.set_option("display.max_colwidth", 80)
    out = {"part1": {}, "part2": {}, "meta": {}, "decision": {}}

    print("BQ2 · HI-Large (IBM) · Joey's method on our data + the fan-in / fan-out new-tactic test\n")
    for key, tag in (("real", ""), ("new_tactic", "_nt")):
        wk = load(f"bq2r_part1_weekly{tag}", DS)
        sh = load(f"bq2r_part1_shares{tag}", DS)
        sm = load(f"bq2r_part1_summary{tag}", DS)
        out["part1"][key] = {"weekly": records(wk), "shares": records(sh), "summary": records(sm)}
        print(f"=== Part 1 · tactics · {key} ({len(wk['week_start'].unique()) if len(wk) else 0} weeks)")
        if len(sm):
            print(sm[sm["population"] == "laundering"][["dimension", "js_threshold", "weeks_after", "weeks_changed",
                                                        "weeks_tactic_change", "first_change", "max_js_after",
                                                        "change_started"]].round(3).to_string(index=False))
            lw = wk[wk["population"] == "laundering"]
            piv = lw.assign(v=[f"{j:.2f}{'*' if t else ''}" for j, t in zip(lw["js"], lw["tactic_change"])]) \
                    .pivot(index="week_start", columns="dimension", values="v")
            print("laundering JS per week (* = tactic change)\n" + piv.to_string() + "\n")

    for key, ds_ in (("real", DS), ("new_tactic", NT)):
        wk = load("bq2r_part2_weekly", ds_)
        rw = load("bq2r_part2_rules", ds_)
        ty = load("bq2r_part2_typology", ds_)
        out["part2"][key] = {"weekly": records(wk), "rules": records(rw), "typology": records(ty)}
        out["decision"][key] = C.get_json(f"{C.ROOT}/models/bq2r/{ds_}/decision.json", {})
        out["meta"][key] = {v: C.get_json(f"{C.ROOT}/models/bq2r/{ds_}/{v}/meta.json", {}) for v in ("v0", "v1")}
        print(f"=== Part 2 · detection · {key} ({ds_})")
        for v, m in out["meta"][key].items():
            if m:
                print(f"  {v}: {m['model_folder']} · threshold {m['threshold']:.4f} · validation recall "
                      f"{m['validation']['recall']:.3f}, precision {m['validation']['precision']:.3f} · "
                      f"never saw {m['excluded_patterns'] or 'nothing'}")
        if len(wk):
            p = wk.pivot_table(index="week_start", columns=["version", "group"], values="recall").round(3)
            print(p.to_string())
        if len(rw):
            print(rw[["week_start", "cases", "recall", "baseline_recall", "r1_recall_drop", "r2_new_typology",
                      "r3_typology_drop", "alert"]].round(3).to_string(index=False))
        print(f"  decision: {json.dumps(out['decision'][key])}\n")

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(out, f, default=str)
    print(f"wrote {os.path.abspath(OUT)}")


if __name__ == "__main__":
    main()
