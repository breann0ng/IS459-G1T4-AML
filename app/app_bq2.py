"""
BQ2 page for the model owner / AML analytics team.
    streamlit run app_bq2.py
Reads data/bq2_data.json (written by bq2/bq2_ramp_report.py) and, for the HI-Large tab,
data/dashboard_data.json (BQ1's test result, written by pipeline/final_report.py).
"""
import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

ORANGE, BLUE, GREY, INK = "#E07B39", "#2F7FAD", "#9A948B", "#262626"
DATA = Path(__file__).parent / "data"
NICE = {"Structuring": "structuring", "Smurfing": "smurfing", "Cash_Withdrawal": "cash withdrawal",
        "Deposit-Send": "deposit-send"}

st.set_page_config(page_title="BQ2 · Tactic change & detection", layout="wide")
st.markdown("""<style>
.tag{display:inline-block;padding:1px 9px;border-radius:999px;font-size:0.82rem;font-weight:600;white-space:nowrap}
.yes{background:#FBE3D6;color:#9A3410} .warn{background:#FBF0D9;color:#7A5200} .no{background:#ECE6DC;color:#3F3E3A}
.wk{width:100%;border-collapse:collapse;font-size:0.9rem} .wk th{text-align:left;color:#5b5750;border-bottom:1.5px solid #d9d2c6;padding:6px}
.wk td{border-bottom:1px solid #eee6da;padding:6px;vertical-align:top} .muted{color:#8a857c}
.cards{display:grid;grid-template-columns:1fr 1fr 2fr;gap:16px;margin:8px 0 18px}
.card{background:#fff;border:1px solid #e6ddd0;border-radius:12px;padding:16px 20px;box-shadow:0 1px 2px rgba(0,0,0,.04)}
.card .l{font-size:0.9rem;color:#5b5750} .card .v{font-size:2.1rem;font-weight:700;color:#262626;margin:4px 0 2px;line-height:1.15}
.card .s{font-size:0.82rem;color:#8a857c} .dot{display:inline-block;width:12px;height:12px;border-radius:50%;margin-right:8px;vertical-align:middle}
</style>""", unsafe_allow_html=True)


@st.cache_data
def load():
    d = json.loads((DATA / "bq2_data.json").read_text())
    bq1 = None
    if (DATA / "dashboard_data.json").exists():
        s1 = pd.DataFrame(json.loads((DATA / "dashboard_data.json").read_text())["stage1"])
        bq1 = s1[(s1["level"] == "account_day") & (s1["cutoff"] == "frozen_threshold")].iloc[0].to_dict()
    p1 = pd.DataFrame(d["part1_weekly"])
    p1["date"] = pd.to_datetime(p1["week_start"])
    for ds_, g in p1.groupby("dataset"):
        p1.loc[g.index, "week"] = ((g["date"] - g["date"].min()).dt.days // 7 + 1).astype(int)
    p1["week"] = p1["week"].astype(int)
    return d, p1, pd.DataFrame(d["part2_weekly"]), pd.DataFrame(d["part2_typology"]), pd.DataFrame(d["design"]), bq1


d, P1, W2, T2, DZ, BQ1 = load()
dec = d.get("decision", {})


def week_of(ds_, date_str):
    s = P1[P1["dataset"] == ds_]["date"].min()
    return int((pd.Timestamp(date_str) - s).days // 7 + 1)


MEASURE = {"format": "payment format", "pattern": "typology mix"}


def tag(status):
    cls = "yes" if status.startswith("confirmed") else "warn" if status.startswith("above") else "no"
    return f'<span class="tag {cls}">{status}</span>'


def week_status(ds_):
    """confirmed change / above threshold / no change / baseline / warm-up, per week (format + typology mix)."""
    w = P1[(P1["dataset"] == ds_) & (P1["population"] == "laundering") & P1["dimension"].isin(["format", "pattern"])]
    base_end = w[w["in_baseline"]]["week"].max()
    base_start = w[w["in_baseline"]]["week"].min()
    out = {}
    for wk, g in w.groupby("week"):
        if wk < base_start:
            out[wk] = "warm-up"
        elif wk <= base_end:
            out[wk] = "baseline"
        elif g["tactic_change"].any():
            out[wk] = "confirmed · " + " + ".join(MEASURE[x] for x in g[g["tactic_change"]]["dimension"])
        elif g["changed"].any():
            out[wk] = "above threshold · " + " + ".join(MEASURE[x] for x in g[g["changed"]]["dimension"])
        else:
            out[wk] = "no change"
    return out, base_start, base_end


def change_chart(lines, thr, bands=(), rules=(), confirmed=None, ymax=0.6):
    """lines: DataFrame week, js, series. bands: (from_week, to_week, label). rules: (week, label)."""
    x = alt.X("week:Q", title="Week", axis=alt.Axis(tickMinStep=1, format="d"),
              scale=alt.Scale(domain=[lines["week"].min(), lines["week"].max()], nice=False))
    layers = []
    if bands:
        b = pd.DataFrame(bands, columns=["w0", "w1", "label"])
        b["op"] = [0.25 + 0.6 * i / max(len(b) - 1, 1) for i in range(len(b))]
        layers.append(alt.Chart(b).mark_rect(color="#EFE6D8").encode(
            x="w0:Q", x2="w1:Q", opacity=alt.Opacity("op:Q", legend=None)))
        layers.append(alt.Chart(b).mark_text(dy=-6, color="#7a7974", fontSize=11, baseline="bottom").encode(
            x=alt.X("mid:Q"), y=alt.value(14), text="label:N").transform_calculate(mid="(datum.w0 + datum.w1) / 2"))
    series = list(lines["series"].unique())
    colors = [ORANGE if s != series[-1] or len(series) == 1 else GREY for s in series]
    layers.append(alt.Chart(lines).mark_line(strokeWidth=2.5).encode(
        x=x, y=alt.Y("js:Q", title="Change vs baseline (JS distance)", scale=alt.Scale(domain=[0, ymax])),
        color=alt.Color("series:N", scale=alt.Scale(domain=series, range=colors), legend=alt.Legend(title=None, orient="top")),
        tooltip=["series", "week", alt.Tooltip("js:Q", format=".2f")]))
    if confirmed is not None and len(confirmed):
        layers.append(alt.Chart(confirmed).mark_point(filled=True, size=55, color=ORANGE, opacity=1).encode(
            x="week:Q", y="js:Q", tooltip=["week", alt.Tooltip("js:Q", format=".2f")]))
    th = pd.DataFrame({"y": [thr], "t": [f"threshold {thr:.2f}"]})
    layers.append(alt.Chart(th).mark_rule(strokeDash=[6, 4], color=INK).encode(y="y:Q"))
    layers.append(alt.Chart(th).mark_text(align="left", dx=4, dy=-7, color=INK, fontWeight="bold").encode(
        x=alt.value(0), y="y:Q", text="t:N"))
    if rules:
        r = pd.DataFrame(rules, columns=["week", "label"])
        layers.append(alt.Chart(r).mark_rule(strokeDash=[2, 3], color="#5b5750").encode(x="week:Q"))
        for i, (wk, lab) in enumerate(rules):
            layers.append(alt.Chart(pd.DataFrame({"week": [wk], "label": [lab]})).mark_text(
                align="left", dx=4, color=INK, fontWeight="bold", fontSize=11).encode(
                x="week:Q", y=alt.value(36 + 17 * i), text="label:N"))
    return alt.layer(*layers).properties(height=380)


def bars(df):
    """df: measure, model, value."""
    order = list(dict.fromkeys(df["model"]))
    pal = {"v0 · before retraining": ORANGE, "v1 · after retraining": BLUE, "BQ1 model · no retraining": BLUE,
           "v1 · before 2nd retraining": BLUE, "v2 · after 2nd retraining": "#1B9E77"}
    base = alt.Chart(df).encode(
        x=alt.X("measure:N", title=None, sort=list(dict.fromkeys(df["measure"])),
                scale=alt.Scale(paddingInner=0.55 if len(order) > 1 else 0.78, paddingOuter=0.3),
                axis=alt.Axis(labelAngle=0, labelLimit=0, labelFontSize=13)),
        xOffset=alt.XOffset("model:N", sort=order, scale=alt.Scale(paddingInner=0)),
        y=alt.Y("value:Q", title=None, axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1.05])))
    b = base.mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, width=alt.RelativeBandSize(1)).encode(
        color=alt.Color("model:N", sort=order, scale=alt.Scale(domain=order, range=[pal[m] for m in order]),
                        legend=alt.Legend(title=None, orient="top")),
        tooltip=["model", "measure", alt.Tooltip("value:Q", format=".1%")])
    t = base.mark_text(dy=-8, color=INK).encode(text=alt.Text("value:Q", format=".0%"))
    return (b + t).properties(height=300)


def cards(items):
    """items: (label, value, sub, dot colour or None)."""
    html = "".join(f'<div class="card"><div class="l">{l}</div><div class="v">'
                   + (f'<span class="dot" style="background:{c}"></span>' if c else "")
                   + f'{v}</div><div class="s">{sub}</div></div>' for l, v, sub, c in items)
    st.markdown(f'<div class="cards">{html}</div>', unsafe_allow_html=True)


def table(rows):
    body = "".join(f"<tr><td style='white-space:nowrap'>Week {w}</td><td>{tag(s)}</td><td>{t}</td></tr>" for w, s, t in rows)
    st.markdown(f'<table class="wk"><tr><th>Week</th><th>Data change (which measure)</th><th>New tactics in the data</th></tr>{body}</table>',
                unsafe_allow_html=True)


# ------------------------------------------------------------------ header
st.title("Are criminals changing their tactics, and is detection keeping up?")
st.markdown("**BQ2:** Are criminals changing their laundering tactics over time, and when they do, does our "
            "detection rate fall, and can retraining recover it?")
st.caption("For the model owner and AML analytics team · Part 1 uses confirmed laundering cases only (no model); "
           "Part 2 grades the model every week against confirmed cases, which arrive 4 weeks late.")

tab_ibm, tab_saml = st.tabs(["HI-Large · IBM AML", "SAML-D · new tactics test"])

# ================================================================== HI-Large
def ibm_tab():
    ds_ = "HI-Large"
    if P1[P1["dataset"] == ds_].empty:
        st.info("No HI-Large results in data/bq2_data.json: run bq2/bq2_ramp_report.py after run_bq2_ramp.sh.")
        return
    stat, b0, b1 = week_status(ds_)
    lw = P1[(P1["dataset"] == ds_) & (P1["population"] == "laundering")]
    after = lw[lw["week"] > b1]
    changed = after["tactic_change"].any()
    n_after = after["week"].nunique()
    cards([("Tactic change detected?", "Yes" if changed else "No", "all 6 measures stable after the baseline",
            ORANGE if changed else GREY),
           ("Week detected", "—", f"no lasting change in {n_after} weeks", None),
           ("Detection impact", "None", (f"no alert, no retraining needed · BQ1 precision {BQ1['precision']:.0%}, "
                                         f"recall {BQ1['recall']:.0%}") if BQ1 else "no alert, no retraining needed", None)])

    st.subheader("Laundering payment-format mix vs baseline")
    f = lw[lw["dimension"] == "format"].sort_values("week")
    thr = float(f["js_threshold"].iloc[0])
    st.altair_chart(change_chart(f.assign(series="laundering")[["week", "js", "series"]], thr,
                                 bands=[(1, b0, "simulator warm-up"), (b0, b1 + 1, "baseline")],
                                 ymax=0.3), width="stretch")
    st.caption(f"Week 1 = {f['date'].min():%d %b %Y}. Weeks 1–{b0 - 1} are the simulator starting up "
               f"(schemes still being created), so the baseline is Weeks {b0}–{b1}. After it, every week "
               "stays far below the threshold, so no alert is raised and the model is not retrained.")

    if BQ1:
        st.subheader(f"Detection on HI-Large · frozen BQ1 model (test window {BQ1['date_from'][:10]} to {BQ1['date_to'][:10]})")
        m = "BQ1 model · no retraining"
        st.altair_chart(bars(pd.DataFrame({"measure": ["Precision", "Recall · all laundering", "Recall · named patterns"],
                                           "model": [m] * 3,
                                           "value": [BQ1["precision"], BQ1["recall"], BQ1["recall_patterns"]]})),
                        width="stretch")
        st.caption("No tactic change, so no alert and no retraining: the BQ1 model keeps its test performance.")


with tab_ibm:
    ibm_tab()

# ================================================================== SAML-D
with tab_saml:
    ds_, ctrl = "SAML-R", "SAML-C"
    stat, b0, b1 = week_status(ds_)
    lw = P1[(P1["dataset"] == ds_) & (P1["population"] == "laundering")]
    em = DZ[(DZ["group"] == "emerging") & (DZ["payments_ramp"] > 0)].copy()
    em["week"] = [week_of(ds_, w) for w in em["week_start"]]
    start_wk = int(em["week"].min())
    tc = lw[lw["tactic_change"] & lw["dimension"].isin(["format", "pattern"])]
    det_wk = int(tc["week"].min())
    dims = sorted(set(tc["dimension"]), key=["format", "pattern"].index)
    dep = dec.get("deploy_from")

    dec1 = d.get("decision_v1", {})
    dep2 = dec1.get("deploy_from") if dec1.get("alert") and "v2" in set(W2["version"]) else None

    def tot(v, g, frm=None):
        a = W2[(W2["version"] == v) & (W2["week_start"] >= (frm or dep)) & (W2["group"] == g)]
        return a["caught"].sum() / a["cases"].sum()

    def prec(v, frm=None):
        a = W2[(W2["version"] == v) & (W2["week_start"] >= (frm or dep)) & (W2["group"] == "all")]
        return a["caught"].sum() / a["alerts"].sum()

    a_wk = week_of(ds_, dec["alert_week"]) if dec.get("alert") else None
    known_wk = week_of(ds_, dec["known_on"]) if dec.get("alert") else None
    dep_wk = week_of(ds_, dep) if dep else None
    a2_wk = week_of(ds_, dec1["alert_week"]) if dep2 else None
    known2_wk = week_of(ds_, dec1["known_on"]) if dep2 else None
    dep2_wk = week_of(ds_, dep2) if dep2 else None
    impact = f"{tot('v0', 'emerging'):.0%} → {tot('v1', 'emerging'):.0%}"
    impact_sub = (f"new-tactic recall, before → after retraining (v1 live Week {dep_wk}) · precision "
                  f"{prec('v0'):.1%} → {prec('v1'):.1%}")
    if dep2:
        impact += f" → {tot('v2', 'emerging', dep2):.0%}"
        impact_sub = (f"new-tactic recall · v0 → v1 (live Week {dep_wk}) → v2 (live Week {dep2_wk}) · precision "
                      f"stays {min(prec('v1'), prec('v2', dep2)):.0%}+")
    cards([("Tactic change detected?", "Yes",
            ", ".join({"format": "payment format", "pattern": "typology mix"}[x] for x in dims) + " · control: no change",
            ORANGE),
           ("Week detected", f"Week {det_wk}", f"{det_wk - start_wk} weeks after the new tactics began (Week {start_wk})", None),
           ("Detection impact", impact, impact_sub, None)])

    st.subheader("Laundering payment-format mix vs baseline")
    fr = lw[lw["dimension"] == "format"].sort_values("week")
    fc = P1[(P1["dataset"] == ctrl) & (P1["population"] == "laundering") & (P1["dimension"] == "format")]
    thr = float(fr["js_threshold"].iloc[0])
    lines = pd.concat([fr.assign(series="SAML-D with new tactics"), fc.assign(series="control (no new tactics)")])
    dz = DZ.groupby("week_start")["dose"].max().reset_index()
    dz["week"] = [week_of(ds_, w) for w in dz["week_start"]]
    dz = dz[dz["dose"] > 0].sort_values("week")
    bands = [(b0, b1 + 1, "baseline · v0 trained")]
    for dose, g in dz.groupby("dose"):
        bands.append((int(g["week"].min()), int(g["week"].max()) + 1, f"new tactics {dose:.0%}"))
    nb = pd.DataFrame(d.get("part1_weekly_nb", []))
    nb_txt = ""
    if len(nb):
        nbw = sorted({week_of(ds_, w) for w in nb[(nb["dataset"] == ds_) & nb["in_baseline"]]["week_start"]})
        after = nb[(nb["dataset"] == ds_) & (nb["population"] == "laundering") & ~nb["in_baseline"]
                   & (nb["week_start"] > nb[nb["in_baseline"]]["week_start"].max())]
        if nbw:
            bands.append((nbw[0], nbw[-1] + 1, "new baseline"))
            nb_txt = (f" After v1, the data baseline moves to Weeks {nbw[0]}–{nbw[-1]} (new tactics fully in): the "
                      f"{after['week_start'].nunique()} weeks after it show "
                      + ("no lasting change." if not after["tactic_change"].any() else "a lasting change."))
    rules = [(start_wk, f"Wk {start_wk} · new tactics start"), (det_wk, f"Wk {det_wk} · change detected")]
    if a_wk:
        rules.append((a_wk, f"Wk {a_wk} · v0 falls behind (known Wk {known_wk})"))
    if dep_wk:
        rules.append((dep_wk, f"Wk {dep_wk} · v1 retrained and live"))
    if dep2:
        rules.append((a2_wk, f"Wk {a2_wk} · v1 falls behind (known Wk {known2_wk})"))
        rules.append((dep2_wk, f"Wk {dep2_wk} · v2 retrained and live"))
    st.altair_chart(change_chart(lines[["week", "js", "series"]], thr, bands=sorted(bands), rules=rules,
                                 confirmed=fr[fr["tactic_change"]][["week", "js"]]), width="stretch")
    st.caption(f"Week 1 = {fr['date'].min():%d %b %Y}. Each week's laundering is compared with the baseline "
               f"(Weeks {b0}–{b1}); dot = confirmed change (above the threshold {thr:.2f} two weeks in a row, "
               "honest payments unchanged). Each model is graded on confirmed cases, which arrive 4 weeks late; "
               "after every retraining the new model's own validation becomes the baseline it is checked against."
               + nb_txt)

    def round_bars(old, new, frm, title, la, lb):
        st.subheader(title)
        rows = [(mname, mdl, val) for mname, f0, f1 in [
            ("Precision", prec(old, frm), prec(new, frm)),
            ("Recall · all laundering", tot(old, "all", frm), tot(new, "all", frm)),
            ("Recall · new tactics", tot(old, "emerging", frm), tot(new, "emerging", frm)),
            ("Recall · established", tot(old, "established", frm), tot(new, "established", frm))]
            for mdl, val in ((la, f0), (lb, f1))]
        st.altair_chart(bars(pd.DataFrame(rows, columns=["measure", "model", "value"])), width="stretch")

    last_wk = int(lw["week"].max())
    if dep:
        c1, c2 = st.columns(2, gap="large") if dep2 else (st.container(), None)
        with c1:
            round_bars("v0", "v1", dep, f"1st retraining · v0 vs v1 (Weeks {dep_wk}–{last_wk})",
                       "v0 · before retraining", "v1 · after retraining")
            tt = T2[T2["period"] == "after deploy"]
            weak = tt[(tt["version"] == "v1") & (tt["group"] == "emerging")].sort_values("recall").head(2)
            st.caption(f"v0 fell behind in Weeks {a_wk - 1}–{a_wk} (rule R2: new typologies mostly missed). The cases "
                       f"were confirmed at the end of Week {known_wk}, so v1 was retrained immediately and went live in "
                       f"Week {dep_wk}. " + ", ".join(f"{t.replace('_', ' ')} ({r:.0%})" for t, r in
                                                      zip(weak['typology'], weak['recall']))
                       + " had few confirmed cases at that point, so v1 learned them only partly.")
        if dep2:
            with c2:
                round_bars("v1", "v2", dep2, f"2nd retraining · v1 vs v2 (Weeks {dep2_wk}–{last_wk})",
                           "v1 · before 2nd retraining", "v2 · after 2nd retraining")
                t1 = pd.DataFrame(d.get("part2_typology_v1", []))
                t1 = t1[(t1["period"] == "after deploy") & (t1["group"] == "emerging")] if len(t1) else t1
                fixed = ""
                if len(t1):
                    p = t1.pivot_table(index="typology", columns="version", values="recall")
                    p = p.assign(gain=p["v2"] - p["v1"]).sort_values("gain", ascending=False).head(2)
                    fixed = ", ".join(f"{t.replace('_', ' ')} {r.v1:.0%} → {r.v2:.0%}" for t, r in p.iterrows())
                st.caption(f"Monitoring continued on v1 against its own validation: v1 fell behind in Weeks "
                           f"{a2_wk - 1}–{a2_wk} ({'; '.join(dec1.get('rules', []))}). Known at the end of Week "
                           f"{known2_wk}; v2 retrained on every case confirmed by then and went live in Week {dep2_wk}. "
                           + (f"Biggest gains: {fixed}." if fixed else ""))
