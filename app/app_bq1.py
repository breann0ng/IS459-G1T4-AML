"""
BQ1 page for the Transaction Monitoring Tuning Team.
    streamlit run app_bq1.py        (data/<DATA_FILE>; written by local/reports/final_report.py)

Order follows the team's decisions: 1 how many alerts a day (accuracy), 2 how early schemes are
caught (speed), 3 which patterns the model can handle and which need analysts (red / yellow / green),
4 what the analyst sees on each alert (the namer's two guesses).
"""
import json
import os
from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

# final model; after the full HI-Large run, point this (or env BQ1_DATA) at that run's file
DATA_FILE = os.environ.get("BQ1_DATA", "dashboard_data_enrui_breann_mld20.json")
BLUE, ORANGE, GREY, INK = "#2F7FAD", "#E07B39", "#B9B2A8", "#262626"
RAG = {"Red": "#F2C9C4", "Yellow": "#F8E7B0", "Green": "#CFE8CC"}
ACTION = {"Red": "Analysts check these by hand", "Yellow": "Spot-check a sample",
          "Green": "Let the model handle it"}
LAYERING = {"stack", "bipartite"}           # stage 2 names these two together as "layering"
VALIDATION_TOP2 = 0.82                      # enrui_breann_mld20_grp, HI-Small + HI-Medium validation

st.set_page_config(page_title="BQ1 · Early detection", layout="wide")


@st.cache_data
def load():
    return json.loads((Path(__file__).parent / "data" / DATA_FILE).read_text())


def pct(x):
    """Percent without overstating: 0.9996 shows as >99.9%, small shares keep a decimal."""
    if x is None or pd.isna(x):
        return "–"
    if 0.999 < x < 1:
        return ">99.9%"
    return f"{x:.1%}" if x < 0.1 or x > 0.9 else f"{x:.0%}"


def per_day(x):
    return f"{x:,.0f}" if x >= 10 else f"{x:.1f}"


def hours(h):
    if h is None or pd.isna(h):
        return "–"
    if h < 1:
        return "under 1 h"
    return f"{h:.0f} h" if h < 48 else f"{h / 24:.1f} days"


def nice(p):
    return "Layering (stack + bipartite)" if p == "layering" else p.replace("_", " ").capitalize()


def stage2_class(p):
    return "layering" if p in LAYERING else p


d = load()
s1 = pd.DataFrame(d["stage1"])
ad = s1[s1["level"] == "account_day"]
frozen = ad[ad["cutoff"] == "frozen_threshold"].iloc[0]
tot = d["schemes_total"]
days = int(frozen["days"])
thr_k = round(frozen["alerts_per_day"])
sc = pd.DataFrame(d.get("schemes") or [])              # one row per scheme (newer final_report.py)
top2 = pd.DataFrame(d.get("top2") or [])

# ------------------------------------------------------------------ header
st.title("Catching laundering early, at a volume analysts can manage")
st.markdown("**For the Transaction Monitoring Tuning Team**, who decide how many alerts go to analysts each day "
            "and which laundering patterns need a person to check.")
st.markdown("**BQ1:** How can we automatically detect laundering patterns **early**, while keeping false alerts "
            "low enough for analysts to manage?")
st.caption(f"Tested once on {days} unseen days of HI-Large ({frozen['date_from'][:10]} to {frozen['date_to'][:10]}), "
           f"{frozen['payments'] / 1e6:.1f}M payments · trained on HI-Small and HI-Medium only")

# ------------------------------------------------------------------ 1. alerts and accuracy
st.subheader("1 · Alerts and accuracy: how many alerts a day?")
st.caption("Each day, accounts are ranked by risk score and the top ones become alerts. "
           "Pick a daily volume to see what it catches and what it costs.")

opts = ad[ad["cutoff"].str.startswith("top") | (ad["cutoff"] == "frozen_threshold")].copy()
opts["k"] = opts["alerts_per_day"].round().astype(int)
opts = opts.sort_values("k")
default = 2000 if 2000 in opts["k"].tolist() else thr_k
pick = st.select_slider("Alerts reviewed per day", options=opts["k"].tolist(), value=default,
                        format_func=lambda v: f"{v:,} (model's threshold)" if v == thr_k else f"{v:,}")
row = opts[opts["k"] == pick].iloc[0]
laundering_day = row["positives"] / days

m1, m2, m3, m4 = st.columns(4)
m1.metric("Alerts per day", f"{pick:,}")
m2.metric("Real laundering alerts", per_day(pick * row["precision"]), f"{pct(row['precision'])} precision",
          delta_color="off", delta_arrow="off",
          help="Alerts that really were laundering. Precision = real alerts / all alerts.")
m3.metric("Laundering missed per day", per_day(laundering_day * (1 - row["recall"])),
          f"{pct(1 - row['recall'])} of laundering (1 - recall)", delta_color="off", delta_arrow="off",
          help="Laundering account-days that got no alert, including laundering that follows no known pattern.")
m4.metric("False alerts per day", per_day(pick * (1 - row["precision"])),
          help="Alerts analysts clear that turn out not to be laundering.")

long = opts.melt(id_vars="k", value_vars=["precision", "recall"], var_name="measure", value_name="value")
long["measure"] = long["measure"].map({"precision": "Alerts that are real (precision)",
                                       "recall": "Laundering caught (recall)"})
x = alt.X("k:O", title="Alerts reviewed per day", sort=opts["k"].tolist(), axis=alt.Axis(labelAngle=0, format=","))
base = alt.Chart(long).encode(
    x=x, y=alt.Y("value:Q", title=None, axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
    color=alt.Color("measure:N", scale=alt.Scale(range=[BLUE, ORANGE]),
                    legend=alt.Legend(title=None, orient="top", labelLimit=0)))
lines = base.mark_line(strokeWidth=2.5) + base.mark_point(filled=True, size=60).encode(
    tooltip=[alt.Tooltip("k:O", title="Alerts/day", format=","), "measure:N", alt.Tooltip("value:Q", format=".1%")])
rule = alt.Chart(pd.DataFrame({"k": [pick]})).mark_rule(color=INK, strokeDash=[4, 4]).encode(x=x)
st.altair_chart((lines + rule).properties(height=280), width="stretch")
st.caption(f"The model's own threshold sends about {thr_k:,} alerts a day; sections 2 to 4 use that setting. "
           "Past that, most extra alerts are false.")

st.divider()

# ------------------------------------------------------------------ 2. speed
st.subheader("2 · Speed: is the scheme caught before it ends?")
st.caption("A scheme is one laundering operation in the answer key (Patterns.txt), from its first payment to its last. "
           "Alerts are ready when the day closes, so a scheme that finishes the same day is over before analysts hear of it.")

pattern_names = sorted(sc["pattern_type"].unique()) if not sc.empty else []
s1c, s2c = st.columns([1, 2], gap="large")
with s1c:
    sel = st.selectbox("Pattern", ["All patterns"] + pattern_names, format_func=lambda p: p if p == "All patterns" else nice(p)) \
        if pattern_names else "All patterns"
    sub = sc if sel == "All patterns" or sc.empty else sc[sc["pattern_type"] == sel]
    if not sub.empty:
        caught = sub[sub["caught"].astype(bool)]
        st.metric("Schemes caught", pct(sub["caught"].astype(bool).mean()),
                  help=f"{len(caught):,} of {len(sub):,} schemes had at least one account alerted.")
        st.metric("Caught before the scheme ends", pct(sub["caught_before_end_eod"].fillna(False).astype(bool).mean()),
                  help="The end-of-day alert arrived before the scheme's last payment.")
        st.metric("Median time to alert", hours(caught["hours_to_alert"].median()),
                  help="From the scheme's first payment to the end-of-day alert reaching analysts (caught schemes).")
        st.metric("Median scheme length", hours(sub["duration_h"].median()), help="First payment to last payment.")
    else:
        st.metric("Schemes caught", pct(tot["caught"]))
        st.metric("Caught before the scheme ends", pct(tot["alert_before_end_eod"]))
        st.metric("Median scheme length", hours(tot["median_duration_h"]))

with s2c:
    if not sub.empty:
        clock = st.radio("When does detection count?", ["End-of-day alert (what analysts get)",
                                                        "First high-risk payment (if scored live)"], horizontal=True)
        det_col = "hours_to_alert" if clock.startswith("End") else "hours_to_first_flag"
        t = np.arange(0, 169, 2)
        dur = sub["duration_h"].to_numpy()
        det = np.where(sub["caught"].astype(bool), sub[det_col].astype(float), np.inf)
        curve = pd.DataFrame({"h": np.concatenate([t, t]),
                              "share": np.concatenate([[(dur <= v).mean() for v in t], [(det <= v).mean() for v in t]]),
                              "line": ["Scheme finished"] * len(t) + ["Scheme detected"] * len(t)})
        med = float(np.median(det[np.isfinite(det)])) if np.isfinite(det).any() else None
        xh = alt.X("h:Q", title="Hours since the scheme's first payment", scale=alt.Scale(domain=[0, 168]),
                   axis=alt.Axis(values=list(range(0, 169, 24))))
        cl = alt.Chart(curve).mark_line(strokeWidth=2.5, interpolate="step-after").encode(
            x=xh, y=alt.Y("share:Q", title="Share of schemes", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
            color=alt.Color("line:N", scale=alt.Scale(domain=["Scheme finished", "Scheme detected"], range=[GREY, ORANGE]),
                            legend=alt.Legend(title=None, orient="top")),
            tooltip=[alt.Tooltip("h:Q", title="Hours"), "line:N", alt.Tooltip("share:Q", format=".0%")])
        layers = cl
        if med is not None and med <= 168:
            mr = pd.DataFrame({"h": [med], "label": [f"median detection {hours(med)}"]})
            layers += alt.Chart(mr).mark_rule(color=ORANGE, strokeDash=[4, 4]).encode(x=xh)
            layers += alt.Chart(mr).mark_text(align="left", dx=4, dy=-120, color=ORANGE).encode(x=xh, text="label:N")
        st.altair_chart(layers.properties(height=320), width="stretch")
        st.caption("Grey: share of schemes already over. Orange: share already detected. Where orange sits below grey, "
                   "schemes are finishing before anyone is told; the gap is the laundering analysts are too late for.")
    else:
        sp = pd.DataFrame(d["schemes_by_pattern"])
        sp["pattern"] = sp["pattern_type"].map(nice)
        bars = alt.Chart(sp).mark_bar(cornerRadiusEnd=3, size=20, color=BLUE).encode(
            y=alt.Y("pattern:N", sort="-x", title=None),
            x=alt.X("alert_before_end_eod:Q", title="Schemes caught before they end", axis=alt.Axis(format="%"),
                    scale=alt.Scale(domain=[0, 1])),
            tooltip=["pattern:N", alt.Tooltip("alert_before_end_eod:Q", format=".1%"),
                     alt.Tooltip("median_duration_h:Q", title="Median length (h)", format=".1f")])
        st.altair_chart((bars + bars.mark_text(align="left", dx=4, color=INK).encode(
            text=alt.Text("alert_before_end_eod:Q", format=".0%"))).properties(height=280), width="stretch")
        st.info("The detected-vs-finished timeline needs per-scheme timings: rerun local/reports/final_report.py "
                "after the full HI-Large run.")

st.divider()

# ------------------------------------------------------------------ 3. which patterns need people
st.subheader("3 · Which patterns can the model handle, and which need analysts?")
st.caption("Each pattern is rated on the three things above: how much of it is caught (recall), how many of its "
           "alerts are real (precision), and how often it is caught before the scheme ends (speed). "
           "A pattern's colour is its worst rating.")

with st.expander("Rating thresholds (red below the first value, green from the second)"):
    t1, t2, t3 = st.columns(3)
    th = {"recall": t1.slider("Laundering caught", 0.0, 1.0, (0.70, 0.85), 0.01),
          "precision": t2.slider("Alerts that are real", 0.0, 1.0, (0.70, 0.90), 0.01),
          "speed": t3.slider("Caught before it ends", 0.0, 1.0, (0.45, 0.60), 0.01)}

pats = pd.DataFrame(d["patterns"])
pats = pats[pats["level"] == "account_day"].copy()
prec = {r["predicted_pattern"]: r["truly_laundering"] for r in d.get("alerts_by_predicted_pattern", [])}
spd = pd.DataFrame(d["schemes_by_pattern"]).set_index("pattern_type")
s2 = pd.DataFrame(d["stage2"])
s2 = s2[s2["mode"] == "caught"].set_index("pattern") if not s2.empty else pd.DataFrame()
t2i = top2.set_index("true_pattern") if not top2.empty else pd.DataFrame()
alert_med = sc[sc["caught"].astype(bool)].groupby("pattern_type")["hours_to_alert"].median() if not sc.empty else pd.Series(dtype=float)

rows = []
for _, r in pats.iterrows():
    p, c2 = r["pattern_label"], stage2_class(r["pattern_label"])
    rows.append({
        "Pattern": nice(p),
        "recall": r.get("recall_used", r["recall"]) if pd.notna(r.get("recall_used")) else r["recall"],
        "precision": prec.get(c2),
        "speed": spd["alert_before_end_eod"].get(p),
        "Median time to alert": hours(alert_med.get(p)),
        "Median scheme length": hours(spd["median_duration_h"].get(p)),
        "Named right (1 guess)": s2["recall"].get(c2) if not s2.empty else None,
        "Named right (2 guesses)": t2i["top2"].get(c2) if not t2i.empty else None,
    })
tbl = pd.DataFrame(rows)


def rate(v, lo_hi):
    if v is None or pd.isna(v):
        return None
    return "Green" if v >= lo_hi[1] else "Yellow" if v >= lo_hi[0] else "Red"


order = {"Red": 0, "Yellow": 1, "Green": 2}
names = {"recall": "few caught", "precision": "many false alerts", "speed": "caught late"}
status, why = [], []
for _, r in tbl.iterrows():
    rs = {k: rate(r[k], th[k]) for k in th}
    rs = {k: v for k, v in rs.items() if v}
    worst = min(rs.values(), key=order.get)
    status.append(worst)
    why.append(", ".join(names[k] for k, v in rs.items() if v == worst) if worst != "Green" else "strong on all three")
tbl.insert(1, "Status", status)
tbl.insert(2, "Why", why)
tbl = tbl.sort_values("Status", key=lambda s: s.map(order), kind="stable").rename(
    columns={"recall": "Laundering caught", "precision": "Alerts that are real", "speed": "Caught before it ends"})
if t2i.empty:
    tbl = tbl.drop(columns="Named right (2 guesses)")
if sc.empty:
    tbl = tbl.drop(columns="Median time to alert")

st.markdown(" &nbsp; ".join(f"<span style='background:{RAG[k]};padding:2px 8px;border-radius:4px'>"
                            f"<b>{k}</b>: {v}</span>" for k, v in ACTION.items()), unsafe_allow_html=True)

cell_key = {"Laundering caught": "recall", "Alerts that are real": "precision", "Caught before it ends": "speed"}
css = pd.DataFrame("", index=tbl.index, columns=tbl.columns)       # colours from the numbers...
css["Status"] = [f"background-color: {RAG[v]}; font-weight: 600" for v in tbl["Status"]]
for c, k in cell_key.items():
    css[c] = [f"background-color: {RAG[rate(v, th[k])]}" if rate(v, th[k]) else "" for v in tbl[c]]
show = tbl.copy()                                                   # ...shown as text (Streamlit prints blanks as "None")
for c in [c for c in tbl.columns if c in cell_key or c.startswith("Named right")]:
    show[c] = tbl[c].map(pct)
st.dataframe(show.style.apply(lambda _: css, axis=None), hide_index=True, width="stretch")

n_red = (tbl["Status"] == "Red").sum()
st.caption(f"{n_red} of {len(tbl)} patterns are red. Fast patterns (fan in, fan out, random) are mostly over within "
           "hours, so a daily alert arrives after they end: those need live scoring or analysts watching the accounts. "
           "Unclassified laundering follows no known shape and is almost never caught.")

st.divider()

# ------------------------------------------------------------------ 4. what the analyst sees
st.subheader("4 · What the analyst sees: the model's two best guesses at the pattern")
st.caption("Each alert comes with the pattern the model thinks it belongs to. One guess is often wrong, so the "
           "analyst sees the two most likely patterns and checks those first instead of all eight.")

one = top2.set_index("true_pattern")["top1"].get("ALL") if not top2.empty else \
    (s2["recall"].get("MACRO (7 patterns)") if not s2.empty else None)
two = top2.set_index("true_pattern")["top2"].get("ALL") if not top2.empty else None
g1, g2, g3 = st.columns(3)
g1.metric("Right with one guess", pct(one), help="Of truly laundering alerts with a known pattern, the share whose "
                                                 "pattern is the model's first guess.")
g2.metric("Right within two guesses" + ("" if two is not None else " (validation)"),
          pct(two) if two is not None else f"~{VALIDATION_TOP2:.0%}",
          help="The true pattern is the first or second guess." +
               ("" if two is not None else " Validation result (HI-Small + HI-Medium); HI-Large comes with the full run."))
g3.metric("Patterns to check", "2 of 8")
st.caption("The value: a second guess roughly doubles how often the analyst starts from the right pattern, "
           "at the cost of reading one more name. A wrong name never hides an alert: whether an account is alerted "
           "comes from the risk score alone.")

ex = pd.DataFrame(d.get("alert_examples") or [])
if not ex.empty:
    show = pd.DataFrame({
        "Day": ex["date"].str[:10], "Account": ex["acct"], "Risk score": ex["score"].round(3),
        "First guess": [f"{nice(p)} ({c:.0%})" for p, c in zip(ex["predicted_pattern"], ex["pattern_confidence"])],
        "Second guess": [f"{nice(p)} ({c:.0%})" for p, c in zip(ex["second_pattern"], ex["second_confidence"])],
        "Linked accounts": ex["g_accounts"], "Actual (answer key)": ex["true_pattern"].map(nice)})
    st.dataframe(show, hide_index=True, width="stretch")
else:
    eg = pd.DataFrame(d.get("example_groups") or []).head(8)
    if not eg.empty:
        st.dataframe(pd.DataFrame({
            "Day": eg["date"].str[:10], "Group": eg["group_id"],
            "First guess": [f"{nice(p)} ({c:.0%})" for p, c in zip(eg["predicted_pattern"], eg["pattern_confidence"])],
            "Linked accounts": eg["g_accounts"], "Payments": eg["g_payments"],
            "Actual (answer key)": eg["true_pattern"].map(nice)}), hide_index=True, width="stretch")
        st.caption("Second guesses appear here after the full HI-Large run (final_group_namer.py now saves them).")

with st.expander("How to read this, and its limits"):
    st.markdown(
        "- An **alert** is one account on one day. **Precision** = share of alerts that are real laundering; "
        "**recall** = share of laundering that gets an alert.\n"
        "- The threshold was fixed on HI-Small and HI-Medium validation days and never tuned on HI-Large.\n"
        "- Schemes and patterns come from the dataset's answer key (`Patterns.txt`), used only to mark results, "
        "never as model input.\n"
        "- Speed assumes alerts are ready at midnight after the day closes. \"First high-risk payment\" shows how "
        "early a live system could react, but each score uses that whole day's payments, so it is an upper bound.\n"
        "- Stack and bipartite are named together as layering, so they share a precision and naming score.\n"
        f"- Tested on {days} days of IBM's synthetic AML data; real banks' volumes and patterns will differ."
    )
