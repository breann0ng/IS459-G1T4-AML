"""
BQ1 page for the Transaction Monitoring Tuning Team.
    streamlit run app_bq1.py        (dashboard_data.json in the same folder; written by final_report.py)
"""
import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

BLUE, GREY, RED, INK = "#2F7FAD", "#B9B2A8", "#C0504D", "#262626"

st.set_page_config(page_title="BQ1 · Early detection", layout="wide")


@st.cache_data
def load():
    d = json.loads((Path(__file__).parent / "data" / "dashboard_data.json").read_text())
    s1 = pd.DataFrame(d["stage1"])
    return d, s1


d, s1 = load()
ad = s1[s1["level"] == "account_day"]
frozen = ad[ad["cutoff"] == "frozen_threshold"].iloc[0]
tot = d["schemes_total"]
caught_n = round(tot["schemes"] * tot["caught"])
running_n = round(tot["schemes"] * tot["alert_before_end_eod"])

# ------------------------------------------------------------------ header
st.title("Catching laundering early, with alerts analysts can handle")
st.markdown(
    "**BQ1:** How can we automatically detect laundering patterns early, while keeping false alerts "
    "low enough for analysts to manage?"
)
st.caption(
    f"For the Transaction Monitoring Tuning Team · Tested on {int(frozen['days'])} unseen days of HI-Large "
    f"({frozen['date_from'][:10]} to {frozen['date_to'][:10]}), "
    f"{frozen['payments'] / 1e6:.1f}M payments · trained on HI-Small and HI-Medium only"
)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Alerts that are real laundering", f"{frozen['precision']:.0%}",
          help="Precision: of the accounts flagged each day, how many were really laundering.")
k2.metric("Laundering caught", f"{frozen['recall']:.0%}",
          help="Recall: share of laundering account-days that get an alert, including laundering with no known pattern.")
k3.metric("Laundering schemes flagged", f"{tot['caught']:.0%}",
          help=f"{caught_n:,} of {tot['schemes']:,} schemes that started in the test window. A scheme counts once "
               "any one of its payments is flagged, so this is higher than laundering caught.")
k4.metric("Schemes still running at alert", f"{tot['alert_before_end_eod']:.0%}",
          help=f"{running_n:,} of {tot['schemes']:,} schemes got their end-of-day alert before their last payment.")
k5.metric("Alerts per day", f"{frozen['alerts_per_day']:,.0f}",
          help="Accounts flagged per day at the model's threshold; steady day to day.")

st.divider()

# ------------------------------------------------------------------ 1. capacity trade-off
st.subheader("1 · Pick how many alerts your team can review a day")
st.caption("Alerts are ranked by score. Reviewing more catches more laundering, but past a point most extra alerts are false.")

topk = ad[ad["cutoff"].str.startswith("top")].copy()
topk["k"] = topk["alerts_per_day"].astype(int)
options = sorted(topk["k"].tolist())
pick = st.select_slider("Alerts reviewed per day", options=options, value=2000,
                        format_func=lambda v: f"{v:,}")
row = topk[topk["k"] == pick].iloc[0]

left, right = st.columns([1, 2], gap="large")
with left:
    st.metric("Real laundering among them", f"{row['precision']:.1%}")
    st.metric("Laundering caught", f"{row['recall']:.1%}")
    st.metric("False alerts per day", f"{pick * (1 - row['precision']):,.0f}")
    st.caption(f"The model's own threshold sends about {frozen['alerts_per_day']:,.0f} a day "
               f"at {frozen['precision']:.0%} precision.")
with right:
    long = topk.melt(id_vars="k", value_vars=["precision", "recall"], var_name="measure", value_name="value")
    long["measure"] = long["measure"].str.capitalize()
    x = alt.X("k:O", title="Alerts reviewed per day", axis=alt.Axis(labelAngle=0, format=","))
    color = alt.Color("measure:N", scale=alt.Scale(domain=["Precision", "Recall"], range=[BLUE, "#E07B39"]),
                      legend=alt.Legend(title=None, orient="top"))
    base = alt.Chart(long).encode(x=x, y=alt.Y("value:Q", title=None, axis=alt.Axis(format="%"),
                                                scale=alt.Scale(domain=[0, 1])), color=color)
    lines = base.mark_line(strokeWidth=2.5) + base.mark_point(filled=True, size=60).encode(
        tooltip=[alt.Tooltip("k:O", title="Alerts/day", format=","), "measure:N",
                 alt.Tooltip("value:Q", format=".1%")])
    rule = alt.Chart(pd.DataFrame({"k": [pick]})).mark_rule(color=INK, strokeDash=[4, 4]).encode(x=x)
    st.altair_chart((lines + rule).properties(height=300), width="stretch")

st.divider()

# ------------------------------------------------------------------ 2. early
st.subheader("2 · About half of schemes are still running when the alert arrives")
st.caption("The model reads each account's whole day, so alerts are ready when the day closes, not as payments arrive.")
c1, c2 = st.columns([1, 2], gap="large")
with c1:
    st.metric("Schemes still running at alert", f"{tot['alert_before_end_eod']:.0%}",
              help=f"{running_n:,} of {tot['schemes']:,} schemes. The rest finished first or were never flagged.")
    st.metric("Median scheme length", f"{tot['median_duration_h']:.0f} h",
              help="From a scheme's first payment to its last.")
    st.info("Schemes that finish before the day closes are over by the time the alert arrives. "
            "Catching them needs a model that scores only the payments seen so far.")
with c2:
    sp = pd.DataFrame(d["schemes_by_pattern"])
    sp["pattern"] = sp["pattern_type"].str.replace("_", " ").str.capitalize()
    bars = alt.Chart(sp).mark_bar(cornerRadiusEnd=3, size=22, color=BLUE).encode(
        y=alt.Y("pattern:N", sort="-x", title=None),
        x=alt.X("alert_before_end_eod:Q", title="Alerted schemes still running at the end-of-day alert",
                axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
        tooltip=["pattern:N", alt.Tooltip("alert_before_end_eod:Q", title="Still running", format=".1%"),
                 alt.Tooltip("median_duration_h:Q", title="Median length (h)", format=".1f")])
    labels = bars.mark_text(align="left", dx=4, color=INK).encode(text=alt.Text("alert_before_end_eod:Q", format=".0%"))
    st.altair_chart((bars + labels).properties(height=300), width="stretch")

st.divider()

# ------------------------------------------------------------------ 3. where it misses
p = pd.DataFrame(d["patterns"])
p = p[p["level"] == "account_day"].copy()
p["pattern"] = p["pattern_label"].str.replace("_", " ").str.capitalize()
p["weak"] = p["pattern_label"] == "unclassified"
known = p.loc[~p["weak"], "recall"]
st.subheader(f"3 · Known patterns are caught {known.min():.0%}–{known.max():.0%} of the time; "
             "laundering with no pattern is mostly missed")
pb = alt.Chart(p).mark_bar(cornerRadiusEnd=3, size=22).encode(
    y=alt.Y("pattern:N", sort="-x", title=None),
    x=alt.X("recall:Q", title="Laundering account-days caught", axis=alt.Axis(format="%"),
            scale=alt.Scale(domain=[0, 1])),
    color=alt.condition("datum.weak", alt.value(RED), alt.value(BLUE)),
    tooltip=["pattern:N", alt.Tooltip("caught:Q", format=","), alt.Tooltip("positives:Q", format=",", title="total"),
             alt.Tooltip("recall:Q", format=".1%")])
pt = pb.mark_text(align="left", dx=4, color=INK).encode(text=alt.Text("recall:Q", format=".0%"))
c3, c4 = st.columns([2, 1], gap="large")
with c3:
    st.altair_chart((pb + pt).properties(height=320), width="stretch")
with c4:
    unc = p[p["weak"]].iloc[0]
    st.metric("Unclassified laundering caught", f"{unc['recall']:.1%}")
    st.caption(f"{unc['positives']:,} of {p['positives'].sum():,} laundering account-days follow no known pattern. "
               "They barely overlap the scheme accounts and need new signals, which is the next thing to tune.")

st.divider()

# ------------------------------------------------------------------ 4. pattern naming (experimental)
st.subheader("4 · Experimental: naming the pattern behind an alert")
e2e = pd.DataFrame(d["stage2"])
e2e = e2e[e2e["mode"] == "end_to_end"]
macro = e2e[e2e["pattern"].str.startswith("MACRO")].iloc[0]
named = e2e[~e2e["pattern"].str.startswith("MACRO") & (e2e["pattern"] != "unclassified")].copy()
named["name"] = named["pattern"].str.replace("_", " ").str.capitalize()
c5, c6 = st.columns([1, 2], gap="large")
with c5:
    st.metric("Pattern naming macro F1", f"{macro['f1']:.3f}",
              help="Average F1 across the 7 named patterns, measured on stage 1's alerts.")
    st.caption("Not reliable enough to act on. Treat a suggested pattern as a hint for the analyst, "
               "not a finding: the alert decision comes from stage 1 alone.")
with c6:
    nb = alt.Chart(named).mark_bar(cornerRadiusEnd=3, size=22, color=GREY).encode(
        y=alt.Y("name:N", sort="-x", title=None),
        x=alt.X("f1:Q", title="F1 for naming this pattern", scale=alt.Scale(domain=[0, 1])),
        tooltip=["name:N", alt.Tooltip("precision:Q", format=".1%"), alt.Tooltip("recall:Q", format=".1%"),
                 alt.Tooltip("f1:Q", format=".3f")])
    nt = nb.mark_text(align="left", dx=4, color=INK).encode(text=alt.Text("f1:Q", format=".2f"))
    st.altair_chart((nb + nt).properties(height=260), width="stretch")

with st.expander("How to read this, and its limits"):
    st.markdown(
        "- An **alert** is one account on one day. **Precision** = share of alerts that are real laundering; "
        "**recall** = share of laundering that gets an alert.\n"
        "- The threshold was fixed on HI-Small and HI-Medium validation days and never tuned on HI-Large.\n"
        "- Schemes come from the dataset's answer key (`Patterns.txt`), used only to mark results, never as model input.\n"
        "- Timing assumes alerts are ready at midnight after the day closes. Time-to-flag within a day is not "
        "reported, because each score uses that day's later payments.\n"
        "- Results are not broken down by payment channel.\n"
        "- Tested on a two-week window of IBM's synthetic AML data; real banks' volumes and patterns will differ."
    )
