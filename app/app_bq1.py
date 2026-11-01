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
hist = pd.DataFrame(d["scheme_hours_hist"])
caught_n = round(tot["schemes"] * tot["caught"])
within_1h = int(hist["schemes"].iloc[0])

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

k1, k2, k3, k4 = st.columns(4)
k1.metric("Alerts that are real laundering", f"{frozen['precision']:.0%}",
          help="Precision: of the accounts flagged each day, how many were really laundering.")
k2.metric("Laundering schemes flagged", f"{tot['caught']:.0%}",
          help=f"{caught_n:,} of {tot['schemes']:,} schemes that started in the test window.")
k3.metric("Median time to first flag", f"{tot['median_hours_to_first_flag']:.0f} h",
          help="From a scheme's first payment to the first payment the model flags.")
k4.metric("Alerts per day", f"{frozen['alerts_per_day']:,.0f}",
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
st.subheader("2 · Most schemes are flagged within an hour of starting")
c1, c2 = st.columns([1, 2], gap="large")
with c1:
    st.metric("Flagged within 1 hour", f"{within_1h / caught_n:.0%}", help="Of flagged schemes.")
    st.metric("Scheme payments still to come at first flag", f"{tot['share_from_first_flag']:.0%}",
              help="On average, the share of a scheme's payments made at or after its first flag: "
                   "what acting on the alert could still stop.")
    st.info(f"Only {tot['alert_before_end_eod']:.0%} of schemes are still running when a once-a-day batch "
            "alert arrives. Score payments as they arrive to keep the early lead.")
with c2:
    names = {"(-0.01, 1.0]": "≤1 h", "(1.0, 3.0]": "1–3 h", "(3.0, 6.0]": "3–6 h", "(6.0, 12.0]": "6–12 h",
             "(12.0, 24.0]": "12–24 h", "(24.0, 48.0]": "1–2 days", "(48.0, 96.0]": "2–4 days"}
    h = hist.assign(when=hist["bucket"].map(names), first=lambda t: t.index == 0)
    bars = alt.Chart(h).mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3, size=38).encode(
        x=alt.X("when:N", sort=list(names.values()), title="Time from scheme start to first flag",
                axis=alt.Axis(labelAngle=0)),
        y=alt.Y("schemes:Q", title="Schemes"),
        color=alt.condition("datum.first", alt.value(BLUE), alt.value(GREY)),
        tooltip=[alt.Tooltip("when:N", title="First flag"), alt.Tooltip("schemes:Q", format=",")])
    labels = bars.mark_text(dy=-8, color=INK).encode(text=alt.Text("schemes:Q", format=","))
    st.altair_chart((bars + labels).properties(height=300), width="stretch")

st.divider()

# ------------------------------------------------------------------ 3. where it misses
st.subheader("3 · Every known pattern is caught; laundering with no pattern is not")
p = pd.DataFrame(d["patterns"])
p = p[p["level"] == "account_day"].copy()
p["pattern"] = p["pattern_label"].str.replace("_", " ").str.capitalize()
p["weak"] = p["pattern_label"] == "unclassified"
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

with st.expander("How to read this, and its limits"):
    st.markdown(
        "- An **alert** is one account on one day. **Precision** = share of alerts that are real laundering; "
        "**recall** = share of laundering that gets an alert.\n"
        "- The threshold was fixed on HI-Small and HI-Medium validation days and never tuned on HI-Large.\n"
        "- Schemes come from the dataset's answer key (`Patterns.txt`), used only to mark results, never as model input.\n"
        "- Tested on a two-week window of IBM's synthetic AML data; real banks' volumes and patterns will differ."
    )
