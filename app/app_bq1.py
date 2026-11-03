"""
BQ1 page for the Transaction Monitoring Tuning Team.
    streamlit run app_bq1.py        (dashboard_data.json in the same folder; written by final_report.py)

Order: the decision the team owns (daily alert volume) first, then what to tune next,
then how early alerts arrive; experimental pattern naming and limits are collapsed.
"""
import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

BLUE, ORANGE, GREY, RED, INK = "#2F7FAD", "#E07B39", "#B9B2A8", "#C0504D", "#262626"

st.set_page_config(page_title="BQ1 · Early detection", layout="wide")


@st.cache_data
def load():
    d = json.loads((Path(__file__).parent / "data" / "dashboard_data.json").read_text())
    return d, pd.DataFrame(d["stage1"])


def pct(x):
    """Percent without overstating: 0.9996 shows as >99.9%, small shares keep a decimal."""
    if 0.999 < x < 1:
        return ">99.9%"
    return f"{x:.1%}" if x < 0.1 or x > 0.9 else f"{x:.0%}"


def per_day(x):
    return f"{x:,.0f}" if x >= 10 else f"{x:.1f}"


d, s1 = load()
ad = s1[s1["level"] == "account_day"]
frozen = ad[ad["cutoff"] == "frozen_threshold"].iloc[0]
tot = d["schemes_total"]
thr_k = round(frozen["alerts_per_day"])

# ------------------------------------------------------------------ header
st.title("Setting alert volumes: what each choice catches, and what it costs analysts")
st.markdown("**For the Transaction Monitoring Tuning Team**, who decide how many alerts go to analysts each day.")
st.markdown("**BQ1:** How can we automatically detect laundering patterns early, while keeping false alerts "
            "low enough for analysts to manage?")
st.caption(f"Tested once on {int(frozen['days'])} unseen days of HI-Large "
           f"({frozen['date_from'][:10]} to {frozen['date_to'][:10]}), {frozen['payments'] / 1e6:.1f}M payments · "
           "trained on HI-Small and HI-Medium only")

# ------------------------------------------------------------------ 1. capacity: the decision
st.subheader("1 · How many alerts should go to analysts each day?")
st.caption("Each day, accounts are ranked by risk score and the top ones become alerts. "
           "Pick a daily volume to see what it buys.")

opts = ad[ad["cutoff"].str.startswith("top") | (ad["cutoff"] == "frozen_threshold")].copy()
opts["k"] = opts["alerts_per_day"].round().astype(int)
opts = opts.sort_values("k")
pick = st.select_slider("Alerts reviewed per day", options=opts["k"].tolist(), value=2000,
                        format_func=lambda v: f"{v:,} (model's threshold)" if v == thr_k else f"{v:,}")
row = opts[opts["k"] == pick].iloc[0]
false_day = pick * (1 - row["precision"])

st.markdown(f"#### At {pick:,} alerts a day, **{pct(row['precision'])}** are real laundering and "
            f"**{pct(row['recall'])}** of all laundering is caught.")

left, right = st.columns([1, 2], gap="large")
with left:
    st.metric("Alerts that are real laundering", pct(row["precision"]),
              help="Precision: of the accounts flagged each day, the share that really were laundering.")
    st.metric("Laundering caught", pct(row["recall"]),
              help="Recall: share of laundering account-days that got an alert, including laundering with no known pattern.")
    st.metric("False alerts per day", per_day(false_day),
              help="Alerts analysts would clear that turn out not to be laundering.")
with right:
    long = opts.melt(id_vars="k", value_vars=["precision", "recall"], var_name="measure", value_name="value")
    long["measure"] = long["measure"].map({"precision": "Alerts that are real", "recall": "Laundering caught"})
    x = alt.X("k:O", title="Alerts reviewed per day", sort=opts["k"].tolist(),
              axis=alt.Axis(labelAngle=0, format=","))
    base = alt.Chart(long).encode(
        x=x, y=alt.Y("value:Q", title=None, axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
        color=alt.Color("measure:N", scale=alt.Scale(domain=["Alerts that are real", "Laundering caught"],
                                                      range=[BLUE, ORANGE]),
                        legend=alt.Legend(title=None, orient="top")))
    lines = base.mark_line(strokeWidth=2.5) + base.mark_point(filled=True, size=60).encode(
        tooltip=[alt.Tooltip("k:O", title="Alerts/day", format=","), "measure:N",
                 alt.Tooltip("value:Q", format=".1%")])
    rule = alt.Chart(pd.DataFrame({"k": [pick]})).mark_rule(color=INK, strokeDash=[4, 4]).encode(x=x)
    st.altair_chart((lines + rule).properties(height=300), width="stretch")

st.caption(f"Past about 2,000 a day most extra alerts are false: at 5,000 a day only "
           f"{pct(opts.loc[opts['k'] == 5000, 'precision'].iloc[0])} are real. "
           f"The model's own threshold sends about {thr_k:,} a day; sections 2 and 3 use that setting.")

st.divider()

# ------------------------------------------------------------------ 2. what to tune next
p = pd.DataFrame(d["patterns"])
p = p[p["level"] == "account_day"].copy()
p["pattern"] = p["pattern_label"].str.replace("_", " ").str.capitalize()
p["weak"] = p["pattern_label"] == "unclassified"
known = p.loc[~p["weak"], "recall"]
unc = p[p["weak"]].iloc[0]
unc_share = unc["positives"] / p["positives"].sum()

st.subheader("2 · What to tune next: laundering with no known pattern")
c1, c2 = st.columns([1, 2], gap="large")
with c1:
    st.metric("Known patterns caught", f"{known.min():.0%}–{known.max():.0%}",
              help="Recall for each of the 8 named laundering patterns, at the model's threshold.")
    st.metric("Laundering with no pattern caught", pct(unc["recall"]),
              help=f"{unc['caught']:,} of {unc['positives']:,} account-days.")
    st.caption(f"{unc_share:.0%} of laundering account-days ({unc['positives']:,} of {p['positives'].sum():,}) "
               "follow no known pattern. They barely overlap the scheme accounts and need new signals.")
with c2:
    pb = alt.Chart(p).mark_bar(cornerRadiusEnd=3, size=20).encode(
        y=alt.Y("pattern:N", sort="-x", title=None),
        x=alt.X("recall:Q", title="Laundering account-days caught", axis=alt.Axis(format="%"),
                scale=alt.Scale(domain=[0, 1])),
        color=alt.condition("datum.weak", alt.value(RED), alt.value(BLUE)),
        tooltip=["pattern:N", alt.Tooltip("caught:Q", format=","), alt.Tooltip("positives:Q", format=",", title="total"),
                 alt.Tooltip("recall:Q", format=".1%")])
    pt = pb.mark_text(align="left", dx=4, color=INK).encode(text=alt.Text("recall:Q", format=".0%"))
    st.altair_chart((pb + pt).properties(height=300), width="stretch")

st.divider()

# ------------------------------------------------------------------ 3. how early
caught_n = round(tot["schemes"] * tot["caught"])
running_n = round(tot["schemes"] * tot["alert_before_end_eod"])
st.subheader("3 · How early the alert arrives")
st.caption("The model reads each account's whole day, so alerts are ready when the day closes, not as payments arrive.")
c3, c4 = st.columns([1, 2], gap="large")
with c3:
    st.metric("Schemes that get an alert", f"{tot['caught']:.0%}",
              help=f"{caught_n:,} of {tot['schemes']:,} schemes that started in the test window: at least one "
                   "account in the scheme was flagged.")
    st.metric("Schemes still running at the alert", f"{tot['alert_before_end_eod']:.0%}",
              help=f"{running_n:,} of {tot['schemes']:,} schemes got their end-of-day alert before their last payment.")
    st.metric("Median scheme length", f"{tot['median_duration_h']:.0f} h",
              help="From a scheme's first payment to its last.")
    st.caption("Schemes that finish before the day closes are over by the time the alert arrives. Catching them "
               "needs a model that scores only the payments seen so far.")
with c4:
    sp = pd.DataFrame(d["schemes_by_pattern"])
    sp["pattern"] = sp["pattern_type"].str.replace("_", " ").str.capitalize()
    bars = alt.Chart(sp).mark_bar(cornerRadiusEnd=3, size=20, color=BLUE).encode(
        y=alt.Y("pattern:N", sort="-x", title=None),
        x=alt.X("alert_before_end_eod:Q", title="Alerted schemes still running at the end-of-day alert",
                axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
        tooltip=["pattern:N", alt.Tooltip("alert_before_end_eod:Q", title="Still running", format=".1%"),
                 alt.Tooltip("median_duration_h:Q", title="Median length (h)", format=".1f")])
    labels = bars.mark_text(align="left", dx=4, color=INK).encode(text=alt.Text("alert_before_end_eod:Q", format=".0%"))
    st.altair_chart((bars + labels).properties(height=280), width="stretch")

st.divider()

# ------------------------------------------------------------------ 4. details, collapsed
e2e = pd.DataFrame(d["stage2"])
e2e = e2e[e2e["mode"] == "end_to_end"]
macro = e2e[e2e["pattern"].str.startswith("MACRO")].iloc[0]
with st.expander(f"Experimental: naming the pattern behind an alert (macro F1 {macro['f1']:.2f})"):
    named = e2e[~e2e["pattern"].str.startswith("MACRO") & (e2e["pattern"] != "unclassified")].copy()
    named["name"] = named["pattern"].str.replace("_", " ").str.capitalize()
    c5, c6 = st.columns([1, 2], gap="large")
    with c5:
        st.metric("Pattern naming macro F1", f"{macro['f1']:.3f}",
                  help="Average F1 across the 7 named patterns, measured on stage 1's alerts.")
        st.caption("Not reliable enough to act on. Treat a suggested pattern as a hint for the analyst, "
                   "not a finding: the alert decision comes from stage 1 alone.")
    with c6:
        nb = alt.Chart(named).mark_bar(cornerRadiusEnd=3, size=20, color=GREY).encode(
            y=alt.Y("name:N", sort="-x", title=None),
            x=alt.X("f1:Q", title="F1 for naming this pattern", scale=alt.Scale(domain=[0, 1])),
            tooltip=["name:N", alt.Tooltip("precision:Q", format=".1%"), alt.Tooltip("recall:Q", format=".1%"),
                     alt.Tooltip("f1:Q", format=".3f")])
        nt = nb.mark_text(align="left", dx=4, color=INK).encode(text=alt.Text("f1:Q", format=".2f"))
        st.altair_chart((nb + nt).properties(height=240), width="stretch")

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
