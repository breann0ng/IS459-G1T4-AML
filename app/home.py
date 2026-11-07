"""
Overview (landing page) of the BQ1 site: the two business questions with their answers, the pipeline, and the team.
BQ1 numbers come from the same dashboard JSON as app_bq1.py; BQ2 numbers are from docs/bq1.md (BQ2 section).
"""
import json
import os
from pathlib import Path

import streamlit as st

DATA_FILE = os.environ.get("BQ1_DATA", "dashboard_data_enrui_breann_mld20.json")
NAMES = ["Breann", "Lesley", "Joey", "Tasha", "En Rui", "Wei Ting"]

d = json.loads((Path(__file__).parent / "data" / DATA_FILE).read_text())
f = next(r for r in d["stage1"] if r["level"] == "account_day" and r["cutoff"] == "frozen_threshold")
tot = d["schemes_total"]

def question(text):
    """The business question, large: it is what the page answers."""
    st.markdown(f"<div style='font-size:1.45rem;line-height:1.35;font-weight:600;margin:.1rem 0 .7rem'>{text}</div>",
                unsafe_allow_html=True)


# ------------------------------------------------------------------ hero
st.title("Catching money laundering early")
st.iframe("""
<div id="t">This project is done by <span id="n"></span><span id="c">|</span></div>
<style>
  body { margin: 0; background: transparent; }
  #t { font-family: "Source Sans Pro", "Source Sans 3", system-ui, sans-serif; font-size: 22px; color: #c9c2dc; }
  #n { color: #b3a9f2; font-weight: 600; }
  #c { color: #b3a9f2; animation: blink 1s steps(1) infinite; margin-left: 1px; }
  @keyframes blink { 50% { opacity: 0; } }
</style>
<script>
  const names = __NAMES__; let i = 0, j = 0, del = false; const el = document.getElementById("n");
  function tick() {
    const w = names[i];
    el.textContent = w.slice(0, j);
    if (!del && j < w.length) { j++; setTimeout(tick, 110); }
    else if (!del) { del = true; setTimeout(tick, 1400); }
    else if (j > 0) { j--; setTimeout(tick, 60); }
    else { del = false; i = (i + 1) % names.length; setTimeout(tick, 350); }
  }
  tick();
</script>
""".replace("__NAMES__", json.dumps(NAMES)), height=44)
st.caption("IS459 Big Data Architecture · Group G1T4 · built for the Transaction Monitoring Tuning Team, who decide "
           "how many alerts analysts get each day")

# ------------------------------------------------------------------ the two questions
c1, c2 = st.columns(2, gap="large")
with c1, st.container(border=True):
    st.markdown("#### :material/notifications_active: BQ1 · Early detection")
    question("How can we automatically detect laundering patterns early, while keeping false alerts low enough "
             "for analysts to manage?")
    a, b, c = st.columns(3)
    a.metric("Alerts a day", f"{f['alerts_per_day']:,.0f}")
    b.metric("Alerts that are real", f"{f['precision']:.0%}")
    c.metric("Schemes caught", f"{tot['caught']:.0%}")
    st.markdown(f"9 in 10 alerts are real, but only **{tot['alert_before_end_eod']:.0%}** of schemes are alerted "
                "while still running. Fast patterns still need people.")
    st.page_link("app_bq1.py", label="See the BQ1 results", icon=":material/arrow_forward:")

with c2, st.container(border=True):
    st.markdown("#### :material/trending_up: BQ2 · Tactic change")
    question("Are criminals changing their laundering tactics over time, and when they do, does our detection "
             "rate fall, and can retraining recover it?")
    a, b, c = st.columns(3)
    a.metric("Change confirmed", "2 weeks", help="SAML-D: confirmed in Week 24, 2 weeks after the new tactics began.")
    b.metric("New tactics caught", "40% → 70%", help="SAML-D: the old model caught 40% of the new tactics; "
                                                     "the retrained model caught 70%.")
    c.metric("HI-Large", "Stable", help="No tactic change on HI-Large, so no retraining was needed.")
    st.markdown("We spot a tactic shift within 2 weeks, and retraining wins back most of the lost detection.")
    st.caption(":material/info: The BQ2 dashboard runs as its own app for now.")

# ------------------------------------------------------------------ pipeline
st.subheader(":material/account_tree: How the pipeline works")
st.graphviz_chart("""
digraph G {
  rankdir=TB; bgcolor="transparent"; nodesep=0.35; ranksep=0.32; newrank=true; compound=true;
  node [shape=box, style="rounded,filled", fillcolor="#221b33", color="#4a3f66", fontcolor="#ece8f4",
        fontname="Helvetica", fontsize=11, margin="0.18,0.07", width=2.1];
  edge [color="#8a80a8", arrowsize=0.6];
  graph [fontname="Helvetica", fontcolor="#c9c2dc", fontsize=11, color="#4a3f66", style="rounded"];

  subgraph cluster_in { label="1 · Data in";
    kaggle [label="Kaggle\nIBM AML · SAML-D"]; raw [label="Amazon S3 · raw/\noriginals, never edited"];
    kaggle -> raw; }

  subgraph cluster_bq1 { label="2a · BQ1 · every day"; color="#3987e5";
    t1 [label="New day lands\n(EventBridge)", style="rounded,dashed", fillcolor="#15111f"];
    clean [label="Clean"]; feat [label="Features\n21-day look-back"];
    score [label="Score: stage 1\nLightGBM"]; name [label="Name the pattern\nstage 2"];
    t1 -> clean -> feat -> score -> name; }

  subgraph cluster_bq2 { label="2b · BQ2 · every Monday"; color="#d95926";
    t2 [label="Weekly schedule\n(EventBridge)", style="rounded,dashed", fillcolor="#15111f"];
    p1 [label="Tactics changed?"]; p2 [label="Detection falling?"];
    alert [label="Alert?", shape=diamond, style="filled", width=1.2];
    retrain [label="Retrain a\nchallenger model"];
    keep [label="No: keep watching", fontsize=10, width=1.6];
    t2 -> p1 -> p2 -> alert;
    alert -> retrain [label=" yes", fontcolor="#c9c2dc", fontsize=10];
    alert -> keep [label=" no", fontcolor="#c9c2dc", fontsize=10]; }

  subgraph cluster_out { label="3 · Results in S3";
    gold [label="analytics/\nalerts, metrics"]; models [label="models/\ntrained models"]; }

  subgraph cluster_serve { label="4 · Serve";
    report [label="Report\nwrites a small JSON"]; app [label="This dashboard\n(Streamlit)"];
    athena [label="Glue + Athena\nSQL on the lake"]; report -> app; }

  raw -> t1; raw -> t2; name -> gold; retrain -> models; gold -> report; gold -> athena;
  { rank=same; t1; t2; } { rank=same; gold; models; }
}
""", width="content")
st.caption("Dashed boxes are planned, not live yet. AWS resources are managed as code with Terraform.")
