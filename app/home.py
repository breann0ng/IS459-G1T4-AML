"""
Overview (landing page) of the BQ1 site: the two business questions with their answers, the pipeline, and the team.
BQ1 numbers come from the same dashboard JSON as app_bq1.py; BQ2 numbers are from docs/bq1.md (BQ2 section).
"""
import json
import os
from pathlib import Path

import streamlit as st

# the weekly pipeline's merged results (local/reports/final_report.py with AML_LIVE=1) when present, else the one-off run
_LIVE = Path(__file__).parent / "data" / "dashboard_data_live_HI-Large-live.json"
DATA_FILE = os.environ.get("BQ1_DATA", _LIVE.name if _LIVE.exists() else "dashboard_data_enrui_breann_mld20.json")
NAMES = ["Breann", "Lesley", "Joey", "Tasha", "En Rui", "Wei Ting"]

def _load():
    """Same source as app_bq1.py: the weekly results live from Athena when AWS is configured (aws_data.load_bq1_live,
    cached 10 min), else the saved file DATA_FILE. BQ1_SOURCE=file always uses the file."""
    if os.environ.get("BQ1_SOURCE", "athena") != "file":
        try:
            import aws_data
            if aws_data.enabled():
                return aws_data.load_bq1_live()
        except Exception as e:                    # never break the page: fall back to the saved file
            print(f"[bq1] Athena unavailable, showing data/{DATA_FILE}: {e}")
    return json.loads((Path(__file__).parent / "data" / DATA_FILE).read_text())


d = _load()
# scheme timings from the one-off run (weekly runs cut schemes at the week boundary); see app_bq1.py
_ONEOFF = Path(__file__).parent / "data" / "dashboard_data_enrui_breann_mld20.json"
if d.get("weeks") and _ONEOFF.exists():
    d["schemes_total"] = json.loads(_ONEOFF.read_text())["schemes_total"]
f = next(r for r in d["stage1"] if r["level"] == "account_day" and r["cutoff"] == "frozen_threshold")
tot = d["schemes_total"]

def question(text, color):
    """The business question: italic, with a coloured bar, so it never reads like the numbers or the answer."""
    st.markdown(f"<div style='border-left:4px solid {color};padding:.15rem 0 .15rem 1rem;margin:.2rem 0 1rem;"
                f"font-size:1.2rem;line-height:1.45;font-style:italic;font-weight:500;color:#d9d3ea'>{text}</div>",
                unsafe_allow_html=True)


def answer(text):
    """The one-line answer, in its own tinted strip under the numbers."""
    st.markdown(f"<div style='background:rgba(144,133,233,.13);border-radius:8px;padding:.65rem 1rem;margin:.3rem 0 .6rem'>"
                f"<b>Answer</b> · {text}</div>", unsafe_allow_html=True)


# number labels in the two cards: violet capitals, so they don't blend into the question
st.html("""<style>
  [class*="st-key-card_"] [data-testid="stMetricLabel"] p { text-transform: uppercase; letter-spacing: .07em;
      font-size: 16px !important; font-weight: 700; color: #b3a9f2 !important; }
  /* never cut numbers or labels off on narrower screens (laptop, projector): wrap labels, size numbers to fit */
  [class*="st-key-card_"] [data-testid="stMetricLabel"] *, [class*="st-key-card_"] [data-testid="stMetricDelta"] * {
      white-space: normal !important; overflow: visible !important; text-overflow: clip !important;
      word-break: keep-all !important; overflow-wrap: normal !important; }
  [class*="st-key-card_"] [data-testid="stMetricLabel"] { min-height: 2.9em; align-items: flex-start; }
  [class*="st-key-card_"] [data-testid="stMetricValue"] * { font-size: clamp(1.6rem, 2.4vw, 2.4rem) !important;
      white-space: nowrap; overflow: visible !important; text-overflow: clip !important; }
</style>""")


# ------------------------------------------------------------------ hero
st.title("Catching money laundering early")
st.iframe("""
<div id="t">This project is done by <span id="n"></span><span id="c">|</span></div>
<style>
  body { margin: 0; background: transparent; }
  #t { font-family: "Source Sans Pro", "Source Sans 3", system-ui, sans-serif; font-size: 28px; color: #c9c2dc; }
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
""".replace("__NAMES__", json.dumps(NAMES)), height=56)
st.markdown("IS459 Big Data Architecture · Group G1T4 · built for the **Transaction Monitoring Tuning Team**, "
            "who decide how many alerts analysts get each day.")

# ------------------------------------------------------------------ the two questions
c1, c2 = st.columns(2, gap="large")
with c1, st.container(border=True, key="card_bq1"):
    st.markdown("#### :material/notifications_active: BQ1 · Early detection")
    question("How can we automatically detect laundering patterns early, while keeping false alerts low enough "
             "for analysts to manage?", "#3987e5")
    a, b, c = st.columns(3)
    a.metric("Alerts a day", f"{f['alerts_per_day']:,.0f}")
    b.metric("Real alerts", f"{f['precision']:.0%}")
    c.metric("Schemes caught", f"{tot['caught']:.0%}")
    answer(f"9 in 10 alerts are real, but only {tot['alert_before_end_eod']:.0%} of schemes are caught while "
           "still running.")
    st.page_link("app_bq1.py", label="See the BQ1 results", icon=":material/arrow_forward:")

with c2, st.container(border=True, key="card_bq2"):
    st.markdown("#### :material/trending_up: BQ2 · Tactic change")
    question("Are criminals changing their laundering tactics over time, and when they do, does our detection "
             "rate fall, and can retraining recover it?", "#d95926")
    a, b, c = st.columns(3)
    a.metric("Shift found in", "2 weeks", help="SAML-D: confirmed in Week 24, 2 weeks after the new tactics began.")
    b.metric("New tactics caught", "70%", "was 40%", delta_arrow="off", help="SAML-D: the old model caught 40% of the new tactics; "
                                                     "the retrained model caught 70%.")
    c.metric("HI-Large", "Stable", help="No tactic change on HI-Large, so no retraining was needed.")
    answer("A tactic shift is spotted within 2 weeks; retraining wins back most of the lost detection.")

# ------------------------------------------------------------------ pipeline
st.subheader(":material/account_tree: How the pipeline works")
st.graphviz_chart("""
digraph G {
  rankdir=LR; bgcolor="transparent"; nodesep=0.22; ranksep=0.32; newrank=true;
  node [shape=box, style="rounded,filled", fillcolor="#221b33", color="#4a3f66", fontcolor="#ece8f4",
        fontname="Helvetica", fontsize=22, margin="0.12,0.06"];
  edge [color="#8a80a8", arrowsize=0.8, fontcolor="#ece8f4", fontname="Helvetica"];
  graph [fontname="Helvetica", fontcolor="#d9d3ea", fontsize=20, color="#4a3f66", style="rounded"];

  subgraph cluster_in { label="Data in"; raw [label="Kaggle data\nin S3"]; }

  subgraph cluster_bq1 { label="BQ1 · each new day's file"; color="#3987e5";
    clean [label="Clean"]; feat [label="Features"]; score [label="Score\n(stage 1)"]; name [label="Name\npattern"];
    clean -> feat -> score -> name; }

  subgraph cluster_bq2 { label="BQ2 · every Monday"; color="#d95926";
    p1 [label="Tactics\nchanged?"]; p2 [label="Detection\nfalling?"];
    alert [label="Alert?", shape=diamond, style="filled"];
    retrain [label="Retrain"]; keep [label="Keep\nwatching"];
    p1 -> p2 -> alert;
    alert -> retrain [label="yes", fontsize=18]; alert -> keep [label="no", fontsize=18];
    { rank=same; retrain; keep; } }

  subgraph cluster_out { label="Results"; gold [label="Alerts,\nmetrics,\nmodels"]; }

  subgraph cluster_serve { label="Serve"; app [label="This\ndashboard"]; athena [label="Athena\nSQL"];
    { rank=same; app; athena; } }

  raw -> clean; raw -> p1; name -> gold; retrain -> gold; gold -> app; gold -> athena;
  { rank=same; clean; p1; }
}
""", width="stretch")
