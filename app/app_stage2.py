"""
BQ1 stage 2 page: how well the alerts are NAMED (fan-in, cycle, layering, ...), on validation days.
    streamlit run app_stage2.py        (data/stage2_data.json; written by pipeline/stage2_report.py)
"""
import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

BLUE, GREY, RED, ORANGE, INK = "#2F7FAD", "#B9B2A8", "#C0504D", "#E07B39", "#262626"

st.set_page_config(page_title="BQ1 · Naming the pattern", layout="wide")


@st.cache_data
def load():
    return json.loads((Path(__file__).parent / "data" / "stage2_data.json").read_text())


def nice(p):
    return {"not_laundering": "Not laundering (false alert)", "none": "Not named"}.get(
        p, p.replace("_", " ").capitalize())


d = load()
board, per = pd.DataFrame(d["board"]), pd.DataFrame(d["per_pattern"])
conf, s1 = pd.DataFrame(d["confusion"]), pd.DataFrame(d["stage1"])
base_cfg, pick = d["baseline"], d["pick"]
same = pick == base_cfg

# ------------------------------------------------------------------ header
st.title("Naming the laundering pattern behind each alert")
st.markdown(
    "**BQ1:** How can we automatically detect laundering patterns early, while keeping false alerts "
    "low enough for analysts to manage?  \nStage 1 flags the account; **stage 2 tells the analyst which "
    "pattern it is**, so they know where to look."
)
ds_name = st.radio("Validation data", sorted(board["dataset"].unique(), reverse=True), horizontal=True,
                   help="Last 25% of each dataset's days. The models never trained on them. HI-Large is not used.")
st.caption(f"Showing run **{pick}** against the baseline **{base_cfg}** · stage 1 = {d['stage1_config']} · "
           "end to end: only stage 1's alerts are named, and naming a false alert counts as wrong")

b = board[board["dataset"] == ds_name]
cur = b[(b["config"] == pick) & (b["method"] == "group")].iloc[0]
ref = b[(b["config"] == base_cfg) & (b["method"] == "group")].iloc[0]
pp = per[(per["dataset"] == ds_name) & (per["mode"] == "end_to_end")]
mine = pp[pp["config"] == pick]
right = mine["correct"].sum() / max(mine["named_as"].sum(), 1)


def delta(col):
    return None if same else f"{cur[col] - ref[col]:+.1%} vs baseline"


k1, k2, k3, k4 = st.columns(4)
k1.metric("Pattern precision", f"{cur['precision']:.0%}", delta(col="precision"),
          help="Of the alerts named X, the share that really are X (average over patterns).")
k2.metric("Pattern recall", f"{cur['recall']:.0%}", delta(col="recall"),
          help="Of all laundering of pattern X, the share flagged AND named X (average over patterns). "
               "Capped by stage 1: laundering it misses can never be named.")
k3.metric("Macro F1", f"{cur['f1']:.3f}", None if same else f"{cur['f1'] - ref['f1']:+.3f} vs baseline")
k4.metric("Alerts given the right label", f"{right:.0%}",
          help="Every alert, including false alerts (right label = 'not a pattern').")

st.divider()

# ------------------------------------------------------------------ 1. experiments
st.subheader("1 · Which stage 2 setting names patterns best")
st.caption("Each bar is one group_stage2.py run on the same stage 1 alerts. Grey = the baseline; blue = the run shown on this page.")
lb = b.assign(kind=lambda t: t["run"].map(lambda r: "pick" if r == pick else ("base" if r == base_cfg else "other")))
bars = alt.Chart(lb).mark_bar(cornerRadiusEnd=4, size=20).encode(
    y=alt.Y("run:N", sort=lb.sort_values("f1", ascending=False)["run"].tolist(), title=None,
            axis=alt.Axis(labelLimit=320)),
    x=alt.X("f1:Q", title="Macro F1, end to end", scale=alt.Scale(domain=[0, max(0.6, lb["f1"].max() * 1.15)])),
    color=alt.Color("kind:N", scale=alt.Scale(domain=["pick", "base", "other"], range=[BLUE, GREY, "#D9D3CA"]),
                    legend=None),
    tooltip=[alt.Tooltip("run:N", title="Run"), alt.Tooltip("f1:Q", format=".3f", title="Macro F1"),
             alt.Tooltip("precision:Q", format=".1%"), alt.Tooltip("recall:Q", format=".1%"),
             alt.Tooltip("settings_json:N", title="Settings")])
text = bars.mark_text(align="left", dx=4).encode(text=alt.Text("f1:Q", format=".3f"), color=alt.value(INK))
st.altair_chart((bars + text).properties(height=alt.Step(34)), width="stretch")
with st.expander("Settings of each run"):
    st.dataframe(lb[["run", "precision", "recall", "f1", "settings_json", "run_at"]]
                 .sort_values("f1", ascending=False).round(3), hide_index=True, width="stretch")

st.divider()

# ------------------------------------------------------------------ 2. per pattern
st.subheader("2 · Per pattern: before and after")
st.caption("Grey dot = baseline, blue dot = this run. Precision: can the analyst trust the label? "
           "Recall: is that pattern found and named?")
pats = pp[pp["pattern"] != "unclassified"].copy()
pats["pattern_name"] = pats["pattern"].map(nice)
pats["run"] = pats["config"].map({pick: "This run", base_cfg: "Baseline"})
order = (pats[pats["config"] == pick].sort_values("f1", ascending=False)["pattern_name"].tolist())


def dumbbell(measure, title):
    x = alt.X(f"{measure}:Q", title=title, axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1]))
    y = alt.Y("pattern_name:N", sort=order, title=None)
    line = alt.Chart(pats).mark_rule(color="#D9D3CA", strokeWidth=2).encode(
        y=y, x=alt.X(f"min({measure}):Q"), x2=alt.X2(f"max({measure}):Q"))
    dots = alt.Chart(pats).mark_point(filled=True, size=120, opacity=1).encode(
        y=y, x=x,
        color=alt.Color("run:N", scale=alt.Scale(domain=["Baseline", "This run"], range=[GREY, BLUE]),
                        legend=alt.Legend(title=None, orient="top")),
        tooltip=[alt.Tooltip("pattern_name:N", title="Pattern"), "run:N",
                 alt.Tooltip(f"{measure}:Q", format=".1%"), alt.Tooltip("true_cases:Q", format=",", title="True cases"),
                 alt.Tooltip("named_as:Q", format=",", title="Alerts named this")])
    return (line + dots).properties(height=alt.Step(36))


c1, c2 = st.columns(2, gap="large")
with c1:
    st.altair_chart(dumbbell("precision", "Precision of the label"), width="stretch")
with c2:
    st.altair_chart(dumbbell("recall", "Recall, end to end"), width="stretch")
with st.expander("Table"):
    t = pats.pivot_table(index="pattern_name", columns="run", values=["precision", "recall", "f1", "true_cases"])
    st.dataframe(t.round(3), width="stretch")

st.divider()

# ------------------------------------------------------------------ 3. confusion
st.subheader("3 · What each pattern gets mistaken for")
if conf.empty:
    st.info(f"No confusion counts for {pick}. Rerun it with the current group_stage2.py, then stage2_report.py.")
else:
    cm = conf[conf["dataset"] == ds_name].copy()
    cm["true_name"], cm["pred_name"] = cm["true_pattern"].map(nice), cm["predicted_pattern"].map(nice)
    cm["share"] = cm["n"] / cm.groupby("true_name")["n"].transform("sum")
    cm["hit"] = cm["true_pattern"] == cm["predicted_pattern"]
    names = sorted(set(cm["true_name"]) | set(cm["pred_name"]), key=lambda n: (n.startswith("Not"), n))
    st.caption("Each row is the true pattern of a stage 1 alert; the cell shows the share named as each column. "
               "A strong diagonal = correct names. The last row is false alerts: whatever they are named, it is wrong.")
    base = alt.Chart(cm).encode(
        x=alt.X("pred_name:N", sort=names, title="Named as", axis=alt.Axis(labelAngle=-30, orient="top")),
        y=alt.Y("true_name:N", sort=names, title="True pattern", axis=alt.Axis(labelLimit=320)))
    cells = base.mark_rect(stroke="#FFFAF3", strokeWidth=2).encode(
        color=alt.Color("share:Q", scale=alt.Scale(range=["#EAF2F8", "#1B4F72"], domain=[0, 1]),
                        legend=alt.Legend(title="Share of row", format="%")),
        tooltip=[alt.Tooltip("true_name:N", title="True"), alt.Tooltip("pred_name:N", title="Named as"),
                 alt.Tooltip("n:Q", format=",", title="Alerts"), alt.Tooltip("share:Q", format=".1%")])
    labels = base.mark_text(fontSize=11).encode(
        text=alt.condition("datum.share >= 0.05", alt.Text("share:Q", format=".0%"), alt.value("")),
        color=alt.condition("datum.share > 0.5", alt.value("white"), alt.value(INK)))
    st.altair_chart((cells + labels).properties(height=alt.Step(36)), width="stretch")

st.divider()

# ------------------------------------------------------------------ 4. top-2
st.subheader("4 · Showing the analyst two guesses")
mine_all = per[(per["dataset"] == ds_name) & (per["config"] == pick) & (per["pattern"] != "unclassified")]
t1 = mine_all[(mine_all["mode"] == "caught") & ~mine_all["pattern"].str.startswith("MACRO")]
t2 = mine_all[(mine_all["mode"] == "caught_top2") & ~mine_all["pattern"].str.startswith("MACRO")]
if t2.empty:
    st.info(f"No top-2 results for {pick}. Runs from the current group_stage2.py save them.")
else:
    st.caption("On alerts that really are a known pattern: how often the right name is the first guess, "
               "and how often it is one of the first two. Look-alike patterns (fan out / scatter gather) are "
               "often confused, but rarely missed by both guesses.")
    tt = pd.concat([t1.assign(guess="First guess"), t2.assign(guess="Top 2 guesses")])
    tt["pattern_name"] = tt["pattern"].map(nice)
    order2 = t2.assign(pattern_name=t2["pattern"].map(nice)).sort_values("recall", ascending=False)["pattern_name"].tolist()
    m1, m2 = st.columns([1, 2], gap="large")
    with m1:
        st.metric("Right name in the first guess", f"{t1['recall'].mean():.0%}")
        st.metric("Right name in the top 2", f"{t2['recall'].mean():.0%}",
                  f"+{t2['recall'].mean() - t1['recall'].mean():.0%} with a second guess")
    with m2:
        y2 = alt.Y("pattern_name:N", sort=order2, title=None)
        rule2 = alt.Chart(tt).mark_rule(color="#D9D3CA", strokeWidth=2).encode(
            y=y2, x=alt.X("min(recall):Q"), x2=alt.X2("max(recall):Q"))
        dots2 = alt.Chart(tt).mark_point(filled=True, size=120, opacity=1).encode(
            y=y2, x=alt.X("recall:Q", title="Truly laundering alerts given the right name", axis=alt.Axis(format="%"),
                          scale=alt.Scale(domain=[0, 1])),
            color=alt.Color("guess:N", scale=alt.Scale(domain=["First guess", "Top 2 guesses"], range=[GREY, BLUE]),
                            legend=alt.Legend(title=None, orient="top")),
            tooltip=[alt.Tooltip("pattern_name:N", title="Pattern"), "guess:N",
                     alt.Tooltip("recall:Q", format=".1%", title="Right"), alt.Tooltip("true_cases:Q", format=",")])
        st.altair_chart((rule2 + dots2).properties(height=alt.Step(36)), width="stretch")

st.divider()

# ------------------------------------------------------------------ 5. ceiling
st.subheader("5 · Stage 2 can only name what stage 1 flags")
s1d = s1[s1["dataset"] == ds_name]
if "stage1_config" not in s1d:
    s1d = s1d.assign(stage1_config=d["stage1_config"])
cur1 = s1d[s1d["stage1_config"] == d["stage1_config"]]
ref1 = s1d[s1d["stage1_config"] == d.get("baseline_stage1_config", d["stage1_config"])]
moved = len(ref1) and len(cur1) and d.get("baseline_stage1_config", d["stage1_config"]) != d["stage1_config"]
caught = per[(per["dataset"] == ds_name) & (per["mode"] == "caught") & (per["config"] == pick)
             & (per["pattern"] != "unclassified")]


def d1(col, fmt="{:+.1%}"):
    return fmt.format(cur1.iloc[0][col] - ref1.iloc[0][col]) + " vs baseline stage 1" if moved else None


e0, e1, e2, e3 = st.columns(4)
if len(cur1):
    r1 = cur1.iloc[0]
    if r1.get("expand", 0):
        st.caption(f"Stage 1 = **{d['stage1_config']}** with alert expansion: a payment scoring ≥ {r1['expand']:g} "
                   "next to a confident alert makes its counterparty an alert too (following the money).")
    e0.metric("Laundering account-days flagged", f"{r1['recall']:.0%}", d1("recall"),
              help="Stage 1 recall at account-day level, on the alerts handed to stage 2.")
    e1.metric("Stage 1 alerts that are laundering", f"{r1['precision']:.0%}", d1("precision"),
              help=f"About {r1['alerts_per_day']:,.0f} account alerts a day on {ds_name}.")
    e2.metric("Patterned laundering flagged by stage 1",
              f"{r1['recall_patterns']:.0%}" if pd.notna(r1.get("recall_patterns")) else "n/a",
              d1("recall_patterns") if pd.notna(r1.get("recall_patterns")) and moved
              and pd.notna(ref1.iloc[0].get("recall_patterns")) else None,
              help="The rest never reaches stage 2: this is the ceiling on pattern recall.")
e3.metric("Pattern F1 on true alerts only", f"{caught['f1'].mean():.3f}" if len(caught) else "n/a",
          help="Naming quality when the false alerts are left out: how good the namer itself is.")
st.caption("If F1 on true alerts is much higher than end-to-end F1, the losses come from false alerts and missed "
           "laundering (stage 1), not from the namer.")

with st.expander("How to read this, and its limits"):
    st.markdown(
        "- An **alert** is one account on one day. Stage 2 gives each alert a pattern name, or *unclassified*.\n"
        "- **Pattern precision / recall** are averaged over the named patterns (macro), so rare patterns count "
        "as much as common ones. Unclassified laundering is left out of the average.\n"
        "- Validation = the last 25% of days of HI-Small and HI-Medium; every setting was chosen on these days, "
        "so the numbers are slightly optimistic. HI-Large was not used.\n"
        "- Bipartite and stack are merged into **layering**: the data cannot tell them apart reliably.\n"
        "- True patterns come from the dataset's answer key (`Patterns.txt`), used only to mark results."
    )
