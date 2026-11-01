"""
BQ2 page for the Transaction Monitoring Tuning Team.
    streamlit run app_bq2.py        (data/bq2_data.json; written by pipeline/bq2_report.py)
"""
import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

BLUE, GREY, RED, INK, AMBER = "#2F7FAD", "#B9B2A8", "#C0504D", "#262626", "#D9922E"
MODEL_NAME = {"all": "Current model (has seen every pattern)", "nofan": "Model that never saw the new tactic"}

st.set_page_config(page_title="BQ2 · Tactics over time", layout="wide")


@st.cache_data
def load():
    d = json.loads((Path(__file__).parent / "data" / "bq2_data.json").read_text())
    f = {k: pd.DataFrame(d[k]) for k in ("weeks", "tactics", "tactics_shift", "scheme_shape", "scorecard",
                                         "scheme_card", "drift", "alarms", "headline", "daily_profile")}
    return d, f


d, f = load()
wk = f["weeks"]
wlabel = {int(r.week): f"W{int(r.week)} · {pd.Timestamp(r.start):%d %b}" for r in wk.itertuples()}
order = [wlabel[w] for w in sorted(wlabel)]
base_w, intro, delay = d["baseline_weeks"], d["intro_week"], d["label_delay_weeks"]
hidden = "+".join(d["hidden"]).replace("_", "-")
HID = "hidden (" + "+".join(d["hidden"]) + ")"
models = list(d["models"])


def with_label(df, col="week"):
    df = df[df[col].isin(wlabel)].copy()
    df["Week"] = df[col].map(wlabel)
    return df


def week_rule(w, color, text):
    if w not in wlabel:
        return None
    x = pd.DataFrame({"Week": [wlabel[w]], "t": [text]})
    return (alt.Chart(x).mark_rule(color=color, strokeDash=[4, 3]).encode(x=alt.X("Week:N", sort=order))
            + alt.Chart(x).mark_text(color=color, align="left", dx=4, dy=-6, fontSize=11)
                 .encode(x=alt.X("Week:N", sort=order), y=alt.value(8), text="t"))


def head(scen, m):
    h = f["headline"]
    r = h[(h["scenario"] == scen) & (h["model"] == m)]
    return r.iloc[0] if len(r) else None


# ------------------------------------------------------------------ header
st.title("Are criminals changing tactics, and would we notice if detection fell behind?")
st.markdown("**BQ2:** Are criminals changing their tactics over time, and can we tell when our detection is falling behind?")
st.caption(f"{d['dataset']}, {len(wk)} weeks ({d['window'][0]} to {d['window'][1]}), scored week by week with the frozen "
           f"BQ1 model. Baseline = weeks {base_w[0]}-{base_w[-1]}. Labels are assumed to arrive {delay} weeks late, "
           "as in a real bank.")

scen = st.radio("What are we looking at?", ["new_tactic", "real"], horizontal=True,
                format_func=lambda s: {"real": "The real data as it is",
                                       "new_tactic": f"Controlled test: {hidden} appear as a new tactic in W{intro}"}[s])
if scen == "new_tactic":
    st.info(f"**Controlled test.** {hidden} laundering is removed from every week before W{intro}, so it looks like a "
            f"brand-new tactic from W{intro}. Two models are compared: the current BQ1 model, and the same model "
            f"trained without {hidden} (it has never seen them, like any model facing a genuinely new tactic).")

card = f["scorecard"][f["scorecard"]["scenario"] == scen]
ov = card[card["group"] == "overall"]
h_n, h_a = head(scen, "nofan" if "nofan" in models else models[0]), head(scen, models[0])
shift = f["tactics_shift"][f["tactics_shift"]["scenario"] == scen]
post = [w for w in sorted(wlabel) if w not in base_w]
max_psi = shift[shift["week"].isin(post)].groupby("dimension")["psi"].max()

k = st.columns(4)
top_dim = max_psi.idxmax() if len(max_psi) else None
k[0].metric("Biggest shift in how laundering looks", f"PSI {max_psi.max():.2f}" if top_dim else "-",
            f"by {top_dim.replace('_', ' ')}" if top_dim else None, delta_color="off",
            help="Population stability index vs the baseline weeks: 0.1 = moderate shift, 0.2 = large shift.")
m_show = "nofan" if (scen == "new_tactic" and "nofan" in models) else models[0]
hc = card[(card["group"] == HID) & (card["model"] == m_show) & (card["week"] >= intro)]
if scen == "new_tactic" and len(hc):
    full = card[(card["group"] == HID) & (card["model"] == models[0]) & (card["week"] >= intro)]
    k[1].metric("New tactic caught (model that never saw it)",
                f"{hc['true_alerts'].sum() / max(hc['positives'].sum(), 1):.0%}",
                f"vs {full['true_alerts'].sum() / max(full['positives'].sum(), 1):.0%} for the current model",
                delta_color="off")
else:
    k[1].metric("Laundering caught, latest week", f"{ov[ov['model'] == models[0]].sort_values('week')['recall'].iloc[-1]:.0%}")
hb = head(scen, m_show)
k[2].metric("Falling behind, from labels",
            f"W{int(hb['falling_behind_week'])}" if hb is not None and pd.notna(hb["falling_behind_week"]) else "not detected",
            f"known in W{int(hb['falling_behind_known_week'])}" if hb is not None and pd.notna(hb["falling_behind_known_week"]) else None,
            delta_color="off", help="Recall below the baseline's normal range two weeks in a row. Labels arrive late, "
                                    f"so it is only known {delay} weeks later.")
k[3].metric("Early warning (no labels needed)",
            f"W{int(hb['early_warning_week'])}" if hb is not None and pd.notna(hb["early_warning_week"]) else "none",
            (f"{int(hb['lead_time_weeks'])} week(s) before the label alarm" if hb is not None and pd.notna(hb["lead_time_weeks"])
             else None), delta_color="off",
            help="First week a label-free signal moved: alerts per day, score distribution, or feature distribution.")
st.divider()

# ------------------------------------------------------------------ 1. tactics
st.subheader("1 · Are tactics changing?")
st.caption("How laundering payments break down each week (from the answer key). Bars that change shape = tactics changing.")
dim = st.selectbox("Break down by", ["pattern", "payment_format", "amount_band", "cross_currency", "cross_bank"],
                   format_func=lambda x: x.replace("_", " "))
t = with_label(f["tactics"][(f["tactics"]["scenario"] == scen) & (f["tactics"]["dimension"] == dim)])
c1, c2 = st.columns([3, 2])
bars = alt.Chart(t).mark_bar().encode(
    x=alt.X("Week:N", sort=order, title=None), y=alt.Y("share:Q", stack="normalize", axis=alt.Axis(format="%"), title="Share of laundering payments"),
    color=alt.Color("category:N", title=dim.replace("_", " ")), tooltip=["Week", "category", alt.Tooltip("share:Q", format=".1%"), "n"])
layers = [bars] + [r for r in [week_rule(intro, RED, "new tactic") if scen == "new_tactic" else None] if r is not None]
c1.altair_chart(alt.layer(*layers).properties(height=320), use_container_width=True)
s = with_label(shift)
lines = alt.Chart(s).mark_line(point=True).encode(
    x=alt.X("Week:N", sort=order, title=None), y=alt.Y("psi:Q", title="Shift vs baseline (PSI)"),
    color=alt.Color("dimension:N", title="by"), tooltip=["Week", "dimension", alt.Tooltip("psi:Q", format=".3f"),
                                                          alt.Tooltip("chi2_p:Q", format=".3g", title="chi-square p")])
lv = pd.DataFrame({"y": d["psi_levels"], "t": ["moderate", "large"]})
c2.altair_chart(alt.layer(lines, alt.Chart(lv).mark_rule(color=GREY, strokeDash=[4, 3]).encode(y="y:Q"))
                .properties(height=320), use_container_width=True)
ta = f["alarms"][(f["alarms"]["scenario"] == scen) & (f["alarms"]["kind"] == "tactic shift") & f["alarms"]["alarm_week"].notna()]
if len(ta):
    st.warning("**Tactics shifted:** " + "; ".join(f"{r.signal} from W{int(r.alarm_week)}" for r in ta.itertuples()))
else:
    st.success("No large, lasting shift in how laundering looks (PSI stayed under 0.2 or did not last two weeks).")

# ------------------------------------------------------------------ 2. detection over time
st.subheader("2 · Is detection falling behind?")
st.caption("Share of laundering account-days the frozen model catches each week, by group. The grey band is the "
           "baseline's normal range (mean ± 2 sd). Two weeks below it = falling behind.")
groups = ["all laundering", HID, "other known patterns", "unclassified"]
g = st.selectbox("Laundering group", groups + [p for p in card["group"].unique() if p not in groups + ["overall"]], index=1 if scen == "new_tactic" else 0)
cg = with_label(card[card["group"] == g].dropna(subset=["recall"]))
cg["Model"] = cg["model"].map(lambda m: MODEL_NAME.get(m, m))
line = alt.Chart(cg).mark_line(point=True).encode(
    x=alt.X("Week:N", sort=order, title=None), y=alt.Y("recall:Q", axis=alt.Axis(format="%"), title="Laundering caught", scale=alt.Scale(domain=[0, 1])),
    color=alt.Color("Model:N", scale=alt.Scale(range=[BLUE, RED])), tooltip=["Week", "Model", alt.Tooltip("recall:Q", format=".1%"), "positives", "true_alerts"])
layers = [line]
b = card[(card["group"] == g) & card["week"].isin(base_w) & (card["model"] == models[0])]["recall"].dropna()
if len(b):
    lcl = b.mean() - 2 * max(b.std(ddof=1) if len(b) > 1 else 0, 0.02)
    band = pd.DataFrame({"lo": [max(lcl, 0)], "hi": [min(b.mean() + 2 * max(b.std(ddof=1) if len(b) > 1 else 0, 0.02), 1)]})
    layers.insert(0, alt.Chart(band).mark_rect(color=GREY, opacity=0.25).encode(y="lo:Q", y2="hi:Q"))
if scen == "new_tactic":
    layers.append(week_rule(intro, RED, "new tactic"))
st.altair_chart(alt.layer(*[x for x in layers if x is not None]).properties(height=320), use_container_width=True)

c1, c2 = st.columns(2)
ovl = with_label(ov.copy())
ovl["Model"] = ovl["model"].map(lambda m: MODEL_NAME.get(m, m))
c1.altair_chart(alt.Chart(ovl).mark_line(point=True).encode(
    x=alt.X("Week:N", sort=order, title=None), y=alt.Y("precision:Q", axis=alt.Axis(format="%"), title="Alerts that are real"),
    color=alt.Color("Model:N", scale=alt.Scale(range=[BLUE, RED]), legend=None)).properties(height=220, title="Precision"),
    use_container_width=True)
sc = f["scheme_card"]
sc = with_label(sc[(sc["scenario"] == scen) & (sc["group"] == (HID if scen == "new_tactic" else "all schemes"))])
if len(sc):
    sc["Model"] = sc["model"].map(lambda m: MODEL_NAME.get(m, m))
    c2.altair_chart(alt.Chart(sc).mark_line(point=True).encode(
        x=alt.X("Week:N", sort=order, title=None), y=alt.Y("before_end:Q", axis=alt.Axis(format="%"), title="Alerted while still running"),
        color=alt.Color("Model:N", scale=alt.Scale(range=[BLUE, RED]), legend=None), tooltip=["Week", "Model", "schemes", alt.Tooltip("before_end:Q", format=".0%")])
        .properties(height=220, title="Schemes alerted before their last payment"), use_container_width=True)

# ------------------------------------------------------------------ 3. early warning
st.subheader("3 · Would we know in time?")
st.caption(f"Labels (what really was laundering) arrive about {delay} weeks late, so recall can only be checked late. "
           "These signals need no labels and are known the same week.")
dr = with_label(f["drift"][f["drift"]["scenario"] == scen].copy())
dr["Model"] = dr["model"].map(lambda m: MODEL_NAME.get(m, m))
c1, c2, c3 = st.columns(3)
for col, y, title, rule in [(c1, "alerts_z", "Alerts per day vs normal (z-score)", 3),
                            (c2, "score_psi", "Shift in risk scores (PSI)", d["psi_levels"][0]),
                            (c3, "feature_psi_max", "Biggest shift in a top feature (PSI)", d["psi_levels"][1])]:
    ch = alt.Chart(dr).mark_line(point=True).encode(
        x=alt.X("Week:N", sort=order, title=None), y=alt.Y(f"{y}:Q", title=None),
        color=alt.Color("Model:N", scale=alt.Scale(range=[BLUE, RED]), legend=None),
        tooltip=["Week", "Model", alt.Tooltip(f"{y}:Q", format=".3f"), "feature_psi_top"])
    rl = alt.Chart(pd.DataFrame({"y": [rule]})).mark_rule(color=GREY, strokeDash=[4, 3]).encode(y="y:Q")
    col.altair_chart(alt.layer(ch, rl).properties(height=220, title=title), use_container_width=True)

al = f["alarms"][(f["alarms"]["scenario"] == scen) & f["alarms"]["alarm_week"].notna()].copy()
if len(al):
    al["model"] = al["model"].map(lambda m: MODEL_NAME.get(m, m) if isinstance(m, str) else "answer key")
    al["happened"] = al["alarm_week"].map(lambda w: wlabel.get(int(w), f"W{int(w)}"))
    al["we could know"] = al["known_week"].map(lambda w: wlabel.get(int(w), f"W{int(w)} (after the window)"))
    st.dataframe(al[["kind", "signal", "model", "happened", "we could know", "detail"]], hide_index=True, use_container_width=True)
else:
    st.success("No alarms in this view.")

# ------------------------------------------------------------------ 4. so what
st.subheader("4 · What the threshold team should do")
acts = []
for m in models:
    r = head(scen, m)
    if r is not None and pd.notna(r["falling_behind_week"]):
        acts.append(f"**{MODEL_NAME.get(m, m)}:** {r['falling_behind_signal']} fell below normal in W{int(r['falling_behind_week'])}. "
                    + (f"A label-free warning fired in W{int(r['early_warning_week'])} ({r['early_warning_signal']}), "
                       f"{int(r['lead_time_weeks'])} week(s) before the labels could confirm it: review alerts and "
                       "thresholds (BQ1 page) as soon as the warning fires, and retrain with the newest labelled weeks."
                       if pd.notna(r["early_warning_week"]) and pd.notna(r["lead_time_weeks"]) and r["lead_time_weeks"] > 0
                       else "No label-free warning fired earlier, so this would only be found from labels: shorten the label delay "
                            "(faster investigations) or add a targeted monitor for the new shape."))
if not acts:
    acts.append("Detection held steady in every group: keep the current thresholds, keep monitoring weekly.")
for a in acts:
    st.markdown("- " + a)

with st.expander("Data check: laundering share per day (the end of the dataset is cut where the simulation winds down)"):
    p = f["daily_profile"].copy()
    p["date"] = pd.to_datetime(p["txn_date"])
    st.altair_chart(alt.Chart(p).mark_line().encode(x="date:T", y=alt.Y("laundering_rate:Q", axis=alt.Axis(format="%"))).properties(height=200),
                    use_container_width=True)
