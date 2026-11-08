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
BLUE, ORANGE, GREY, INK = "#3987e5", "#d95926", "#898781", "#c9c2dc"   # validated on the dark purple surface
RAG = {"Red": "#5a2330", "Yellow": "#5a4517", "Green": "#1e4a2c"}   # dark status tints; the word is always shown too
ACTION = {"Red": "Analysts check these by hand", "Yellow": "Spot-check a sample",
          "Green": "Let the model handle it"}
LAYERING = {"stack", "bipartite"}           # stage 2 names these two together as "layering"
VALIDATION_TOP2 = 0.82                      # enrui_breann_mld20_grp, HI-Small + HI-Medium validation
# Red / green cut-offs, frozen from the validation baseline (txn_v3m, results/final/validate_report.txt) before
# testing, like the alert threshold, so every run is judged against the same bar:
#   recall: named-pattern recall 88.9% / 87.7% -> green from 88%, red 10 points below
#   precision: validation precision 90.9% / 89.4% -> green from 90%, red 10 points below
#   speed: green when most schemes (50%) are caught while still running, red below 40%
CUTOFFS = {"recall": (0.78, 0.88), "precision": (0.80, 0.90), "speed": (0.40, 0.50)}

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


def chart(c, **kw):
    """Every chart with larger axis and legend text (the site default is hard to read)."""
    st.altair_chart(c.configure_axis(labelFontSize=17, titleFontSize=18, titlePadding=12)
                     .configure_legend(labelFontSize=17, titleFontSize=17, symbolSize=200, labelLimit=0)
                     .configure_header(labelFontSize=17), **kw)


def takeaway(text, why, read=None):
    """Under every chart: the finding in one or two lines, then ONE folded box: how to read it, then why it matters."""
    st.info(f"**Takeaway:** {text}", icon=":material/lightbulb:")
    with st.expander("How to read this, and why it matters"):
        if read:
            st.markdown(f"**How to read it.** {read}")
        st.markdown("\n".join(f"- {w}" for w in why))


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
st.title("BQ1 · Early detection")
st.markdown("<div style='font-size:1.35rem;line-height:1.4;font-weight:600;margin:.1rem 0 .5rem'>How can we "
            "automatically detect laundering patterns early, while keeping false alerts low enough for analysts "
            "to manage?</div>", unsafe_allow_html=True)
st.markdown(f"<div style='display:inline-block;background:rgba(144,133,233,.13);border:1px solid #3a3150;"
            f"border-radius:999px;padding:.35rem 1rem;margin:.2rem 0 1rem'><b>Test data:</b> {days} unseen "
            f"days of HI-Large ({pd.to_datetime(frozen['date_from']):%d %b} to {pd.to_datetime(frozen['date_to']):%d %b %Y}, "
            f"{frozen['payments'] / 1e6:.1f}M payments). The model never saw these days in training.</div>",
            unsafe_allow_html=True)
tab1, tab2, tab3, tab4 = st.tabs([":material/notifications: Alerts & accuracy", ":material/timer: Speed",
                                  ":material/groups: Who checks what", ":material/inventory_2: Archived findings"])

with tab1:
    # ------------------------------------------------------------------ 1. alerts and accuracy
    st.subheader(f"{pct(frozen['precision'])} of alerts are real, but they reach only {pct(frozen['recall'])} of laundering")
    st.markdown(f":material/touch_app: **How to use:** drag the slider to the number of alerts your team can review a day. The four numbers and "
            f"the chart follow it. It starts at the model's own setting ({thr_k:,} a day), which sections 2 to 4 use.")

    opts = ad[ad["cutoff"].str.startswith("top") | (ad["cutoff"] == "frozen_threshold")].copy()
    opts["k"] = opts["alerts_per_day"].round().astype(int)
    opts = opts.sort_values("k")
    pick = st.select_slider("Alerts your team reviews per day", options=opts["k"].tolist(), value=thr_k,
                            format_func=lambda v: f"{v:,} (model's setting)" if v == thr_k else f"{v:,}")
    row = opts[opts["k"] == pick].iloc[0]
    n_alerts, n_true, n_pos = int(row["alerts"]), int(row["true_alerts"]), int(row["positives"])
    n_false, n_missed = n_alerts - n_true, n_pos - n_true

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Alerts per day", per_day(n_alerts / days),
              help=f"{n_alerts:,} alerts over the {days} test days, divided by {days}.")
    m2.metric("Real laundering alerts per day", per_day(n_true / days), f"{pct(row['precision'])} of alerts (precision)",
              delta_color="off", delta_arrow="off",
              help=f"Alerts on an account that really laundered money that day, checked against the dataset's answer key: "
                   f"{n_true:,} of {n_alerts:,} alerts. Precision = {n_true:,} ÷ {n_alerts:,} = {pct(row['precision'])}.")
    m3.metric("Laundering missed per day", per_day(n_missed / days), f"{pct(1 - row['recall'])} of all laundering (1 − recall)",
              delta_color="off", delta_arrow="off",
              help=f"Laundering that got no alert. The answer key has {n_pos:,} laundering account-days in the test days; "
                   f"{n_true:,} got an alert, so {n_missed:,} were missed ({per_day(n_missed / days)} a day). "
                   f"Recall = {n_true:,} ÷ {n_pos:,} = {pct(row['recall'])}.")
    m4.metric("False alerts per day", per_day(n_false / days), f"{pct(1 - row['precision'])} of alerts",
              delta_color="off", delta_arrow="off",
              help=f"Alerts on an account that did not launder that day: {n_alerts:,} − {n_true:,} = {n_false:,} "
                   f"({per_day(n_false / days)} a day). Analysts clear these for nothing.")
    pc = opts[["k", "precision"]].copy()
    xp = alt.X("k:O", title="Alerts reviewed per day", sort=pc["k"].tolist(), axis=alt.Axis(labelAngle=0, format=","))
    pline = alt.Chart(pc).mark_line(strokeWidth=2.5, color=BLUE, point=alt.OverlayMarkDef(filled=True, size=60)).encode(
        x=xp, y=alt.Y("precision:Q", title="Real alerts (precision)", axis=alt.Axis(format="%"),
                      scale=alt.Scale(domain=[0, 1])),
        tooltip=[alt.Tooltip("k:O", title="Alerts/day", format=","), alt.Tooltip("precision:Q", format=".1%")])
    prule = alt.Chart(pd.DataFrame({"k": [pick]})).mark_rule(color=INK, strokeDash=[4, 4]).encode(x=xp)
    chart((pline + prule).properties(height=260),
                    width="stretch")
    pk = pc.set_index("k")["precision"]
    real = lambda k: k * pk[k]                                    # real alerts a day at k alerts a day
    if {1000, 5000, thr_k} <= set(pk.index) and 5000 > thr_k:
        extra = (real(5000) - real(thr_k)) / (5000 - thr_k)
        false5k = 5000 * (1 - pk[5000])
        missed = 1 - frozen["recall"]
        takeaway(f"up to about 2,000 alerts a day, the team's capacity, not the model, limits how much laundering is "
                 f"caught: almost every extra alert there is real. Past the model's setting the model has run out of "
                 f"laundering it can recognise: each extra 100 alerts find only about {extra * 100:.0f} launderers. So "
                 f"the {pct(missed)} of laundering still missed will not be found by hiring more analysts; it needs new "
                 "signals in the model.",
                 [f"**Which half of BQ1 this answers.** BQ1 asks for early detection *and* false alerts low enough for "
                  f"analysts to manage. This chart is the second half: it maps the number of alerts a team can review a "
                  f"day to how many of them are worth their time.",
                  f"**What the shape tells us.** The line stays near 100% up to 1,000 alerts a day, falls slowly to "
                  f"{pct(pk[thr_k])} at the model's setting ({thr_k:,}), then drops to {pct(pk[5000])} at 5,000. A flat "
                  "start means the model ranks well: its most suspicious accounts really are laundering. The sharp drop "
                  "means it has used up the laundering it can tell apart from normal activity, not that it ranks badly.",
                  f"**Why the drop happens where it does.** About 60% of laundering follows a known pattern, and the model "
                  "finds about 85% of that. Once those cases are in the queue, what is left looks like ordinary payments, "
                  "so extra alerts are mostly ordinary customers.",
                  f"**The decision it supports.** The Tuning Team should set the daily alert budget near the model's "
                  f"setting. Going to 5,000 a day would add about {false5k:,.0f} false alerts a day for a small gain. "
                  "Money is better spent on new model signals (for example links to accounts already confirmed as "
                  "laundering) than on more analysts.",
                  "**Limits.** Synthetic data; one alert = one account on one day; the threshold was fixed on "
                  "validation days before this test."], read=(
                 "Each day the model gives every account a risk score; the riskiest accounts become alerts, and one "
                 "alert is one account on one day. The line shows, for each daily alert volume on the bottom axis, the "
                 "share of alerts that are real laundering (precision); the dashed line is the volume on the slider. "
                 "Precision = real alerts / all alerts; recall = laundering that got an alert / all laundering. Every "
                 f"number comes from one test on {days} unseen days of HI-Large ({frozen['date_from'][:10]} to "
                 f"{frozen['date_to'][:10]}, {frozen['payments'] / 1e6:.1f}M payments); the threshold was fixed on "
                 "HI-Small and HI-Medium validation days and never tuned on HI-Large. The data is IBM's synthetic AML "
                 "data, so real banks' volumes and patterns will differ."))


    # ---------------------------------------------------------------- 1b. two queues
    qs = {q["queue"]: q for q in d.get("queues") or []}
    if {"main", "second", "all"} <= set(qs):
        main_q, second_q, all_q = qs["main"], qs["second"], qs["all"]
        st.divider()
        st.markdown(f"#### :material/call_split: Two queues lift the main queue to {pct(main_q['precision'])} real alerts")
        q1, q2, q3 = st.columns(3)
        q1.metric("Main queue: alerts a day", per_day(main_q["alerts"] / main_q["days"]),
                  f"{pct(main_q['precision'])} real (was {pct(all_q['precision'])})", delta_arrow="off",
                  help=f"Alerts the namer gives a pattern name: {main_q['true_alerts']:,} of {main_q['alerts']:,} are real. "
                       f"With every alert in one queue: {all_q['true_alerts']:,} of {all_q['alerts']:,}.")
        q2.metric("Laundering the main queue catches", pct(main_q["recall"]), f"was {pct(all_q['recall'])}",
                  delta_color="off", delta_arrow="off",
                  help=f"{main_q['true_alerts']:,} of the {main_q['positives']:,} laundering account-days.")
        q3.metric("Second queue: alerts a day", per_day(second_q["alerts"] / second_q["days"]),
                  f"{pct(second_q['precision'])} real", delta_color="off", delta_arrow="off",
                  help=f"Alerts the namer calls 'unclassified': {second_q['true_alerts']:,} of {second_q['alerts']:,} are real.")
        takeaway(f"the pattern namer doubles as a noise filter. Alerts it cannot match to any pattern are mostly false "
                 f"({pct(second_q['precision'])} real), so moving them to a second queue lifts the main queue from "
                 f"{pct(all_q['precision'])} to {pct(main_q['precision'])} real while it still catches "
                 f"{pct(main_q['recall'])} of laundering (was {pct(all_q['recall'])}). Analysts get fewer wasted reviews "
                 "for almost no loss, and nothing is thrown away.",
                 ["**Which half of BQ1 this answers.** 'False alerts low enough for analysts to manage': it raises the "
                  "share of real alerts without changing the model or the alert threshold.",
                  f"**What the numbers tell us.** About {per_day(second_q['alerts'] / second_q['days'])} alerts a day go to "
                  f"the second queue, and most of them are false. The main queue loses only "
                  f"{(all_q['recall'] - main_q['recall']) * 100:.1f} points of laundering caught.",
                  "**Checked before trusting it.** The idea was spotted on these test results, so it was checked on the "
                  "validation days first: precision rose from 91.2% to 94.1% on HI-Medium and from 89.7% to 92.1% on "
                  "HI-Small. It holds on both, a little smaller than here.",
                  "**The decision it supports.** Run two queues: the main one at full priority, the second one checked "
                  "when the main queue is done. Every alert is still reviewed eventually."],
                 read="Every alert gets a pattern name from stage 2. Alerts named 'unclassified' (no known pattern) go to "
                      "the second queue; every other alert stays in the main queue. Counts include alert expansion (linked "
                      "accounts alerted too), so the all-alerts precision here is slightly below the number at the top.")

with tab2:
    # ------------------------------------------------------------------ 2. speed
    _eod = sc["caught_before_end_eod"].fillna(False).astype(bool).mean() if not sc.empty else tot["alert_before_end_eod"]
    st.subheader(f"{pct(_eod)} of schemes are alerted while they are still running")
    st.markdown(":material/touch_app: **How to use:** pick a pattern (or all), then choose when a scheme counts as detected: when the end-of-day "
            "alert reaches analysts (today's process) or when its first payment is flagged (if payments were scored as "
            "they happen). The numbers on the left and the chart both follow your choice.")

    if sc.empty:
        st.info("Per-scheme timings are missing from the data file: rerun local/reports/final_report.py.")
    else:
        EOD, LIVE = "End-of-day alert (today's process)", "First flagged payment (if scored live)"
        c1, c2 = st.columns([1, 2], gap="large")
        sel = c1.selectbox("Pattern", ["All patterns"] + sorted(sc["pattern_type"].unique()),
                           format_func=lambda p: p if p == "All patterns" else nice(p))
        clock = c2.radio("When does a scheme count as detected?", [EOD, LIVE], horizontal=True,
                         help="End-of-day: alerts are made when the day closes, so analysts hear at midnight after the first "
                              "day any of the scheme's accounts is alerted. First flagged payment: the moment one of the "
                              "scheme's payments scores above the threshold; only reachable if payments were scored as "
                              "they happen, so it is a best case.")
        sub = sc if sel == "All patterns" else sc[sc["pattern_type"] == sel]
        live = clock == LIVE
        det = sub["hours_to_first_flag" if live else "hours_to_alert"].astype(float)
        dur = sub["duration_h"].astype(float)
        found = det.notna()
        running = found & ((det < dur) if live else sub["caught_before_end_eod"].fillna(False).astype(bool))
        when = "the first flagged payment" if live else "the end-of-day alert reaching analysts"

        s1c, s2c = st.columns([1, 2], gap="large")
        with s1c:
            st.metric("Schemes", f"{len(sub):,}", help="Schemes in Patterns.txt that started in the test days.")
            st.metric("Detected", pct(found.mean()),
                      help=f"Schemes with {when} at any time: {found.sum():,} of {len(sub):,}. End-of-day can be slightly "
                           "higher than live: when an account is alerted, its linked accounts are alerted too (alert "
                           "expansion), so a scheme can get an alert without any of its own payments scoring above the "
                           "threshold.")
            st.metric("Detected while still running", pct(running.mean()),
                      help=f"Schemes where {when} came before the scheme's last payment, so there was still something to "
                           f"stop: {running.sum():,} of {len(sub):,}.")
            st.metric("Median time to detection", hours(det[found].median()),
                      help=f"Hours from the scheme's first payment to {when}, middle value over the {found.sum():,} "
                           "detected schemes.")
            st.metric("Median scheme length", hours(dur.median()),
                      help=f"Hours from the scheme's first payment to its last, middle value over {len(sub):,} schemes.")

        with s2c:
            t = np.arange(0, 169, 2)
            det_inf = det.fillna(np.inf).to_numpy()
            over_lbl, found_lbl = "Schemes already over (last payment made)", "Schemes already detected"
            curve = pd.DataFrame({"h": np.concatenate([t, t]),
                                  "share": np.concatenate([[(dur <= v).mean() for v in t], [(det_inf <= v).mean() for v in t]]),
                                  "line": [over_lbl] * len(t) + [found_lbl] * len(t)})
            xh = alt.X("h:Q", title="Hours since the scheme's first payment", scale=alt.Scale(domain=[0, 168]),
                       axis=alt.Axis(values=list(range(0, 169, 24))))
            cl = alt.Chart(curve).mark_line(strokeWidth=2.5, interpolate="step-after").encode(
                x=xh, y=alt.Y("share:Q", title="Share of schemes", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
                color=alt.Color("line:N", scale=alt.Scale(domain=[over_lbl, found_lbl], range=[GREY, ORANGE]),
                                legend=alt.Legend(title=None, orient="bottom", labelLimit=0)),
                tooltip=[alt.Tooltip("h:Q", title="Hours"), "line:N", alt.Tooltip("share:Q", format=".0%")])
            chart(cl.properties(height=320), width="stretch")
            at24 = lambda s: pct((s <= 24).mean())

        d24, o24 = (pd.Series(det_inf) <= 24).mean(), (dur <= 24).mean()
        takeaway(f"the model is not the slow part; the daily schedule is. {pct(d24)} of these schemes are detected "
                 f"within a day of starting, yet {pct(o24)} have already made their last payment by then. Faster "
                 "alerts would therefore save more laundering than a better model would.",
                 ["**Which half of BQ1 this answers.** The 'early' half. Early only means something next to how long "
                  "schemes last, so detection (orange) is drawn against how fast schemes finish (grey).",
                  f"**What the shape tells us.** Orange climbs steeply in the first day because alerts arrive at the end "
                  f"of the day a scheme starts. Grey starts at {pct((dur <= 0).mean())}: that many schemes make every "
                  "payment in the same minute and are over before any alert can exist. Where orange is above grey, "
                  "detection is ahead of the schemes; where it is below, schemes are finishing before anyone is told.",
                  f"**What follows.** The median detection ({hours(det[found].median())}) is close to the median scheme "
                  f"length ({hours(dur.median())}), so about half the schemes end before analysts can act. Because the "
                  "model already flags most schemes on their first payment, the lost time is the wait for the day to "
                  "close, not the model.",
                  "**The decision it supports.** Scoring payments as they arrive (live) is an architecture change, not a "
                  "model change, and it is the lever for speed. The charts below show which patterns and which scheme "
                  "lengths would gain from it."], read=(
                 "A scheme is one laundering operation in the dataset's answer key (Patterns.txt), from its first to its "
                 "last payment; only schemes that started in the test days count. Pick an hour on the bottom axis: grey "
                 "is the share of schemes already over, orange the share already detected. The end-of-day clock assumes "
                 "alerts reach analysts at midnight after the day closes; the live clock is a best case, because each "
                 "score uses that whole day's payments."))

        # ---------------------------------------------------------- 2a. per pattern: length vs time to detection
        det_col = "hours_to_first_flag" if live else "hours_to_alert"
        clock_word = "the first flagged payment" if live else "the end-of-day alert"
        st.divider()
        st.markdown("#### :material/schedule: Per pattern: how long schemes last vs how long until they are detected")
        pm = pd.DataFrame([{"pattern": nice(p), "length_h": g["duration_h"].astype(float).median(),
                            "detect_h": g[det_col].astype(float).median(), "schemes": len(g)}
                           for p, g in sc.groupby("pattern_type")])
        pml = pm.melt(id_vars=["pattern", "schemes"], value_vars=["length_h", "detect_h"], var_name="m", value_name="hours")
        pml["m"] = pml["m"].map({"length_h": "Median scheme length", "detect_h": "Median time to detection"})
        ya = alt.Y("pattern:N", sort=pm.sort_values("length_h")["pattern"].tolist(), title=None,
                   axis=alt.Axis(labelLimit=260))
        link_a = alt.Chart(pm).mark_rule(color=GREY, strokeWidth=2).encode(y=ya, x="length_h:Q", x2="detect_h:Q")
        dots_a = alt.Chart(pml).mark_point(filled=True, size=110, opacity=1).encode(
            y=ya, x=alt.X("hours:Q", title="Hours from the scheme's first payment (median)"),
            color=alt.Color("m:N", scale=alt.Scale(domain=["Median scheme length", "Median time to detection"],
                                                   range=[GREY, ORANGE]), legend=alt.Legend(title=None, orient="top")),
            tooltip=[alt.Tooltip("pattern:N", title="Pattern"), "m:N", alt.Tooltip("hours:Q", format=".1f"),
                     alt.Tooltip("schemes:Q", format=",")])
        chart((link_a + dots_a).properties(height=alt.Step(34)), width="stretch")
        inst_p = pm[pm["length_h"] <= 0]["pattern"].tolist()
        late = pm[(pm["length_h"] > 0) & (pm["detect_h"] > pm["length_h"])].sort_values("length_h")["pattern"].tolist()
        on_time = pm[(pm["length_h"] > 0) & (pm["detect_h"] <= pm["length_h"])].sort_values(
            "length_h", ascending=False)["pattern"].tolist()
        low = lambda ps: ", ".join(p.lower() for p in ps)
        parts = []
        if inst_p:
            parts.append(f"most {low(inst_p)} schemes make all their payments within a minute, so they can only be "
                         "detected after they end")
        if late:
            parts.append(f"the typical {low(late)} scheme is over before {clock_word} (orange right of grey)")
        if on_time:
            parts.append(f"{low(on_time)} schemes run long enough to be caught in time")
        takeaway(("speed is a pattern problem, not a model problem. "
                  + (f"{low(inst_p).capitalize()} schemes happen in one burst, so for them the realistic goal is fast "
                     "follow-up after the fact, not prevention. " if inst_p else "")
                  + (f"{low(late).capitalize()} schemes end a few hours before {clock_word}: these are where a faster "
                     "alert would turn late detection into prevention. " if late else "")
                  + (f"{low(on_time).capitalize()} schemes run for a day or more, so the current alert already leaves "
                     "time to act on them." if on_time else "")),
                 ["**Which half of BQ1 this answers.** The 'early' half, pattern by pattern. A single average speed hides "
                  "that 13 hours is fast for a scheme that runs for days and far too late for one that ends in an hour.",
                  "**What the shape tells us.** Detection (orange) sits in a narrow band for every pattern, because the "
                  "model flags most schemes on their first payment and alerts come at the end of that day. Scheme "
                  "length (grey) varies hugely between patterns. So whether a pattern is caught in time depends almost "
                  "entirely on how long that kind of scheme runs, not on how hard it is to spot.",
                  "**What follows.** Patterns split into three groups that need different treatment: one-burst "
                  "patterns (detect afterwards and freeze funds fast), patterns that end within a day (live scoring or "
                  "watching the accounts would turn late into on time) and long-running patterns (today's daily alert "
                  "is enough).",
                  "**The decision it supports.** The Tuning Team can target speed work at the middle group instead of "
                  "the whole system, and give analysts a rule of thumb per pattern."], read=(
                 "Each row is a pattern. Grey dot: how long its schemes usually last (median, first to last payment). "
                 "Orange dot: how long until they are usually detected (median, with the clock chosen above). Orange "
                 "to the right of grey means the typical scheme is over before it is detected."))

        # ---------------------------------------------------------- 2b. caught within the scheme's own period
        st.divider()
        st.markdown("#### :material/fact_check: Per pattern: caught within the scheme's own time window")
        timed = sc[sc["duration_h"].astype(float) > 0]
        instant = sc[sc["duration_h"].astype(float) <= 0]

        def outcome(g):
            dg = g[det_col].astype(float)
            run_ = dg.notna() & ((dg < g["duration_h"].astype(float)) if live
                                 else g["caught_before_end_eod"].fillna(False).astype(bool))
            return np.select([run_, dg.notna()], ["Caught while running", "Caught after it ended"], "Not caught")

        oc = timed.assign(outcome=outcome(timed), pattern=timed["pattern_type"].map(nice))
        share = oc.groupby("pattern")["outcome"].apply(lambda s: (s == "Caught while running").mean())
        oc_order = share.sort_values(ascending=False).index.tolist()
        bars_b = alt.Chart(oc).mark_bar(size=18).encode(
            y=alt.Y("pattern:N", sort=oc_order, title=None, axis=alt.Axis(labelLimit=260)),
            x=alt.X("count():Q", stack="normalize", title="Share of the pattern's schemes", axis=alt.Axis(format="%")),
            color=alt.Color("outcome:N", sort=["Caught while running", "Caught after it ended", "Not caught"],
                            scale=alt.Scale(domain=["Caught while running", "Caught after it ended", "Not caught"],
                                            range=[BLUE, ORANGE, GREY]), legend=alt.Legend(title=None, orient="top")),
            order=alt.Order("outcome_rank:Q"),
            tooltip=[alt.Tooltip("pattern:N", title="Pattern"), "outcome:N", alt.Tooltip("count():Q", title="Schemes")])
        oc["outcome_rank"] = oc["outcome"].map({"Caught while running": 0, "Caught after it ended": 1, "Not caught": 2})
        chart(bars_b.properties(height=alt.Step(30)), width="stretch")
        all_run = (outcome(timed) == "Caught while running").mean() if len(timed) else None
        inst_found = instant[det_col].notna().mean() if len(instant) else None
        st.markdown(f"{len(timed):,} schemes that last longer than a minute. Another {len(instant):,} schemes "
                   f"({pct(len(instant) / len(sc))}) make all their payments in the same minute: no alert can arrive while "
                   f"they run, and {pct(inst_found)} of them are detected afterwards.")
        gap = 1 - share.min()
        takeaway(f"for schemes that can be stopped at all, the model is usually in time ({pct(all_run)} caught while "
                 f"running), so the remaining risk is concentrated, not spread out: {oc_order[-1].lower()} loses "
                 f"{pct(gap)} of its schemes to late or missed alerts, against {pct(1 - share.max())} for "
                 f"{oc_order[0].lower()}. Extra effort pays most on the patterns at the bottom of this chart.",
                 ["**Which half of BQ1 this answers.** 'Early' in the strictest sense: did the alert arrive while the "
                  "scheme was still moving money, using each scheme's own first and last payment from the answer key "
                  "(Patterns.txt)?",
                  "**What the shape tells us.** Blue (caught while running) dominates every pattern, so the model is not "
                  "systematically late. Orange (caught after it ended) is what grows where the model falls short; grey "
                  "(not caught) is small everywhere, so missing schemes entirely is rare.",
                  f"**What follows.** Prevention is realistic for most schemes. The {len(instant):,} same-minute schemes "
                  f"are left out because no system can beat them in time ({pct(inst_found)} are still found afterwards); "
                  "counting them would make the model look worse for a reason no setting can fix.",
                  "**The decision it supports.** Put analysts' close watching, or the first live-scoring pilot, on the "
                  "patterns at the bottom: that is where an earlier alert changes the outcome."], read=(
                 "Each bar is one pattern's schemes that last longer than a minute: blue = caught while still running, "
                 "orange = caught after its last payment, grey = never caught. Schemes that make every payment in the "
                 "same minute are counted separately in the line above the takeaway."))



with tab3:
    # ------------------------------------------------------------------ 3. which patterns need people
    st.subheader("Which patterns the model can handle, and which need analysts")
    th = CUTOFFS

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
    css["Pattern"] = [f"background-color: {RAG[v]}; font-weight: 600" for v in tbl["Status"]]   # overall colour
    for c, k in cell_key.items():
        css[c] = [f"background-color: {RAG[rate(v, th[k])]}" if rate(v, th[k]) else "" for v in tbl[c]]
    show = tbl.copy()                                                   # ...shown as text (Streamlit prints blanks as "None")
    for c in [c for c in tbl.columns if c in cell_key or c.startswith("Named right")]:
        show[c] = tbl[c].map(pct)
    col_help = {
        "Pattern": "Coloured by the pattern's worst number: red = analysts check by hand, yellow = spot-check, "
                   "green = let the model handle it.",
        "Why": "Which numbers set the colour.",
        "Laundering caught": "Of this pattern's laundering account-days in the answer key, the share that got an alert "
                             "(recall for this pattern).",
        "Alerts that are real": "Of the alerts the model named this pattern, the share that really were laundering "
                                "(precision for this name). Stack and bipartite share the name 'layering'.",
        "Caught before it ends": "Of this pattern's schemes, the share where the end-of-day alert reached analysts before "
                                 "the scheme's last payment.",
        "Median time to alert": "Hours from a scheme's first payment to its end-of-day alert, middle value over the "
                                "pattern's detected schemes.",
        "Median scheme length": "Hours from a scheme's first payment to its last, middle value.",
        "Named right (1 guess)": "Of this pattern's laundering that got an alert, the share where the model's pattern name "
                                 "was right.",
        "Named right (2 guesses)": "Same, counting the alert as right when the true pattern is either of the model's two "
                                   "guesses.",
    }
    show, css = show.drop(columns="Status"), css.drop(columns="Status")
    st.dataframe(show.style.apply(lambda _: css, axis=None), hide_index=True, width="stretch",
                 column_config={c: st.column_config.Column(help=h) for c, h in col_help.items() if c in show.columns})

    by = lambda s: tbl.loc[tbl["Status"] == s, "Pattern"].tolist()
    red, yellow, green = by("Red"), by("Yellow"), by("Green")
    not_green = tbl[tbl["Status"] != "Green"]
    n_late = not_green["Why"].str.contains("caught late").sum()
    named = tbl[tbl["Pattern"] != "Unclassified"]
    n_rec = sum(rate(v, th["recall"]) != "Green" for v in named["Laundering caught"] if pd.notna(v))
    n_spd = sum(rate(v, th["speed"]) != "Green" for v in named["Caught before it ends"] if pd.notna(v))
    n_prc = sum(rate(v, th["precision"]) != "Green" for v in named["Alerts that are real"] if pd.notna(v))
    takeaway(f"analysts are needed to catch more and to catch sooner, not to filter noise. Alerts of every named "
             f"pattern are 93% to 99% real ({n_prc} of {len(named)} below green), so no pattern needs people to sort "
             f"true alerts from false ones. The shortfalls are in how much is caught ({n_rec} of {len(named)} named "
             f"patterns under the 88% bar) and in timing ({n_spd} of {len(named)} finish before the daily alert), "
             f"plus unclassified laundering the model cannot see. Red: {', '.join(red) or 'none'}.",
             ["**Which half of BQ1 this answers.** Both, per pattern: is it caught (laundering caught), are its alerts "
              "worth reviewing (alerts that are real) and is it caught in time (caught before it ends)?",
              f"**What the colours tell us.** 'Alerts that are real' is green for the named patterns. The yellow sits "
              f"in 'Laundering caught' ({n_rec} patterns, mostly a few points under 88%) and in 'Caught before it "
              f"ends' ({n_spd} patterns, the fast ones). The weakness is coverage and timing, not trust in the alerts.",
              "**What follows.** Analysts add the most value watching fast patterns live and investigating unclassified "
              "activity, not double-checking alerts the model is already sure about. Green patterns can be handled with "
              "light sampling.",
              "**The decision it supports.** How to split analyst time across patterns. The cut-offs were fixed on the "
              "validation days before testing, so a colour can't be tuned to look good."], read=(
                 "Each number gets a colour, with cut-offs fixed on the validation days (HI-Small and HI-Medium) before "
                 "testing. Laundering caught: green from 88%, red below 78%. Alerts that are real: green from 90%, red "
                 "below 80%. Caught before it ends: green from 50%, red below 40%. The pattern name takes the colour of "
                 "its worst number, and 'Why' names that number. Stack and bipartite are named together as layering, so "
                 "they share a precision and naming score. Hover a column header's (?) for how it is calculated."))

with tab4:
    # ------------------------------------------------------------------ 4. what the analyst sees
    st.subheader("Two guesses put the right pattern in front of the analyst far more often than one")
    st.markdown(":material/touch_app: **How to use:** the three numbers say how often the pattern name is right. The table below shows real "
            "alerts as an analyst would get them, next to the true pattern from the answer key, so you can see the "
            "guesses that go wrong.")

    tg_file = Path(__file__).parent / "data" / "two_guesses_validation.json"
    tg = json.loads(tg_file.read_text()) if tg_file.exists() else None
    one_large = s2["recall"].get("MACRO (7 patterns)") if not s2.empty else None
    g1, g2, g3 = st.columns(3)
    if tg:
        g1.metric("Right with one guess", pct(tg["first_macro"]),
                  help="Validation days (HI-Small + HI-Medium), final namer: of truly laundering alerts with a known "
                       "pattern, the share whose pattern is the first guess, averaged over the 7 patterns."
                       + (f" On the HI-Large test the first guess is right {pct(one_large)} of the time." if one_large else ""))
        g2.metric("Right within two guesses", pct(tg["top2_macro"]),
                  f"+{(tg['top2_macro'] - tg['first_macro']) * 100:.0f} points with a second guess", delta_arrow="off",
                  help="Same alerts: the true pattern is the first or the second guess. HI-Large's two-guess score "
                       "comes with the next full run, which saves second guesses.")
    g3.metric("Patterns to check", "2 of 8", help="7 pattern names plus 'unclassified'.")

    if tg:
        tp = pd.DataFrame(tg["patterns"]).assign(name=lambda t: t["pattern"].map(nice))
        long2 = tp.melt(id_vars=["name", "cases"], value_vars=["first", "top2"], var_name="guess", value_name="right")
        long2["guess"] = long2["guess"].map({"first": "First guess", "top2": "Within two guesses"})
        order2 = tp.sort_values("top2", ascending=False)["name"].tolist()
        y2 = alt.Y("name:N", sort=order2, title=None, axis=alt.Axis(labelLimit=260))
        link = alt.Chart(tp).mark_rule(color=GREY, strokeWidth=2).encode(y=y2, x="first:Q", x2="top2:Q")
        dots = alt.Chart(long2).mark_point(filled=True, size=110, opacity=1).encode(
            y=y2, x=alt.X("right:Q", title="Alerts where the right pattern is shown", axis=alt.Axis(format="%"),
                          scale=alt.Scale(domain=[0, 1])),
            color=alt.Color("guess:N", scale=alt.Scale(domain=["First guess", "Within two guesses"], range=[GREY, BLUE]),
                            legend=alt.Legend(title=None, orient="top")),
            tooltip=[alt.Tooltip("name:N", title="Pattern"), "guess:N", alt.Tooltip("right:Q", format=".0%"),
                     alt.Tooltip("cases:Q", format=",", title="Alerts checked")])
        chart((link + dots).properties(height=alt.Step(34)), width="stretch")
        tp["gain"] = tp["top2"] - tp["first"]
        big, worst = tp.sort_values("gain").iloc[-1], tp.sort_values("top2").iloc[0]
        weak = tp[tp["first"] < 0.5].sort_values("first")["name"].str.lower().tolist()
        takeaway(f"a single pattern name is only a reliable starting point for long, structured schemes; for "
                 f"{', '.join(weak) or 'some patterns'} the first guess is right less than half the time, so analysts "
                 f"should treat it as a hint. A second guess turns the name into a short list that holds the right "
                 f"answer {pct(tg['top2_macro'])} of the time, worst case {worst['name'].lower()} at {pct(worst['top2'])}.",
                 ["**Which half of BQ1 this answers.** The 'manageable for analysts' half. Stage 1 sets how many alerts "
                  "arrive; the pattern name sets how much work each one is, because it tells the analyst where to start "
                  "(a cycle: follow the money coming back; fan in: check the many senders).",
                  "**What the shape tells us.** Patterns with a clear shape over several days (layering, scatter "
                  "gather) are named right first time; short or shapeless ones (cycle, random) are not, but their right "
                  "name is usually the second guess. The gain from the second guess is largest exactly where the first "
                  "guess is weakest.",
                  "**What follows.** Showing two names costs one extra word per alert and removes most wrong starting "
                  "points. It is the problem behind our proposal's MAS case: 8 of 9 fined banks had flagged transactions "
                  "that nobody properly investigated.",
                  "**Limits.** These are validation days; the HI-Large figure comes with the next full run. A wrong name "
                  "never hides an alert: the risk score alone decides whether an account is alerted."], read=(
                 "Each row is a pattern. Grey dot: how often the model's first guess is the right pattern; blue dot: "
                 "how often the right pattern is one of its two guesses. Validation days (HI-Small and HI-Medium), "
                 "final namer, truly laundering alerts of a known pattern."))
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
    shown = ex if not ex.empty else pd.DataFrame(d.get("example_groups") or []).head(8)
    if not shown.empty:
        hit = (shown["predicted_pattern"] == shown["true_pattern"]).sum()
        hit2 = ((shown["predicted_pattern"] == shown["true_pattern"])
                | (shown["second_pattern"] == shown["true_pattern"])).sum() if "second_pattern" in shown else None
        takeaway(f"wrong names cluster where cases are most complex. In these {len(shown)} alerts, among the "
                 f"biggest linked groups, the first guess matches the answer key only {hit} times; across all HI-Large "
                 "alerts it is right 56% of the time when two accounts are linked and 35% when more are. Big linked groups "
                 "deserve a senior analyst, because the label is least trustworthy exactly where the case is hardest.",
                 ["**Which half of BQ1 this answers.** 'Manageable': what an analyst actually works with, one row per "
                  "alert with the model's guesses and how many accounts are linked to it.",
                  "**What it tells us.** In big groups several schemes overlap, so one name cannot describe the whole "
                  "group; the model falls back to the most common pattern (layering).",
                  "**What follows.** Route alerts by group size: small groups to the general queue with the model's "
                  "name, big groups to experienced analysts who map the network themselves.",
                  "The answer key column is for this report only; analysts would not have it."], read=(
                 "One row per alert as an analyst would get it: the day, the risk score, the model's guesses with their "
                 "confidence and how many accounts are linked. The last column is the answer key, for this report only."))



with tab4:
    # ---------------------------------------------------------- 2c. the window period
    st.divider()
    st.markdown("#### :material/hourglass_bottom: The window: how long must a scheme run to be caught in time?")
    lab = ["Same minute", "Under 6 h", "6 to 24 h", "1 to 3 days", "Over 3 days"]
    cut = pd.cut(sc["duration_h"].astype(float), [-0.01, 0, 6, 24, 72, 1e9], labels=lab)
    fe, fl = sc["hours_to_alert"].astype(float), sc["hours_to_first_flag"].astype(float)
    run_eod = fe.notna() & sc["caught_before_end_eod"].fillna(False).astype(bool)
    run_live = fl.notna() & (fl < sc["duration_h"].astype(float))
    win = pd.concat([
        pd.DataFrame({"length": cut, "clock": "End-of-day alert (today)", "ok": run_eod}),
        pd.DataFrame({"length": cut, "clock": "First flagged payment (live)", "ok": run_live})])
    win = win.groupby(["length", "clock"], observed=True).agg(share=("ok", "mean"), schemes=("ok", "size")).reset_index()
    xw = alt.X("length:N", sort=lab, title="How long the scheme runs (first to last payment)", axis=alt.Axis(labelAngle=0))
    lines_c = alt.Chart(win).encode(
        x=xw, y=alt.Y("share:Q", title="Caught while still running", axis=alt.Axis(format="%"),
                      scale=alt.Scale(domain=[0, 1])),
        color=alt.Color("clock:N", scale=alt.Scale(domain=["End-of-day alert (today)", "First flagged payment (live)"],
                                                   range=[ORANGE, BLUE]), legend=alt.Legend(title=None, orient="top")),
        tooltip=["length:N", "clock:N", alt.Tooltip("share:Q", format=".0%"), alt.Tooltip("schemes:Q", format=",")])
    chart((lines_c.mark_line(strokeWidth=2.5) + lines_c.mark_point(filled=True, size=80))
                    .properties(height=300), width="stretch")
    eodw = win[win["clock"].str.startswith("End")].set_index("length")["share"]
    first_ok = next((b for b in lab if eodw.get(b, 0) >= 0.8), None)
    livew = win[win["clock"].str.endswith("(live)")].set_index("length")["share"]
    takeaway(f"the daily alert creates a hard window of about one day: a scheme shorter than that is effectively "
             f"invisible in time ({pct(eodw.get('6 to 24 h'))} of 6 to 24 hour schemes are caught while running), "
             f"however good the model is. The same model scored live would catch {pct(livew.get('Under 6 h'))} of "
             "schemes under 6 hours, so live scoring is the single change with the biggest gain in speed.",
             ["**Which half of BQ1 this answers.** It turns 'how early is early?' into one number: the minimum time "
              "a scheme has to keep moving money for today's daily alert to reach analysts before it ends.",
              "**What the shape tells us.** The orange line (end of day) is flat at 0% for short schemes and jumps "
              "above 80% once schemes run longer than a day. That step is the daily batch, not the model: the blue "
              "line, the same model scored as payments arrive, is high for every length except same-minute schemes.",
              "**What follows.** The gap between the two lines is what live scoring would add, and it sits entirely "
              "in schemes shorter than a day.",
              "**The decision it supports.** It is the cost case for live scoring: an architecture change (scoring "
              "each payment as it lands) rather than more model tuning. Same-minute schemes stay out of reach for "
              "any design."], read=(
                 "Schemes are grouped by how long they run (bottom axis). Each line is the share caught while still "
                 "running: orange with today's end-of-day alert, blue if payments were scored as they arrive."))
