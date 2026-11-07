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
st.markdown("<div style='font-size:1.6rem;line-height:1.35;font-weight:600;margin:.1rem 0 .5rem'>How can we "
            "automatically detect laundering patterns early, while keeping false alerts low enough for analysts "
            "to manage?</div>", unsafe_allow_html=True)
st.caption(f"Tested once on {days} "
           f"unseen days of HI-Large ({frozen['date_from'][:10]} to {frozen['date_to'][:10]}, "
           f"{frozen['payments'] / 1e6:.1f}M payments), trained on HI-Small and HI-Medium only.")


tab1, tab2, tab3, tab4 = st.tabs([":material/notifications: Alerts & accuracy", ":material/timer: Speed", ":material/groups: Who checks what", ":material/search: The alert"])

with tab1:
    # ------------------------------------------------------------------ 1. alerts and accuracy
    st.subheader(f"{pct(frozen['precision'])} of alerts are real, but they reach only {pct(frozen['recall'])} of laundering")
    st.caption("How many alerts a day, and what they catch")
    with st.expander("How to read this"):
        st.caption("Each day the model gives every account a risk score, and the riskiest accounts become alerts. "
        "One alert is one account on one day.")
    st.caption(f"**How to use:** drag the slider to the number of alerts your team can review a day. The four numbers and "
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
        x=xp, y=alt.Y("precision:Q", title="Alerts that are real (precision)", axis=alt.Axis(format="%"),
                      scale=alt.Scale(domain=[0, 1])),
        tooltip=[alt.Tooltip("k:O", title="Alerts/day", format=","), alt.Tooltip("precision:Q", format=".1%")])
    prule = alt.Chart(pd.DataFrame({"k": [pick]})).mark_rule(color=INK, strokeDash=[4, 4]).encode(x=xp)
    st.altair_chart((pline + prule).properties(height=260),
                    width="stretch")
    with st.expander("How to read this"):
        st.caption(f"Up to about 1,000 alerts a day almost every alert is real; past 2,000 the extra alerts are mostly false. "
        f"About {n_pos / days:,.0f} accounts launder money on an average test day, so {pick:,} alerts a day can "
        f"find at most {pick:,} of them: reviewing {pick:,} finds {per_day(n_true / days)}.")


with tab2:
    # ------------------------------------------------------------------ 2. speed
    _eod = sc["caught_before_end_eod"].fillna(False).astype(bool).mean() if not sc.empty else tot["alert_before_end_eod"]
    st.subheader(f"{pct(_eod)} of schemes are alerted while they are still running")
    st.caption("How fast schemes are caught, compared with how long they last")
    with st.expander("How to read this"):
        st.caption("A scheme is one laundering operation in the dataset's answer key (Patterns.txt): a set of payments with a "
        "first and a last payment time. Only schemes that started in the test days are counted.")
    st.caption("**How to use:** pick a pattern (or all), then choose when a scheme counts as detected: when the end-of-day "
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
            st.altair_chart(cl.properties(height=320), width="stretch")
            at24 = lambda s: pct((s <= 24).mean())
            with st.expander("How to read this"):
                st.caption(f"How to read it: pick an hour on the bottom axis. At 24 hours, {at24(dur)} of these schemes have "
                f"made their last payment (grey) and {at24(pd.Series(det_inf))} have been detected (orange). "
                f"Grey starts at {pct((dur <= 0).mean())} because that many schemes make all their payments within "
                "the same minute, so they are over before any alert exists. The two lines compare the whole group; "
                "the per-scheme answer is 'Detected while still running' on the left.")

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
        st.altair_chart((link_a + dots_a).properties(height=alt.Step(34)), width="stretch")
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
        st.info(f"**Takeaway:** with {clock_word}, " + "; ".join(parts) + ".", icon=":material/lightbulb:")
        with st.expander("How this answers BQ1, and why it matters"):
            st.markdown(
                "- BQ1 asks whether we detect laundering **early**. Early only means something next to how long a scheme "
                "lasts: 13 hours is fast for a scheme that runs for days, and too late for one that ends in an hour.\n"
                "- The gap between the two dots is the time an analyst has to act (orange left of grey) or the time the "
                "scheme had already finished (orange right of grey).\n"
                "- Use: patterns with orange right of grey need a faster process (live scoring) or manual monitoring of "
                "the accounts involved; the others can rely on the daily alert.")

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
        st.altair_chart(bars_b.properties(height=alt.Step(30)), width="stretch")
        all_run = (outcome(timed) == "Caught while running").mean() if len(timed) else None
        inst_found = instant[det_col].notna().mean() if len(instant) else None
        st.caption(f"{len(timed):,} schemes that last longer than a minute. Another {len(instant):,} schemes "
                   f"({pct(len(instant) / len(sc))}) make all their payments in the same minute: no alert can arrive while "
                   f"they run, and {pct(inst_found)} of them are detected afterwards.")
        st.info(f"**Takeaway:** {pct(all_run)} of schemes that last longer than a minute are caught while still running "
                f"with {clock_word}. Best: {oc_order[0].lower()} ({pct(share.max())}); worst: {oc_order[-1].lower()} "
                f"({pct(share.min())}).", icon=":material/lightbulb:")
        with st.expander("How this answers BQ1, and why it matters"):
            st.markdown(
                "- Each scheme's own window comes from the answer key (Patterns.txt): its first to its last payment. "
                "Blue means the alert arrived inside that window, while there was still money left to stop.\n"
                "- Orange schemes were still found, but only after the last payment: useful for investigation and "
                "reporting, too late to prevent the loss.\n"
                "- Same-minute schemes are shown apart because no system can beat them in time; counting them in would "
                "hide how the model does on schemes it could catch.\n"
                "- Use: the patterns at the bottom are where analysts should watch accounts closely or where live "
                "scoring would pay off most.")

        # ---------------------------------------------------------- 2c. the window period
        st.divider()
        st.markdown("#### :material/hourglass_bottom: The window: how long must a scheme run to be caught in time?")
        lab = ["Same minute", "Under 6 h", "6 to 24 h", "1 to 3 days", "Over 3 days"]
        cut = pd.cut(sub["duration_h"].astype(float), [-0.01, 0, 6, 24, 72, 1e9], labels=lab)
        fe, fl = sub["hours_to_alert"].astype(float), sub["hours_to_first_flag"].astype(float)
        run_eod = fe.notna() & sub["caught_before_end_eod"].fillna(False).astype(bool)
        run_live = fl.notna() & (fl < sub["duration_h"].astype(float))
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
        st.altair_chart((lines_c.mark_line(strokeWidth=2.5) + lines_c.mark_point(filled=True, size=80))
                        .properties(height=300), width="stretch")
        eodw = win[win["clock"].str.startswith("End")].set_index("length")["share"]
        first_ok = next((b for b in lab if eodw.get(b, 0) >= 0.8), None)
        st.info(f"**Takeaway:** the daily alert catches a scheme in time only if it runs for about "
                f"{'a day or more' if first_ok in ('1 to 3 days', 'Over 3 days') else 'the ' + str(first_ok).lower()}: "
                f"{pct(eodw.get('6 to 24 h'))} of 6 to 24 hour schemes, {pct(eodw.get('1 to 3 days'))} of 1 to 3 day "
                "schemes. Scoring payments as they happen would close most of that gap for schemes shorter than a day.",
                icon=":material/lightbulb:")
        with st.expander("How this answers BQ1, and why it matters"):
            st.markdown(
                "- This is the window period: the minimum time a scheme has to keep moving money for today's daily "
                "alert to reach analysts before it ends.\n"
                "- The orange line (end of day) jumps between 'under a day' and 'over a day' because alerts are only "
                "made when the day closes. The blue line shows the same model scored live: the gap between the lines is "
                "what live scoring would add.\n"
                "- Use: it turns 'how early is early?' into a number the Tuning Team can act on, and it gives the cost "
                "case for live scoring (an architecture change, not a model change).\n"
                "- Follows the Pattern filter above.")


with tab3:
    # ------------------------------------------------------------------ 3. which patterns need people
    st.subheader("Which patterns the model can handle, and which need analysts")
    with st.expander("How to read this"):
        st.caption("Each pattern gets a colour from three numbers, with cut-offs fixed on the validation data (HI-Small and "
        "HI-Medium) before testing. Laundering caught: green from 88%, red below 78%. Alerts that are real: green "
        "from 90%, red below 80%. Caught before it ends: green from 50%, red below 40%. A pattern takes the colour of "
        "its worst number.")
    st.caption("**How to use:** read the Status column first. Red patterns need analysts checking by hand, yellow ones a "
            "spot-check of a sample, green ones can be left to the model. 'Why' names the number that set the colour. "
            "Hover a column header's (?) for how that column is calculated.")
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
    css["Status"] = [f"background-color: {RAG[v]}; font-weight: 600" for v in tbl["Status"]]
    for c, k in cell_key.items():
        css[c] = [f"background-color: {RAG[rate(v, th[k])]}" if rate(v, th[k]) else "" for v in tbl[c]]
    show = tbl.copy()                                                   # ...shown as text (Streamlit prints blanks as "None")
    for c in [c for c in tbl.columns if c in cell_key or c.startswith("Named right")]:
        show[c] = tbl[c].map(pct)
    col_help = {
        "Status": "The colour of the pattern's worst number (see the cut-offs above).",
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
    st.dataframe(show.style.apply(lambda _: css, axis=None), hide_index=True, width="stretch",
                 column_config={c: st.column_config.Column(help=h) for c, h in col_help.items() if c in show.columns})

    red = tbl.loc[tbl["Status"] == "Red", "Pattern"].tolist()
    late = tbl.loc[[rate(v, th["speed"]) != "Green" and pd.notna(v) for v in tbl["Caught before it ends"]], "Pattern"].tolist()
    st.caption(f"{len(red)} of {len(tbl)} patterns are red ({', '.join(red) or 'none'}). "
               f"Caught late ({', '.join(p.lower() for p in late)}): these schemes are mostly over within hours, so a daily "
               "alert arrives after they end, and they need live scoring or analysts watching the accounts. "
               "Unclassified laundering follows no known shape and is almost never caught.")


with tab4:
    # ------------------------------------------------------------------ 4. what the analyst sees
    st.subheader("Two guesses put the right pattern in front of the analyst far more often than one")
    st.caption("Each alert comes with the pattern the model thinks it belongs to. One guess is often wrong, so the "
               "analyst sees the two most likely patterns and checks those first instead of all eight.")
    st.caption("**How to use:** the three numbers say how often the pattern name is right. The table below shows real "
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
        st.altair_chart((link + dots).properties(height=alt.Step(34)), width="stretch")
        st.caption("Validation days (HI-Small + HI-Medium), final namer. Cycle and random gain the most from a second "
                   "guess: their first guess is usually wrong, but the right name is often second.")
    with st.expander("How to read this"):
        st.caption("Stage 1 decides how many alerts arrive; the pattern name decides how much work each alert is. The "
                   "name tells the analyst where to look (a cycle: follow the money coming back; fan in: check the many "
                   "senders). A wrong name never hides an alert: whether an account is alerted comes from the risk "
                   "score alone.")

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

