"""
bq2_ibm_tab.py: the HI-Large tab of app_bq2.py, as a story (answer first, then the questions a manager asks:
is anything changing? would we notice a new tactic? did it slip past the model? are alerts manageable? do we retrain?).
J = the dict that aws/jobs/bq2/tasha/bq2_ibm_report.py writes to data/bq2_ibm_data.json, or the same dict read live
from S3 by aws_data.load_ibm().
"""
import altair as alt
import pandas as pd
import streamlit as st

ORANGE, BLUE, GREEN, GREY = "#E07B39", "#2F7FAD", "#1B9E77", "#9A948B"
INK = "#cfc9bf"                      # text and rules drawn on the charts (the app theme is dark)
R1_DROP = 0.10                       # rule R1: recall more than this below validation recall
NAMES = {"pattern": "laundering pattern mix", "format": "payment type (cheque, wire, ...)", "amount": "amount size",
         "hour": "time of day", "cross_bank": "sent to another bank", "cross_ccy": "currency switch"}


def render(J):
    if not J or not J.get("part1", {}).get("real", {}).get("weekly"):
        st.info("No HI-Large results yet: they appear after the weekly pipeline (aml_bq2_weekly_ibm) has run, or "
                "after aws/jobs/bq2/tasha/bq2_ibm_report.py has written data/bq2_ibm_data.json.")
        return


    def frames(scen):
        p1, p2 = J["part1"][scen], J["part2"][scen]
        return (pd.DataFrame(p1["weekly"]), pd.DataFrame(p1["shares"]), pd.DataFrame(p2["weekly"]),
                pd.DataFrame(p2.get("typology", [])))


    RW, RS, R2, _ = frames("real")
    NW, NS, N2, NT = frames("new_tactic")
    START = pd.to_datetime(RW["week_start"]).min()
    wno = lambda s: int((pd.Timestamp(s) - START).days // 7 + 1)
    for df in (RW, RS, R2, NW, NS, N2):
        if len(df):
            df["week"] = [wno(w) for w in df["week_start"]]
    # Part 2 (detection) only exists once some cases are confirmed: 4 weeks after a week ends
    HAS2 = len(R2) > 0 and len(N2) > 0 and bool((J.get("meta") or {}).get("real", {}).get("v0"))
    WEEKS = sorted(set(RW["week"]))
    LABEL = {w: f"W{w} · {START + pd.Timedelta(weeks=w - 1):%-d %b}" for w in range(1, 40)}
    LABEL_EXPR = " : ".join(f"datum.value == {w} ? '{LABEL[w]}'" for w in range(1, 40)) + " : ''"

    lw = RW[RW["population"] == "laundering"]
    B0, B1 = int(lw[lw["in_baseline"]]["week"].min()), int(lw[lw["in_baseline"]]["week"].max())
    THR = float(lw["js_threshold"].iloc[0])
    real_after = lw[lw["week"] > B1]
    real_max = real_after.groupby("dimension")["js"].max()
    real_change = bool(real_after["tactic_change"].any())

    nl = NW[NW["population"] == "laundering"]
    em = N2[(N2["group"] == "emerging") & (N2["cases"] > 0)] if HAS2 else N2
    INTRO = int(em["week"].min()) if len(em) else wno("2022-10-03")
    tc = nl[nl["tactic_change"] & (nl["week"] > B1)]
    FIRST_TC = int(tc["week"].min()) if len(tc) else None
    TC_DIMS = sorted(set(tc[tc["week"] == FIRST_TC]["dimension"])) if FIRST_TC else []
    def js_at(w):
        x = nl[(nl["dimension"] == "pattern") & (nl["week"] == w)]["js"]
        return float(x.iloc[0]) if len(x) else float("nan")

    NT_JS, NT_JS_BEFORE = js_at(INTRO), js_at(INTRO - 1)


    def caught(df, v, g, frm=None):
        a = df[(df["version"] == v) & (df["group"] == g)]
        a = a[a["week"] >= frm] if frm else a
        return a["caught"].sum() / a["cases"].sum() if a["cases"].sum() else float("nan")


    DEC_REAL, DEC_NT = J["decision"]["real"], J["decision"]["new_tactic"]
    if HAS2:
        blind_new, known_new = caught(N2, "v0", "emerging", INTRO), caught(N2, "v1", "emerging", INTRO)
        blind_other = caught(N2, "v0", "established", INTRO)
        VAL_REAL = J["meta"]["real"]["v0"]["validation"]
        VAL_BLIND = J["meta"]["new_tactic"]["v0"]["validation"]
        ra = R2[R2["group"] == "all"].sort_values("week")
        rec_lo, rec_hi = ra["recall"].min(), ra["recall"].max()
        me = ra[ra["week"].isin(set(lw[lw["month_end_week"]]["week"]))] if "month_end_week" in lw else ra.iloc[0:0]
        normal = ra[~ra["week"].isin(set(me["week"]))]


    def cards(items):
        st.markdown('<div class="cards">' + "".join(
            f'<div class="card"><div class="l">{l}</div><div class="v">{v}</div><div class="s">{s}</div></div>'
            for l, v, s in items) + "</div>", unsafe_allow_html=True)


    def week_x(lo=1, hi=None):
        hi = hi or max(WEEKS)
        return alt.X("week:Q", title=None, scale=alt.Scale(domain=[lo, hi], nice=False),
                     axis=alt.Axis(values=[w for w in WEEKS if lo <= w <= hi], labelExpr=LABEL_EXPR, labelAngle=-40))


    def shade(lo=1, warm=True):
        b = ([(1, B0 - 0.5, "warm-up · ignore")] if warm and lo < B0 else []) + [(B0 - 0.5, B1 + 0.5, "baseline (normal)")]
        b = pd.DataFrame(b, columns=["w0", "w1", "label"])
        return [alt.Chart(b).mark_rect(color="#EFE6D8", opacity=0.14).encode(x="w0:Q", x2="w1:Q"),
                alt.Chart(b).mark_text(color=INK, fontSize=11, dy=12, baseline="top").encode(
                    x="mid:Q", y=alt.value(0), text="label:N").transform_calculate(mid="(datum.w0 + datum.w1) / 2")]


    def vline(week, text, color=ORANGE):
        d = pd.DataFrame({"week": [week], "t": [text]})
        return [alt.Chart(d).mark_rule(color=color, strokeDash=[4, 3], strokeWidth=2).encode(x="week:Q"),
                alt.Chart(d).mark_text(color=color, align="left", dx=5, dy=30, fontWeight="bold").encode(
                    x="week:Q", y=alt.value(0), text="t:N")]


    def hline(y, text, color=INK, below=False):
        d = pd.DataFrame({"y": [y], "t": [text]})
        return [alt.Chart(d).mark_rule(color=color, strokeDash=[6, 4]).encode(y="y:Q"),
                alt.Chart(d).mark_text(color=color, align="left", dx=4, dy=9 if below else -7, fontSize=11).encode(
                    x=alt.value(0), y="y:Q", text="t:N")]


    # ================================================================== the answer first
    st.caption("IBM HI-Large (simulated bank, 1 Aug – 30 Oct 2022) · our own data has no new tactic, so we also stage one: fan-in / fan-out appear in Week 10")
    tc_txt = (f"<b>Week {FIRST_TC}</b>, one week after it appeared (Week {INTRO})" if FIRST_TC else "not yet")
    if not HAS2:
        st.markdown(f"""<div class="answer">
<p><b>Are criminals changing tactics?</b> Not in the real data so far. In the staged test (fan-in / fan-out appear in
Week {INTRO}) the monitor has flagged it: {tc_txt}.</p>
<p><b>Is detection falling behind?</b> Not known yet: the pipeline grades a week once its cases are confirmed, 4 weeks
after it ends.</p></div>""", unsafe_allow_html=True)
    else:
      st.markdown(f"""<div class="answer">
<p><b>Are criminals changing tactics?</b> Not in the real data: every measure stayed stable for {len(real_after['week'].unique())} weeks.
So we staged one: fan-in and fan-out laundering “appear” in Week {INTRO}. Our monitor flagged it in {tc_txt}.</p>
<p><b>Is detection falling behind?</b> No. Without ever being trained on the new tactic, the model caught
<b>{blind_new:.0%}</b> of it; a retrained model would catch {known_new:.0%}. So no retraining alarm was raised, and that was the right call.</p>
</div>""", unsafe_allow_html=True)
    st.caption("How we check, every week: ① compare this week's confirmed laundering with a normal period (no model needed) · "
               "② check how much of it the model caught (cases are confirmed 4 weeks late) · ③ retrain only if catching "
               "falls clearly behind for 2 weeks in a row.")

    # ================================================================== 1. normal times
    st.markdown('<div class="step">Step 1 · real data</div>', unsafe_allow_html=True)
    st.subheader("In normal times, is anything changing?")
    if not HAS2:
        cards([("Biggest change after the baseline", f"{real_max.max():.2f}" if len(real_max) else "—",
                f"on a 0–1 change score; a change needs {THR:.2f}+"),
               ("Laundering caught each week", "—", "appears once cases are confirmed (4 weeks after a week ends)"),
               ("Alerts that were real laundering", "—", "appears once cases are confirmed")])
    else:
      cards([("Biggest change after the baseline", f"{real_max.max():.2f}",
            f"on a 0–1 change score ({NAMES[real_max.idxmax()]}); a change needs {THR:.2f}+"),
           ("Laundering caught each week", f"{rec_lo:.0%}–{rec_hi:.0%}",
            f"expected {VAL_REAL['recall']:.0%} (validation); alarm only below {VAL_REAL['recall'] - R1_DROP:.0%}"),
           ("Alerts that were real laundering", f"{normal['caught'].sum() / normal['alerts'].sum():.0%}",
            f"in normal weeks ({me['caught'].sum() / max(me['alerts'].sum(), 1):.0%} in month-end weeks, see Step 4)")])
    d = lw.copy()
    d["measure"] = d["dimension"].map(NAMES)
    ch = alt.Chart(d).mark_line(strokeWidth=2).encode(
        x=week_x(), y=alt.Y("js:Q", title="Change score vs normal", scale=alt.Scale(domain=[0, 0.35])),
        color=alt.Color("measure:N", title=None, legend=alt.Legend(orient="top", columns=3)),
        tooltip=[alt.Tooltip("measure:N"), alt.Tooltip("week_start:N", title="week of"), alt.Tooltip("js:Q", format=".3f")])
    st.altair_chart(alt.layer(*shade(), ch, *hline(THR, f"change threshold {THR:.2f}")).properties(height=300), width="stretch")
    st.caption(f"Each line compares one feature of laundering with the baseline (Weeks {B0}–{B1}). Weeks 1–{B0 - 1} are the "
               "simulator starting up its schemes, so we ignore them. After the baseline every line stays near 0: the real "
               "data has no new tactic to find, so we staged one (Step 2).")

    # ================================================================== 2. would we notice?
    st.markdown('<div class="step">Step 2 · staged new tactic</div>', unsafe_allow_html=True)
    st.subheader("If criminals tried something new, would we notice?")
    st.caption(f"Test: fan-in and fan-out laundering are removed from every week before Week {INTRO} "
               f"({LABEL[INTRO].split(' · ')[1]}), so to the monitor they look brand new from that week.")
    cards([("New tactic appears", f"Week {INTRO}", "fan-in + fan-out start showing up"),
           ("Monitor confirms the change", f"Week {FIRST_TC}" if FIRST_TC else "—",
            "needs 2 weeks in a row, so 1 week later is the fastest possible"),
           ("What changed", "Pattern mix only" if TC_DIMS == ["pattern"] else ", ".join(NAMES[x] for x in TC_DIMS),
            f"change score {NT_JS_BEFORE:.2f} → {NT_JS:.2f}; amounts, times, banks unchanged")])
    mix = NS[(NS["population"] == "laundering") & (NS["dimension"] == "pattern")].copy()
    mix["Week"] = mix["week"].map(LABEL)
    mix["pattern"] = mix["category"].str.replace("_", "-")
    order = [LABEL[w] for w in WEEKS]
    pats = sorted(mix["pattern"].unique(), key=lambda p: (p == "unclassified", p not in ("fan-in", "fan-out"), p))
    pal = {"fan-in": "#D7301F", "fan-out": "#F4A6A0", "unclassified": "#8a857c"}
    others = ["#2F7FAD", "#8CB9D9", "#1B9E77", "#9ED9B5", "#E8A33D", "#F3D27B", "#7E6BB6"]
    rng = [pal.get(p) or others.pop(0) for p in pats]
    mix["rank"] = mix["pattern"].map({p: i for i, p in enumerate(pats)})
    bars = alt.Chart(mix).mark_bar().encode(
        x=alt.X("Week:N", sort=order, title=None, axis=alt.Axis(labelAngle=-40)),
        y=alt.Y("share:Q", stack="normalize", axis=alt.Axis(format="%"), title="Share of laundering payments"),
        color=alt.Color("pattern:N", scale=alt.Scale(domain=pats, range=rng), sort=pats, title="pattern"),
        order=alt.Order("rank:Q"),
        tooltip=["Week", "pattern", alt.Tooltip("share:Q", format=".1%"), alt.Tooltip("n:Q", title="payments", format=",")])
    r = pd.DataFrame({"Week": [LABEL[INTRO]], "t": ["new tactic"]})
    st.markdown("**What laundering looks like each week** (confirmed cases, by pattern)")
    st.altair_chart(alt.layer(bars, alt.Chart(r).mark_rule(color=INK, strokeDash=[4, 3], strokeWidth=2).encode(
        x=alt.X("Week:N", sort=order)), alt.Chart(r).mark_text(color=INK, align="left", dx=4, fontWeight="bold").encode(
        x=alt.X("Week:N", sort=order), y=alt.value(-6), text="t:N")).properties(height=340), width="stretch")
    st.caption(f"Red and pink = fan-in and fan-out. They are missing until Week {INTRO}, then take about "
               f"{mix[(mix['week'] >= INTRO) & mix['pattern'].isin(['fan-in', 'fan-out'])].groupby('week')['share'].sum().mean():.0%} "
               "of laundering. Grey = laundering with no named pattern.")

    g = NW[(NW["dimension"] == "pattern") & (NW["population"] == "laundering")].copy()
    conf = g[g["tactic_change"] & (g["week"] > B1)]
    ch = alt.Chart(g).mark_line(strokeWidth=2.5, color=ORANGE).encode(
        x=week_x(), y=alt.Y("js:Q", title="Change score vs normal", scale=alt.Scale(domain=[0, 0.35])),
        tooltip=[alt.Tooltip("week_start:N", title="week of"), alt.Tooltip("js:Q", format=".3f")])
    pts = alt.Chart(conf).mark_point(filled=True, size=70, color=ORANGE, opacity=1).encode(x="week:Q", y="js:Q")
    st.markdown("**How different is each week's pattern mix from normal?**")
    st.altair_chart(alt.layer(*shade(), ch, pts, *hline(THR, f"change threshold {THR:.2f}"),
                              *vline(INTRO, "new tactic appears")).properties(height=300), width="stretch")
    st.caption(f"Dots = confirmed tactic change. A week is confirmed only when all four hold: above {THR:.2f}, a statistical "
               "test says it is not chance, it lasts 2 weeks, and honest payments did not shift too (so it is not month-end "
               "or a holiday).")

    if not HAS2:
        st.info("Detection results (Steps 3–5) appear once the pipeline has graded a week: cases are confirmed 4 weeks "
                "after a week ends.")
        return

    # ================================================================== 3. did it slip past the model?
    st.markdown('<div class="step">Step 3 · detection</div>', unsafe_allow_html=True)
    st.subheader("Did the new tactic slip past our model?")
    cards([("New tactic caught · model that never saw it", f"{blind_new:.0%}", f"from Week {INTRO} · what the bank actually had"),
           ("New tactic caught · retrained model", f"{known_new:.0%}", "what retraining would have given: no better"),
           ("Other laundering caught", f"{blind_other:.0%}", "unchanged by the new tactic")])
    n0 = N2[(N2["version"] == "v0") & N2["group"].isin(["all", "emerging"]) & (N2["cases"] > 0)].copy()
    n0["what"] = n0["group"].map({"all": "all laundering", "emerging": "new tactic (fan-in + fan-out)"})
    ch = alt.Chart(n0).mark_line(point=True, strokeWidth=2.5).encode(
        x=week_x(B0, int(n0["week"].max())), y=alt.Y("recall:Q", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1]), title="% caught"),
        color=alt.Color("what:N", scale=alt.Scale(domain=["new tactic (fan-in + fan-out)", "all laundering"],
                                                  range=[ORANGE, BLUE]), title=None, legend=alt.Legend(orient="top")),
        tooltip=["what", alt.Tooltip("week_start:N", title="week of"), alt.Tooltip("recall:Q", format=".1%"),
                 alt.Tooltip("caught:Q", format=","), alt.Tooltip("cases:Q", title="laundering payments", format=",")])
    alarm = VAL_BLIND["recall"] - R1_DROP
    st.markdown("**% of laundering caught each week** by the model that never saw fan-in / fan-out")
    st.altair_chart(alt.layer(ch, *hline(alarm, f"overall alarm line {alarm:.0%} (expected {VAL_BLIND['recall']:.0%} minus 10 points)", GREY, below=True),
                              alt.Chart(pd.DataFrame({"x": [INTRO], "x2": [int(n0["week"].max())], "y": [0.5]})).mark_rule(
                                  color="#C0504D", strokeDash=[6, 4]).encode(x="x:Q", x2="x2:Q", y="y:Q"),
                              alt.Chart(pd.DataFrame({"x": [INTRO], "y": [0.5], "t": ["new-tactic alarm line 50%"]})).mark_text(
                                  color="#C0504D", align="left", dx=4, dy=-7, fontSize=11).encode(x="x:Q", y="y:Q", text="t:N"),
                              *vline(INTRO, "new tactic appears")).properties(height=300), width="stretch")
    st.caption("Blue must stay above the grey line and orange above the red line; dropping below either for 2 weeks in a row "
               "would raise the retraining alarm. Neither comes close. Why: fan-in / fan-out means many payments into or out "
               "of one account, and our features already count that, so the model flags it without having been taught it.")

    # ================================================================== 4. alerts manageable?
    st.markdown('<div class="step">Step 4 · analyst workload</div>', unsafe_allow_html=True)
    st.subheader("Are the alerts still manageable for analysts?")
    pr = ra.copy()
    pr["kind"] = ["month-end week" if w in set(me["week"]) else "normal week" for w in pr["week"]]
    pr["false"] = pr["alerts"] - pr["caught"]
    long = pd.concat([pr.assign(part="real laundering", n=pr["caught"]), pr.assign(part="false alarm", n=pr["false"])])
    c1, c2 = st.columns([3, 2], gap="large")
    with c1:
        st.altair_chart(alt.Chart(long).mark_bar().encode(
            x=alt.X("week:O", title=None, axis=alt.Axis(labelExpr=LABEL_EXPR, labelAngle=-40)),
            y=alt.Y("n:Q", title="Alerts per week", stack="zero"),
            color=alt.Color("part:N", scale=alt.Scale(domain=["real laundering", "false alarm"], range=[BLUE, "#E8B4A0"]),
                            title=None, legend=alt.Legend(orient="top")),
            tooltip=[alt.Tooltip("week_start:N", title="week of"), "part", alt.Tooltip("n:Q", format=","), "kind"]
        ).properties(height=280), width="stretch")
    with c2:
        cards_html = [("Normal weeks", f"{normal['caught'].sum() / normal['alerts'].sum():.0%} real",
                       f"about {normal['alerts'].mean():,.0f} alerts a week")]
        if len(me):
            cards_html.append(("Month-end weeks", f"{me['caught'].sum() / me['alerts'].sum():.0%} real",
                               f"about {me['alerts'].mean():,.0f} alerts a week"))
        st.markdown('<div class="cards" style="grid-template-columns:1fr">' + "".join(
            f'<div class="card"><div class="l">{l}</div><div class="v">{v}</div><div class="s">{s}</div></div>'
            for l, v, s in cards_html) + "</div>", unsafe_allow_html=True)
    st.caption("Month-end brings a surge of honest payments, and more of them look suspicious, so false alarms rise even "
               "though tactics did not change. The retraining rules only look at how much laundering is caught, so this "
               "is tracked separately: it is a workload issue (staff up at month-end), not a reason to retrain.")

    # ================================================================== 5. decision
    st.markdown('<div class="step">Step 5 · decision</div>', unsafe_allow_html=True)
    st.subheader("So, do we retrain?")
    if DEC_NT.get("alert") or DEC_REAL.get("alert"):
        st.warning("A retraining alarm was raised: see the rule checks below.")
    else:
        st.success("**No.** In both the real data and the staged test, no alarm rule held for 2 weeks in a row. The model keeps "
                   "catching the new tactic, and retraining would not have improved it.")
    st.markdown("**If the alarm ever turns red, the analyst would:** ① check which rule fired and on which pattern · "
                "② confirm it is not a data or calendar issue · ③ retrain on the latest confirmed cases (they arrive 4 weeks "
                "late) · ④ run the new model next to the old one for a week, then switch.")

    with st.expander("How the alarm works (rules R1–R3)"):
        st.markdown(f"""
- **Expected catch rate** = how much laundering the model caught on held-out practice days before going live
  ({VAL_REAL['recall']:.0%} for the BQ1 model, {VAL_BLIND['recall']:.0%} for the model that never saw fan-in / fan-out).
- **R1 · overall drop:** this week's catch rate is more than 10 points below expected.
- **R2 · new pattern missed:** a pattern the model never trained on is caught less than 50% of the time (at least 20 cases).
- **R3 · known pattern drop:** a pattern it did train on is caught 20+ points below its expected rate.
- A rule must hold **2 weeks in a row** before the alarm goes off, so one odd week does not trigger a retrain.
""")
        rw = pd.DataFrame(J["part2"]["new_tactic"]["rules"])
        if len(rw):
            rw["Week"] = rw["week_start"].map(lambda s: LABEL[wno(s)])
            show = rw[["Week", "cases", "recall", "r1_recall_drop", "r2_new_typology", "r3_typology_drop", "alert"]].rename(
                columns={"cases": "laundering payments", "recall": "% caught", "r1_recall_drop": "R1",
                         "r2_new_typology": "R2", "r3_typology_drop": "R3"})
            show["% caught"] = show["% caught"].map(lambda v: f"{v:.0%}")
            st.dataframe(show, hide_index=True, width="stretch")

    with st.expander("Limits of this analysis and settings we chose"):
        st.markdown(f"""
- **The staged tactic was an easy one.** Fan-in / fan-out leave an obvious trace that our features already measure. A
  tactic our features cannot see (e.g. spread thinly across many accounts) would be the real stress test.
- **About half of all laundering is missed.** The model is strict on purpose (BQ1: keep false alarms manageable); most misses
  are laundering with no named pattern.
- **Settings are team choices, not industry standards:** change threshold {THR:.2f}, "2 weeks in a row", the 10-point and 50%
  alarm lines. Stricter = fewer false alarms but slower detection.
- **Baseline = Weeks {B0}–{B1}**, the first settled weeks; Weeks 1–{B0 - 1} are the simulator warming up (chosen from the data).
- **Scores are per payment.** Confirmed cases are assumed to arrive 4 weeks after the payment.
- **The new tactic was staged by masking labels:** fan-in / fan-out cases before Week {INTRO} were removed from the
  answer key and from grading, not from the payments. Part 1 and Weeks {INTRO}+ are unaffected (features are built one
  week at a time); false alarms before Week {INTRO} may be slightly higher than in a world without the tactic.
""")
