"""
BQ2 page for the model owner / AML analytics team.
    streamlit run app_bq2.py
Reads data/bq2_data.json (written by bq2/bq2_ramp_report.py) and, for the HI-Large tab,
data/dashboard_data.json (BQ1's test result, written by pipeline/final_report.py).
"""
import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

import aws_data

ORANGE, BLUE, GREY, INK = "#E07B39", "#2F7FAD", "#9A948B", "#262626"
DATA = Path(__file__).parent / "data"
NICE = {"Structuring": "structuring", "Smurfing": "smurfing", "Cash_Withdrawal": "cash withdrawal",
        "Deposit-Send": "deposit-send"}

st.set_page_config(page_title="BQ2 · Tactic change & detection", layout="wide")
st.markdown("""<style>
.tag{display:inline-block;padding:1px 9px;border-radius:999px;font-size:0.82rem;font-weight:600;white-space:nowrap}
.yes{background:#FBE3D6;color:#9A3410} .warn{background:#FBF0D9;color:#7A5200} .no{background:#ECE6DC;color:#3F3E3A}
.wk{width:100%;border-collapse:collapse;font-size:0.9rem} .wk th{text-align:left;color:#5b5750;border-bottom:1.5px solid #d9d2c6;padding:6px}
.wk td{border-bottom:1px solid #eee6da;padding:6px;vertical-align:top} .muted{color:#8a857c}
.cards{display:grid;grid-template-columns:1fr 1fr 2fr;gap:16px;margin:8px 0 18px}
.card{background:#fff;border:1px solid #e6ddd0;border-radius:12px;padding:16px 20px;box-shadow:0 1px 2px rgba(0,0,0,.04)}
.card .l{font-size:0.9rem;color:#5b5750} .card .v{font-size:2.1rem;font-weight:700;color:#262626;margin:4px 0 2px;line-height:1.15}
.card .s{font-size:0.82rem;color:#8a857c} .dot{display:inline-block;width:12px;height:12px;border-radius:50%;margin-right:8px;vertical-align:middle}
</style>""", unsafe_allow_html=True)


@st.cache_data(ttl=300)
def load_aws():
    return aws_data.load_bq2()


REPLAY_START = pd.Timestamp("2023-01-30")      # week_index 0
LABEL_LAG_DAYS = 28
WEEKLY_KEYS = ("part1_weekly", "part1_weekly_nb", "part2_weekly", "part2_rules", "part2_rules_v1", "design")


def only_processed(d):
    """With a pipeline pointer present, show the world as of the last processed week: hide later weeks, and hide
    alerts/decisions that only became known after it (labels arrive 28 days after a week ends)."""
    nxt = (d.get("progress") or {}).get("week_index")
    if nxt is None:
        return d
    cut_ts = REPLAY_START + pd.Timedelta(days=7 * int(nxt))
    cut, asof_end = cut_ts.strftime("%Y-%m-%d"), (cut_ts - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    d = dict(d, asof_end=asof_end)
    # a week's cases are confirmed 28 days after it ends: that is all any chart on this page can know
    label_end = (cut_ts - pd.Timedelta(days=1 + LABEL_LAG_DAYS))
    d["label_end"] = label_end.strftime("%Y-%m-%d")
    label_cut = (label_end - pd.Timedelta(days=6)).strftime("%Y-%m-%d")
    for k in WEEKLY_KEYS:
        d[k] = [r for r in d.get(k, []) if r["week_start"] <= label_cut]
    for k in ("decision", "decision_v1", "decision_latest"):
        x = d.get(k) or {}
        if x.get("alert") and x.get("known_on", "") > asof_end:
            d[k] = {}
    return d


@st.cache_data(ttl=300)
def load():
    d = load_aws() if aws_data.enabled() else json.loads((DATA / "bq2_data.json").read_text())
    d = only_processed(d)
    bq1 = None
    if (DATA / "dashboard_data.json").exists():
        s1 = pd.DataFrame(json.loads((DATA / "dashboard_data.json").read_text())["stage1"])
        bq1 = s1[(s1["level"] == "account_day") & (s1["cutoff"] == "frozen_threshold")].iloc[0].to_dict()
    p1 = pd.DataFrame(d["part1_weekly"])
    p1["date"] = pd.to_datetime(p1["week_start"])
    for ds_, g in p1.groupby("dataset"):
        p1.loc[g.index, "week"] = ((g["date"] - g["date"].min()).dt.days // 7 + 1).astype(int)
    p1["week"] = p1["week"].astype(int)
    return d, p1, pd.DataFrame(d["part2_weekly"]), pd.DataFrame(d["part2_typology"]), pd.DataFrame(d["design"]), bq1


d, P1, W2, T2, DZ, BQ1 = load()
dec = d.get("decision", {})


GATES = {"retrain_gate": ("Retraining approval", "Approving trains a challenger model on every case confirmed so far, "
                          "scores it and evaluates it. Nothing goes live yet."),
         "deploy_gate": ("Deployment approval", "Approving makes the challenger the live model from the date below. "
                         "Rejecting keeps the current model.")}


def case_for_retraining(dc):
    """Evidence for gate 1: how bad is it, which typologies, and are there enough confirmed cases to learn them."""
    old = dc.get("monitored", "v0")
    models = d.get("models") or {}
    seen = set((models.get(old) or {}).get("train_patterns") or [])
    R_ = pd.DataFrame(d.get("part2_rules", []))
    t = T2[(T2["version"] == old) & (T2["group"] == "emerging")] if len(T2) else T2
    if len(R_):
        base = float(R_["baseline_recall"].iloc[-1])
        recent = R_.tail(2)
        hit = int(R_["any_rule"].sum())
        st.markdown(f"- **Recent recall** (last 2 weeks) **{float(recent['recall'].mean()):.0%}** vs the model's "
                    f"validation baseline **{base:.0%}**\n- **{hit} of {len(R_)} weeks** broke at least one rule")
    if len(t):
        miss = int((t["cases"] - t["caught"]).sum())
        tt = t.assign(**{"typology": t["typology"].str.replace("_", " "),
                         "in model's training?": ["yes" if x in seen else "never seen" for x in t["typology"]],
                         "recall": t["recall"].map("{:.0%}".format)}).rename(
            columns={"cases": "confirmed cases", "caught": "caught by " + old})
        st.markdown(f"**New-tactic typologies the model is missing** · {miss} of {int(t['cases'].sum())} "
                    "confirmed cases slipped through")
        st.dataframe(tt[["typology", "confirmed cases", "caught by " + old, "recall", "in model's training?"]],
                     hide_index=True)
    st.caption("Approve if the missed typologies have enough confirmed cases to learn from. Approving only trains and "
               "evaluates a challenger (about $0.2 of compute); nothing goes live until the next gate. Reject to keep "
               f"{old} and keep monitoring.")


def compare_models(old, new):
    """Evidence for gate 2: the challenger against the live model on their own held-out validation data."""
    mm = d.get("models") or {}
    a, b = mm.get(old) or {}, mm.get(new) or {}
    if not b:
        return
    va, vb = a.get("validation", {}), b.get("validation", {})
    rows = [("Recall", va.get("recall"), vb.get("recall"), "{:.1%}"),
            ("Precision", va.get("precision"), vb.get("precision"), "{:.1%}"),
            ("F1", va.get("f1"), vb.get("f1"), "{:.3f}"),
            ("Alerts per day", va.get("alerts_per_day"), vb.get("alerts_per_day"), "{:.1f}"),
            ("Trained on (confirmed laundering cases)", a.get("train_laundering"), b.get("train_laundering"), "{:,.0f}"),
            ("Typologies in training", len(a.get("train_patterns") or []), len(b.get("train_patterns") or []), "{}")]
    st.dataframe(pd.DataFrame([{"": n, f"{old} (live)": f.format(x) if x is not None else "-",
                                f"{new} (challenger)": f.format(y) if y is not None else "-"} for n, x, y, f in rows]),
                 hide_index=True)
    pa, pb = a.get("validation_by_pattern") or {}, b.get("validation_by_pattern") or {}
    new_pat = [p for p in pb if p not in (a.get("train_patterns") or [])]
    rest = sorted((p for p in pb if p in pa), key=lambda p: -abs(pb[p]["recall"] - pa[p]["recall"]))[:3]
    pats = new_pat + rest
    if pats:
        lo, ln = f"{old} (live)", f"{new} (challenger)"
        df = pd.DataFrame([(p.replace("_", " ") + (" *" if p in new_pat else ""), m, (src.get(p) or {}).get("recall", 0.0))
                           for p in pats for m, src in ((lo, pa), (ln, pb))], columns=["measure", "model", "value"])
        pal = {lo: ORANGE, ln: BLUE}
        order = [lo, ln]
        base = alt.Chart(df).encode(
            x=alt.X("measure:N", title=None, sort=list(dict.fromkeys(df["measure"])),
                    axis=alt.Axis(labelAngle=-20, labelLimit=140)),
            xOffset=alt.XOffset("model:N", sort=order),
            y=alt.Y("value:Q", title=None, axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1.05])))
        bar = base.mark_bar().encode(color=alt.Color("model:N", sort=order, legend=alt.Legend(title=None, orient="top"),
                                                     scale=alt.Scale(domain=order, range=[pal[m] for m in order])),
                                     tooltip=["model", "measure", alt.Tooltip("value:Q", format=".0%")])
        txt = base.mark_text(dy=-8, color=INK).encode(text=alt.Text("value:Q", format=".0%"))
        st.markdown("**Validation recall by typology** (held-out cases)")
        st.altair_chart((bar + txt).properties(height=260), width="stretch")
        st.caption(f"* never seen by {old}, so its recall is 0. Each model is validated on its own held-out period, "
                   "so the two columns are comparable but not identical test sets; real-world proof comes after "
                   "deployment, once new cases are confirmed.")


def approvals_panel():
    """Human gates of the weekly pipeline: approve or reject retraining / deploying the challenger."""
    nxt = (d.get("progress") or {}).get("week_index")
    depl = d.get("deployed") or {}
    c1, c2, c3 = st.columns(3)
    c1.metric("Weeks processed", "reference replay" if nxt is None else nxt)
    c2.metric("Live model", depl.get("version", "v0"), f"from {depl['deploy_from']}" if depl else "initial model",
              delta_color="off")
    if c3.button("Refresh from AWS"):
        load_aws.clear()
        load.clear()
        st.session_state.pop("pending", None)
        st.rerun()
    latest = d.get("decision_latest") or {}
    if latest.get("alert"):
        st.warning(f"Alert for the week of {latest.get('alert_week')}, known {latest.get('known_on')}: "
                   + "; ".join(latest.get("rules", [])))
    # the queue hides a message for a few seconds after it is read, so keep what we read for this session
    if "pending" not in st.session_state:
        st.session_state["pending"] = aws_data.pending_approvals()
    items = st.session_state["pending"]
    for i, it in enumerate(items):
        title, what = GATES.get(it["gate"], (it["gate"], ""))
        with st.container(border=True):
            st.markdown(f"#### {title} · week ending {it['week_end']}")
            st.write(what)
            dc = it.get("decision") or {}
            if dc:
                st.caption(f"Challenger {dc.get('retrain')} · trained on data to {dc.get('retrain_to')} · "
                           f"live from {dc.get('deploy_from')} · monitored model {dc.get('monitored')}")
            if it["gate"] == "retrain_gate":
                case_for_retraining(dc)
            if it["gate"] == "deploy_gate":
                compare_models(dc.get("monitored", "v0"), dc.get("retrain", "v1"))
            who = st.text_input("Your name (recorded as the approver)", key=f"who{i}")
            b1, b2 = st.columns(2)
            choice = "approve" if b1.button("Approve", key=f"ok{i}", type="primary", disabled=not who) else \
                "reject" if b2.button("Reject", key=f"no{i}", disabled=not who) else None
            if choice:
                err = aws_data.answer(it, choice == "approve", who)
                st.session_state["pending"] = [x for x in items if x is not it]
                load_aws.clear()
                load.clear()
                st.session_state["flash"] = err or f"{choice.title()}d. The pipeline continues; refresh in a few minutes."
                st.rerun()
    if st.session_state.get("flash"):
        st.info(st.session_state.pop("flash"))
    if not items:
        st.success("Nothing waiting for approval.")


if aws_data.enabled():
    approvals_panel()
    if (d.get("progress") or {}).get("week_index") is None:
        st.info("The weekly pipeline has not processed any week yet, so there is nothing to show. "
                "Results appear here after each weekly run.")
        st.stop()
    st.caption(f"**Pipeline has run through {d['asof_end']}.** Every chart below uses confirmed cases only, and cases "
               f"are confirmed 28 days after their week ends, so the charts end at the **last week with confirmed "
               f"cases: the week ending {d['label_end']}**. The weeks after that are not shown because their "
               "cases are not confirmed yet. An alert for a week appears once its cases are confirmed.")


def week_of(ds_, date_str):
    s = P1[P1["dataset"] == ds_]["date"].min()
    return int((pd.Timestamp(date_str) - s).days // 7 + 1)


MEASURE = {"format": "payment format", "pattern": "typology mix"}


def tag(status):
    cls = "yes" if status.startswith("confirmed") else "warn" if status.startswith("above") else "no"
    return f'<span class="tag {cls}">{status}</span>'


def week_status(ds_):
    """confirmed change / above threshold / no change / baseline / warm-up, per week (format + typology mix)."""
    w = P1[(P1["dataset"] == ds_) & (P1["population"] == "laundering") & P1["dimension"].isin(["format", "pattern"])]
    base_end = w[w["in_baseline"]]["week"].max()
    base_start = w[w["in_baseline"]]["week"].min()
    out = {}
    for wk, g in w.groupby("week"):
        if wk < base_start:
            out[wk] = "warm-up"
        elif wk <= base_end:
            out[wk] = "baseline"
        elif g["tactic_change"].any():
            out[wk] = "confirmed · " + " + ".join(MEASURE[x] for x in g[g["tactic_change"]]["dimension"])
        elif g["changed"].any():
            out[wk] = "above threshold · " + " + ".join(MEASURE[x] for x in g[g["changed"]]["dimension"])
        else:
            out[wk] = "no change"
    return out, base_start, base_end


def change_chart(lines, thr, bands=(), rules=(), confirmed=None, ymax=None):
    """lines: DataFrame week, js, series. bands: (from_week, to_week, label). rules: (week, label).
    The y range follows the data (never below the threshold) unless ymax is given."""
    ymax = ymax or max(float(lines["js"].max()), thr) * 1.15
    x = alt.X("week:Q", title="Week", axis=alt.Axis(tickMinStep=1, format="d"),
              scale=alt.Scale(domain=[lines["week"].min(), lines["week"].max()], nice=False))
    layers = []
    if bands:
        b = pd.DataFrame(bands, columns=["w0", "w1", "label"])
        b["op"] = [0.25 + 0.6 * i / max(len(b) - 1, 1) for i in range(len(b))]
        layers.append(alt.Chart(b).mark_rect(color="#EFE6D8").encode(
            x="w0:Q", x2="w1:Q", opacity=alt.Opacity("op:Q", legend=None)))
        layers.append(alt.Chart(b).mark_text(dy=-6, color="#7a7974", fontSize=11, baseline="bottom").encode(
            x=alt.X("mid:Q"), y=alt.value(14), text="label:N").transform_calculate(mid="(datum.w0 + datum.w1) / 2"))
    series = list(lines["series"].unique())
    colors = [ORANGE if s != series[-1] or len(series) == 1 else GREY for s in series]
    layers.append(alt.Chart(lines).mark_line(strokeWidth=2.5).encode(
        x=x, y=alt.Y("js:Q", title="Change vs baseline (JS distance)", scale=alt.Scale(domain=[0, ymax])),
        color=alt.Color("series:N", scale=alt.Scale(domain=series, range=colors), legend=alt.Legend(title=None, orient="top")),
        tooltip=["series", "week", alt.Tooltip("js:Q", format=".2f")]))
    if confirmed is not None and len(confirmed):
        layers.append(alt.Chart(confirmed).mark_point(filled=True, size=55, color=ORANGE, opacity=1).encode(
            x="week:Q", y="js:Q", tooltip=["week", alt.Tooltip("js:Q", format=".2f")]))
    th = pd.DataFrame({"y": [thr], "t": [f"threshold {thr:.2f}"]})
    layers.append(alt.Chart(th).mark_rule(strokeDash=[6, 4], color=INK).encode(y="y:Q"))
    layers.append(alt.Chart(th).mark_text(align="left", dx=4, dy=-7, color=INK, fontWeight="bold").encode(
        x=alt.value(0), y="y:Q", text="t:N"))
    if rules:
        r = pd.DataFrame(rules, columns=["week", "label"])
        layers.append(alt.Chart(r).mark_rule(strokeDash=[2, 3], color="#5b5750").encode(x="week:Q"))
        for i, (wk, lab) in enumerate(rules):
            layers.append(alt.Chart(pd.DataFrame({"week": [wk], "label": [lab]})).mark_text(
                align="left", dx=4, color=INK, fontWeight="bold", fontSize=11).encode(
                x="week:Q", y=alt.value(36 + 17 * i), text="label:N"))
    return alt.layer(*layers).properties(height=380)


def bars(df):
    """df: measure, model, value."""
    order = list(dict.fromkeys(df["model"]))
    pal = {"v0 · before retraining": ORANGE, "v1 · after retraining": BLUE, "BQ1 model · no retraining": BLUE,
           "v1 · before 2nd retraining": BLUE, "v2 · after 2nd retraining": "#1B9E77"}
    base = alt.Chart(df).encode(
        x=alt.X("measure:N", title=None, sort=list(dict.fromkeys(df["measure"])),
                scale=alt.Scale(paddingInner=0.55 if len(order) > 1 else 0.78, paddingOuter=0.3),
                axis=alt.Axis(labelAngle=0, labelLimit=0, labelFontSize=13)),
        xOffset=alt.XOffset("model:N", sort=order, scale=alt.Scale(paddingInner=0)),
        y=alt.Y("value:Q", title=None, axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1.05])))
    b = base.mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, width=alt.RelativeBandSize(1)).encode(
        color=alt.Color("model:N", sort=order, scale=alt.Scale(domain=order, range=[pal[m] for m in order]),
                        legend=alt.Legend(title=None, orient="top")),
        tooltip=["model", "measure", alt.Tooltip("value:Q", format=".1%")])
    t = base.mark_text(dy=-8, color=INK).encode(text=alt.Text("value:Q", format=".0%"))
    return (b + t).properties(height=300)


def cards(items):
    """items: (label, value, sub, dot colour or None)."""
    html = "".join(f'<div class="card"><div class="l">{l}</div><div class="v">'
                   + (f'<span class="dot" style="background:{c}"></span>' if c else "")
                   + f'{v}</div><div class="s">{sub}</div></div>' for l, v, sub, c in items)
    st.markdown(f'<div class="cards">{html}</div>', unsafe_allow_html=True)


def table(rows):
    body = "".join(f"<tr><td style='white-space:nowrap'>Week {w}</td><td>{tag(s)}</td><td>{t}</td></tr>" for w, s, t in rows)
    st.markdown(f'<table class="wk"><tr><th>Week</th><th>Data change (which measure)</th><th>New tactics in the data</th></tr>{body}</table>',
                unsafe_allow_html=True)


# ------------------------------------------------------------------ header
st.title("Are criminals changing their tactics, and is detection keeping up?")
st.markdown("**BQ2:** Are criminals changing their laundering tactics over time, and when they do, does our "
            "detection rate fall, and can retraining recover it?")
st.caption("For the model owner and AML analytics team · Part 1 uses confirmed laundering cases only (no model); "
           "Part 2 grades the model every week against confirmed cases, which arrive 4 weeks late.")

tab_ibm, tab_saml = st.tabs(["HI-Large · IBM AML", "SAML-D · new tactics test"])

# ================================================================== HI-Large
def ibm_tab():
    ds_ = "HI-Large"
    if P1[P1["dataset"] == ds_].empty:
        st.info("No HI-Large results in data/bq2_data.json: run bq2/bq2_ramp_report.py after run_bq2_ramp.sh.")
        return
    stat, b0, b1 = week_status(ds_)
    lw = P1[(P1["dataset"] == ds_) & (P1["population"] == "laundering")]
    after = lw[lw["week"] > b1]
    changed = after["tactic_change"].any()
    n_after = after["week"].nunique()
    cards([("Tactic change detected?", "Yes" if changed else "No", "all 6 measures stable after the baseline",
            ORANGE if changed else GREY),
           ("Week detected", "—", f"no lasting change in {n_after} weeks", None),
           ("Detection impact", "None", (f"no alert, no retraining needed · BQ1 precision {BQ1['precision']:.0%}, "
                                         f"recall {BQ1['recall']:.0%}") if BQ1 else "no alert, no retraining needed", None)])

    st.subheader("Laundering payment-format mix vs baseline")
    f = lw[lw["dimension"] == "format"].sort_values("week")
    thr = float(f["js_threshold"].iloc[0])
    st.altair_chart(change_chart(f.assign(series="laundering")[["week", "js", "series"]], thr,
                                 bands=[(1, b0, "simulator warm-up"), (b0, b1 + 1, "baseline")]), width="stretch")
    st.caption(f"Week 1 = {f['date'].min():%d %b %Y}. Weeks 1–{b0 - 1} are the simulator starting up "
               f"(schemes still being created), so the baseline is Weeks {b0}–{b1}. After it, every week "
               "stays far below the threshold, so no alert is raised and the model is not retrained.")

    if BQ1:
        st.subheader(f"Detection on HI-Large · frozen BQ1 model (test window {BQ1['date_from'][:10]} to {BQ1['date_to'][:10]})")
        m = "BQ1 model · no retraining"
        st.altair_chart(bars(pd.DataFrame({"measure": ["Precision", "Recall · all laundering", "Recall · named patterns"],
                                           "model": [m] * 3,
                                           "value": [BQ1["precision"], BQ1["recall"], BQ1["recall_patterns"]]})),
                        width="stretch")
        st.caption("No tactic change, so no alert and no retraining: the BQ1 model keeps its test performance.")


@st.cache_data
def load_ibm():
    p = DATA / "bq2_ibm_data.json"
    return json.loads(p.read_text()) if p.exists() else None


def ibm_test_section():
    """HI-Large: Joey's Part 1 + Part 2 on our data, and the fan-in / fan-out new-tactic test
    (data/bq2_ibm_data.json, written by aws/jobs/bq2/tasha/bq2_ibm_report.py after run_bq2_ibm.sh)."""
    J = load_ibm()
    if not J:
        return
    st.divider()
    st.subheader("New-tactic test · what if fan-in and fan-out were brand new?")
    st.caption("HI-Large's tactics do not change, so we built a test: fan-in and fan-out laundering are left out of "
               "every week before the day they 'appear'. The bank's model then is the BQ1 model trained without "
               "them (it has never seen them); the BQ1 model that knows them shows what retraining would give. "
               "Same Part 1 and Part 2 rules as the SAML-D tab; cases are confirmed 4 weeks late.")
    scen = st.radio("View", ["new_tactic", "real"], horizontal=True, key="ibm_scen",
                    format_func=lambda s: {"new_tactic": "New-tactic test (fan-in + fan-out appear)",
                                           "real": "Real data (nothing hidden)"}[s])
    p1, p2 = J["part1"].get(scen, {}), J["part2"].get(scen, {})
    wk1, sh1, sm1 = (pd.DataFrame(p1.get(k, [])) for k in ("weekly", "shares", "summary"))
    if wk1.empty:
        st.info("No Part 1 results for this view yet.")
        return
    start = pd.to_datetime(wk1["week_start"]).min()
    wno = lambda s: int((pd.Timestamp(s) - start).days // 7 + 1)
    wk1["week"] = [wno(w) for w in wk1["week_start"]]
    lw = wk1[wk1["population"] == "laundering"]
    b0, b1 = int(lw[lw["in_baseline"]]["week"].min()), int(lw[lw["in_baseline"]]["week"].max())
    m0, m1 = (J["meta"].get(scen, {}).get(v, {}) for v in ("v0", "v1"))
    dec = J["decision"].get(scen, {})
    excl = (m0.get("excluded_patterns") or []) if scen == "new_tactic" else []
    hid = " + ".join(x.replace("_", "-") for x in excl) or "fan-in + fan-out"
    w2 = pd.DataFrame(p2.get("weekly", []))
    if len(w2):
        w2["week"] = [wno(w) for w in w2["week_start"]]
    intro = None
    if scen == "new_tactic":
        em = w2[(w2["group"] == "emerging") & (w2["cases"] > 0)] if len(w2) else w2
        intro = int(em["week"].min()) if len(em) else None

    # ---- headline cards
    tc = lw[lw["tactic_change"] & (lw["week"] > b1)]
    first_tc = int(tc["week"].min()) if len(tc) else None
    tc_dims = ", ".join(sorted(set(tc[tc["week"] == first_tc]["dimension"]))) if first_tc else ""

    def tot(v, g, frm=None):
        a = w2[(w2["version"] == v) & (w2["group"] == g)] if len(w2) else w2
        a = a[a["week"] >= frm] if (frm and len(a)) else a
        return a["caught"].sum() / a["cases"].sum() if len(a) and a["cases"].sum() else float("nan")

    if scen == "new_tactic" and intro:
        impact = f"{tot('v0', 'emerging', intro):.0%} vs {tot('v1', 'emerging', intro):.0%}"
        impact_sub = (f"{hid} caught from Week {intro}: model that never saw them vs BQ1 model "
                      f"(validation recall {m0.get('validation', {}).get('recall', float('nan')):.0%})")
    else:
        impact = f"{tot('v0', 'all'):.0%}"
        impact_sub = (f"laundering payments caught each week by the BQ1 model (validation "
                      f"{m0.get('validation', {}).get('recall', float('nan')):.0%})") if m0 else ""
    a_wk = wno(dec["alert_week"]) if dec.get("alert") else None
    cards([("Tactic change detected? (Part 1)", f"Week {first_tc}" if first_tc else "No",
            (f"{tc_dims} · {'appeared Week ' + str(intro) if intro else ''}" if first_tc
             else "no lasting change after the baseline"), ORANGE if first_tc else GREY),
           ("Detection alert? (Part 2 rules)", f"Week {a_wk}" if a_wk else "No",
            ("; ".join(dec.get("rules", [])) + f" · known {dec.get('known_on', '')} (cases confirmed 4 weeks late)")
            if a_wk else "no rule held 2 weeks in a row: no retraining needed", ORANGE if a_wk else GREY),
           ("Detection impact", impact, impact_sub, None)])

    # ---- stacked laundering mix by pattern (Part 1 shares)
    st.markdown("**How laundering breaks down each week** (confirmed cases, by pattern)")
    mix = pd.DataFrame(sh1)
    if len(mix):
        mix = mix[(mix["population"] == "laundering") & (mix["dimension"] == "pattern")].copy()
        mix["week"] = [wno(w) for w in mix["week_start"]]
        mix["Week"] = [f"W{w} · {pd.Timestamp(s):%d %b}" for w, s in zip(mix["week"], mix["week_start"])]
        order = list(mix.sort_values("week")["Week"].unique())
        mix["pattern"] = mix["category"].str.replace("_", "-")
        ch = alt.Chart(mix).mark_bar().encode(
            x=alt.X("Week:N", sort=order, title=None, axis=alt.Axis(labelAngle=-45)),
            y=alt.Y("share:Q", stack="normalize", axis=alt.Axis(format="%"), title="Share of laundering payments"),
            color=alt.Color("pattern:N", title="pattern"),
            tooltip=["Week", "pattern", alt.Tooltip("share:Q", format=".1%"), "n"])
        layers = [ch]
        if intro:
            r = pd.DataFrame({"Week": [o for o in order if o.startswith(f"W{intro} ")][:1], "t": ["new tactic"]})
            if len(r):
                layers += [alt.Chart(r).mark_rule(color=ORANGE, strokeDash=[4, 3], strokeWidth=2).encode(
                               x=alt.X("Week:N", sort=order)),
                           alt.Chart(r).mark_text(color=ORANGE, align="left", dx=4, dy=-6, fontWeight="bold").encode(
                               x=alt.X("Week:N", sort=order), y=alt.value(8), text="t:N")]
        st.altair_chart(alt.layer(*layers).properties(height=340), width="stretch")
        st.caption(f"Weeks 1–{b0 - 1}: simulator warm-up · Weeks {b0}–{b1}: baseline. "
                   + (f"The {hid} slices appear from Week {intro}." if intro else "The mix stays the same week to week."))

    # ---- JS per measure (Part 1)
    dim = st.selectbox("Change vs baseline, by measure", ["pattern", "format", "amount", "cross_bank", "cross_ccy", "hour"],
                       key="ibm_dim", format_func=lambda x: {"pattern": "typology mix", "format": "payment format",
                                                             "amount": "amount", "cross_bank": "cross-bank",
                                                             "cross_ccy": "cross-currency", "hour": "hour of day"}[x])
    g = wk1[wk1["dimension"] == dim].sort_values("week")
    lines = pd.concat([g[g["population"] == "laundering"].assign(series="laundering"),
                       g[g["population"] == "honest"].assign(series="honest payments (control)")])
    thr = float(g[g["population"] == "laundering"]["js_threshold"].iloc[0]) if len(g) else 0.1
    conf = g[(g["population"] == "laundering") & g["tactic_change"]][["week", "js"]]
    st.altair_chart(change_chart(lines[["week", "js", "series"]], thr,
                                 bands=[(1, b0, "warm-up"), (b0, b1 + 1, "baseline")],
                                 rules=[(intro, "new tactic appears")] if intro else (), confirmed=conf,
                                 ymax=max(0.3, float(lines["js"].max()) * 1.15)), width="stretch")
    st.caption("A week counts as a tactic change when laundering moves above the threshold (bigger than any normal "
               "baseline week), the chi-square test says it is real, it lasts 2 weeks, and honest payments did not "
               "move too (so it is not the calendar, e.g. month-end). Dots = confirmed tactic change.")

    # ---- Part 2: recall per week
    if len(w2):
        st.markdown("**Is detection affected?** Share of laundering payments caught each week")
        names = ({"v0": "model that never saw " + hid, "v1": "BQ1 model (knows them)"} if scen == "new_tactic"
                 else {"v0": "BQ1 model"})
        grp = {"all": "all laundering", "emerging": hid, "established": "other patterns"}
        r = w2[w2["version"].isin(names) & w2["group"].isin(grp)].dropna(subset=["recall"]).copy()
        r["model"], r["laundering"] = r["version"].map(names), r["group"].map(grp)
        x = alt.X("week:Q", title="Week", axis=alt.Axis(tickMinStep=1, format="d"))
        ch = alt.Chart(r).mark_line(point=True).encode(
            x=x, y=alt.Y("recall:Q", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1]), title="Caught"),
            color=alt.Color("model:N", scale=alt.Scale(range=[ORANGE, BLUE]), legend=alt.Legend(orient="top", title=None)),
            strokeDash=alt.StrokeDash("laundering:N", legend=alt.Legend(orient="top", title=None)),
            tooltip=["model", "laundering", "week", alt.Tooltip("recall:Q", format=".1%"), "cases", "caught"])
        layers = [ch]
        if m0.get("validation", {}).get("recall") is not None:
            v = m0["validation"]["recall"] - C_RECALL_DROP
            layers.append(alt.Chart(pd.DataFrame({"y": [v]})).mark_rule(color=GREY, strokeDash=[6, 4]).encode(y="y:Q"))
        if intro:
            layers.append(alt.Chart(pd.DataFrame({"week": [intro]})).mark_rule(color=ORANGE, strokeDash=[2, 3]).encode(x="week:Q"))
        st.altair_chart(alt.layer(*layers).properties(height=320), width="stretch")
        st.caption(f"Grey dashed line = rule R1: the model's validation recall minus {C_RECALL_DROP:.0%}. "
                   "R2 = a never-seen typology caught under 50% (at least 20 cases); R3 = a known typology 20 points "
                   "below its validation recall. A rule must hold 2 weeks in a row to raise the alert.")
        rw = pd.DataFrame(p2.get("rules", []))
        if len(rw):
            rw["Week"] = [wno(w) for w in rw["week_start"]]
            show = rw[["Week", "cases", "recall", "r1_recall_drop", "r2_new_typology", "r3_typology_drop", "alert"]].rename(
                columns={"cases": "laundering payments", "r1_recall_drop": "R1 recall drop",
                         "r2_new_typology": "R2 new typology", "r3_typology_drop": "R3 typology drop"})
            show["recall"] = show["recall"].map(lambda v: f"{v:.0%}" if v == v else "")
            with st.expander("Rule checks week by week (the bank's model)"):
                st.dataframe(show, hide_index=True, width="stretch")
        if dec.get("alert"):
            st.warning(f"Alert in Week {a_wk}: {'; '.join(dec.get('rules', []))}. The bank knows on {dec['known_on']} "
                       f"(cases confirmed 4 weeks late), retrains with cases up to {dec['retrain_to']} and deploys on "
                       f"{dec['deploy_from']}" + (" (after the last scored day; the BQ1 model line shows what the "
                                                  "retrained model catches)." if scen == "new_tactic" else "."))
        else:
            st.success("No rule held two weeks in a row: no alert, no retraining needed.")


C_RECALL_DROP = 0.10          # bq2_common.RECALL_DROP (rule R1); kept here so the app needs no aws/ code

with tab_ibm:
    ibm_tab()
    ibm_test_section()

# ================================================================== SAML-D
with tab_saml:
    ds_, ctrl = "SAML-R", "SAML-C"
    stat, b0, b1 = week_status(ds_)
    lw = P1[(P1["dataset"] == ds_) & (P1["population"] == "laundering")]
    em = DZ[(DZ["group"] == "emerging") & (DZ["payments_ramp"] > 0)].copy()
    em["week"] = [week_of(ds_, w) for w in em["week_start"]]
    start_wk = int(em["week"].min()) if len(em) else None
    tc = lw[lw["tactic_change"] & lw["dimension"].isin(["format", "pattern"])]
    det_wk = int(tc["week"].min()) if len(tc) else None
    dims = sorted(set(tc["dimension"]), key=["format", "pattern"].index)
    # what is live comes from the pipeline's deployed.json; without it (JSON fallback) use the decision file
    depl = d.get("deployed") or {}
    live = depl.get("version", "v0") if aws_data.enabled() else None
    dec1 = d.get("decision_v1", {})
    if aws_data.enabled():
        dep = depl.get("deploy_from") if live in ("v1", "v2") else None
        dep2 = dec1.get("deploy_from") if live == "v2" else None
    else:
        dep = dec.get("deploy_from")
        dep2 = dec1.get("deploy_from") if dec1.get("alert") and "v2" in set(W2["version"]) else None

    def ratio(a, num, den):
        return a[num].sum() / a[den].sum() if a[den].sum() else float("nan")

    def tot(v, g, frm=None):
        return ratio(W2[(W2["version"] == v) & (W2["week_start"] >= (frm or dep)) & (W2["group"] == g)],
                     "caught", "cases")

    def prec(v, frm=None):
        return ratio(W2[(W2["version"] == v) & (W2["week_start"] >= (frm or dep)) & (W2["group"] == "all")],
                     "caught", "alerts")

    a_wk = week_of(ds_, dec["alert_week"]) if dec.get("alert") else None
    known_wk = week_of(ds_, dec["known_on"]) if dec.get("alert") else None
    dep_wk = week_of(ds_, dep) if dep else None
    a2_wk = week_of(ds_, dec1["alert_week"]) if dep2 else None
    known2_wk = week_of(ds_, dec1["known_on"]) if dep2 else None
    dep2_wk = week_of(ds_, dep2) if dep2 else None
    if dep:
        impact = f"{tot('v0', 'emerging'):.0%} → {tot('v1', 'emerging'):.0%}"
        impact_sub = (f"new-tactic recall, before → after retraining (v1 live Week {dep_wk}) · precision "
                      f"{prec('v0'):.1%} → {prec('v1'):.1%}")
    else:
        since = em["week_start"].min() if len(em) else None
        r0 = tot("v0", "emerging", since) if since else float("nan")
        impact = "No retraining yet"
        impact_sub = ("model v0 is live" + (f" · new-tactic recall so far {r0:.0%}" if r0 == r0 else "")
                      + (" · alert raised, waiting on approval" if a_wk else ""))
    if dep2:
        impact += f" → {tot('v2', 'emerging', dep2):.0%}"
        impact_sub = (f"new-tactic recall · v0 → v1 (live Week {dep_wk}) → v2 (live Week {dep2_wk}) · precision "
                      f"stays {min(prec('v1'), prec('v2', dep2)):.0%}+")
    cards([("Tactic change detected?", "Yes" if det_wk else "Not yet",
            (", ".join({"format": "payment format", "pattern": "typology mix"}[x] for x in dims) + " · control: no change")
            if det_wk else "no lasting change after the baseline", ORANGE if det_wk else GREY),
           ("Week detected", f"Week {det_wk}" if det_wk else "—",
            f"{det_wk - start_wk} weeks after the new tactics began (Week {start_wk})" if det_wk and start_wk
            else "new tactics have not started yet" if not start_wk else "waiting for a lasting change", None),
           ("Detection impact", impact, impact_sub, None)])

    st.subheader("Laundering payment-format mix vs baseline")
    fr = lw[lw["dimension"] == "format"].sort_values("week")
    fc = P1[(P1["dataset"] == ctrl) & (P1["population"] == "laundering") & (P1["dimension"] == "format")]
    thr = float(fr["js_threshold"].iloc[0])
    lines = pd.concat([fr.assign(series="SAML-D with new tactics"), fc.assign(series="control (no new tactics)")])
    dz = DZ.groupby("week_start")["dose"].max().reset_index()
    dz["week"] = [week_of(ds_, w) for w in dz["week_start"]]
    dz = dz[dz["dose"] > 0].sort_values("week")
    bands = [(b0, b1 + 1, "baseline · v0 trained")]
    for dose, g in dz.groupby("dose"):
        bands.append((int(g["week"].min()), int(g["week"].max()) + 1, f"new tactics {dose:.0%}"))
    nb = pd.DataFrame(d.get("part1_weekly_nb", []))
    nb_txt = ""
    if len(nb):
        nbw = sorted({week_of(ds_, w) for w in nb[(nb["dataset"] == ds_) & nb["in_baseline"]]["week_start"]})
        after = nb[(nb["dataset"] == ds_) & (nb["population"] == "laundering") & ~nb["in_baseline"]
                   & (nb["week_start"] > nb[nb["in_baseline"]]["week_start"].max())]
        if nbw:
            bands.append((nbw[0], nbw[-1] + 1, "new baseline"))
            nb_txt = (f" After v1, the data baseline moves to Weeks {nbw[0]}–{nbw[-1]} (new tactics fully in): the "
                      f"{after['week_start'].nunique()} weeks after it show "
                      + ("no lasting change." if not after["tactic_change"].any() else "a lasting change."))
    rules = []
    if start_wk:
        rules.append((start_wk, f"Wk {start_wk} · new tactics start"))
    if det_wk:
        rules.append((det_wk, f"Wk {det_wk} · change detected"))
    if a_wk:
        rules.append((a_wk, f"Wk {a_wk} · v0 falls behind (known Wk {known_wk})"))
    if dep_wk:
        rules.append((dep_wk, f"Wk {dep_wk} · v1 retrained and live"))
    if dep2:
        rules.append((a2_wk, f"Wk {a2_wk} · v1 falls behind (known Wk {known2_wk})"))
        rules.append((dep2_wk, f"Wk {dep2_wk} · v2 retrained and live"))
    shown_to = int(lines["week"].max())
    later = [lab for wk, lab in rules if wk > shown_to]
    rules = [(wk, lab) for wk, lab in rules if wk <= shown_to]
    st.altair_chart(change_chart(lines[["week", "js", "series"]], thr, bands=sorted(bands), rules=rules,
                                 confirmed=fr[fr["tactic_change"]][["week", "js"]]), width="stretch")
    if later:
        st.info("Happened after the last week shown (its cases are not confirmed yet): " + "; ".join(later) + ".")
    st.caption(f"Week 1 = {fr['date'].min():%d %b %Y}. Each week's laundering is compared with the baseline "
               f"(Weeks {b0}–{b1}); dot = confirmed change (above the threshold {thr:.2f} two weeks in a row, "
               "honest payments unchanged). Each model is graded on confirmed cases, which arrive 4 weeks late; "
               "after every retraining the new model's own validation becomes the baseline it is checked against."
               + nb_txt)

    R = pd.DataFrame(d.get("part2_rules", []))
    if len(R):
        R = R.assign(week=[week_of(ds_, w) for w in R["week_start"]]).sort_values("week")
        st.subheader("Detection rules · is the live model keeping up on confirmed cases?")
        st.caption("Unlike the chart above (a drift signal from the data), alerts come from grading the live model v0 "
                   "on confirmed cases. A rule must hold two weeks in a row to raise an alert.")
        ca, cb = st.columns(2, gap="large")
        with ca:
            st.markdown("**Weekly recall vs the model's validation baseline** (rule R1)")
            rl = R.melt(id_vars="week", value_vars=["recall", "baseline_recall"], var_name="s", value_name="v")
            rl["s"] = rl["s"].map({"recall": "weekly recall on confirmed cases", "baseline_recall": "validation baseline"})
            line = alt.Chart(rl).mark_line(point=True, strokeWidth=2.5).encode(
                x=alt.X("week:Q", title="Week", axis=alt.Axis(tickMinStep=1)),
                y=alt.Y("v:Q", title=None, axis=alt.Axis(format="%"), scale=alt.Scale(zero=False)),
                color=alt.Color("s:N", legend=alt.Legend(title=None, orient="top"),
                                scale=alt.Scale(domain=["weekly recall on confirmed cases", "validation baseline"],
                                                range=[ORANGE, GREY])),
                strokeDash=alt.condition(alt.datum.s == "validation baseline", alt.value([6, 4]), alt.value([0])),
                tooltip=["week", "s", alt.Tooltip("v:Q", format=".1%")])
            st.altair_chart(line.properties(height=260), width="stretch")
        with cb:
            st.markdown("**Which rule held each week** (filled = rule hit, red = alert raised)")
            hit = []
            for _, r in R.iterrows():
                for rule, on in (("R1 recall drop", bool(r["r1_recall_drop"])),
                                 ("R2 new typology", bool(r["r2_new_typology"])),
                                 ("R3 typology drop", bool(r["r3_typology_drop"]))):
                    hit.append((int(r["week"]), rule, "alert" if on and r["alert"] else "hit" if on else "ok"))
            hit = pd.DataFrame(hit, columns=["week", "rule", "state"])
            strip = alt.Chart(hit).mark_rect(stroke="white", strokeWidth=2, cornerRadius=3).encode(
                x=alt.X("week:O", title="Week"), y=alt.Y("rule:N", title=None),
                color=alt.Color("state:N", legend=None,
                                scale=alt.Scale(domain=["ok", "hit", "alert"], range=["#EEF0F3", ORANGE, "#C0392B"])),
                tooltip=["week", "rule", "state"])
            st.altair_chart(strip.properties(height=260), width="stretch")
        src = R[R["alert"]].tail(1)
        if src.empty:
            src = R[R["any_rule"]].tail(1)
        rows = []
        for _, r in src.iterrows():
            for kind, col in (("new typology (R2)", "r2_new_typology"), ("typology drop (R3)", "r3_typology_drop")):
                for item in filter(None, [x.strip() for x in str(r[col] or "").split(";")]):
                    name, frac = item.rsplit(" ", 1)
                    caught, cases = (int(x) for x in frac.split("/"))
                    rows.append((name.replace("_", " "), kind, caught, cases - caught, cases))
        if rows:
            wk_ = int(src["week"].iloc[0])
            st.markdown(f"**Typologies behind the rule · Week {wk_}**")
            ty = pd.DataFrame(rows, columns=["typology", "rule", "caught", "missed", "cases"]) \
                .melt(id_vars=["typology", "rule", "cases"], value_vars=["caught", "missed"], var_name="outcome",
                      value_name="n")
            order = list(ty.groupby("typology")["cases"].first().sort_values(ascending=False).index)
            tb = alt.Chart(ty).mark_bar(size=28).encode(
                y=alt.Y("typology:N", title=None, sort=order), x=alt.X("n:Q", title="confirmed cases"),
                color=alt.Color("outcome:N", legend=alt.Legend(title=None, orient="top"),
                                scale=alt.Scale(domain=["caught", "missed"], range=[BLUE, ORANGE])),
                tooltip=["typology", "rule", "outcome", "n", "cases"])
            st.altair_chart(tb.properties(height=max(90, 55 * ty["typology"].nunique())), width="stretch")
            st.caption("The model never saw these typologies in validation, so it has no baseline for them. "
                       "Caught = flagged by v0; missed = laundering it let through.")

    if dep and "v1" in set(W2["version"]):
        st.subheader("Old vs new model · weekly recall on confirmed cases")
        cols = st.columns(2, gap="large")
        for col, (grp, ttl) in zip(cols, (("all", "All laundering"), ("emerging", "New tactics only"))):
            ww = W2[(W2["group"] == grp) & (W2["cases"] > 0)]
            ww = ww[(ww["version"] == "v0") | (ww["week_start"] >= dep)]
            ww = ww[ww["version"].isin(["v0", "v1"] + (["v2"] if dep2 else []))].copy()
            ww["week"] = [week_of(ds_, w) for w in ww["week_start"]]
            ww["model"] = ww["version"].map({"v0": "v0 (before retraining)", "v1": "v1 (after retraining)",
                                             "v2": "v2 (after 2nd retraining)"})
            with col:
                st.markdown(f"**{ttl}**")
                ch = alt.Chart(ww).mark_line(point=True, strokeWidth=2.5).encode(
                    x=alt.X("week:Q", title="Week", axis=alt.Axis(tickMinStep=1)),
                    y=alt.Y("recall:Q", title=None, axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
                    color=alt.Color("model:N", legend=alt.Legend(title=None, orient="top"),
                                    scale=alt.Scale(domain=["v0 (before retraining)", "v1 (after retraining)",
                                                            "v2 (after 2nd retraining)"],
                                                    range=[ORANGE, BLUE, "#1B9E77"])),
                    tooltip=["week", "model", "cases", "caught", alt.Tooltip("recall:Q", format=".1%")])
                vr = alt.Chart(pd.DataFrame({"week": [dep_wk]})).mark_rule(strokeDash=[4, 4], color=GREY).encode(x="week:Q")
                st.altair_chart((ch + vr).properties(height=260), width="stretch")
        st.caption("v1 is shown only from its go-live week, because it was trained on earlier cases. The dashed line "
                   "marks when v1 went live; both models are graded on the same confirmed cases.")

    def round_bars(old, new, frm, title, la, lb):
        st.subheader(title)
        rows = [(mname, mdl, val) for mname, f0, f1 in [
            ("Precision", prec(old, frm), prec(new, frm)),
            ("Recall · all laundering", tot(old, "all", frm), tot(new, "all", frm)),
            ("Recall · new tactics", tot(old, "emerging", frm), tot(new, "emerging", frm)),
            ("Recall · established", tot(old, "established", frm), tot(new, "established", frm))]
            for mdl, val in ((la, f0), (lb, f1))]
        st.altair_chart(bars(pd.DataFrame(rows, columns=["measure", "model", "value"])), width="stretch")

    last_wk = int(lw["week"].max())
    if dep:
        c1, c2 = st.columns(2, gap="large") if dep2 else (st.container(), None)
        with c1:
            round_bars("v0", "v1", dep, f"1st retraining · v0 vs v1 (Weeks {dep_wk}–{last_wk})",
                       "v0 · before retraining", "v1 · after retraining")
            tt = T2[T2["period"] == "after deploy"]
            weak = tt[(tt["version"] == "v1") & (tt["group"] == "emerging")].sort_values("recall").head(2)
            st.caption(f"v0 fell behind in Weeks {a_wk - 1}–{a_wk} (rule R2: new typologies mostly missed). The cases "
                       f"were confirmed at the end of Week {known_wk}, so v1 was retrained immediately and went live in "
                       f"Week {dep_wk}. " + ", ".join(f"{t.replace('_', ' ')} ({r:.0%})" for t, r in
                                                      zip(weak['typology'], weak['recall']))
                       + " had few confirmed cases at that point, so v1 learned them only partly.")
        if dep2:
            with c2:
                round_bars("v1", "v2", dep2, f"2nd retraining · v1 vs v2 (Weeks {dep2_wk}–{last_wk})",
                           "v1 · before 2nd retraining", "v2 · after 2nd retraining")
                t1 = pd.DataFrame(d.get("part2_typology_v1", []))
                t1 = t1[(t1["period"] == "after deploy") & (t1["group"] == "emerging")] if len(t1) else t1
                fixed = ""
                if len(t1):
                    p = t1.pivot_table(index="typology", columns="version", values="recall")
                    p = p.assign(gain=p["v2"] - p["v1"]).sort_values("gain", ascending=False).head(2)
                    fixed = ", ".join(f"{t.replace('_', ' ')} {r.v1:.0%} → {r.v2:.0%}" for t, r in p.iterrows())
                st.caption(f"Monitoring continued on v1 against its own validation: v1 fell behind in Weeks "
                           f"{a2_wk - 1}–{a2_wk} ({'; '.join(dec1.get('rules', []))}). Known at the end of Week "
                           f"{known2_wk}; v2 retrained on every case confirmed by then and went live in Week {dep2_wk}. "
                           + (f"Biggest gains: {fixed}." if fixed else ""))
