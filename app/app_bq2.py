"""
BQ2 page for the model owner / AML analytics team: one dashboard for both datasets.
    streamlit run app_bq2.py                 (?approve=<dataset> opens a waiting approval; ?approve=demo shows one)
Top: status of both weekly pipelines, the approvals inbox, and the approval page the alert emails link to.
Tabs: HI-Large (bq2_ibm_tab.py, Tasha) and SAML-D (below, Wei Ting), both as answer -> tactics -> detection ->
retraining & alerts -> decisions. With AML_ROOT set, data comes live from AWS (aws_data.py); otherwise from
data/bq2_data.json (bq2/bq2_ramp_report.py) and data/bq2_ibm_data.json (bq2/tasha/bq2_ibm_report.py).
"""
import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

import aws_data
import bq2_ibm_tab

ORANGE, BLUE, GREY = "#E07B39", "#2F7FAD", "#9A948B"
INK = "#d8d3ca"             # text and rules drawn on charts: the app theme is dark (.streamlit/config.toml)
DATA = Path(__file__).parent / "data"
NICE = {"Structuring": "structuring", "Smurfing": "smurfing", "Cash_Withdrawal": "cash withdrawal",
        "Deposit-Send": "deposit-send"}

try:
    st.set_page_config(page_title="BQ2 · Tactic change & detection", layout="wide")
except st.errors.StreamlitAPIException:     # already set by bq1_site.py when this page runs inside the site menu
    pass
st.markdown("""<style>
.tag{display:inline-block;padding:1px 9px;border-radius:999px;font-size:0.82rem;font-weight:600;white-space:nowrap}
.yes{background:#FBE3D6;color:#9A3410} .warn{background:#FBF0D9;color:#7A5200} .no{background:#ECE6DC;color:#3F3E3A}
.wk{width:100%;border-collapse:collapse;font-size:0.9rem} .wk th{text-align:left;color:#5b5750;border-bottom:1.5px solid #d9d2c6;padding:6px}
.wk td{border-bottom:1px solid #eee6da;padding:6px;vertical-align:top} .muted{color:#8a857c}
.cards{display:grid;grid-template-columns:1fr 1fr 2fr;gap:16px;margin:8px 0 18px}
.card{background:#fff;border:1px solid #e6ddd0;border-radius:12px;padding:16px 20px;box-shadow:0 1px 2px rgba(0,0,0,.04)}
.card .l{font-size:0.9rem;color:#5b5750} .card .v{font-size:2.1rem;font-weight:700;color:#262626;margin:4px 0 2px;line-height:1.15}
.card .s{font-size:0.82rem;color:#8a857c} .dot{display:inline-block;width:12px;height:12px;border-radius:50%;margin-right:8px;vertical-align:middle}
.answer{background:#fff;border-left:6px solid #E07B39;border-radius:8px;padding:12px 18px;margin:6px 0 12px;color:#262626}
.answer p{margin:4px 0;color:#262626;font-size:1.02rem}
.step{color:#a8a197;font-size:0.78rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;margin-top:24px}
.status{display:flex;gap:24px;align-items:center;background:#F6F1E9;border-radius:10px;padding:9px 16px;margin:2px 0 8px;
        color:#3f3e3a;font-size:.9rem;flex-wrap:wrap} .status b{color:#262626}
.pill{padding:2px 10px;border-radius:999px;font-size:.78rem;font-weight:600} .ok{background:#DDF0E6;color:#1B6B49}
.wait{background:#FBE3D6;color:#9A3410}
.inbox{border:1.5px solid #E07B39;border-radius:12px;padding:10px 16px;background:#FFF8F2;margin:4px 0 12px;color:#3f3e3a;line-height:1.8}
.inbox b{color:#262626}
.ms{font-size:.86rem;line-height:1.9;margin:-4px 0 6px}
.badge{display:inline-block;width:20px;height:20px;border-radius:50%;background:#e8e4dc;color:#111;text-align:center;
       font-size:.75rem;font-weight:700;line-height:20px;margin-right:5px}
</style>""", unsafe_allow_html=True)


@st.cache_data(ttl=300)
def load_aws():
    return aws_data.load_bq2()


# pipeline facts come from progress/bq2_config.json (written by build_bq2_weekly.py); these are only fallbacks
CFG_DEFAULT = {"ramp": "SAML-R", "control": "SAML-C", "label_lag_days": 28, "replay_start": "2023-01-30",
               "week_anchors": {"HI-Large": "2022-08-01"}}
CFG = dict(CFG_DEFAULT)
REPLAY_START = pd.Timestamp(CFG["replay_start"])      # week_index 0
LABEL_LAG_DAYS = CFG["label_lag_days"]
ANCHOR = {k: pd.Timestamp(v) for k, v in CFG["week_anchors"].items()}   # Monday of each dataset's week 1


def apply_config(c):
    global CFG, REPLAY_START, LABEL_LAG_DAYS, ANCHOR
    CFG = {**CFG_DEFAULT, **(c or {})}
    REPLAY_START = pd.Timestamp(CFG["replay_start"])
    LABEL_LAG_DAYS = int(CFG["label_lag_days"])
    ANCHOR = {k: pd.Timestamp(v) for k, v in CFG["week_anchors"].items()}


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
    apply_config(d.get("config"))
    d = only_processed(d)
    bq1 = None
    if (DATA / "dashboard_data.json").exists():
        s1 = pd.DataFrame(json.loads((DATA / "dashboard_data.json").read_text())["stage1"])
        bq1 = s1[(s1["level"] == "account_day") & (s1["cutoff"] == "frozen_threshold")].iloc[0].to_dict()
    p1 = pd.DataFrame(d["part1_weekly"])
    p1["date"] = pd.to_datetime(p1["week_start"])
    for ds_, g in p1.groupby("dataset"):
        first = ANCHOR.get(ds_, g["date"].min())
        p1.loc[g.index, "week"] = ((g["date"] - first).dt.days // 7 + 1).astype(int)
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
    if len(t) and "period" in t:                 # the evidence is what the live model did before any redeploy
        t = t[t["period"] == "before deploy"]
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
               "evaluates a challenger; nothing goes live until the next gate. Reject to keep "
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


def week_of(ds_, date_str):
    s = ANCHOR.get(ds_, P1[P1["dataset"] == ds_]["date"].min())
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


def change_chart(lines, thr, bands=(), rules=(), confirmed=None, ymax=None, label_expr=None):
    """lines: DataFrame week, js, series. bands: (from_week, to_week, label). rules: (week, label).
    The y range follows the data (never below the threshold) unless ymax is given."""
    ymax = ymax or max(float(lines["js"].max()), thr) * 1.15
    x = alt.X("week:Q", title=None if label_expr else "Week",
              axis=alt.Axis(tickMinStep=1, format="d", labelAngle=-35 if label_expr else 0,
                            **({"labelExpr": label_expr} if label_expr else {})),
              scale=alt.Scale(domain=[lines["week"].min(), lines["week"].max()], nice=False))
    layers = []
    if bands:
        b = pd.DataFrame(bands, columns=["w0", "w1", "label"])
        b["op"] = [0.06 + 0.16 * i / max(len(b) - 1, 1) for i in range(len(b))]
        layers.append(alt.Chart(b).mark_rect(color="#EFE6D8").encode(
            x="w0:Q", x2="w1:Q", opacity=alt.Opacity("op:Q", legend=None, scale=None)))
        layers.append(alt.Chart(b).mark_text(dy=-6, color="#a8a197", fontSize=10, baseline="bottom").encode(
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
    if rules:                       # numbered badges; milestones() prints what each number means under the chart
        r = pd.DataFrame([(wk, str(i + 1)) for i, (wk, _) in enumerate(rules)], columns=["week", "n"])
        layers.append(alt.Chart(r).mark_rule(strokeDash=[2, 3], color="#a8a197").encode(x="week:Q"))
        layers.append(alt.Chart(r).mark_circle(size=320, color="#e8e4dc", opacity=1).encode(x="week:Q", y=alt.value(36)))
        layers.append(alt.Chart(r).mark_text(color="#111", fontWeight="bold").encode(x="week:Q", y=alt.value(36), text="n:N"))
    return alt.layer(*layers).properties(height=380)


def milestones(rules):
    if rules:
        st.markdown('<div class="ms">' + " &nbsp;·&nbsp; ".join(f'<span class="badge">{i + 1}</span>{lab}'
                                                              for i, (_, lab) in enumerate(rules)) + "</div>",
                    unsafe_allow_html=True)


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


# ------------------------------------------------------------------ header: status, inbox, approval page
IBM_START = pd.Timestamp("2022-08-29")         # week_index 0 of aml_bq2_weekly_ibm (HI-Large Week 5)


@st.cache_data(ttl=300)
def load_ibm():
    """HI-Large results: live from S3 (aws_data.load_ibm) when AWS is configured, else the laptop report's JSON."""
    if aws_data.enabled():
        try:
            return aws_data.load_ibm()
        except Exception as e:                                    # e.g. no read access to bq2_ibm/ yet
            st.session_state["ibm_err"] = str(e)
    p = DATA / "bq2_ibm_data.json"
    return json.loads(p.read_text()) if p.exists() else None


IBM = load_ibm()
DS_NAME = {"SAML-R": "SAML-D", "HI-Large": "HI-Large", "HI-Large-NT": "HI-Large (new-tactic test)"}


def req_id(it):
    return f"{it.get('dataset', 'SAML-R')}:{it['gate']}"


def pending():
    if not aws_data.enabled():
        return []
    if "pending" not in st.session_state:               # the queue hides a read message for a few seconds
        st.session_state["pending"] = aws_data.pending_approvals()
    return st.session_state["pending"]


def demo_request():
    """The SAML-D pipeline's first real alert, as a request: shown with ?approve=demo (no buttons work)."""
    dc = dict(dec, monitored="v0", retrain="v1")
    return {"dataset": "SAML-R", "gate": "retrain_gate", "week_end": dc.get("known_on", ""), "decision": dc,
            "demo": True}


def status_strip():
    def part(name, ptr, start, live, waiting):
        wi = (ptr or {}).get("week_index")
        thru = "not started" if wi is None else f"through {(start + pd.Timedelta(days=7 * int(wi) - 1)):%-d %b %Y}"
        pill = (f'<span class="pill wait">{waiting} approval{"s" if waiting > 1 else ""} waiting</span>' if waiting
                else '<span class="pill ok">no action needed</span>')
        return f'<span><b>{name}</b> · {thru} · live model <b>{live}</b> {pill}</span>'
    items = pending()
    n = lambda pre: sum(1 for x in items if str(x.get("dataset", "SAML-R")).startswith(pre))
    ibm_live = ((IBM or {}).get("deployed", {}).get("new_tactic") or {}).get("version", "v0")
    html = part("SAML-D", d.get("progress"), REPLAY_START, (d.get("deployed") or {}).get("version", "v0"), n("SAML")) \
        + part("HI-Large", (IBM or {}).get("progress"), IBM_START, ibm_live, n("HI-Large"))
    if not aws_data.enabled():
        html = '<span>Local snapshot (not connected to AWS): approvals and the live pipeline status appear on the hosted dashboard.</span>'
    st.markdown(f'<div class="status">{html}</div>', unsafe_allow_html=True)


def inbox():
    items = pending()
    if not items:
        return
    rows = "".join(f'<div>📬 <b>{GATES.get(it["gate"], (it["gate"],))[0]}</b> · <b>{DS_NAME.get(it.get("dataset"), it.get("dataset"))}</b> '
                   f'· week ending {it.get("week_end")} · {"; ".join((it.get("decision") or {}).get("rules", []))} &nbsp; '
                   f'<a href="?approve={req_id(it)}" target="_self">Review and decide →</a></div>' for it in items)
    st.markdown(f'<div class="inbox"><b>{len(items)} request{"s" if len(items) > 1 else ""} waiting for you</b>{rows}</div>',
                unsafe_allow_html=True)


def approval_page(key):
    """What the link in the alert email opens: why, evidence, what each choice does, then approve / reject."""
    if key == "demo":
        it = demo_request()
    else:
        items = pending()
        it = next((x for x in items if req_id(x) == key or x.get("dataset") == key), None)
        if it is None:                                   # just read by another tab: the queue hides it ~5 s
            st.session_state.pop("pending", None)
            it = next((x for x in pending() if req_id(x) == key or x.get("dataset") == key), None)
    if st.button("← Back to the dashboard"):
        st.query_params.clear()
        st.rerun()
    if it is None:
        st.info("This request is no longer waiting: it was already approved or rejected, or its 7 days ran out.")
        return
    ds_, gate, dc = it.get("dataset", "SAML-R"), it["gate"], it.get("decision") or {}
    title = GATES.get(gate, (gate, ""))[0]
    st.caption("BQ2 pipeline · approval request" + (" · DEMO (the SAML-D pipeline's first real alert; nothing is sent)"
                                                     if it.get("demo") else " (opened from the alert email)"))
    st.header(f"{title} · {DS_NAME.get(ds_, ds_)}")
    rules = "; ".join(dc.get("rules", [])) or "a detection rule held 2 weeks in a row"
    old, new = dc.get("monitored", "v0"), dc.get("retrain", "v1")
    if gate == "retrain_gate":
        what = ("trains a challenger on every case confirmed so far, scores it and grades it" if ds_.startswith("SAML")
                else "grades the ready challenger (the BQ1 model that knows fan-in / fan-out) on the same confirmed cases")
        why = (f"the live model <b>{old}</b> fell behind: <b>{rules.replace('_', ' ')}</b> (cases confirmed by "
               f"{dc.get('known_on', it.get('week_end'))}).")
        yes = f"the pipeline {what} and emails you again. <b>Nothing goes live yet</b>: deploying needs a second approval."
        no = f"{old} stays live; the pipeline moves on to the next week and keeps watching."
    else:
        why = f"the challenger <b>{new}</b> has been graded and is ready to replace <b>{old}</b>."
        yes = f"<b>{new}</b> becomes the live model from <b>{dc.get('deploy_from', 'the next week')}</b>; your name is recorded."
        no = f"<b>{old}</b> stays live; the challenger is kept for reference."
    st.markdown(f'<div class="answer"><p><b>Why you got this email:</b> {why}</p><p><b>If you approve:</b> {yes}</p>'
                f'<p><b>If you reject:</b> {no}</p></div>', unsafe_allow_html=True)
    if ds_.startswith("SAML"):
        if gate == "retrain_gate":
            case_for_retraining(dc)
        else:
            compare_models(old, new)
    else:
        n2 = pd.DataFrame(((IBM or {}).get("part2", {}).get("new_tactic", {}) or {}).get("weekly", []))
        if len(n2):
            e = n2[(n2["version"] == old) & (n2["group"] == "emerging") & (n2["cases"] > 0)]
            st.markdown("**Evidence · share of the new tactic the live model caught each week**")
            st.altair_chart((alt.Chart(e).mark_line(point=True, strokeWidth=3, color=ORANGE).encode(
                x=alt.X("week_start:N", title=None), y=alt.Y("recall:Q", axis=alt.Axis(format="%"),
                                                            scale=alt.Scale(domain=[0, 1]), title=None))
                + alt.Chart(pd.DataFrame({"y": [0.5]})).mark_rule(color="#C0504D", strokeDash=[6, 4]).encode(y="y:Q"))
                .properties(height=230), width="stretch")
    who = st.text_input("Your name (recorded as the approver)", disabled=bool(it.get("demo")))
    b1, b2, _ = st.columns([1, 1, 4])
    ok = b1.button("Approve", type="primary", disabled=bool(it.get("demo")) or not who)
    nok = b2.button("Reject", disabled=bool(it.get("demo")) or not who)
    if ok or nok:
        err = aws_data.answer(it, ok, who)
        st.session_state.pop("pending", None)
        load_aws.clear()
        load.clear()
        load_ibm.clear()
        st.success(err or f"{'Approved' if ok else 'Rejected'} by {who}. The pipeline continues; this page will show "
                          "the result after the next step (refresh in a few minutes).")
    st.caption("Requests expire after 7 days. Every decision is recorded with the approver's name in "
               f"models/bq2r/{ds_}/ (deployed.json) and in the Step Functions execution history.")


st.title("Are criminals changing their tactics, and is detection keeping up?")
if st.query_params.get("approve"):
    approval_page(st.query_params["approve"])
    st.stop()
st.markdown("**BQ2:** Are criminals changing their laundering tactics over time, and when they do, does our "
            "detection rate fall, and can retraining recover it?")
c_s, c_r = st.columns([6, 1])
with c_s:
    status_strip()
if aws_data.enabled() and c_r.button("Refresh from AWS"):
    load_aws.clear()
    load.clear()
    load_ibm.clear()
    st.session_state.pop("pending", None)
    st.rerun()
inbox()
if st.session_state.get("ibm_err"):
    st.caption(f"HI-Large live results could not be read ({st.session_state['ibm_err'][:120]}); showing the last snapshot.")

tab_ibm, tab_saml = st.tabs(["HI-Large · IBM AML", "SAML-D · new tactics test"])

C_RECALL_DROP = 0.10          # bq2_common.RECALL_DROP (rule R1); kept here so the app needs no aws/ code

with tab_ibm:
    bq2_ibm_tab.render(IBM)

# ================================================================== SAML-D
with tab_saml:
    ds_, ctrl = CFG["ramp"], CFG["control"]
    if aws_data.enabled():
        st.caption(f"**Pipeline has run through {d.get('asof_end', '—')}.** Charts use confirmed cases only, which arrive "
                   f"28 days after a week ends, so they end at the week ending {d.get('label_end', '—')}.")
    stat, b0, b1 = week_status(ds_)
    S0 = P1[P1["dataset"] == ds_]["date"].min()
    WL = {w: f"W{w} · {(S0 + pd.Timedelta(weeks=w - 1)):%-d %b}" for w in range(1, 80)}
    WEXPR = " : ".join(f"datum.value == {w} ? '{WL[w]}'" for w in range(1, 80)) + " : ''"
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
    since0 = em["week_start"].min() if len(em) else None
    r0_all = tot("v0", "emerging", since0) if since0 else float("nan")
    steps = [f"the original model caught only <b>{r0_all:.0%}</b> of the new tactics" if r0_all == r0_all else ""]
    if dep:
        steps.append(f"<b>{tot('v1', 'emerging'):.0%}</b> after the 1st approved retraining (v1, live {WL[dep_wk]})")
    if dep2:
        steps.append(f"<b>{tot('v2', 'emerging', dep2):.0%}</b> after the 2nd (v2, live {WL[dep2_wk]})")
    st.markdown('<div class="answer"><p><b>Are criminals changing tactics?</b> '
                + (f"Yes: new typologies start in <b>{WL[start_wk]}</b> and the monitor confirms a lasting change in "
                   f"<b>{WL[det_wk]}</b>, {det_wk - start_wk} weeks later; the control data (no new tactics) never moves."
                   if det_wk and start_wk else "No lasting change detected yet.")
                + "</p><p><b>Is detection falling behind?</b> "
                + (("It was: " + ", then ".join(x for x in steps if x) + ".") if dep or a_wk else
                   "Not so far: no rule has held for 2 weeks in a row.") + "</p></div>", unsafe_allow_html=True)
    cards([("Tactic change detected?", "Yes" if det_wk else "Not yet",
            (", ".join({"format": "payment format", "pattern": "typology mix"}[x] for x in dims) + " · control: no change")
            if det_wk else "no lasting change after the baseline", ORANGE if det_wk else GREY),
           ("Week detected", f"Week {det_wk}" if det_wk else "—",
            f"{det_wk - start_wk} weeks after the new tactics began (Week {start_wk})" if det_wk and start_wk
            else "new tactics have not started yet" if not start_wk else "waiting for a lasting change", None),
           ("Detection impact", impact, impact_sub, None)])

    st.markdown('<div class="step">Step 1 · did tactics change?</div>', unsafe_allow_html=True)
    st.subheader("How different is each week's laundering (payment format) from normal?")
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
        rules.append((start_wk, f"{WL[start_wk]}: new tactics start"))
    if det_wk:
        rules.append((det_wk, f"{WL[det_wk]}: change confirmed"))
    if a_wk:
        rules.append((a_wk, f"{WL[a_wk]}: v0 falls behind (known {WL[known_wk]})"))
    if dep_wk:
        rules.append((dep_wk, f"{WL[dep_wk]}: v1 retrained and live"))
    if dep2:
        rules.append((a2_wk, f"{WL[a2_wk]}: v1 falls behind (known {WL[known2_wk]})"))
        rules.append((dep2_wk, f"{WL[dep2_wk]}: v2 retrained and live"))
    shown_to = int(lines["week"].max())
    later = [lab for wk, lab in rules if wk > shown_to]
    rules = [(wk, lab) for wk, lab in rules if wk <= shown_to]
    st.altair_chart(change_chart(lines[["week", "js", "series"]], thr, bands=sorted(bands), rules=rules,
                                 confirmed=fr[fr["tactic_change"]][["week", "js"]], label_expr=WEXPR), width="stretch")
    milestones(rules)
    if later:
        st.info("Happened after the last week shown (its cases are not confirmed yet): " + "; ".join(later) + ".")
    st.caption(f"Week 1 = {fr['date'].min():%d %b %Y}. Each week's laundering is compared with the baseline "
               f"(Weeks {b0}–{b1}); dot = confirmed change (above the threshold {thr:.2f} two weeks in a row, "
               "honest payments unchanged). Each model is graded on confirmed cases, which arrive 4 weeks late; "
               "after every retraining the new model's own validation becomes the baseline it is checked against."
               + nb_txt)

    st.markdown('<div class="step">Step 2 · did detection fall behind?</div>', unsafe_allow_html=True)
    st.subheader("Share of the new tactics the live model caught each week")
    lv_ = lambda wk: "v2" if dep2 and wk >= dep2_wk else "v1" if dep and wk >= dep_wk else "v0"
    w2e = W2[(W2["group"] == "emerging") & (W2["cases"] > 0)].copy()
    if len(w2e):
        w2e["week"] = [week_of(ds_, w) for w in w2e["week_start"]]
        w2e = w2e[[v == lv_(k) for v, k in zip(w2e["version"], w2e["week"])]]
        w2e["model"] = w2e["version"].map({"v0": "v0 (original)", "v1": "v1 (1st retraining)", "v2": "v2 (2nd retraining)"})
        lch = alt.Chart(w2e).mark_line(point=True, strokeWidth=3).encode(
            x=alt.X("week:Q", title=None, axis=alt.Axis(labelExpr=WEXPR, labelAngle=-35, tickMinStep=1)),
            y=alt.Y("recall:Q", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1]), title=None),
            color=alt.Color("model:N", title=None, legend=alt.Legend(orient="top"),
                            scale=alt.Scale(domain=["v0 (original)", "v1 (1st retraining)", "v2 (2nd retraining)"],
                                            range=[ORANGE, BLUE, "#1B9E77"])),
            tooltip=["model", "week", "cases", "caught", alt.Tooltip("recall:Q", format=".0%")])
        ar = pd.DataFrame({"y": [0.5], "t": ["alarm line: a never-seen typology caught under 50% (rule R2)"]})
        st.altair_chart((lch + alt.Chart(ar).mark_rule(color="#C0504D", strokeDash=[6, 4]).encode(y="y:Q")
                         + alt.Chart(ar).mark_text(color="#E8857A", align="left", dx=4, dy=-7).encode(
                             x=alt.value(0), y="y:Q", text="t:N")).properties(height=280), width="stretch")
        st.caption("Each colour is the model that was live that week, graded on confirmed cases. Under the alarm line for "
                   "2 weeks in a row = a retraining request (email + approval); each new model is then checked against "
                   "its own validation.")
    with st.expander("Rule checks week by week (R1–R3) and the typologies behind each alert"):
        R = pd.DataFrame(d.get("part2_rules", []))
        if len(R):
            R = R.assign(week=[week_of(ds_, w) for w in R["week_start"]]).sort_values("week")
            st.markdown("**Detection rules · is the live model keeping up on confirmed cases?**")
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

    st.markdown('<div class="step">Step 3 · did retraining help, and are alerts manageable?</div>', unsafe_allow_html=True)
    with st.expander("Weekly recall, old vs new model (all laundering and new tactics)"):
        if dep and "v1" in set(W2["version"]):
            st.markdown("**Old vs new model · weekly recall on confirmed cases**")
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
    if dep and dep_wk > last_wk:
        first_graded = pd.Timestamp(dep) + pd.Timedelta(days=6 + LABEL_LAG_DAYS)
        st.subheader("1st retraining · v0 vs v1")
        st.info(f"v1 is live from Week {dep_wk} ({dep}), but the charts only show weeks with confirmed cases "
                f"(up to Week {last_wk}). v1 has no confirmed cases yet; its first graded week appears once the "
                f"pipeline has processed the week ending {first_graded:%Y-%m-%d} (cases are confirmed "
                f"{LABEL_LAG_DAYS} days after a week ends).")
    elif dep:
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

    st.markdown("**Alerts each week · real laundering vs false alarms** (the model live that week)")
    w2a = W2[W2["group"] == "all"].copy()
    if len(w2a):
        w2a["week"] = [week_of(ds_, w) for w in w2a["week_start"]]
        w2a = w2a[[v == lv_(k) for v, k in zip(w2a["version"], w2a["week"])]]
        w2a = pd.concat([w2a.assign(kind="real laundering", n=w2a["caught"]),
                         w2a.assign(kind="false alarm", n=w2a["alerts"] - w2a["caught"])])
        st.altair_chart(alt.Chart(w2a).mark_bar().encode(
            x=alt.X("week:O", title=None, axis=alt.Axis(labelExpr=WEXPR, labelAngle=-50)),
            y=alt.Y("n:Q", title=None, stack="zero"),
            color=alt.Color("kind:N", title=None, legend=alt.Legend(orient="top"),
                            scale=alt.Scale(domain=["real laundering", "false alarm"], range=[BLUE, "#E8B4A0"])),
            tooltip=["week", "version", "kind", "n"]).properties(height=240), width="stretch")
        st.caption("Workload for the analysts: almost every alert is real laundering, before and after retraining, so "
                   "retraining did not buy recall with extra false alarms.")

    st.markdown('<div class="step">Step 4 · decisions & limits</div>', unsafe_allow_html=True)
    acts = []
    if a_wk:
        acts.append(f"- **{WL[known_wk]}**: alarm ({'; '.join(dec.get('rules', [])).replace('_', ' ')}) → retraining approved"
                    + (f" → **v1 live {WL[dep_wk]}**" if dep else " (waiting on approval)"))
    if dep2:
        acts.append(f"- **{WL[known2_wk]}**: alarm again ({'; '.join(dec1.get('rules', [])).replace('_', ' ')}) → "
                    f"**v2 live {WL[dep2_wk]}**")
    st.markdown("\n".join(acts) if acts else "No alarm so far: v0 stays live.")
    with st.expander("Settings and limits"):
        st.markdown(f"""
- **Change threshold {thr:.2f}** (bigger than any normal baseline week, at least 0.10) and **2 weeks in a row**: team
  choices, not industry standards; stricter = fewer false alarms, slower detection.
- **Confirmed cases arrive 4 weeks late**, so an alarm (and a retraining) is always at least 4 weeks behind the data.
- **Rules:** R1 overall recall 10+ points below validation; R2 a never-seen typology under 50% (20+ cases); R3 a known
  typology 20+ points below validation. Each new model is graded against its own validation.
- **Control:** SAML-C has no new tactics, so a change that shows up there too is the calendar, not criminals.
""")
