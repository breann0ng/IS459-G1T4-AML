"""
BQ1 and BQ2 site: the Overview landing page, the BQ1 results page and the BQ2 page, with the menu across the top.
    streamlit run bq1_site.py
Not in the menu: app_stage2.py (the stage 2
experiment log: tuning runs on validation data, kept for the report, not for the Tuning Team).
"""
import streamlit as st

ACCENT, MUTED, BAR, LINE = "#9085e9", "#a8a1c2", "#221b33", "#3a3150"

st.set_page_config(page_title="AML · G1T4", layout="wide")
st.html(f"""<style>
  /* ---- top menu: centred, bigger, spaced; the current page is underlined instead of boxed */
  [data-testid="stTopNav"] {{ position: absolute; left: 50%; transform: translateX(-50%);
                              width: 680px !important; max-width: 70vw; justify-content: center; }}
  [data-testid="stTopNav"] .rc-overflow-item {{ margin: 0 1.6rem; }}
  [data-testid="stTopNavLink"] {{ background: transparent !important; border-radius: 0 !important;
                                  padding: .55rem .2rem !important; border-bottom: 4px solid transparent; }}
  [data-testid="stTopNavLink"] p, [data-testid="stTopNavLink"] span {{ font-size: 20px !important; color: {MUTED}; }}
  [data-testid="stTopNavLink"]:hover {{ border-bottom-color: {LINE}; }}
  [data-testid="stTopNavLink"][aria-current="page"] {{ border-bottom-color: {ACCENT}; }}
  [data-testid="stTopNavLink"][aria-current="page"] p,
  [data-testid="stTopNavLink"][aria-current="page"] span {{ color: #ffffff; font-weight: 700; }}

  /* ---- tabs inside a page: a clear bar of big, spaced tabs; the open one is underlined */
  [role="tablist"] {{ gap: 0 !important; background: {BAR}; border-radius: 10px; padding: 0 .5rem;
                      border-bottom: 2px solid {LINE}; margin-bottom: 1.2rem; }}
  [data-testid="stTab"] {{ padding: 1rem 1.6rem !important; height: auto !important; margin: 0 .3rem !important;
                           border-bottom: 4px solid transparent; }}
  [data-testid="stTab"] p {{ font-size: 21px !important; font-weight: 600; color: {MUTED}; }}
  [data-testid="stTab"] span[role="img"] {{ font-size: 24px !important; }}
  [data-testid="stTab"]:hover p {{ color: #ffffff; }}
  [data-testid="stTab"][aria-selected="true"] {{ border-bottom-color: {ACCENT}; }}
  [data-testid="stTab"][aria-selected="true"] p {{ color: #ffffff; }}
  [data-baseweb="tab-highlight"], [data-baseweb="tab-border"] {{ display: none; }}

  /* ---- small print is not small any more */
  [data-testid="stCaptionContainer"], [data-testid="stCaptionContainer"] p {{ font-size: 1rem !important; }}
  [data-testid="stExpander"] summary p {{ font-size: 1.05rem !important; font-weight: 600; }}
  [data-testid="stMetricLabel"] p {{ font-size: 1.05rem !important; }}
  [data-testid="stWidgetLabel"] p {{ font-size: 1.1rem !important; font-weight: 600; }}
  [data-testid="stRadio"] label p, [data-baseweb="select"] div, [data-testid="stSliderThumbValue"],
  [data-testid="stSliderTickBarMin"], [data-testid="stSliderTickBarMax"] {{ font-size: 19px !important; }}
</style>""")
pg = st.navigation([
    st.Page("home.py", title="Overview", icon=":material/home:", default=True),
    st.Page("app_bq1.py", title="BQ1 · Early detection", icon=":material/notifications_active:"),
    st.Page("app_bq2.py", title="BQ2 · Tactic change", icon=":material/trending_up:"),
], position="top")
pg.run()
