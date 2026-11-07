"""
BQ1 site: the Overview landing page plus the BQ1 results page, with the menu across the top.
    streamlit run bq1_site.py
Not in the menu: app_bq2.py (runs on its own until the BQ2 team decides to join) and app_stage2.py (the stage 2
experiment log: tuning runs on validation data, kept for the report, not for the Tuning Team).
"""
import streamlit as st

st.set_page_config(page_title="AML · G1T4", layout="wide")
# centre the top menu in the header (Streamlit puts it on the left)
st.html("""<style>
  [data-testid="stTopNav"] { position: absolute; left: 50%; transform: translateX(-50%);
                             width: auto !important; justify-content: center; }
</style>""")
pg = st.navigation([
    st.Page("home.py", title="Overview", icon=":material/home:", default=True),
    st.Page("app_bq1.py", title="BQ1 · Early detection", icon=":material/notifications_active:"),
], position="top")
pg.run()
