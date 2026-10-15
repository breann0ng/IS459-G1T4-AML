"""Streamlit dashboard over Athena.

BQ1 tab: recall / precision / time-to-detection / alerts-per-day scorecards,
         slowest scheme types & payment formats, threshold slider + speed-vs-accuracy chart,
         alert drill-down (scheme lookup via DynamoDB).
BQ2 tab: weekly scheme mix vs baseline, rule health per week, flagged weeks with reasons.

TODO (owner: ___)
"""
import streamlit as st

st.set_page_config(page_title="AML Monitoring", layout="wide")
st.title("AML Detection & Drift Monitoring")
st.info("Dashboard under construction.")
