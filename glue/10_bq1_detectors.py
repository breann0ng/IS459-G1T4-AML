"""Glue Spark job (BQ1): fan-in / fan-out / bounded-cycle detectors + threshold sweep.

Input:  curated/transactions, curated/schemes
Output: analytics/sweep_results, analytics/alert_log, analytics/thresholds.json

TODO (owner: ___):
- distinct counterparties per account per sliding window
- apply rules per threshold, record first alert time per account
- precision / recall / time-to-detection vs known schemes
- pick thresholds on validation weeks, freeze, evaluate on test weeks
"""
