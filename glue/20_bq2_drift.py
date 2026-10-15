"""Glue Spark job (BQ2): weekly drift and rule-health monitoring.

Input:  curated/transactions, curated/schemes, analytics/thresholds.json
Output: analytics/baseline_fingerprint, analytics/weekly_drift, analytics/weekly_detection

TODO (owner: ___):
- baseline fingerprint: scheme-type and payment-format mix over baseline weeks
- PSI of each later week vs baseline
- apply frozen thresholds per week; recall / precision / alert count
- on drop: retune on eligible weeks -> "stale thresholds" vs "uncovered pattern"
"""
