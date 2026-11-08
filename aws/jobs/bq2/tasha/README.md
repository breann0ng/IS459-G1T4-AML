# BQ2 · HI-Large tab (Tasha)

Joey's BQ2 method (Part 1 tactics, Part 2 rules R1-R3) on IBM HI-Large, plus the fan-in / fan-out
new-tactic test. Details and results: [`docs/bq2.md`](../../../../docs/bq2.md#hi-large-tab-joeys-method-on-ibm--the-fan-in--fan-out-new-tactic-test-tasha).

| File | Runs on | Does |
|---|---|---|
| `run_bq2_ibm.sh` | EMR primary | runs everything in order; resumable (`progress/bq2/`) |
| `check_cluster.sh` | EMR primary | checks Java 17, libraries, bucket and scripts before the run |
| `bq2_ibm_score_spark.py` | Spark | weekly HI-Large scores with the BQ1 model and the model that never saw fan-in / fan-out |
| `bq2_ibm_part2_prep_spark.py` | Spark | scores + validation baselines in Part 2's format, so `../bq2_part2_eval.py` runs unchanged |
| `bq2_ibm_report.py` | laptop | `results/bq2/bq2_ibm_report.txt` + `app/data/bq2_ibm_data.json` (HI-Large tab) |

Uses from `../`: `bq2_part1_tactics_spark.py` (switch `BQ2_HIDE`), `bq2_part2_eval.py`, `bq2_common.py`
(switches `BQ2_EMERGING`, `BQ2_FIRST_DOSE`); all switches are off by default.
