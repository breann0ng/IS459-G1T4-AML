"""
clean_transactions_duckdb.py  -  laptop version of clean_transactions_spark.py (no Java, no Spark)

Same rules and same output layout as the Spark job, and the SAME txn_id for every row, so
curated data made here lines up with curated data made on AWS. Use it for local work
(notebooks, testing); keep clean_transactions_spark.py for the AWS pipeline.

    # in a notebook
    from clean_transactions_duckdb import clean
    dq = clean(source="../data", output="../data/curated", dataset="HI-Small")
    dq

    # or from a terminal
    python clean_transactions_duckdb.py --source data --output data/curated --dataset HI-Small

Reads   <source>/<dataset>_Trans.csv
Writes  <output>/transactions/dataset=<dataset>/week=<n>/*.parquet   (no label)
        <output>/labels/...        txn_id + is_laundering (grading only)
        <output>/quarantine/...    rejected rows + reason
        <output>/dq_report/...     counts per week
Re-running replaces that dataset's folders. Do NOT commit the output to GitHub.

Read it back with:
    duckdb.sql("SELECT * FROM read_parquet('../data/curated/transactions/**/*.parquet', hive_partitioning=true)")

Requires: pip install duckdb pandas
"""

import argparse
import os
import shutil
from datetime import datetime, timezone

import duckdb

ALLOWED_FORMATS = ["ACH", "Bitcoin", "Cash", "Cheque", "Credit Card", "Reinvestment", "Wire"]
RAW_COLS = ["ts_raw", "from_bank_raw", "from_account", "to_bank_raw", "to_account",
            "amount_received_raw", "receiving_currency", "amount_paid_raw",
            "payment_currency", "payment_format", "is_laundering_raw"]


def _write(con, query, out_dir, name, dataset, order_by=None):
    """Write a query as Parquet partitioned by dataset/week, replacing this dataset's folder."""
    target = os.path.join(out_dir, name)
    old = os.path.join(target, f"dataset={dataset}")
    if os.path.isdir(old):
        shutil.rmtree(old)
    os.makedirs(target, exist_ok=True)
    order = f" ORDER BY {order_by}" if order_by else ""
    con.execute(f"""COPY ({query}{order}) TO '{target}'
                    (FORMAT PARQUET, PARTITION_BY (dataset, week), OVERWRITE_OR_IGNORE,
                     FILENAME_PATTERN 'part-{{i}}')""")


def clean(source="data", output="data/curated", dataset="HI-Small",
          max_timestamp="2022-09-10 23:59", verbose=True):
    """Clean <source>/<dataset>_Trans.csv into <output>/. Returns the data-quality report (pandas).
    max_timestamp: rows after it are quarantined as AFTER_CUTOFF (None = keep everything)."""
    path = os.path.join(source, f"{dataset}_Trans.csv")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"{path} not found (working directory: {os.getcwd()})")
    run_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    fmts = ", ".join(f"'{f}'" for f in ALLOWED_FORMATS)
    cutoff = (f"WHEN txn_time IS NOT NULL AND txn_time > TIMESTAMP '{max_timestamp}:00' "
              f"THEN 'AFTER_CUTOFF'") if max_timestamp else ""

    # ---- read every column as text, columns named by position (the file has two "Account" columns)
    con.execute(f"""
        CREATE TEMP TABLE raw AS
        SELECT {", ".join(f"trim({c}) AS {c}" for c in RAW_COLS)},
               '{path}' AS source_file, '{dataset}' AS dataset
        FROM read_csv('{path}', header=true, all_varchar=true, names={RAW_COLS})""")

    # ---- parse (failed conversions become NULL) + week number (Monday weeks, week 1 = first week)
    con.execute(f"""
        CREATE TEMP TABLE parsed AS
        WITH p AS (
          SELECT *,
                 try_strptime(ts_raw, '%Y/%m/%d %H:%M')               AS txn_time,
                 TRY_CAST(from_bank_raw AS INTEGER)                   AS from_bank,
                 TRY_CAST(to_bank_raw AS INTEGER)                     AS to_bank,
                 upper(from_account)                                  AS from_account_u,
                 upper(to_account)                                    AS to_account_u,
                 TRY_CAST(amount_received_raw AS DECIMAL(28,8))       AS amount_received,
                 TRY_CAST(amount_paid_raw AS DECIMAL(28,8))           AS amount_paid,
                 TRY_CAST(is_laundering_raw AS INTEGER)               AS is_laundering
          FROM raw)
        SELECT * EXCLUDE (from_account_u, to_account_u),
               from_account_u AS from_account_c, to_account_u AS to_account_c,
               CAST(txn_time AS DATE) AS txn_date,
               coalesce(CAST(floor(date_diff('day',
                   (SELECT min(CAST(date_trunc('week', txn_time) AS DATE)) FROM p),
                   CAST(date_trunc('week', txn_time) AS DATE)) / 7) + 1 AS INTEGER), 0) AS week
        FROM p""")

    # ---- validation: first failing rule wins (same order as the Spark job)
    con.execute(f"""
        CREATE TEMP TABLE checked AS
        SELECT *, CASE
            {cutoff}
            WHEN txn_time IS NULL THEN 'BAD_TIMESTAMP'
            WHEN from_bank IS NULL OR to_bank IS NULL THEN 'BAD_BANK_ID'
            WHEN coalesce(from_account_c, '') = '' OR coalesce(to_account_c, '') = '' THEN 'BAD_ACCOUNT'
            WHEN regexp_matches(from_account_c, 'E\\+|\\.') OR regexp_matches(to_account_c, 'E\\+|\\.')
                 THEN 'CORRUPT_ACCOUNT_ID'
            WHEN amount_paid IS NULL OR amount_paid <= 0 OR amount_received IS NULL OR amount_received <= 0
                 THEN 'BAD_AMOUNT'
            WHEN coalesce(payment_currency, '') = '' OR coalesce(receiving_currency, '') = ''
                 THEN 'MISSING_CURRENCY'
            WHEN payment_format NOT IN ({fmts}) OR payment_format IS NULL THEN 'UNKNOWN_FORMAT'
            WHEN is_laundering IS NULL OR is_laundering NOT IN (0, 1) THEN 'BAD_LABEL'
          END AS reject_reason
        FROM parsed""")

    # ---- good rows: exact duplicates removed, txn_id = SHA-256 of the cleaned values
    # (string forms match Spark: 'yyyy-MM-dd HH:mm', bank as integer, amounts with 8 decimals)
    con.execute("""
        CREATE TEMP TABLE good AS
        WITH d AS (
          SELECT DISTINCT txn_time, txn_date, from_bank, from_account_c AS from_account,
                 to_bank, to_account_c AS to_account, amount_received, receiving_currency,
                 amount_paid, payment_currency, payment_format, is_laundering, dataset, week
          FROM checked WHERE reject_reason IS NULL),
        h AS (
          SELECT *, sha256(concat_ws('|', strftime(txn_time, '%Y-%m-%d %H:%M'),
                       CAST(from_bank AS VARCHAR), from_account, CAST(to_bank AS VARCHAR), to_account,
                       CAST(amount_paid AS VARCHAR), payment_currency,
                       CAST(amount_received AS VARCHAR), receiving_currency, payment_format)) AS _h
          FROM d),
        n AS (
          SELECT *, row_number() OVER (PARTITION BY _h ORDER BY is_laundering DESC) AS _rn FROM h)
        SELECT CASE WHEN _rn = 1 THEN _h ELSE _h || '-' || CAST(_rn AS VARCHAR) END AS txn_id,
               * EXCLUDE (_h, _rn),
               (from_bank = to_bank AND from_account = to_account) AS is_self_transfer,
               (payment_currency <> receiving_currency)             AS is_cross_currency
        FROM n""")

    _write(con, """SELECT txn_id, txn_time, txn_date, from_bank, from_account, to_bank, to_account,
                          amount_received, receiving_currency, amount_paid, payment_currency,
                          payment_format, is_self_transfer, is_cross_currency, dataset, week
                   FROM good""", output, "transactions", dataset, order_by="txn_time")
    _write(con, "SELECT txn_id, is_laundering, dataset, week FROM good", output, "labels", dataset)
    _write(con, f"""SELECT ts_raw, from_bank_raw, from_account, to_bank_raw, to_account,
                           amount_received_raw, receiving_currency, amount_paid_raw, payment_currency,
                           payment_format, is_laundering_raw, reject_reason, source_file, dataset, week,
                           '{run_at}' AS loaded_at
                    FROM checked WHERE reject_reason IS NOT NULL""", output, "quarantine", dataset)

    # ---- data-quality report (one row per week)
    dq_query = f"""
        WITH r AS (SELECT week, count(*) AS rows_raw,
                          count(*) FILTER (WHERE reject_reason IS NOT NULL) AS rows_quarantined,
                          count(*) FILTER (WHERE reject_reason IS NULL) AS rows_good
                   FROM checked GROUP BY week),
             q AS (SELECT week, CAST(to_json(map(list(reject_reason), list(n))) AS VARCHAR) AS reasons
                   FROM (SELECT week, reject_reason, count(*) AS n FROM checked
                         WHERE reject_reason IS NOT NULL GROUP BY ALL) GROUP BY week),
             g AS (SELECT week, count(*) AS rows_clean,
                          sum(CAST(is_self_transfer AS INT)) AS self_transfers,
                          sum(CAST(is_cross_currency AS INT)) AS cross_currency,
                          sum(CAST(payment_currency = receiving_currency
                                   AND amount_paid <> amount_received AS INT)) AS amount_mismatch,
                          sum(is_laundering) AS laundering_rows,
                          CAST(min(txn_time) AS VARCHAR) AS min_time,
                          CAST(max(txn_time) AS VARCHAR) AS max_time
                   FROM good GROUP BY week)
        SELECT '{dataset}' AS dataset, r.week, r.rows_raw, r.rows_quarantined,
               coalesce(q.reasons, '{{}}') AS quarantine_by_reason,
               r.rows_good - coalesce(g.rows_clean, 0) AS duplicates_removed,
               coalesce(g.rows_clean, 0) AS rows_clean, coalesce(g.self_transfers, 0) AS self_transfers,
               coalesce(g.cross_currency, 0) AS cross_currency,
               coalesce(g.amount_mismatch, 0) AS amount_mismatch,
               coalesce(g.laundering_rows, 0) AS laundering_rows, g.min_time, g.max_time,
               '{run_at}' AS run_at
        FROM r LEFT JOIN q USING (week) LEFT JOIN g USING (week)"""
    _write(con, dq_query, output, "dq_report", dataset)
    dq = con.execute(dq_query + " ORDER BY week").df()

    if verbose:
        raw_n, good_n = con.execute("SELECT count(*), count(*) FILTER (WHERE reject_reason IS NULL) "
                                    "FROM checked").fetchone()
        clean_n = con.execute("SELECT count(*) FROM good").fetchone()[0]
        print(f"[clean] {dataset}: raw={raw_n} quarantined={raw_n - good_n} "
              f"duplicates_removed={good_n - clean_n} clean={clean_n}  -> {output}")
    con.close()
    return dq


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Clean raw IBM AML transactions into curated Parquet (DuckDB).")
    ap.add_argument("--source", default="data")
    ap.add_argument("--output", default="data/curated")
    ap.add_argument("--dataset", default="HI-Small")
    ap.add_argument("--max-timestamp", default="2022-09-10 23:59", help='or "none"')
    a = ap.parse_args()
    mt = None if a.max_timestamp.lower() == "none" else a.max_timestamp
    print(clean(a.source, a.output, a.dataset, mt).to_string(index=False))