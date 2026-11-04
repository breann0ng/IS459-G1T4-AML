"""
unclassified_profile.py
Round 3, step 1d: what does "unclassified" laundering look like?
(laundering payments that belong to no scheme in Patterns.txt: ~40% of laundering account-days,
and the current models almost never catch them)

Plain Python on the EMR primary node (reads only the small txn_features_sample):
    python3 unclassified_profile.py > unclassified_profile.txt

Compares three groups of payments, HI-Small + HI-Medium, all days:
    normal         label 0 (the 2% sample of normal payments)
    scheme         laundering in a named scheme
    unclassified   laundering in no scheme
on: payment format, currency, amount, timing, how new the pair is, and what the sender and
receiver accounts do. Also checks whether unclassified payments come from the SAME accounts as
scheme payments (if yes, account-level signals can reach them; if no, they are a separate
population that needs its own features).
Writes analytics/unclassified_profile (the same table) for the report.
"""

import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs

BUCKET = "pw-aml-g1t4"
ROOT = os.environ.get("AML_ROOT", f"s3://{BUCKET}")
DATASETS = ["HI-Small", "HI-Medium"]
PATTERNS = ["fan_in", "fan_out", "gather_scatter", "scatter_gather", "cycle",
            "bipartite", "stack", "random"]
FORMATS = ["ach", "bitcoin", "cash", "cheque", "credit_card", "reinvestment", "wire"]


def fs_path(uri):
    if uri.startswith("s3://"):
        fs = pafs.S3FileSystem(region=os.environ.get("AWS_REGION", "us-east-1"),
                               retry_strategy=pafs.AwsStandardS3RetryStrategy(max_attempts=20))
        return fs, uri[len("s3://"):]
    return pafs.FileSystem.from_uri(uri)


def main():
    fs, path = fs_path(f"{ROOT}/curated/txn_features_sample")
    d = ds.dataset(path, filesystem=fs, format="parquet", partitioning="hive")
    want = ["dataset", "date", "s_id", "r_id", "label", "pattern_label",
            "t_amt_rel", "t_hour", "t_round", "t_cross_ccy", "t_self", "t_same_bank",
            "t_pair_n_7d", "t_pair_age_d", "t_s_seq", "t_r_seq"] + [f"t_fmt_{f}" for f in FORMATS] + [
            "s_n_out", "s_n_receivers", "s_fanout_max_7d", "s_n_match_24h", "s_age_days",
            "s_active_days_14d", "r_n_in", "r_n_senders", "r_fanin_max_7d", "r_age_days",
            "s_n_currencies", "r_n_currencies"]
    cols = [c for c in want if c in d.schema.names]
    t = d.to_table(columns=cols, filter=ds.field("dataset").isin(DATASETS)).to_pandas()
    t["group"] = np.where(t["label"] == 0, "normal",
                          np.where(t["pattern_label"].isin(PATTERNS), "scheme", "unclassified"))
    g = t.groupby("group")
    print(f"payments in the sample: {g.size().to_dict()}  (normal = 2% sample of normal payments)\n")

    rows = []
    def add(section, name, fn):
        r = {"section": section, "measure": name}
        for k, x in g:
            r[k] = fn(x)
        rows.append(r)

    for f in FORMATS:
        if f"t_fmt_{f}" in t:
            add("payment format", f"share {f}", lambda x, f=f: x[f"t_fmt_{f}"].mean())
    add("payment", "share cross-currency", lambda x: x["t_cross_ccy"].mean())
    add("payment", "share same bank", lambda x: x["t_same_bank"].mean())
    add("payment", "share self-transfer", lambda x: x["t_self"].mean())
    add("payment", "share round amount (÷100)", lambda x: x["t_round"].mean())
    add("payment", "amount ÷ currency median, median", lambda x: x["t_amt_rel"].median())
    add("payment", "amount ÷ currency median, 90th pct", lambda x: x["t_amt_rel"].quantile(0.9))
    add("timing", "hour of day, median", lambda x: x["t_hour"].median())
    add("timing", "share at night (00–06)", lambda x: (x["t_hour"] < 6).mean())
    add("pair", "share first-ever payment between the pair", lambda x: (x["t_pair_age_d"] == 0).mean())
    add("pair", "payments by the pair in previous 7 days, mean", lambda x: x["t_pair_n_7d"].mean())
    add("pair", "position in sender's day, median", lambda x: x["t_s_seq"].median())
    for c, lab in [("s_n_out", "sender: payments out that day"), ("s_n_receivers", "sender: receivers that day"),
                   ("s_fanout_max_7d", "sender: max fan-out in 7 days"), ("s_n_match_24h", "sender: pass-through matches 24h"),
                   ("s_age_days", "sender: days since first seen"), ("s_active_days_14d", "sender: active days of last 14"),
                   ("s_n_currencies", "sender: currencies used that day"),
                   ("r_n_in", "receiver: payments in that day"), ("r_n_senders", "receiver: senders that day"),
                   ("r_fanin_max_7d", "receiver: max fan-in in 7 days"), ("r_age_days", "receiver: days since first seen"),
                   ("r_n_currencies", "receiver: currencies used that day")]:
        if c in t:
            add("accounts (median)", lab, lambda x, c=c: x[c].median())

    # do unclassified payments come from scheme accounts?
    sch = t[t["group"] == "scheme"]
    sch_acc = set(sch["s_id"]) | set(sch["r_id"])
    sch_acc_day = set(zip(sch["s_id"], sch["date"])) | set(zip(sch["r_id"], sch["date"]))
    add("overlap with schemes", "sender or receiver ever in a scheme payment",
        lambda x: (x["s_id"].isin(sch_acc) | x["r_id"].isin(sch_acc)).mean())
    add("overlap with schemes", "sender or receiver in a scheme payment the same day",
        lambda x: np.mean([(s, dd) in sch_acc_day or (r, dd) in sch_acc_day
                           for s, r, dd in zip(x["s_id"], x["r_id"], x["date"])]))

    out = pd.DataFrame(rows)[["section", "measure", "normal", "scheme", "unclassified"]]
    with pd.option_context("display.width", 200, "display.max_rows", 200, "display.max_colwidth", 60):
        for sec, part in out.groupby("section", sort=False):
            print(f"== {sec}")
            print(part.drop(columns="section").round(3).to_string(index=False))
            print()

    fs2, p2 = fs_path(f"{ROOT}/analytics/unclassified_profile")
    fs2.create_dir(p2, recursive=True)
    import pyarrow.parquet as pq
    pq.write_table(pa.Table.from_pandas(out, preserve_index=False), f"{p2}/profile.parquet", filesystem=fs2)
    print("saved analytics/unclassified_profile/profile.parquet")


if __name__ == "__main__":
    main()
