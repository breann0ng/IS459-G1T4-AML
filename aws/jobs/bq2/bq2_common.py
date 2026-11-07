"""
bq2_common.py                                                            (BQ2, shared settings)
Settings every BQ2 script shares, so the emerging typologies and dates are defined once.
Edit here, then rerun from saml_ramp_spark.py (the ramp datasets depend on them).
"""

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
JOBS = os.path.join(HERE, "..")                             # repo: aws/jobs/<load|clean|features|bq1|bq2>/
for p in [HERE] + [os.path.join(JOBS, d) for d in ("bq1", "features", "clean", "load")]:
    if os.path.isdir(p) and p not in sys.path:               # EMR Serverless: one flat zip, nothing to add
        sys.path.insert(0, p)

RAMP, CONTROL = "SAML-R", "SAML-C"
START = "2022-10-03"             # Monday of SAML-D's first week (data starts Fri 7 Oct 2022)
TRAIN_TO = "2023-01-29"          # end of the training period = Part 1 baseline (weeks 1-17)
FIRST_DOSE = "2023-02-27"        # first day emerging tactics appear (see DOSES)
DOSES = [("2023-01-30", 0.00),   # weeks 18-21: nothing new yet (a check for false alarms)
         ("2023-02-27", 0.10),   # weeks 22-25
         ("2023-03-27", 0.25),   # weeks 26-29
         ("2023-04-24", 0.50),   # weeks 30-33
         ("2023-05-22", 1.00)]   # week 34 to the end (23 Aug 2023)
# The emerging tactics: cash-based and amount-splitting laundering
EMERGING = ["Structuring", "Smurfing", "Cash_Withdrawal", "Deposit-Send"]

# Part 2 alert rules (on the deployed model, using confirmed cases)
LABEL_LAG_DAYS = 28              # a week's cases are confirmed (investigated) 4 weeks later
RECALL_DROP = 0.10               # R1: weekly recall below validation recall minus this
TYPOLOGY_DROP = 0.20             # R3: a known typology's recall below its validation recall minus this
NEW_TYPOLOGY_MIN = 20            # R2: confirmed cases of a typology the model never trained on ...
NEW_TYPOLOGY_RECALL = 0.50       #     ... with cumulative recall below this
MIN_WEEK_CASES = 30              # R1 only judges weeks with at least this many laundering payments
MIN_TYPOLOGY_CASES = 20          # R3 only judges a typology-week with at least this many
CONSECUTIVE = 2                  # a rule must hold this many weeks in a row to raise the alert


def pattern_name(typology):
    """Answer-key name. parse_patterns_spark.py accepts only letters and '-':
    Stacked Bipartite -> STACKED-BIPARTITE, Behavioural_Change_1 -> BEHAVIOURAL-CHANGE-A."""
    return typology.upper().replace("_", "-").replace(" ", "-").replace("1", "A").replace("2", "B")


def pattern_label(typology):
    """What the BQ1 tables call it: Stacked Bipartite -> stacked_bipartite."""
    return pattern_name(typology).lower().replace("-", "_")


EMERGING_LABELS = [pattern_label(t) for t in EMERGING]


def group_of(pattern):
    if pattern in ("none", None):
        return "normal"
    return "emerging" if pattern in EMERGING_LABELS else "established"


# ---- storage helpers (plain pyarrow, so the laptop report needs neither train_txn.py nor Linux)
ROOT = os.environ["AML_ROOT"]      # s3://<bucket>; no default, so a job never writes to the wrong bucket
ANALYTICS = f"{ROOT}/analytics"


def fs_path(uri):
    import pyarrow.fs as pafs
    if uri.startswith("s3://"):
        fs = pafs.S3FileSystem(region=os.environ.get("AWS_REGION", "us-east-1"),
                               connect_timeout=30, request_timeout=300,
                               retry_strategy=pafs.AwsStandardS3RetryStrategy(max_attempts=20))
        return fs, uri[len("s3://"):]
    return pafs.FileSystem.from_uri(uri)


def open_dataset(uri):
    import pyarrow.dataset as ds
    fs, path = fs_path(uri)
    return ds.dataset(path, filesystem=fs, format="parquet", partitioning="hive")


def put_text(uri, text):
    fs, p = fs_path(uri)
    fs.create_dir(p.rsplit("/", 1)[0], recursive=True)
    with fs.open_output_stream(p) as f:
        f.write(text.encode())


def get_text(uri):
    fs, p = fs_path(uri)
    with fs.open_input_stream(p) as f:
        return f.read().decode()


def deployed_info(dataset):
    return get_json(f"{ROOT}/models/bq2r/{dataset}/deployed.json", default={"version": "v0"})


def deployed_version(dataset):
    """Model version scoring/monitoring uses: models/bq2r/<dataset>/deployed.json (v0 until a deploy)."""
    return deployed_info(dataset)["version"]


def get_json(uri, default=None):
    try:
        return json.loads(get_text(uri))
    except (FileNotFoundError, OSError):
        if default is not None:
            return default
        raise
