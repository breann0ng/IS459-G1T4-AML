"""
saml_ramp_spark.py                                                       (BQ2, step 1)
Builds the BQ2 test world from SAML-D: criminals gradually ADOPT new tactics over time.

    SAML-D: Oztas et al. 2023, "Enhancing Anti-Money Laundering: Development of a Synthetic
    Transaction Monitoring Dataset", IEEE ICEBE, doi 10.1109/ICEBE59045.2023.00028

Why: in IBM HI-Small/Medium/Large the tactics barely change over time, and IBM's 8 patterns are
close relatives (fan-in, fan-out, gather-scatter and scatter-gather are built from the same
one-to-many / many-to-one shapes), so even a held-out pattern hardly hurts detection. SAML-D has
17 more varied typologies. Here a group of them ("emerging") is absent at the start and is
switched on step by step, so we can test whether (1) the change is detected, (2) detection
drops, and (3) retraining with the newly confirmed cases recovers it.

Run on EMR (SAML-D.csv already in s3://<bucket>/raw/saml/):
    spark-submit [S3 confs] saml_ramp_spark.py

Two datasets, both in the IBM file layout (so the UNCHANGED BQ1 steps read them):
    SAML-R  ramp:    emerging typologies 0% until TEST_FROM, then switched on gradually (DOSES)
    SAML-C  control: emerging typologies 0% for the whole period (nothing changes). Part 1 must
                     stay quiet here; any "change" it reports is a false alarm.
Honest payments and established laundering are identical in both.

"Switched on": each emerging scheme (typology x key account) gets a fixed random number u in
[0, 1). Its payments on a day are kept when u < dose(day). As the dose rises more schemes start,
and a scheme that has started keeps going (criminals adopting a tactic, not flickering).

Writes
    raw/transactions/set=<DS>/   the 11 IBM columns       (clean_transactions_spark.py reads)
    raw/patterns/set=<DS>/<DS>_Patterns.txt   answer key  (parse_patterns_spark.py reads)
    raw/accounts/set=<DS>/       IBM accounts layout      (build_features_v3_spark.py reads)
    analytics/bq2r_design/       per week x typology: payments in SAML-D, kept in each dataset

How each IBM field is made (approximations are marked ~; same as saml_to_ibm_spark.py)
    Timestamp         Date + Time, seconds dropped (IBM is to the minute)
    From/To Bank   ~  one bank per COUNTRY (SAML-D has bank locations, not bank IDs)
    Amount Received ~ Amount converted with FIXED approximate 2023 exchange rates
    Payment Format  ~ onto IBM's 7: Credit/Debit card -> Credit Card, Cross-border -> Wire,
                      Cash Deposit / Cash Withdrawal -> Cash, Cheque, ACH
    Answer key        one scheme = one typology x one key account (receiver for many-to-one
                      typologies, else sender), named after the SAML-D typology
    Owners          ~ none in SAML-D: every account is its own owner
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =============================================================================== SETTINGS
BUCKET = "pw-aml-g1t4"
ROOT = os.environ.get("AML_ROOT", f"s3://{BUCKET}")
SRC = f"{ROOT}/raw/saml/SAML-D.csv"
import os as _os, sys as _sys                # repo layout: bq2_common.py is in aws/jobs/bq2/
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "..", "bq2"))
import bq2_common as C     # emerging typologies, dates and doses are set in bq2_common.py
RAMP, CONTROL, EMERGING, TRAIN_TO, DOSES = C.RAMP, C.CONTROL, C.EMERGING, C.TRAIN_TO, C.DOSES
# =========================================================================================

FORMAT = {"Credit card": "Credit Card", "Debit card": "Credit Card", "Cheque": "Cheque", "ACH": "ACH",
          "Cross-border": "Wire", "Cash Deposit": "Cash", "Cash Withdrawal": "Cash"}
# GBP per 1 unit of each currency: fixed, approximate 2023 averages (only for Amount Received)
FX_GBP = {"UK pounds": 1.0, "Euro": 0.87, "US dollar": 0.80, "Swiss franc": 0.90, "Dirham": 0.22,
          "Turkish lira": 0.034, "Pakistani rupee": 0.0029, "Naira": 0.0013, "Yen": 0.0057,
          "Moroccan dirham": 0.079, "Mexican Peso": 0.045, "Albanian lek": 0.0079, "Indian rupee": 0.0098}
LAUNDERING = ["Fan_In", "Fan_Out", "Cycle", "Bipartite", "Stacked Bipartite", "Gather-Scatter",
              "Scatter-Gather", "Structuring", "Cash_Withdrawal", "Deposit-Send", "Smurfing",
              "Layered_Fan_In", "Layered_Fan_Out", "Behavioural_Change_1", "Behavioural_Change_2",
              "Single_large", "Over-Invoicing"]
GROUP_BY_RECEIVER = {"Fan_In", "Layered_Fan_In", "Gather-Scatter", "Smurfing"}   # many senders -> one

spark = SparkSession.builder.appName("BQ2SamlRamp").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


def write_text(path, text):
    """One plain text file at an exact name (parse_patterns_spark.py looks for *Patterns*.txt)."""
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(spark._jsc.hadoopConfiguration())
    out = fs.create(p, True)
    out.write(bytearray(text.encode("utf-8")))
    out.close()


def lit_map(d):
    return F.create_map(*[F.lit(x) for kv in d.items() for x in kv])


pattern_name = C.pattern_name


def dose_col(day):
    d = F.lit(0.0)
    for start, dose in DOSES:
        d = F.when(day >= F.lit(start).cast("date"), F.lit(dose)).otherwise(d)
    return d


def check_maps(raw):
    vals = lambda *c: {r[0] for col in c for r in raw.select(col).distinct().collect()}
    bad_fmt = vals("Payment_type") - set(FORMAT)
    bad_ccy = vals("Payment_currency", "Received_currency") - set(FX_GBP)
    types = vals("Laundering_type")
    bad_typ = {t for t in types if not t.startswith("Normal")} - set(LAUNDERING)
    missing = set(EMERGING) - types
    if bad_fmt or bad_ccy or bad_typ or missing:
        raise SystemExit(f"unmapped payment types {bad_fmt}, currencies {bad_ccy}, typologies "
                         f"{bad_typ}, or EMERGING not in the data {missing}: fix the settings")


def write_dataset(t, ds_name, keep):
    """t: converted SAML-D rows; keep: boolean column. Writes the three IBM-layout inputs."""
    k = t.filter(keep)
    (k.select(F.col("ts_raw").alias("Timestamp"), F.col("from_bank").alias("From Bank"),
              F.col("Sender_account").alias("Account"), F.col("to_bank").alias("To Bank"),
              F.col("Receiver_account").alias("Account.1"), F.col("amount_received").alias("Amount Received"),
              F.col("Received_currency").alias("Receiving Currency"), F.col("amount_paid").alias("Amount Paid"),
              F.col("Payment_currency").alias("Payment Currency"), F.col("payment_format").alias("Payment Format"),
              F.col("Is_laundering").alias("Is Laundering"))
      .repartition(16).write.mode("overwrite").option("header", True)
      .csv(f"{ROOT}/raw/transactions/set={ds_name}/"))

    # answer key: IBM BEGIN / END blocks, one scheme per typology x key account
    lines = ["ts_raw", "from_bank", "Sender_account", "to_bank", "Receiver_account", "amount_received",
             "Received_currency", "amount_paid", "Payment_currency", "payment_format"]
    la = k.filter(F.col("Is_laundering") == "1").select("ts", "Laundering_type", "key", *lines) \
          .toPandas().sort_values("ts")
    out = []
    for (ty, _), g in la.groupby(["Laundering_type", "key"], sort=False):
        name = pattern_name(ty)
        out.append(f"BEGIN LAUNDERING ATTEMPT - {name}:  SAML-D {ty}")
        out += [",".join([*(str(v) for v in row), "1"]) for row in g[lines].itertuples(index=False)]
        out.append(f"END LAUNDERING ATTEMPT - {name}")
        out.append("")
    write_text(f"{ROOT}/raw/patterns/set={ds_name}/{ds_name}_Patterns.txt", "\n".join(out))

    acc = (k.select(F.col("Sender_account").alias("acct"), F.col("from_bank").alias("bank"),
                    F.col("Sender_bank_location").alias("loc"))
            .union(k.select("Receiver_account", "to_bank", "Receiver_bank_location")).distinct())
    (acc.select(F.concat(F.col("loc"), F.lit(" Bank #"), F.col("bank")).alias("Bank Name"),
                F.col("bank").alias("Bank ID"), F.col("acct").alias("Account Number"),
                F.concat(F.lit("E"), F.col("acct")).alias("Entity ID"), F.lit("Unknown #1").alias("Entity Name"))
        .repartition(4).write.mode("overwrite").option("header", True)
        .csv(f"{ROOT}/raw/accounts/set={ds_name}/"))
    n_l = len(la)
    print(f"[ramp] {ds_name}: {k.count()} payments, {n_l} laundering, "
          f"{la.groupby(['Laundering_type', 'key']).ngroups} schemes")


def main():
    cols = ["Time", "Date", "Sender_account", "Receiver_account", "Amount", "Payment_currency",
            "Received_currency", "Sender_bank_location", "Receiver_bank_location", "Payment_type",
            "Is_laundering", "Laundering_type"]
    raw = (spark.read.option("header", True).schema(", ".join(f"`{c}` string" for c in cols)).csv(SRC)
           .select(*[F.trim(c).alias(c) for c in cols]))
    check_maps(raw)
    locs = sorted(r[0] for r in raw.select("Sender_bank_location").union(raw.select("Receiver_bank_location"))
                  .distinct().collect() if r[0])
    bank = {loc: str(i + 1) for i, loc in enumerate(locs)}

    amt = F.col("Amount").cast("double")
    fx = lit_map(FX_GBP)
    same_ccy = F.col("Payment_currency") == F.col("Received_currency")
    is_l = F.col("Is_laundering") == "1"
    ty = F.col("Laundering_type")
    key = F.when(ty.isin(list(GROUP_BY_RECEIVER)), F.col("Receiver_account")).otherwise(F.col("Sender_account"))
    u = (F.abs(F.xxhash64(ty, key)) % 1000000) / 1000000.0
    t = (raw.withColumn("ts", F.to_timestamp(F.concat_ws(" ", "Date", "Time"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("day", F.to_date("ts"))
            .withColumn("ts_raw", F.date_format("ts", "yyyy/MM/dd HH:mm"))
            .withColumn("from_bank", lit_map(bank)[F.col("Sender_bank_location")])
            .withColumn("to_bank", lit_map(bank)[F.col("Receiver_bank_location")])
            .withColumn("amount_paid", F.format_string("%.2f", amt))
            .withColumn("amount_received", F.when(same_ccy, F.format_string("%.2f", amt)).otherwise(
                F.format_string("%.2f", amt * fx[F.col("Payment_currency")] / fx[F.col("Received_currency")])))
            .withColumn("payment_format", lit_map(FORMAT)[F.col("Payment_type")])
            .withColumn("key", key)
            .withColumn("group", F.when(~is_l, "normal").when(ty.isin(EMERGING), "emerging")
                                  .otherwise("established"))
            .withColumn("dose", dose_col(F.col("day")))
            .withColumn("keep_ramp", (F.col("group") != "emerging") | (u < F.col("dose")))
            .withColumn("keep_ctrl", F.col("group") != "emerging")
            .cache())
    bad = t.filter(F.col("ts").isNull()).count()
    if bad:
        raise SystemExit(f"{bad} rows with an unreadable Date/Time: expected yyyy-MM-dd HH:mm:ss "
                         "(was SAML-D.csv re-saved from Excel? use the original Kaggle file)")
    print(f"[ramp] SAML-D: {t.count()} payments, {t.filter(is_l).count()} laundering; banks {bank}")
    print(f"[ramp] emerging {EMERGING}; training to {TRAIN_TO}; doses {DOSES}")

    write_dataset(t, RAMP, F.col("keep_ramp"))
    write_dataset(t, CONTROL, F.col("keep_ctrl"))

    wk = F.to_date(F.date_trunc("week", "ts"))
    design = (t.filter(is_l).groupBy(wk.alias("week_start"), "Laundering_type", "group")
               .agg(F.count(F.lit(1)).alias("payments_saml_d"),
                    F.sum(F.col("keep_ramp").cast("int")).alias("payments_ramp"),
                    F.sum(F.col("keep_ctrl").cast("int")).alias("payments_control"),
                    F.max("dose").alias("dose"))
               .withColumnRenamed("Laundering_type", "typology")
               .withColumn("week_start", F.date_format("week_start", "yyyy-MM-dd"))
               .withColumn("dataset", F.lit(RAMP)))
    design.repartition(1).write.mode("overwrite").partitionBy("dataset").parquet(f"{ROOT}/analytics/bq2r_design")
    s = design.groupBy("group").agg(F.sum("payments_saml_d").alias("saml_d"), F.sum("payments_ramp").alias("ramp"),
                                     F.sum("payments_control").alias("control")).toPandas()
    print("[ramp] laundering payments by group:\n" + s.to_string(index=False))


if __name__ == "__main__":
    main()
    spark.stop()
