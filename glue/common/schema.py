"""Shared schema and column helpers for the IBM AML transaction files.

The raw *_Trans.csv header has two columns both named "Account", so we never
rely on the header: we read with an explicit schema and these names instead.
"""
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

RAW_TRANS_SCHEMA = StructType([
    StructField("timestamp_str", StringType()),
    StructField("from_bank", StringType()),
    StructField("from_account", StringType()),
    StructField("to_bank", StringType()),
    StructField("to_account", StringType()),
    StructField("amount_received", DoubleType()),
    StructField("receiving_currency", StringType()),
    StructField("amount_paid", DoubleType()),
    StructField("payment_currency", StringType()),
    StructField("payment_format", StringType()),
    StructField("is_laundering", IntegerType()),
])

TIMESTAMP_FORMAT = "yyyy/MM/dd HH:mm"


def account_key(bank: Column, account: Column) -> Column:
    """Composite key: account numbers are only unique within a bank."""
    return F.concat_ws("_", bank, account)


def txn_key(df_ts: Column, from_key: Column, to_key: Column, amount_paid: Column) -> Column:
    """Key used to match a transaction to a scheme row in DynamoDB / Parquet."""
    return F.concat_ws("|", F.date_format(df_ts, "yyyy-MM-dd HH:mm"), from_key, to_key,
                       F.format_number(amount_paid, 2))


def week_start(ts: Column) -> Column:
    """Monday of the week containing ts (weeks run Monday–Sunday)."""
    return F.date_trunc("week", ts).cast("date")
