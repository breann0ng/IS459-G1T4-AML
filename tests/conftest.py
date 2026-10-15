import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder.master("local[2]")
               .appName("aml-tests")
               .config("spark.sql.shuffle.partitions", "2")
               .getOrCreate())
    yield session
    session.stop()
