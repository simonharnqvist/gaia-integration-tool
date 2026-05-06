from pyspark.sql import Row
from gaia_integration_tool.join.compare import columns_identical

def test_columns_identical_true(spark):
    df = spark.createDataFrame([Row(a=1, b=1), Row(a=2, b=2)])
    assert columns_identical(df, "a", "b")

def test_columns_identical_false(spark):
    df = spark.createDataFrame([Row(a=1, b=2)])
    assert not columns_identical(df, "a", "b")
