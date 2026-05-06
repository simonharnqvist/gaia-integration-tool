from pyspark.sql import Row
from gaia_integration_tool.join.joiner import join_catalogues

def test_joiner_identical_columns(spark):
    left = spark.createDataFrame([Row(id=1, x=10)])
    right = spark.createDataFrame([Row(id=1, x=10)])
    out = join_catalogues(left, right, "id", "id", "l", "r")
    assert set(out.columns) == {"id", "x"}

def test_joiner_different_columns(spark):
    left = spark.createDataFrame([Row(id=1, x=10)])
    right = spark.createDataFrame([Row(id=1, x=99)])
    out = join_catalogues(left, right, "id", "id", "l", "r")
    assert set(out.columns) == {"id", "l_x", "r_x"}, "actual: {out.columns}"
