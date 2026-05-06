from pyspark.sql import Row
from gaia_integration_tool.join.collapse import collapse_prefixed_columns

def test_collapse_identical(spark):
    df = spark.createDataFrame([Row(l_x=1, r_x=1)])
    out = collapse_prefixed_columns(df, "l", "r")
    assert set(out.columns) == {"x"}

def test_collapse_different(spark):
    df = spark.createDataFrame([Row(l_x=1, r_x=2)])
    out = collapse_prefixed_columns(df, "l", "r")
    assert set(out.columns) == {"l_x", "r_x"}
