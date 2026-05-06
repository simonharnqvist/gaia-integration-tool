from pyspark.sql import Row
from gaia_integration_tool.join.prefix import prefix_columns

def test_prefix_columns(spark):
    df = spark.createDataFrame([Row(id = 0, a=1, b=2)])
    out = prefix_columns(df, "p", exclude=["id"])
    assert set(out.columns) == {"id", "p_a", "p_b"}
