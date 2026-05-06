from pyspark.sql import Row
from gaia_integration_tool.io.write import write_parquet

def test_write_parquet(spark, tmp_path):
    path = tmp_path / "p"
    df = spark.createDataFrame([Row(a=1)])
    write_parquet(df, str(path))

    out = spark.read.parquet(str(path))
    assert out.collect() == df.collect()
