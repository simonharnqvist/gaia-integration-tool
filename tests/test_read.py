from pyspark.sql import Row
from gaia_integration_tool.io.read import read_parquet

def test_read_parquet(spark, tmp_path):
    path = tmp_path / "p"
    df = spark.createDataFrame([Row(a=1)])
    df.write.parquet(str(path))

    out = read_parquet(spark, str(path))
    assert out.collect() == df.collect()
