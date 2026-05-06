from gaia_integration_tool.ingestion.parquet_ingest import ingest_parquet
from pyspark.sql import Row

def test_parquet_ingest(spark, tmp_path):
    path = tmp_path / "p"
    df = spark.createDataFrame([Row(a=1)])
    df.write.parquet(str(path))

    out = ingest_parquet(spark, str(path))
    assert out.collect() == df.collect()
