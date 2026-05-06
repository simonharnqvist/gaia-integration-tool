import csv
from gaia_integration_tool.ingestion.unl_ingest import ingest_unl_directory
from pyspark.sql.types import StructType, StructField, IntegerType
import bz2

def test_unl_ingest(spark, tmp_path):
    raw = tmp_path / "raw"
    out = tmp_path / "out"
    cp = tmp_path / "cp"
    raw.mkdir()

    f = raw / "test.unl.bz2"
    with bz2.open(f, "wt") as fh:
        fh.write("1|2\n3|4\n")

    schema = StructType([
        StructField("a", IntegerType()),
        StructField("b", IntegerType()),
    ])

    ingest_unl_directory(
        spark=spark,
        input_dir=str(raw),
        output_parquet_dir=str(out),
        schema=schema,
        checkpoint_path=str(cp),
        delimiter="|",
        pattern="*.unl.bz2",
    )

    df = spark.read.parquet(str(out))
    assert df.count() == 2
    assert set(df.columns) == {"a", "b"}
