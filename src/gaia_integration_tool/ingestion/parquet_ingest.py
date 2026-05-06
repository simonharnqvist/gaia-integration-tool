from pyspark.sql import SparkSession, DataFrame
from pathlib import Path


def ingest_parquet(spark: SparkSession, path: str | Path) -> DataFrame:
    return spark.read.parquet(str(path))