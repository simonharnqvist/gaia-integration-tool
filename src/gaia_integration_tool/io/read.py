from pyspark.sql import SparkSession, DataFrame
from pathlib import Path


def read_parquet(spark: SparkSession, path: str | Path) -> DataFrame:
    """
    Read a Parquet dataset from the given path.
    """
    return spark.read.parquet(str(path))


def read_csv(
    spark: SparkSession,
    path: str | Path,
    delimiter: str = ",",
    header: bool = True,
    schema=None,
) -> DataFrame:
    """
    Read a CSV dataset with optional schema and delimiter.
    """
    reader = spark.read.options(sep=delimiter, header=header)
    if schema is not None:
        reader = reader.schema(schema)
    return reader.csv(str(path))


def read_unl(
    spark: SparkSession,
    path: str | Path,
    schema,
    delimiter: str = "|",
) -> DataFrame:
    """
    Read a UNL-style dataset (pipe-delimited, no header).
    """
    return spark.read.csv(
        str(path),
        sep=delimiter,
        header=False,
        schema=schema,
    )
