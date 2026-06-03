from pyspark.sql import DataFrame
from pathlib import Path


def write_parquet(df: DataFrame, path: str | Path, mode: str = "overwrite"):
    """
    Write a DataFrame to Parquet at the given path.
    """
    df.write.mode(mode).parquet(str(path))


def save_bucketed(
    df: DataFrame,
    bucket_key: str,
    table_name: str,
    spark,
    num_buckets: int = 2048,
):
    """
    Write a DataFrame as a bucketed Parquet table.
    The caller must ensure the warehouse directory is configured in Spark.
    """
    warehouse = spark.conf.get("spark.sql.warehouse.dir")
    path = f"{warehouse}/{table_name}"

    spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")

    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )
    fs.delete(spark._jvm.org.apache.hadoop.fs.Path(path), True)

    (
        df.write
        .format("parquet")
        .mode("overwrite")
        .repartition(num_buckets, bucket_key) 
        .bucketBy(num_buckets, bucket_key)
        .sortBy(bucket_key)
        .option("path", path)
        .saveAsTable(table_name)
    )
