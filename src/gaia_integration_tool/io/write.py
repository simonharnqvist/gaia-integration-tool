from pyspark import sql
from pathlib import Path
from gaia_integration_tool.utils.bucketing import sanitise_identifier, check_table_is_in_catalog, delete_table_location, table_location_exists
from pyspark.sql import DataFrame, SparkSession


def write_parquet(df: DataFrame, path: str | Path, mode: str = "overwrite"):
    """
    Write a DataFrame to Parquet at the given path.
    """
    df.write.mode(mode).parquet(str(path))


def bucket_save(
        df: sql.DataFrame,
        bucket_key: str,
        table_name: str,
        spark: sql.SparkSession,
        num_buckets: int,
        overwrite: bool = False
) -> None:
    """Save table to Spark warehouse, bucketed by column 'bucket_key' into 'num_buckets' buckets.

    Args:
        df (DataFrame): Source data.
        num_buckets (int): Number of desired buckets.
        bucket_key (str): Column to make buckets on.
        table_name (str): Name of table in Spark warehouse.
        spark (SparkSession): Spark instance.
        overwrite(bool): Force overwrite if table exists?

    Raises:
        ValueError: if number of buckets is not a positive integer.
    """

    key = sanitise_identifier(bucket_key)
    table_name = sanitise_identifier(table_name)

    if not isinstance(num_buckets, int) or num_buckets == 0:
        raise ValueError("num_buckets must be integer > 0")

    table_exists = spark.catalog.tableExists(table_name)
    location_exists = table_location_exists(spark = spark, table_name = table_name)

    if (table_exists or location_exists) and not overwrite:
        raise RuntimeError(
            f"Table '{table_name}' already exists. Pass overwrite=True to replace it."
        )

    if (table_exists or location_exists) and overwrite: # redundant 'and overwrite' for safety
        spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")
        delete_table_location(spark = spark, table_name = table_name)

    if not (table_exists or location_exists):
        print(f"table {table_name} does not exist")


    df = df.repartition(num_buckets, key)
    df.createOrReplaceTempView("tmp_view")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING PARQUET
        CLUSTERED BY ({key}) INTO {num_buckets} BUCKETS
        AS SELECT * FROM tmp_view
    """
    )