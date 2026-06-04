import shutil
import os
import pytest
import random
from collections import namedtuple
from gaia_integration_tool.utils.bucketing import is_correctly_bucketed, count_parquet_files
from gaia_integration_tool.io.write import bucket_save
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id


def make_table(n_rows, spark):
    columns = ["index", "col1", "col2", "col3"]
    data = [
        (
            random.choice(["A", "B", "C", "D"]),
            random.randint(1, 100),
            random.uniform(1.0, 100.0),
        )
        for _ in range(n_rows)
    ]
    df = spark.createDataFrame(data, schema=columns[1:])
    df = df.withColumn("index", monotonically_increasing_id())
    return df


def get_table_location(spark, table_name):
    result = spark.sql(f"DESCRIBE FORMATTED {table_name}").collect()
    for row in result:
        if row.col_name.strip() == "Location":
            return row.data_type.strip()
    raise RuntimeError(f"Could not find location for table {table_name}")


@pytest.mark.parametrize("n_rows,buckets,key", [(1000, 2, "index")])
def test_bucket_save(spark, n_rows, buckets, key):
    table_name = "test_bucketed_table"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    df = make_table(n_rows, spark)
    bucket_save(
        df = df, num_buckets=buckets, bucket_key=key, table_name=table_name, spark=spark
    )

    assert is_correctly_bucketed(table_name, spark, buckets, key)

    actual_path = get_table_location(spark, table_name)
    n_files = count_parquet_files(spark, table_name)
    assert n_files == buckets, f"Expected {buckets} parquet files, got {n_files}"