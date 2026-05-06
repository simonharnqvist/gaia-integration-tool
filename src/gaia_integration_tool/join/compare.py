from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def columns_identical(df: DataFrame, col1: str, col2: str) -> bool:
    """
    Return True if two columns contain identical values.
    """
    diff = df.select(
        F.count(F.when(F.col(col1) != F.col(col2), 1)).alias("d")
    ).collect()[0]["d"]
    return diff == 0
