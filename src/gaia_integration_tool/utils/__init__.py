def is_table_bucketed(spark, table_name: str) -> dict:
    """
    Check if a Spark table is bucketed using DESCRIBE EXTENDED.
    
    Args:
        spark: SparkSession
        table_name: fully qualified table name (e.g., 'db.table' or 'table')
    
    Returns:
        dict with keys: is_bucketed, num_buckets, bucket_columns, sort_columns
    """
    desc_df = spark.sql(f"DESCRIBE EXTENDED {table_name}")
    desc = {row["col_name"].strip(): row["data_type"].strip() for row in desc_df.collect()}

    num_buckets = int(desc.get("Num Buckets", "0"))
    bucket_cols = desc.get("Bucket Columns", "[]")
    sort_cols   = desc.get("Sort Columns", "[]")

    return {
        "is_bucketed":     num_buckets > 0,
        "num_buckets":     num_buckets,
        "bucket_columns":  bucket_cols,
        "sort_columns":    sort_cols,
    }