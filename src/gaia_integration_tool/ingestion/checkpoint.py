from pyspark.sql import SparkSession

def ensure_checkpoint_table(spark: SparkSession, checkpoint_path: str):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS ingest_checkpoint (
            filename STRING,
            status STRING,
            error STRING
        )
        USING parquet
        LOCATION '{checkpoint_path}'
    """)

def is_processed(spark: SparkSession, filename: str) -> bool:
    checkpoint = spark.table("ingest_checkpoint")
    df = checkpoint.filter(
        (checkpoint.filename == filename) &
        (checkpoint.status == "success")
    ).limit(1)
    return df.count() > 0

def mark_processed(spark, filename: str, status: str, error: str | None = None):
    df = spark.createDataFrame(
        [(filename, status, error or "")],
        ["filename", "status", "error"],
    )
    df.write.mode("append").insertInto("ingest_checkpoint")
