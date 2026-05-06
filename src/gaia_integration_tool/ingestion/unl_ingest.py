from pathlib import Path
from pyspark.sql import SparkSession
from tqdm import tqdm

from gaia_integration_tool.ingestion.checkpoint import (
    ensure_checkpoint_table,
    is_processed,
    mark_processed,
)

def ingest_unl_directory(
    spark: SparkSession,
    input_dir: str,
    output_parquet_dir: str,
    schema,
    checkpoint_path: str,
    delimiter: str = "|",
    pattern: str = "*.unl.bz2",
):
    """
    Generic UNL → Parquet ingestion engine.
    Applies schema, checkpointing, and append semantics.
    """
    ensure_checkpoint_table(spark, checkpoint_path)

    files = sorted(Path(input_dir).glob(pattern))
    if not files:
        print(f"No files matching {pattern} found in {input_dir}")
        return

    not_processed = [f for f in files if not is_processed(spark, f.name)]

    for f in tqdm(not_processed, desc="Ingesting UNL files"):
        filename = f.name
        full_path = str(f)

        try:
            df = spark.read.csv(
                full_path,
                sep=delimiter,
                header=False,
                schema=schema,
            )

            df.write.mode("append").parquet(output_parquet_dir)

            mark_processed(spark, filename, "success")

        except Exception as e:
            mark_processed(spark, filename, "error", str(e))
            raise
