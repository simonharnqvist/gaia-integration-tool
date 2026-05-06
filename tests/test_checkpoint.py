from gaia_integration_tool.ingestion.checkpoint import (
    ensure_checkpoint_table,
    is_processed,
    mark_processed,
)

def test_checkpoint_lifecycle(spark, tmp_path):
    cp = tmp_path / "checkpoint"
    ensure_checkpoint_table(spark, str(cp))

    assert not is_processed(spark, "file1")

    mark_processed(spark, "file1", "success")
    assert is_processed(spark, "file1")
