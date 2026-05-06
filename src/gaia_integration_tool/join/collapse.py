from pyspark.sql import DataFrame
from .compare import columns_identical

def collapse_prefixed_columns(df: DataFrame, left_prefix: str, right_prefix: str) -> DataFrame:
    """
    Collapse prefixed columns:
      - If identical, keep a single unprefixed column.
      - If different, keep both prefixed columns.
    """
    left_cols = [c for c in df.columns if c.startswith(f"{left_prefix}_")]
    right_cols = [c for c in df.columns if c.startswith(f"{right_prefix}_")]

    left_base = {c[len(left_prefix) + 1:]: c for c in left_cols}
    right_base = {c[len(right_prefix) + 1:]: c for c in right_cols}

    shared = set(left_base).intersection(right_base)

    for base in shared:
        lcol = left_base[base]
        rcol = right_base[base]

        if columns_identical(df, lcol, rcol):
            df = df.drop(rcol)
            df = df.withColumnRenamed(lcol, base)

    return df
