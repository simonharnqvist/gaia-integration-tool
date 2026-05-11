from pyspark.sql import DataFrame
from .prefix import prefix_columns
from .collapse import collapse_prefixed_columns

def join_catalogues(
    left: DataFrame,
    right: DataFrame,
    left_key: str,
    right_key: str,
    left_prefix: str,
    right_prefix: str,
) -> DataFrame:
    """
    Join two catalogues with prefixing, collision resolution,
    and identical-column collapsing.
    """
    left_p = prefix_columns(left, left_prefix, exclude=[left_key])
    right_p = prefix_columns(right, right_prefix, exclude=[right_key])

    if left_key not in left_p.columns:
        raise ValueError(f"Left key '{left_key}' not found in left DataFrame.")
    if right_key not in right_p.columns:
        raise ValueError(f"Right key '{right_key}' not found in right DataFrame.")

    joined = left_p.join(
        right_p,
        on=left_p[left_key] == right_p[right_key],
        how="inner",
    )

    joined = joined.drop(f"{right_prefix}_{right_key}")

    joined = collapse_prefixed_columns(joined, left_prefix, right_prefix)

    return joined
