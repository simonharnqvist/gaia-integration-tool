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

    joined = left_p.join(
        right_p,
        on=left_p[f"{left_key}"] == right_p[f"{right_key}"],
        how="inner",
    )

    joined = joined.drop(f"{right_prefix}_{right_key}")

    joined = collapse_prefixed_columns(joined, left_prefix, right_prefix)

    return joined
