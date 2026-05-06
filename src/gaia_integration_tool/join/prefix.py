from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def prefix_columns(df, prefix, exclude=None):
    exclude = set(exclude or [])
    return df.select(
        *[
            F.col(c).alias(f"{prefix}_{c}") if c not in exclude else F.col(c)
            for c in df.columns
        ]
    )