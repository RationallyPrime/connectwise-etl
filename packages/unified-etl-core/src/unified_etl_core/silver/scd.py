# unified_etl/silver/scd.py
import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from unified_etl.utils import logging
from unified_etl.utils.exceptions import SCDHandlingError


def apply_scd_type_1(
    df: DataFrame, business_keys: list[str], timestamp_col: str = "SystemModifiedAt"
) -> DataFrame:
    """
    Apply Type 1 SCD handling (overwrite).

    Args:
        df: DataFrame to process
        business_keys: List of business key columns
        timestamp_col: Column name for the modification timestamp

    Returns:
        DataFrame with only the latest version of each record
    """
    try:
        # Create a window partitioned by business keys
        window = Window.partitionBy(*business_keys).orderBy(F.desc(timestamp_col))

        # Add row number within each partition
        df_with_row_num = df.withColumn("row_num", F.row_number().over(window))

        # Keep only the latest version (row_num = 1)
        result_df = df_with_row_num.filter(F.col("row_num") == 1).drop("row_num")

        # Log stats
        total_count = df.count()
        distinct_keys = df.select(*business_keys).distinct().count()
        kept_count = result_df.count()

        logging.info(
            "Applied SCD Type 1 processing",
            total_records=total_count,
            distinct_keys=distinct_keys,
            kept_records=kept_count,
            removed_records=total_count - kept_count,
        )

        return result_df

    except Exception as e:
        error_msg = f"Failed to apply SCD Type 1 handling: {e!s}"
        logging.error(error_msg, business_keys=business_keys)
        raise SCDHandlingError(error_msg) from e


def apply_scd_type_2(
    df: DataFrame,
    business_keys: list[str],
    timestamp_col: str = "SystemModifiedAt",
    current_flag_col: str = "IsCurrent",
    effective_from_col: str = "EffectiveFrom",
    effective_to_col: str = "EffectiveTo",
    version_col: str = "Version",
    hash_col: str = "data_hash",
) -> DataFrame:
    """
    Apply Type 2 SCD handling (track history).

    Args:
        df: DataFrame to process
        business_keys: List of business key columns
        timestamp_col: Column name for the modification timestamp
        current_flag_col: Column name for the current flag
        effective_from_col: Column name for effective from date
        effective_to_col: Column name for effective to date
        version_col: Column name for version number
        hash_col: Column name for data hash used for change detection

    Returns:
        DataFrame with SCD Type 2 handling applied
    """
    try:
        # Create source key by concatenating business keys
        source_key_col = "SourceKey"
        source_key_expr = F.concat_ws("|", *[F.col(key) for key in business_keys])

        # Create a list of non-key columns to include in the hash
        # Exclude SCD tracking columns and business keys
        scd_cols = [current_flag_col, effective_from_col, effective_to_col, version_col, hash_col]
        non_key_cols = [
            col for col in df.columns if col not in business_keys and col not in scd_cols
        ]

        # Generate hash of non-key columns for change detection
        df_with_hash = df.withColumn(
            hash_col, F.sha2(F.concat_ws("||", *[F.col(c) for c in non_key_cols]), 256)
        )

        # Add source key
        df_with_key = df_with_hash.withColumn(source_key_col, source_key_expr)

        # Create window spec for version and dates
        window_spec = Window.partitionBy(source_key_col).orderBy(timestamp_col)

        # Add SCD Type 2 columns
        result_df = (
            df_with_key.withColumn(version_col, F.row_number().over(window_spec))
            .withColumn(effective_from_col, F.col(timestamp_col))
            .withColumn(effective_to_col, F.lead(timestamp_col).over(window_spec))
            .withColumn(current_flag_col, F.col(effective_to_col).isNull())
        )

        # Log stats
        total_count = df.count()
        distinct_keys = result_df.select(source_key_col).distinct().count()
        current_count = result_df.filter(F.col(current_flag_col)).count()

        logging.info(
            "Applied SCD Type 2 processing",
            total_records=total_count,
            distinct_entities=distinct_keys,
            current_versions=current_count,
            historical_versions=total_count - current_count,
        )

        return result_df

    except Exception as e:
        error_msg = f"Failed to apply SCD Type 2 handling: {e!s}"
        logging.error(error_msg, business_keys=business_keys)
        raise SCDHandlingError(error_msg) from e
