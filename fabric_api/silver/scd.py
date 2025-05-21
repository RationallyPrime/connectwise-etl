import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from typing import List, Optional
from datetime import datetime

from core_etl.logging_utils import etl_logger
from delta.tables import DeltaTable

def apply_scd_type_2(
    spark: SparkSession,
    source_df: DataFrame, # This is the new data from the current batch
    target_table_path: str, # Full path to the target silver delta table
    business_keys: List[str],
    effective_date_col: str = "SilverEffectiveDate",
    end_date_col: str = "SilverEndDate",
    current_flag_col: str = "IsSilverCurrent",
    # SilverProcessedAt is expected to be on source_df from the batch run
    # SilverCreatedAt will be set by this function for new versions
) -> Optional[DataFrame]:
    """
    Applies SCD Type 2 logic to a source DataFrame against a target Delta table.

    Args:
        spark: SparkSession object.
        source_df: DataFrame containing new/changed data from the current batch. 
                   Expected to have a 'SilverProcessedAt' column from the batch.
        target_table_path: Full path to the target Silver Delta table.
        business_keys: List of column names that uniquely identify a business record.
        effective_date_col: Name of the column to store the effective start date of the record version.
        end_date_col: Name of the column to store the end date of the record version.
        current_flag_col: Name of the column to flag the current active version of a record.

    Returns:
        A DataFrame containing all rows that need to be appended to the target table.
        This includes:
        - New versions of records that have changed.
        - Old versions of records that have changed (with their end dates updated).
        - Completely new records.
        Returns None if an error occurs or no changes are to be made.
    """
    etl_logger.info(f"Starting SCD Type 2 processing for target table: {target_table_path}")
    etl_logger.info(f"Business keys: {business_keys}")

    if not business_keys:
        etl_logger.error("Business keys cannot be empty for SCD Type 2 processing.")
        return None
    
    if "SilverProcessedAt" not in source_df.columns:
        etl_logger.error("'SilverProcessedAt' column not found in source_df. This is required for SCD2.")
        # Add it here as a fallback, though it should come from the batch.
        source_df = source_df.withColumn("SilverProcessedAt", F.current_timestamp())
        # return None 

    far_future_date = datetime(9999, 12, 31)
    current_batch_timestamp_col = "SilverProcessedAt" # This is on source_df

    # Create a hash of all columns except business keys and metadata for change detection
    # Exclude known metadata/SCD columns from the hash calculation
    cols_to_hash = [c for c in source_df.columns if c not in business_keys + [current_batch_timestamp_col, "SilverCreatedAt", effective_date_col, end_date_col, current_flag_col]]
    if not cols_to_hash:
        etl_logger.warning(f"No columns found to hash for change detection in table {target_table_path} (source columns: {source_df.columns}, keys: {business_keys}). Assuming all non-key records are changes if keys match.")
        # If no columns to hash, any matched record is treated as unchanged unless keys are different (which means new record)
        # This scenario needs careful thought. For now, we'll proceed, and comparison will rely on key presence.
        # A more robust way would be to ensure there's always something to compare, or handle this state explicitly.
        source_df = source_df.withColumn("data_hash", F.lit(1)) # Dummy hash if no data columns
    else:
        etl_logger.debug(f"Columns to hash for source: {cols_to_hash}")
        source_df = source_df.withColumn("data_hash", F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in cols_to_hash]), 256))
    
    source_df = source_df.alias("source")

    # Check if target Delta table exists
    if not DeltaTable.isDeltaTable(spark, target_table_path):
        etl_logger.info(f"Target table {target_table_path} does not exist or is not a Delta table. Treating as initial load.")
        initial_load_df = (
            source_df.withColumn(effective_date_col, F.col(f"source.{current_batch_timestamp_col}"))
            .withColumn(end_date_col, F.lit(far_future_date).cast("timestamp"))
            .withColumn(current_flag_col, F.lit(True))
            .withColumn("SilverCreatedAt", F.col(f"source.{current_batch_timestamp_col}"))
            .select(
                "source.*", # Includes data_hash, SilverProcessedAt
                effective_date_col,
                end_date_col,
                current_flag_col,
                "SilverCreatedAt"
            )
        )
        etl_logger.info(f"Schema for initial_load_df for {target_table_path}:")
        initial_load_df.printSchema()
        return initial_load_df.drop("data_hash") # Drop hash before returning

    # Target table exists, read current records
    etl_logger.info(f"Reading current records from target table {target_table_path}")
    target_current_df = DeltaTable.forPath(spark, target_table_path).toDF().filter(F.col(current_flag_col) == True).alias("target")
    
    # Hash columns for target must match source logic
    # Ensure cols_to_hash are present in target, if not, it implies schema drift or first run with new columns
    target_cols_to_hash = [c for c in cols_to_hash if c in target_current_df.columns]
    if len(target_cols_to_hash) != len(cols_to_hash):
        etl_logger.warning(f"Schema difference in hashable columns between source and target {target_table_path}. Source hash cols: {cols_to_hash}, Target compatible hash cols: {target_cols_to_hash}")
        # If target is missing some hashable columns, its hash will be different.
        # This will correctly mark records as changed if new columns with data appear in source.
        # If source is missing columns present in target, those won't be in source's cols_to_hash.
    
    if not target_cols_to_hash: # Similar to source, if no common hashable columns
        target_current_df = target_current_df.withColumn("data_hash", F.lit(1))
    else:
        etl_logger.debug(f"Columns to hash for target: {target_cols_to_hash}")
        target_current_df = target_current_df.withColumn("data_hash", F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in target_cols_to_hash]), 256))

    # Join condition for business keys
    join_condition = F.lit(True)
    for key_col in business_keys:
        join_condition = join_condition & (F.col(f"source.{key_col}") == F.col(f"target.{key_col}"))

    # Full outer join to find new, changed, and potentially deleted (though not handled explicitly for deletion here)
    merged_df = source_df.join(
        target_current_df,
        on=join_condition,
        how="fullouter"
    )
    
    etl_logger.info(f"Schema for merged_df for {target_table_path}:")
    merged_df.printSchema() # Log schema to debug joins

    # Records that are new (in source, not in target)
    # source.any_key IS NOT NULL AND target.any_key IS NULL
    # Use first business key to check for nullity on target side
    new_records_filter = F.col(f"source.{business_keys[0]}").isNotNull() & F.col(f"target.{business_keys[0]}").isNull()
    new_records_df = merged_df.filter(new_records_filter).select(
        "source.*", # all columns from source_df, including its data_hash and SilverProcessedAt
        F.col(f"source.{current_batch_timestamp_col}").alias(effective_date_col),
        F.lit(far_future_date).cast("timestamp").alias(end_date_col),
        F.lit(True).alias(current_flag_col),
        F.col(f"source.{current_batch_timestamp_col}").alias("SilverCreatedAt")
    )
    etl_logger.info(f"Found {new_records_df.count()} new records for {target_table_path}.")


    # Records that existed and might have changed (in source and in target)
    # source.key IS NOT NULL AND target.key IS NOT NULL
    existing_records_filter = F.col(f"source.{business_keys[0]}").isNotNull() & F.col(f"target.{business_keys[0]}").isNotNull()
    
    # Changed records: data_hash is different
    changed_records_filter = existing_records_filter & (F.col("source.data_hash") != F.col("target.data_hash"))
    
    # New versions of changed records (from source)
    new_versions_of_changed_df = merged_df.filter(changed_records_filter).select(
        "source.*", # all columns from source_df
        F.col(f"source.{current_batch_timestamp_col}").alias(effective_date_col),
        F.lit(far_future_date).cast("timestamp").alias(end_date_col),
        F.lit(True).alias(current_flag_col),
        F.col(f"source.{current_batch_timestamp_col}").alias("SilverCreatedAt") # New version, so new SilverCreatedAt
    )
    etl_logger.info(f"Found {new_versions_of_changed_df.count()} new versions of changed records for {target_table_path}.")

    # Expired versions of changed records (from target)
    # These are the old versions that need to be closed out.
    # Select original target columns, but update SCD control columns.
    # Important: Select all original columns from 'target' that should be preserved for the historical record.
    target_original_cols = [F.col(f"target.{c}").alias(c) for c in target_current_df.columns if c != "data_hash"] # Exclude target's data_hash

    expired_versions_df = merged_df.filter(changed_records_filter).select(
        *target_original_cols, # Keep original values, including original SilverCreatedAt
        F.col(f"source.{current_batch_timestamp_col}").alias(end_date_col), # End date is current batch processing time
        F.lit(False).alias(current_flag_col),
        F.col(f"source.{current_batch_timestamp_col}").alias("SilverProcessedAt") # This record version is processed (closed) now
        # EffectiveDate remains original effective date of this version
        # SilverCreatedAt remains original SilverCreatedAt of this version
    )
    # Ensure effective_date_col is present, it should be from target_original_cols
    if effective_date_col not in expired_versions_df.columns and f"target.{effective_date_col}" in merged_df.columns:
         expired_versions_df = expired_versions_df.withColumn(effective_date_col, F.col(f"target.{effective_date_col}"))


    etl_logger.info(f"Found {expired_versions_df.count()} versions to expire for {target_table_path}.")

    # Records to be closed because they are no longer in source (soft delete)
    # target.key IS NOT NULL AND source.key IS NULL
    # This implementation does not explicitly handle soft deletes for records disappearing from source.
    # If needed, similar logic to "expired_versions_df" would be used,
    # where filter is F.col(f"target.{business_keys[0]}").isNotNull() & F.col(f"source.{business_keys[0]}").isNull()
    # and end_date_col is set to current_batch_timestamp.

    # Union the DataFrames: new records, new versions of changed, and expired versions of changed
    # Ensure consistent column order and selection for union
    # All DFs to be unioned should have the same schema structure as the target table + new SCD columns.
    
    # Define the final list of columns expected in the target silver table after SCD2 processing
    # This should be source_df's original columns (without its data_hash) + the SCD2 control columns + SilverCreatedAt
    final_columns = [c for c in source_df.columns if c not in ["data_hash"]] \
                    + [effective_date_col, end_date_col, current_flag_col, "SilverCreatedAt"]
    
    # Select and align columns for each part
    new_records_df = new_records_df.select(*final_columns)
    new_versions_of_changed_df = new_versions_of_changed_df.select(*final_columns)
    
    # For expired_versions_df, it's based on target's schema. We need to align it.
    # Target schema + new end_date, current_flag, SilverProcessedAt
    # Original SilverCreatedAt and EffectiveDate are preserved from target.
    # It should have all columns from `final_columns` list.
    # `target_original_cols` already selected all of target's columns.
    # We added `end_date_col`, `current_flag_col`, `SilverProcessedAt`.
    # Need to ensure `SilverCreatedAt` is there (it should be from target) and `effective_date_col`
    
    # Reconstruct expired_versions_df with explicit selection to ensure schema alignment
    # Keep the original SilverCreatedAt and SilverEffectiveDate from the version being closed.
    expired_versions_df_aligned = merged_df.filter(changed_records_filter).select(
        *[F.col(f"target.{c}") for c in target_current_df.columns if c not in [end_date_col, current_flag_col, "SilverProcessedAt", "data_hash"]], # Select original values from target
        F.col(f"source.{current_batch_timestamp_col}").alias(end_date_col),
        F.lit(False).alias(current_flag_col),
        F.col(f"source.{current_batch_timestamp_col}").alias("SilverProcessedAt") # This version's processing is this batch
    )
    # Ensure all final_columns are present, adding nulls if any are missing (e.g. if a new column was added to source and is now in final_columns)
    for col_name in final_columns:
        if col_name not in expired_versions_df_aligned.columns:
            etl_logger.warning(f"Column {col_name} missing in expired_versions_df_aligned for {target_table_path}. Adding as null. This might indicate schema drift or an issue.")
            expired_versions_df_aligned = expired_versions_df_aligned.withColumn(col_name, F.lit(None))
    expired_versions_df_aligned = expired_versions_df_aligned.select(*final_columns)


    # Combine all records that need to be written
    df_to_append = new_records_df.unionByName(new_versions_of_changed_df, allowMissingColumns=True)
    df_to_append = df_to_append.unionByName(expired_versions_df_aligned, allowMissingColumns=True)

    rows_to_append_count = df_to_append.count()
    etl_logger.info(f"Total rows to append for {target_table_path} (new + changed_new_version + changed_old_version_expired): {rows_to_append_count}")

    if rows_to_append_count == 0:
        etl_logger.info(f"No changes, new records, or records to expire for {target_table_path}. Nothing to append.")
        return None # Or return an empty DataFrame with the correct schema: spark.createDataFrame([], schema=df_to_append.schema)

    etl_logger.info(f"Final schema for df_to_append for {target_table_path}:")
    df_to_append.printSchema()
    return df_to_append
