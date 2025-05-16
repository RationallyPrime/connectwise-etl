from datetime import datetime as dt
from datetime import timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.column import Column

from bc_etl.bronze.reader import read_bc_table
from bc_etl.silver.cleanse import apply_data_types
from bc_etl.silver.scd import apply_scd_type_1, apply_scd_type_2
from bc_etl.silver.standardize import get_silver_table_name, standardize_dataframe_from_config
from bc_etl.utils import logging, watermark_manager
from bc_etl.utils.config_loader import get_table_config
from bc_etl.utils.config_utils import extract_table_config
from bc_etl.utils.naming import standardize_table_reference


def get_incremental_filter(
    df: DataFrame,
    last_processed_time: dt | None = None,
    lookback_days: int | None = None,
    table_base_name: str | None = None,
) -> Column:
    """
    Create a filter condition for incremental loading based on timestamp columns.

    Args:
        df: DataFrame to filter
        last_processed_time: Optional timestamp of last successful processing
        lookback_days: Optional number of days to look back (overrides config default)
        table_base_name: Optional table base name to get incremental column from config

    Returns:
        PySpark Column expression for filtering
    """
    # If no last processed time, use lookback days from config
    if last_processed_time is None:
        days = lookback_days if lookback_days is not None else 7  # Default to 7 days lookback
        last_processed_time = dt.now() - timedelta(days=days)

    # If table_base_name is provided, get incremental column from config
    if table_base_name:
        try:
            table_config = extract_table_config(table_base_name)
            incremental_column = table_config.get("incremental_column")

            if incremental_column and incremental_column in df.columns:
                logging.info(
                    f"Using configured incremental column '{incremental_column}' from config"
                )
                return F.col(incremental_column) >= F.lit(last_processed_time)
        except Exception as e:
            logging.warning(f"Error getting incremental column from config: {str(e)}")
            # Fall through to standard columns

    # Standard tracking columns in order of preference (fallback)
    timestamp_cols = [
        "SystemModifiedAt",
        "SystemCreatedAt",
        "SilverModifiedAt",
        "SilverCreatedAt",
        "timestamp",
    ]

    # Find the first available timestamp column
    available_cols = [col for col in timestamp_cols if col in df.columns]

    if not available_cols:
        # If no timestamp columns are available, return a condition that includes all rows
        logging.warning(
            "No timestamp columns found for incremental filtering, using all rows",
            available_columns=df.columns[:10],  # First 10 columns for brevity
        )
        return F.lit(True)

    # Use the first available timestamp column
    timestamp_col = available_cols[0]
    logging.info(f"Using fallback timestamp column '{timestamp_col}' for incremental filtering")

    # Create filter condition: rows modified after last processed time
    return F.col(timestamp_col) >= F.lit(last_processed_time)


def create_empty_table_if_not_exists(
    spark: SparkSession, full_table_path: str, df: DataFrame, partition_by: list[str] | None = None
) -> bool:
    """
    Create an empty table if it doesn't exist with the schema of the provided DataFrame.

    Args:
        spark: SparkSession to use
        full_table_path: Full path to the target table
        df: DataFrame to get schema from
        partition_by: Optional list of partitioning columns

    Returns:
        True if table was created, False if it already existed
    """
    table_parts = full_table_path.split(".")
    escaped_table_path = ".".join([f"`{part}`" for part in table_parts])

    # Check if table exists already
    try:
        spark.sql(f"DESCRIBE TABLE {escaped_table_path}")
        logging.debug(f"Table {full_table_path} already exists")
        return False
    except Exception:
        # If we get here, table doesn't exist
        try:
            # Add audit columns to schema if they don't already exist
            original_columns = df.columns
            has_created_at = "SilverCreatedAt" in original_columns
            has_modified_at = "SilverModifiedAt" in original_columns

            if not has_created_at or not has_modified_at:
                # Need to add the audit columns to the DataFrame
                for col_name in ["SilverCreatedAt", "SilverModifiedAt"]:
                    if col_name not in original_columns:
                        df = df.withColumn(col_name, F.current_timestamp())

            logging.debug(f"Creating empty table {full_table_path} with schema:")
            logging.debug(df.schema.simpleString())

            # Always create the table with the most permissive options
            writer = df.limit(0).write.format("delta").option("overwriteSchema", "true")

            # Apply partitioning if specified
            escaped_partition_columns = []
            if partition_by:
                escaped_partition_columns = [
                    f"`{col}`" if "$" in col or "." in col else col for col in partition_by
                ]
                writer = writer.partitionBy(*escaped_partition_columns)

            # Create the table
            writer.saveAsTable(full_table_path)
            logging.info(f"Created empty table {full_table_path}")
            return True
        except Exception as e:
            logging.error(f"Failed to create empty table {full_table_path}: {str(e)}")
            raise


def bronze_to_silver_transformation(
    spark,
    bronze_path: str,
    silver_db: str,
    table_name: str,
    is_full_refresh: bool = False,
) -> tuple[bool, str | None]:
    """
    Processes a table from bronze to silver using incremental logic and Delta MERGE.

    Args:
        spark: SparkSession to use
        bronze_path: Path to the bronze tables
        silver_db: Name of the silver database
        table_name: Name of the table to process (base name, no suffix)
        is_full_refresh: Whether to perform a full refresh

    Returns:
        Tuple of (success, watermark_value)
    """
    # Get table config
    table_base_name = standardize_table_reference(table_name, include_suffix=False)
    table_config = get_table_config(table_base_name)

    if not table_config:
        logging.warning(f"No configuration found for {table_base_name}, using defaults")
        # Set defaults for required parameters
        scd_type = 1
        business_keys = None
        incremental_col_name = None
        silver_target_name = get_silver_table_name(table_name)
        partition_columns = ["$Company"]
    else:
        # Extract config parameters
        scd_type = table_config.get("scd_type", 1)
        business_keys = table_config.get("business_keys")
        incremental_col_name = table_config.get("incremental_column")
        silver_target_name = table_config.get("silver_target", get_silver_table_name(table_name))
        partition_columns = table_config.get("partition_by_silver", ["$Company"])

    bronze_table_name = table_name
    silver_full_path = f"{silver_db}.{silver_target_name}"

    with logging.span(
        "bronze_to_silver",
        table=bronze_table_name,
        silver_target=silver_full_path,
        incremental_col=incremental_col_name,
    ):
        # 1. Read Bronze Data
        try:
            bronze_df = read_bc_table(spark, bronze_path, bronze_table_name)
            # Add $Company if missing (some exports might not include it)
            if "$Company" not in bronze_df.columns:
                bronze_df = bronze_df.withColumn("$Company", F.lit("DEFAULT"))
        except Exception as e:
            logging.error(f"Failed to read bronze data: {str(e)}")
            return False, None

        # 2. Apply incremental filter if needed
        if not is_full_refresh and incremental_col_name:
            last_watermark_str = watermark_manager.get_watermark(spark, bronze_table_name)

            if not last_watermark_str:
                logging.info(
                    f"No previous watermark found for {bronze_table_name}, processing all data"
                )
                filtered_df = bronze_df
                # Calculate new watermark for next run
                if incremental_col_name in bronze_df.columns:
                    max_df = bronze_df.agg(F.max(F.col(incremental_col_name))).collect()
                    new_watermark_val = max_df[0][0] if max_df[0][0] else None
                else:
                    new_watermark_val = None
            else:
                # Convert watermark string to appropriate timestamp
                try:
                    # Try parsing as a timestamp first (most common case)
                    last_processed_time = dt.fromisoformat(
                        last_watermark_str.replace("Z", "+00:00")
                    )
                except (ValueError, TypeError):
                    # If not ISO format, try other common formats
                    formats_to_try = [
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%d",
                        "%Y%m%d%H%M%S",
                    ]

                    parsed = False
                    for fmt in formats_to_try:
                        try:
                            last_processed_time = dt.strptime(last_watermark_str, fmt)
                            parsed = True
                            break
                        except (ValueError, TypeError):
                            continue

                    if not parsed:
                        # If we can't parse the timestamp, default to a lookback period
                        logging.warning(
                            f"Could not parse timestamp '{last_watermark_str}', defaulting to 7-day lookback"
                        )
                        last_processed_time = dt.now() - timedelta(days=7)

                logging.info(
                    f"Filtering {bronze_table_name} based on last processed time {last_processed_time}"
                )

                # Use our improved get_incremental_filter function
                filter_expr = get_incremental_filter(
                    df=bronze_df,
                    last_processed_time=last_processed_time,
                    table_base_name=table_base_name,
                )

                # Apply the filter
                filtered_df = bronze_df.filter(filter_expr)

                # Calculate new watermark for next run
                if incremental_col_name in bronze_df.columns:
                    max_df = filtered_df.agg(F.max(F.col(incremental_col_name))).collect()
                    new_watermark_val = max_df[0][0] if max_df[0][0] else None
                else:
                    new_watermark_val = None
        elif is_full_refresh:
            logging.info(f"Performing full refresh for {bronze_table_name}")
            # Calculate watermark from the full dataset for next incremental run
            if incremental_col_name and incremental_col_name in bronze_df.columns:
                max_df = bronze_df.agg(F.max(F.col(incremental_col_name))).collect()
                new_watermark_val = max_df[0][0] if max_df[0][0] else None
            else:
                new_watermark_val = None

            filtered_df = bronze_df
        else:
            # No incremental column defined, or full refresh requested
            filtered_df = bronze_df
            new_watermark_val = None

        # Skip further processing if no data to process
        if filtered_df.isEmpty():
            logging.info(f"No data to process for {bronze_table_name} after filtering")
            return True, str(new_watermark_val) if new_watermark_val else None

        # 3. Standardize column names (using pattern-based or config-based approach)
        try:
            standardized_df = standardize_dataframe_from_config(filtered_df, table_base_name)
        except Exception as e:
            logging.error(f"Failed to standardize column names: {str(e)}")
            # Continue with original column names
            standardized_df = filtered_df

        # 4. Apply proper data types
        try:
            typed_df = apply_data_types(standardized_df, table_base_name)
        except Exception as e:
            logging.error(f"Failed to apply data types: {str(e)}")
            # Continue with original types
            typed_df = standardized_df

        # Add Silver audit columns BEFORE SCD handling
        typed_df = typed_df.withColumn("SilverCreatedAt", F.current_timestamp())
        typed_df = typed_df.withColumn("SilverModifiedAt", F.current_timestamp())
        logging.debug(
            f"Added SilverCreatedAt and SilverModifiedAt to DataFrame for {table_base_name}"
        )

        # 5. Apply SCD handling based on configuration
        try:
            if scd_type == 1 and business_keys:
                # Type 1 SCD - keep only latest version
                processed_df = apply_scd_type_1(typed_df, business_keys)
            elif scd_type == 2 and business_keys:
                # Type 2 SCD - track history
                processed_df = apply_scd_type_2(typed_df, business_keys)
            else:
                # No SCD handling
                processed_df = typed_df
        except Exception as e:
            logging.error(f"Failed to apply SCD handling: {str(e)}")
            processed_df = typed_df

        # 6. Write to silver table using the appropriate method based on full refresh flag
        try:
            # Prepare escaped partition columns (needed for both overwrite and merge setup)
            escaped_partition_columns = []
            if partition_columns:
                escaped_partition_columns = [
                    f"`{col}`" if "$" in col or "." in col else col for col in partition_columns
                ]

            if is_full_refresh:
                # Full Refresh: Overwrite the table, ensuring schema includes audit columns
                logging.info(f"Performing full refresh overwrite for {silver_full_path}")
                writer = (
                    processed_df.write.format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                )  # Crucial for full refresh

                if escaped_partition_columns:
                    writer = writer.partitionBy(*escaped_partition_columns)

                writer.saveAsTable(silver_full_path)
                logging.info(f"Successfully overwrote {silver_full_path} during full refresh.")

            else:
                # Incremental Load: Use MERGE
                from delta.tables import DeltaTable

                # Check if target table exists before attempting merge
                table_exists = spark.catalog.tableExists(silver_full_path)

                if not table_exists:
                    # If target doesn't exist (e.g., first incremental run), create it by writing
                    logging.warning(
                        f"Target table {silver_full_path} not found for incremental merge. Creating and writing."
                    )
                    writer = (
                        processed_df.write.format("delta")
                        .mode("overwrite")
                        .option("overwriteSchema", "true")
                    )
                    if escaped_partition_columns:
                        writer = writer.partitionBy(*escaped_partition_columns)
                    writer.saveAsTable(silver_full_path)
                    logging.info(
                        f"Created and populated {silver_full_path} for first incremental run."
                    )
                elif scd_type == 2:
                    # SCD Type 2 always appends (history tracking)
                    logging.info(f"Appending SCD Type 2 data to {silver_full_path}")
                    writer = processed_df.write.format("delta").mode("append")
                    if escaped_partition_columns:
                        writer = writer.partitionBy(*escaped_partition_columns)
                    writer.saveAsTable(silver_full_path)
                    logging.info(f"Successfully appended SCD Type 2 data to {silver_full_path}")
                elif scd_type == 1 and business_keys:
                    # SCD Type 1: Perform MERGE (target table exists)
                    logging.info(f"Performing incremental merge (SCD1) into {silver_full_path}")

                    # Get table metadata to find physical location
                    table_metadata = spark.sql(f"DESCRIBE DETAIL {silver_full_path}").collect()[0]
                    table_location = table_metadata.location

                    logging.debug(f"Found table at location: {table_location}")

                    # Use forPath with the physical location for best Delta API compatibility
                    delta_table = DeltaTable.forPath(spark, table_location)

                    # Create merge condition based on business keys
                    merge_condition_parts = []
                    for key in business_keys:
                        # Check if key name contains special characters that need escaping
                        if key.startswith("$") or "-" in key or any(c in key for c in "[]().,;:"):
                            escaped_key = f"`{key}`"
                            merge_condition_parts.append(
                                f"target.{escaped_key} = source.{escaped_key}"
                            )
                        else:
                            merge_condition_parts.append(f"target.`{key}` = source.`{key}`")
                    if "$Company" in processed_df.columns and "$Company" not in business_keys:
                        merge_condition_parts.append("target.`$Company` = source.`$Company`")
                    merge_condition = " AND ".join(merge_condition_parts)

                    # Define columns to exclude from updates (business keys + creation timestamp)
                    excluded_update_cols = business_keys + ["SilverCreatedAt"]
                    if "$Company" in processed_df.columns:
                        excluded_update_cols.append("$Company")  # Exclude $Company if present

                    # Build the dictionary for the UPDATE set clause - WITH FIX
                    update_set_dict = {}
                    for col in processed_df.columns:
                        # Skip SilverModifiedAt in this loop to avoid duplicate column error
                        if col not in excluded_update_cols and col != "SilverModifiedAt":
                            update_set_dict[f"target.`{col}`"] = f"source.`{col}`"

                    # Add the SilverModifiedAt update - now guaranteed to happen only once
                    update_set_dict["target.SilverModifiedAt"] = "current_timestamp()"

                    # Build the dictionary for the INSERT values clause (includes audit columns from source)
                    insert_values_dict = {}
                    for col in processed_df.columns:
                        insert_values_dict[f"target.`{col}`"] = f"source.`{col}`"

                    logging.debug(f"Merge condition: {merge_condition}")
                    logging.debug(f"Update set dict: {update_set_dict}")
                    logging.debug(f"Insert values dict keys: {list(insert_values_dict.keys())}")

                    # Execute the merge with explicit clauses
                    (
                        delta_table.alias("target")
                        .merge(processed_df.alias("source"), merge_condition)
                        .whenMatchedUpdate(set=update_set_dict)
                        .whenNotMatchedInsert(values=insert_values_dict)
                        .execute()
                    )
                    logging.info(
                        f"Successfully applied incremental SCD Type 1 merge to {silver_full_path}"
                    )

                else:  # SCD Type 0 or SCD1 without business keys - treat as overwrite for incremental
                    logging.warning(
                        f"SCD Type {scd_type} without keys or SCD0 - performing overwrite for incremental update on {silver_full_path}"
                    )
                    writer = (
                        processed_df.write.format("delta")
                        .mode("overwrite")
                        .option("overwriteSchema", "true")
                    )  # Allow schema evolution
                    if escaped_partition_columns:
                        writer = writer.partitionBy(*escaped_partition_columns)
                    writer.saveAsTable(silver_full_path)
                    logging.info(
                        f"Overwrote {silver_full_path} for incremental SCD0/SCD1-no-key update."
                    )

            # Update watermark only if processing was successful AND it was an incremental run
            if not is_full_refresh and new_watermark_val:
                watermark_str = str(new_watermark_val) if new_watermark_val else None
                if watermark_str:
                    watermark_manager.update_watermark(spark, bronze_table_name, watermark_str)
                    logging.info(
                        f"Updated watermark for {bronze_table_name}", new_watermark=watermark_str
                    )

            return True, str(new_watermark_val) if new_watermark_val else None

        except Exception as e:
            logging.error(
                f"Failed during write/merge operation for {silver_full_path}: {str(e)}",
                exc_info=True,
            )  # Add traceback
            return False, None
