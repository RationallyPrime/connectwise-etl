from datetime import datetime as dt, timedelta

from pyspark.sql import (
    SparkSession,
    functions as F,  # noqa: N812
)

from unified_etl.gold.facts import create_fact_table, process_gold_layer
from unified_etl.gold.hierarchy import build_account_hierarchy
from unified_etl.gold.keys import generate_surrogate_key
from unified_etl.pipeline.writer import write_with_schema_conflict_handling
from unified_etl.silver.standardize import get_silver_table_name
from unified_etl.utils import logging
from unified_etl.utils.config_loader import get_all_table_configs, get_table_config
from unified_etl.utils.exceptions import ConfigurationError
from unified_etl.utils.naming import standardize_table_reference
from unified_etl.utils.table_utils import construct_table_path
from unified_etl.utils.watermark_manager import watermark_manager


def _convert_watermark_to_datetime(watermark_str: str | None) -> dt | None:
    """
    Convert a watermark string to a datetime object.

    Args:
        watermark_str: The watermark string to convert

    Returns:
        A datetime object if the conversion was successful, None otherwise
    """
    if not watermark_str:
        return None

    try:
        # Try parsing as timestamp first
        return dt.fromisoformat(watermark_str.replace("Z", "+00:00"))
    except ValueError:
        # Fall back to date parsing if timestamp fails
        try:
            return dt.strptime(watermark_str, "%Y-%m-%d")
        except ValueError:
            logging.warning(f"Could not parse watermark as datetime: {watermark_str}")
            return None


def silver_to_gold_transformation(
    spark: SparkSession,
    silver_db: str,
    gold_db: str,
    table_name: str,
    is_full_refresh: bool = False,
) -> bool:
    """
    Processes a table from silver to gold layer based on configuration and incremental data.

    Args:
        spark: SparkSession to use
        silver_db: Name of the silver database
        gold_db: Name of the gold database
        table_name: Name of the table to process (base name, no suffix)
        is_full_refresh: Whether to perform a full refresh

    Returns:
        True if successful, False otherwise
    """
    # Get table config
    table_base_name = standardize_table_reference(table_name, include_suffix=False)
    table_config = get_table_config(table_base_name)

    if not table_config:
        logging.warning(
            f"No configuration found for {table_base_name}, skipping gold transformation"
        )
        return False

    # Extract config parameters
    table_type = table_config.get("type")
    business_keys = table_config.get("business_keys")

    # Prioritize gold_name
    gold_target_name = table_config.get("gold_name")

    # Get gold partitioning config specifically
    partition_config = table_config.get("partition_by", {})
    gold_partition_columns = (
        partition_config.get("gold") if isinstance(partition_config, dict) else None
    )

    if not gold_target_name:
        logging.info(f"No gold_name defined for {table_base_name}, skipping gold transformation")
        return False

    silver_table_name = table_config.get("silver_name", get_silver_table_name(table_name))
    silver_full_path = construct_table_path(silver_db, silver_table_name)

    # Use the utility function for gold path construction as well
    gold_full_path = construct_table_path(gold_db, gold_target_name)
    logging.debug(f"Constructed gold target path: {gold_full_path}")

    # Determine write mode based on type and refresh flag
    if (
        table_type in ["ledger_entries", "transaction_lines", "transaction_headers"]
        and not is_full_refresh
    ):
        write_mode = "append"  # Facts append incrementally
    else:
        write_mode = "overwrite"  # Dimensions, bridges, setups, and full refreshes overwrite

    # Get last processed time for incremental processing
    last_processed_time = None
    if not is_full_refresh:
        try:
            last_processed_time_str = watermark_manager.get_watermark(spark, gold_full_path)
            logging.info(f"Last processed time for {gold_full_path}: {last_processed_time_str}")
            # Convert string to datetime if we have a value
            last_processed_time = _convert_watermark_to_datetime(last_processed_time_str)
        except Exception as e:
            logging.warning(f"Failed to get watermark for {gold_full_path}: {e!s}")
            logging.info("Proceeding with full refresh")
            is_full_refresh = True

    with logging.span(
        "silver_to_gold",
        table=silver_table_name,
        gold_target=gold_full_path,
        type=table_type,
        incremental=not is_full_refresh,
    ):
        # Handle special singleton dimensions first
        if table_base_name == "DateDimension":
            try:
                # Generate date dimension based on overall min/max dates if needed
                from unified_etl.gold.dimensions import generate_date_dimension  # Deferred import

                date_df = generate_date_dimension(
                    spark,
                    start_date=dt.combine(
                        dt.now().date() - timedelta(days=365 * 5), dt.min.time()
                    ),  # 5 years back
                    end_date=dt.combine(
                        dt.now().date() + timedelta(days=365 * 5), dt.min.time()
                    ),  # 5 years ahead
                )
                date_df.write.format("delta").mode("overwrite").saveAsTable(gold_full_path)
                logging.info(f"Generated and wrote date dimension to {gold_full_path}")
                return True
            except Exception as e:
                logging.error(f"Failed to generate date dimension: {e!s}")
                return False

        # Read silver table
        try:
            silver_df = spark.table(silver_full_path)
        except Exception as e:
            logging.error(f"Failed to read silver table {silver_full_path}: {e!s}")
            return False

        # Apply gold transformations based on table type
        try:
            gold_df = None  # Initialize gold_df

            # --- Dimension Handling ---
            if table_type == "master_data":
                if not business_keys:
                    raise ConfigurationError(
                        f"Business keys required for master data gold table {gold_target_name}"
                    )

                # Generate surrogate key
                gold_df = generate_surrogate_key(
                    silver_df, business_keys, key_name=f"{table_base_name}Key"
                )

                # Apply hierarchy if configured
                if table_config.get("hierarchy", False):
                    gold_df = build_account_hierarchy(gold_df)

                # For master data, use overwrite for incremental updates
                write_mode = "overwrite"  # Set write_mode to a valid value for DataFrameWriter

            # --- Fact Handling ---
            elif table_type in ["ledger_entries", "transaction_lines", "transaction_headers"]:
                # --- Get min_year and fiscal_year_start from global config if needed ---
                all_configs = get_all_table_configs()
                global_settings = all_configs.get("global_settings", {})
                min_year = global_settings.get("min_year")  # Or pass via args if preferred
                fiscal_year_start = global_settings.get("fiscal_year_start", 7)  # Default to 7

                # --- Route to specific fact creation function ---
                target_fact_name = gold_target_name  # Use the configured gold_name

                # Use generic fact creator
                try:
                    # Read silver table
                    silver_df = spark.table(f"{silver_db}.{silver_table_name}")
                    
                    # Create fact table using generic creator
                    gold_df = create_fact_table(
                        spark=spark,
                        silver_df=silver_df,
                        entity_name=table_base_name,
                        gold_path=gold_db
                    )
                except Exception as e:
                    logging.error(f"Failed to create fact table for {table_base_name}: {str(e)}")
                    return False
                    # Inventory fact uses ledger entries
                    if table_type != "ledger_entries":
                        logging.warning(
                            f"Skipping {table_base_name} for {target_fact_name}: requires ledger_entries table type"
                        )
                        return False

                    gold_df = create_item_fact(
                        spark,
                        silver_table_name,
                        silver_db,
                        gold_db,
                        min_year,
                        not is_full_refresh,
                        last_processed_time,
                        fiscal_year_start=fiscal_year_start,
                    )
                elif target_fact_name == "fact_Sales":
                    # Sales fact uses transaction lines, not headers
                    if table_type != "transaction_lines":
                        logging.warning(
                            f"Skipping {table_base_name} for {target_fact_name}: requires transaction_lines table type"
                        )
                        return False

                    gold_df = create_sales_fact(
                        spark,
                        silver_table_name,
                        silver_db,
                        gold_db,
                        min_year,
                        not is_full_refresh,
                        last_processed_time,
                        fiscal_year_start=fiscal_year_start,
                    )
                elif target_fact_name == "fact_Purchase":
                    # Purchase fact uses transaction lines, not headers
                    if table_type != "transaction_lines":
                        logging.warning(
                            f"Skipping {table_base_name} for {target_fact_name}: requires transaction_lines table type"
                        )
                        return False

                    gold_df = create_purchase_fact(
                        spark,
                        silver_table_name,
                        silver_db,
                        gold_db,
                        min_year,
                        not is_full_refresh,
                        last_processed_time,
                        fiscal_year_start=fiscal_year_start,
                    )
                elif target_fact_name == "fact_Agreement":
                    # Agreement fact can use both lines and headers
                    # But the main entry point is through lines
                    if table_type != "transaction_lines" and "Line" not in table_base_name:
                        logging.warning(
                            f"Skipping {table_base_name} for {target_fact_name}: requires transaction_lines table type"
                        )
                        return False

                    gold_df = create_agreement_fact(
                        spark,
                        silver_path=silver_db,
                        gold_path=gold_db,  # Corrected paths
                        min_year=min_year,
                        incremental=not is_full_refresh,
                        last_processed_time=last_processed_time,
                    )
                elif target_fact_name == "fact_AccountsReceivable":
                    # Accounts Receivable fact uses ledger entries
                    if table_type != "ledger_entries":
                        logging.warning(
                            f"Skipping {table_base_name} for {target_fact_name}: requires ledger_entries table type"
                        )
                        return False

                    gold_df = create_accounts_receivable_fact(
                        spark,
                        source_table=silver_table_name,
                        silver_path=silver_db,
                        gold_path=gold_db,
                        min_year=min_year,
                        incremental=not is_full_refresh,
                        last_processed_time=last_processed_time,
                        fiscal_year_start=fiscal_year_start,
                    )
                elif target_fact_name == "fact_AccountsPayable":
                    # Accounts Payable fact uses ledger entries
                    if table_type != "ledger_entries":
                        logging.warning(
                            f"Skipping {table_base_name} for {target_fact_name}: requires ledger_entries table type"
                        )
                        return False

                    gold_df = create_accounts_payable_fact(
                        spark,
                        source_table=silver_table_name,
                        silver_path=silver_db,
                        gold_path=gold_db,
                        min_year=min_year,
                        incremental=not is_full_refresh,
                        last_processed_time=last_processed_time,
                        fiscal_year_start=fiscal_year_start,
                    )
                elif target_fact_name == "fact_Jobs":
                    # Jobs fact uses ledger entries
                    if table_type != "ledger_entries":
                        logging.warning(
                            f"Skipping {table_base_name} for {target_fact_name}: requires ledger_entries table type"
                        )
                        return False

                    gold_df = create_jobs_fact(
                        spark,
                        source_table=silver_table_name,
                        silver_path=silver_db,
                        gold_path=gold_db,
                        min_year=min_year,
                        incremental=not is_full_refresh,
                        last_processed_time=last_processed_time,
                        fiscal_year_start=fiscal_year_start,
                    )
                elif target_fact_name == "fact_InventoryValue":
                    # Inventory Value fact uses ledger entries
                    if table_type != "ledger_entries":
                        logging.warning(
                            f"Skipping {table_base_name} for {target_fact_name}: requires ledger_entries table type"
                        )
                        return False

                    gold_df = create_inventory_value_fact(
                        spark,
                        source_table=silver_table_name,
                        silver_path=silver_db,
                        gold_path=gold_db,
                        min_year=min_year,
                        incremental=not is_full_refresh,
                        last_processed_time=last_processed_time,
                        fiscal_year_start=fiscal_year_start,
                    )
                else:
                    logging.warning(
                        f"No specific fact creation logic for {target_fact_name} derived from {table_base_name}. Skipping."
                    )
                    return False  # Or handle as generic pass-through if intended

                # Ensure the fact DF is not None before proceeding
                if gold_df is None:
                    logging.error(f"Fact creation function for {target_fact_name} returned None.")
                    return False

                # Add PostingYear if it's a partition column and not already present
                if (
                    gold_partition_columns
                    and "PostingYear" in gold_partition_columns
                    and "PostingYear" not in gold_df.columns
                ):
                    if "PostingDate" in gold_df.columns:
                        gold_df = gold_df.withColumn("PostingYear", F.year(F.col("PostingDate")))
                        logging.info(f"Added PostingYear column for partitioning {gold_full_path}")
                    else:
                        logging.error(
                            f"Cannot add partition column PostingYear: PostingDate missing in {gold_full_path}"
                        )
                        return False  # Cannot partition correctly

            # --- Bridge/Dimension Framework Handling ---
            elif table_base_name == "DimensionSetEntry":  # Trigger for the dimension bridge
                all_configs = get_all_table_configs()
                global_settings = all_configs.get("global_settings", {})
                dimension_types = global_settings.get("dimension_types", {})
                from unified_etl.gold.dimensions import create_dimension_bridge  # Deferred import

                gold_df = create_dimension_bridge(spark, silver_db, dimension_types)
                write_mode = "overwrite"  # Bridges are overwritten

            elif table_base_name == "ItemAttributeValueMapping":
                from unified_etl.gold.dimensions import create_item_attribute_bridge  # Deferred import

                gold_df = create_item_attribute_bridge(spark, silver_db, gold_db)
                write_mode = "overwrite"  # Bridges are overwritten

            elif table_base_name == "ItemAttributeValue":
                from unified_etl.gold.dimensions import (
                    create_item_attribute_dimension,
                )  # Deferred import

                gold_df = create_item_attribute_dimension(
                    spark, silver_db, gold_db, incremental=not is_full_refresh
                )
                write_mode = "overwrite"  # Dimensions are generally overwritten

            # --- Default Pass-through (Setups, etc.) ---
            else:
                logging.info(
                    f"Applying default pass-through for {table_base_name} to {gold_target_name}"
                )
                gold_df = silver_df
                # Add surrogate key if business keys are defined, even for pass-through
                if business_keys:
                    gold_df = generate_surrogate_key(
                        gold_df, business_keys, table_name=table_base_name
                    )
                write_mode = "overwrite"

            # --- Perform the Write ---
            if gold_df is not None:
                # Validate partitioning columns exist in the final DataFrame
                if gold_partition_columns:
                    missing_partitions = [
                        p for p in gold_partition_columns if p.strip("`") not in gold_df.columns
                    ]
                    if missing_partitions:
                        logging.error(
                            f"Partition columns {missing_partitions} not found in DataFrame for {gold_full_path}. Columns: {gold_df.columns}"
                        )
                        return False  # Stop before write

                # Use the robust write function
                write_with_schema_conflict_handling(
                    df=gold_df,
                    table_path=gold_full_path,
                    write_mode=write_mode,
                    partition_by=gold_partition_columns,  # Pass correctly derived partitioning
                    spark=spark,
                )
                logging.info(
                    f"Successfully wrote {table_base_name} to {gold_full_path}",
                    mode=write_mode,
                    rows=gold_df.count(),  # Use count() for potentially large DFs
                )

                # Update watermark only if it was an incremental run for facts
                if write_mode == "append":
                    current_time = dt.now()
                    # Use gold_full_path as the identifier for the watermark
                    watermark_manager.update_watermark(spark, gold_full_path, current_time)

                return True
            else:
                logging.warning(
                    f"No gold DataFrame generated for {table_base_name}. Skipping write."
                )
                return False  # Indicate nothing was written

        except Exception as e:
            logging.error(
                f"Failed gold transformation for {table_base_name} -> {gold_target_name}: {e!s}",
                exc_info=True,
            )
            return False
