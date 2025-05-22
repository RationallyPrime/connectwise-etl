# unified_etl/gold/dimensions.py
from datetime import date, timedelta

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

from unified_etl.utils import config_loader, construct_table_path, logging
from unified_etl.utils.exceptions import DimensionResolutionError


def generate_date_dimension(
    spark: SparkSession,
    start_date: date,
    end_date: date,
    fiscal_year_start_month: int = 1,
) -> DataFrame:
    """
    Generate a comprehensive date dimension with calendar and fiscal hierarchies.

    Args:
        spark: Spark session
        start_date: Start date for the dimension
        end_date: End date for the dimension
        fiscal_year_start_month: Month when fiscal year starts (1=Jan, 2=Feb, etc.)

    Returns:
        DataFrame with date dimension

    Raises:
        DimensionError: If date dimension generation fails
    """
    try:
        with logging.span("generate_date_dimension"):
            # Validate input parameters
            if start_date > end_date:
                raise ValueError("start_date must be before end_date")

            if fiscal_year_start_month < 1 or fiscal_year_start_month > 12:
                raise ValueError("fiscal_year_start_month must be between 1 and 12")

            # Generate date range
            dates = []
            current_date = start_date
            while current_date <= end_date:
                dates.append(current_date)
                current_date += timedelta(days=1)

            logging.info(
                "Generating date dimension",
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                fiscal_year_start_month=fiscal_year_start_month,
                total_days=len(dates),
            )

            # Create base dataframe
            date_df = spark.createDataFrame([(d,) for d in dates], ["Date"])

            # Add DateKey (yyyyMMdd format) for joins
            date_df = date_df.withColumn("DateKey", F.date_format("Date", "yyyyMMdd").cast("int"))

            # Standard calendar hierarchy
            date_df = date_df.withColumn("CalendarYear", F.year("Date"))
            date_df = date_df.withColumn("CalendarQuarterNo", F.quarter("Date"))
            date_df = date_df.withColumn(
                "CalendarQuarter", F.concat(F.lit("Q"), F.col("CalendarQuarterNo"))
            )
            date_df = date_df.withColumn("CalendarMonthNo", F.month("Date"))
            date_df = date_df.withColumn("CalendarMonth", F.date_format("Date", "MMMM"))
            date_df = date_df.withColumn("CalendarDay", F.dayofmonth("Date"))

            # Week hierarchy
            date_df = date_df.withColumn("WeekNumber", F.weekofyear("Date"))
            date_df = date_df.withColumn("WeekDay", F.date_format("Date", "EEEE"))
            date_df = date_df.withColumn("WeekDayNo", F.dayofweek("Date"))

            # YearMonth for sorting and filtering
            date_df = date_df.withColumn("YearMonth", F.date_format("Date", "yyyy-MM"))
            date_df = date_df.withColumn(
                "YearMonthNo", F.expr("CalendarYear * 100 + CalendarMonthNo")
            )

            # Add fiscal year calculations based on fiscal_year_start_month
            date_df = date_df.withColumn(
                "FiscalYear",
                F.when(F.month("Date") >= fiscal_year_start_month, F.year("Date") + 1).otherwise(
                    F.year("Date")
                ),
            )

            # Calculate fiscal quarter (1-4) based on fiscal year start
            date_df = date_df.withColumn(
                "FiscalQuarterNo",
                F.expr(
                    f"mod(floor((month(Date) - {fiscal_year_start_month} + 12) % 12 / 3) + 1, 4) + 1"
                ),
            )

            # Create FiscalQuarter as a formatted string (e.g., "FQ1")
            # Ensure we don't create a duplicate column
            if "FiscalQuarter" not in date_df.columns:
                date_df = date_df.withColumn(
                    "FiscalQuarter", F.concat(F.lit("FQ"), F.col("FiscalQuarterNo"))
                )

            # Calculate fiscal month (1-12) based on fiscal year start
            date_df = date_df.withColumn(
                "FiscalMonthNo",
                F.expr(f"mod((month(Date) - {fiscal_year_start_month} + 12), 12) + 1"),
            )

            # Add IsStartOfMonth, IsEndOfMonth, etc. indicators
            date_df = date_df.withColumn("IsStartOfMonth", F.dayofmonth("Date") == 1)

            date_df = date_df.withColumn(
                "IsEndOfMonth", F.dayofmonth("Date") == F.dayofmonth(F.last_day("Date"))
            )

            date_df = date_df.withColumn(
                "IsWeekend", (F.dayofweek("Date") == 1) | (F.dayofweek("Date") == 7)
            )

            # Add previous/next date keys for easy navigation
            date_df = date_df.withColumn(
                "PreviousDateKey", F.date_format(F.date_sub("Date", 1), "yyyyMMdd").cast("int")
            )

            date_df = date_df.withColumn(
                "NextDateKey", F.date_format(F.date_add("Date", 1), "yyyyMMdd").cast("int")
            )

            logging.info(
                "Date dimension generation complete",
                row_count=date_df.count(),
                column_count=len(date_df.columns),
            )

            # Use the robust writer instead of direct write
            dim_date_target = construct_table_path("gold", "dim_Date")  # Construct full path
            logging.info(f"Writing Date Dimension using robust writer to {dim_date_target}")
            from unified_etl.pipeline import write_with_schema_conflict_handling

            write_with_schema_conflict_handling(
                df=date_df,
                table_path=dim_date_target,
                write_mode="overwrite",  # Always overwrite date dim
                spark=spark,
            )
            logging.info(f"Successfully generated and wrote date dimension to {dim_date_target}")

            return date_df

    except Exception as e:
        error_msg = f"Failed to generate date dimension: {e!s}"
        logging.error(error_msg)
        raise DimensionResolutionError(error_msg) from e


def create_dimension_bridge(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,  # Add gold_path parameter
    dimension_types: dict[str, str] | None = None,
) -> DataFrame:
    """
    Create a dimension bridge table mapping dimension sets to dimension values.

    Args:
        spark: Active SparkSession
        silver_path: Path to silver layer
        gold_path: Path to gold layer
        dimension_types: Optional dictionary mapping dimension types to codes
            (e.g. {'TEAM': 'TEAM', 'PRODUCT': 'PRODUCT'})
            If None, will try to load from configuration

    Returns:
        DataFrame with dimension bridge

    Raises:
        DimensionError: If dimension bridge creation fails
    """
    try:
        with logging.span("create_dimension_bridge"):
            # Get table configurations
            dimension_set_entries_config = config_loader.get_table_config("DimensionSetEntry")
            dimension_values_config = config_loader.get_table_config("DimensionValue")
            dimensions_config = config_loader.get_table_config("Dimension")

            # Get dimension bridge config
            dimension_bridge_config = config_loader.get_table_config("DimensionBridge")

            if (
                not dimension_set_entries_config
                or not dimension_values_config
                or not dimensions_config
            ):
                logging.warning("Missing configuration for dimension tables, using default names")
                dimension_set_entries_table = "DimensionSetEntry"
                dimension_values_table = "DimensionValue"
                dimensions_table = "Dimension"
            else:
                # Use silver_name instead of silver_target for consistency
                dimension_set_entries_table = dimension_set_entries_config.get(
                    "silver_name", "DimensionSetEntry"
                )
                dimension_values_table = dimension_values_config.get(
                    "silver_name", "DimensionValue"
                )
                dimensions_table = dimensions_config.get("silver_name", "Dimension")

            # Load dimension types from config if not provided
            if dimension_types is None:
                global_config = config_loader.get_all_table_configs()
                dimension_types_config = global_config.get("global_settings", {}).get(
                    "dimension_types", {}
                )
                if dimension_types_config:
                    dimension_types = dimension_types_config
                else:
                    # Default dimension types if not in config
                    dimension_types = {"TEAM": "TEAM", "PRODUCT": "PRODUCT"}
                    logging.warning(
                        "No dimension types found in config, using defaults",
                        defaults=dimension_types,
                    )

            # Read dimension framework tables
            logging.info("Reading dimension framework tables")
            dim_set_entries = spark.table(
                construct_table_path(silver_path, dimension_set_entries_table)
            )
            dim_values = spark.table(construct_table_path(silver_path, dimension_values_table))
            # Load dimensions table for metadata reference, even though not directly used in this function
            _ = spark.table(construct_table_path(silver_path, dimensions_table))

            # Create a base dataframe with all unique dimension set IDs
            base_df = dim_set_entries.select(
                F.col("`$Company`"), F.col("DimensionSetID")
            ).distinct()

            logging.info(
                "Created base bridge",
                unique_dimension_sets=base_df.count(),
                dimension_types=list(dimension_types.keys()),
            )

            result_df = base_df

            # Process each dimension type
            for dim_type, dim_code in dimension_types.items():
                logging.info(f"Processing dimension type: {dim_type} with code: {dim_code}")

                # Count entries for this dimension type for debugging
                type_entries = dim_set_entries.filter(F.col("DimensionCode") == dim_code).count()
                type_values = dim_values.filter(F.col("DimensionCode") == dim_code).count()

                logging.debug(
                    f"Found entries for {dim_type}",
                    dimension_set_entries=type_entries,
                    dimension_values=type_values,
                )

                # Skip if no entries for this dimension type
                if type_entries == 0:
                    logging.warning(f"No entries found for dimension type {dim_type}, skipping")
                    continue

                # Create aliases for clear join conditions
                dim_set_entries_type = dim_set_entries.alias(f"dse_{dim_type.lower()}")
                dim_values_type = dim_values.alias(f"dv_{dim_type.lower()}")

                # Join dimension values to set entries
                type_df = (
                    dim_set_entries_type.filter(
                        F.col(f"dse_{dim_type.lower()}.DimensionCode") == dim_code
                    )
                    .join(
                        dim_values_type.filter(
                            F.col(f"dv_{dim_type.lower()}.DimensionCode") == dim_code
                        ),
                        [
                            F.col(f"dse_{dim_type.lower()}.DimensionValueCode").cast("string")
                            == F.col(f"dv_{dim_type.lower()}.Code"),
                            F.col(f"dse_{dim_type.lower()}.`$Company`")
                            == F.col(f"dv_{dim_type.lower()}.`$Company`"),
                        ],
                        "left",  # Use left join to preserve all dimension sets
                    )
                    .select(
                        F.col(f"dse_{dim_type.lower()}.`$Company`"),
                        F.col(f"dse_{dim_type.lower()}.DimensionSetID"),
                        F.col(f"dv_{dim_type.lower()}.Code").alias(f"{dim_type}Code"),
                        F.col(f"dv_{dim_type.lower()}.Name").alias(f"{dim_type}Name"),
                    )
                )

                # Join to the result dataframe
                result_df = result_df.join(type_df, ["$Company", "DimensionSetID"], "left")

                logging.info(f"Added {dim_type} dimension to bridge")

            # Add dimension presence flags
            if dimension_types:  # Check if dimension_types is not None
                for dim_type in dimension_types:
                    flag_col = f"Has{dim_type}Dimension"
                    code_col = f"{dim_type}Code"

                    if code_col in result_df.columns:
                        result_df = result_df.withColumn(
                            flag_col,
                            F.when(F.col(code_col).isNotNull(), F.lit(True)).otherwise(
                                F.lit(False)
                            ),
                        )

                        # Log dimension presence stats
                        dim_present = result_df.filter(F.col(flag_col)).count()
                        dim_total = result_df.count()
                        dim_pct = round((dim_present / dim_total * 100), 2) if dim_total > 0 else 0

                        logging.info(
                            f"{dim_type} dimension presence",
                            present_count=dim_present,
                            total_count=dim_total,
                            percentage=dim_pct,
                        )

            # Generate surrogate key for the bridge
            window = Window.partitionBy("`$Company`").orderBy("DimensionSetID")
            result_df = result_df.withColumn("DimensionBridgeKey", F.row_number().over(window))

            # Get the target table name from config or use default
            target_table_name = dimension_bridge_config.get("gold_name")

            # Ensure target_table_name is not None
            if target_table_name is None:
                target_table_name = "dim_DimensionBridge"  # Default name if not specified in config

            # Write result to gold layer using the write_with_schema_conflict_handling utility
            from unified_etl.pipeline import write_with_schema_conflict_handling

            # Use the utility function to construct the full path
            full_target_path = construct_table_path(gold_path, target_table_name)

            logging.info(f"Writing dimension bridge to {full_target_path}")

            write_with_schema_conflict_handling(
                df=result_df,
                table_path=full_target_path,
                write_mode="overwrite",
                partition_by=["$Company"],
                spark=spark,
            )

            logging.info(
                "Dimension bridge creation complete",
                total_rows=result_df.count(),
                column_count=len(result_df.columns),
                target_table=full_target_path,
            )

            return result_df

    except Exception as e:
        error_msg = f"Failed to create dimension bridge: {e!s}"
        logging.error(error_msg)
        raise DimensionResolutionError(error_msg) from e


def create_item_attribute_dimension(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
    incremental: bool = False,
) -> DataFrame:
    """
    Create item attribute dimension table that consolidates attributes and values.

    Args:
        spark: Spark session
        silver_path: Path to silver layer
        gold_path: Path to gold layer
        incremental: Whether to perform incremental loading

    Returns:
        DataFrame containing the item attribute dimension
    """
    try:
        with logging.span("create_item_attribute_dimension"):
            logging.info("Starting create_item_attribute_dimension")

            # Fix gold path construction to prevent schema/database problems
            # Extract just the last part of the gold_path if it contains multiple parts
            gold_db_parts = gold_path.split(".")
            if len(gold_db_parts) > 1:
                clean_gold_path = gold_db_parts[-1]  # Use just the last part (schema name)
                logging.debug(f"Using simplified gold_path: {clean_gold_path} from {gold_path}")
            else:
                clean_gold_path = gold_path

            # Get table configurations
            item_attribute_config = config_loader.get_table_config("ItemAttribute")
            item_attribute_value_config = config_loader.get_table_config("ItemAttributeValue")

            if not item_attribute_config or not item_attribute_value_config:
                logging.warning(
                    "Missing configuration for item attribute tables, using default names"
                )
                item_attribute_table = "ItemAttribute"
                item_attribute_value_table = "ItemAttributeValue"
            else:
                item_attribute_table = item_attribute_config.get("silver_target", "ItemAttribute")
                item_attribute_value_table = item_attribute_value_config.get(
                    "silver_target", "ItemAttributeValue"
                )

            # Get column configurations
            item_attr_columns = config_loader.get_column_schema("ItemAttribute")
            item_attr_value_columns = config_loader.get_column_schema("ItemAttributeValue")

            # Define business keys from configuration or use defaults
            business_keys = ["ID", "Value", "$Company"]
            if item_attr_value_columns:
                # Extract business keys from configuration if available
                config_keys = [
                    col
                    for col, attrs in item_attr_value_columns.items()
                    if attrs.get("is_business_key", False)
                ]
                if config_keys:
                    business_keys = config_keys + ["$Company"]
                    logging.info(f"Using business keys from config: {business_keys}")

            # Read the tables
            try:
                attributes_df = spark.table(construct_table_path(silver_path, item_attribute_table))
                logging.info(f"Loaded {item_attribute_table} with {attributes_df.count()} rows")
            except Exception as err:
                error_msg = f"Failed to read {item_attribute_table} table: {err!s}"
                logging.error(error_msg)
                raise DimensionResolutionError(error_msg) from err

            try:
                values_df = spark.table(
                    construct_table_path(silver_path, item_attribute_value_table)
                )
                logging.info(f"Loaded {item_attribute_value_table} with {values_df.count()} rows")
            except Exception as err:
                error_msg = f"Failed to read {item_attribute_value_table} table: {err!s}"
                logging.error(error_msg)
                raise DimensionResolutionError(error_msg) from err

            # Apply incremental filtering if requested
            if incremental:
                # Check if dimension already exists
                try:
                    existing_dim = spark.table(
                        construct_table_path(clean_gold_path, "dim_ItemAttribute")
                    )
                    logging.info(f"Found existing dimension with {existing_dim.count()} rows")

                    # Get last processed timestamp
                    last_ts = existing_dim.agg(F.max("dimension_created_at")).collect()[0][0]
                    if last_ts:
                        logging.info(f"Filtering data modified after {last_ts}")
                        attributes_df = attributes_df.filter(
                            F.col("SystemModifiedAt") > F.lit(last_ts)
                        )
                        values_df = values_df.filter(F.col("SystemModifiedAt") > F.lit(last_ts))

                        logging.info(
                            "Applied incremental filter",
                            attributes_count=attributes_df.count(),
                            values_count=values_df.count(),
                        )
                except Exception as err:
                    logging.warning(
                        f"Could not apply incremental filtering, using full load: {err!s}"
                    )

            # Join attributes with values
            try:
                # Get join column names from config or use defaults
                attr_id_col = "ID"
                value_attr_id_col = "ID"

                if item_attr_columns and item_attr_value_columns:
                    # Find ID column in attribute table
                    for col, attrs in item_attr_columns.items():
                        if attrs.get("is_primary_key", False) or col.lower() == "id":
                            attr_id_col = col
                            break

                    # Find attribute ID column in value table
                    for col, attrs in item_attr_value_columns.items():
                        if (
                            attrs.get("is_foreign_key", False)
                            and attrs.get("references", {}).get("table") == "ItemAttribute"
                        ):
                            value_attr_id_col = col
                            break

                logging.info(f"Using join columns: {attr_id_col} and {value_attr_id_col}")

                # Join the tables
                item_attributes = attributes_df.join(
                    values_df,
                    (attributes_df[attr_id_col] == values_df[value_attr_id_col])
                    & (attributes_df["`$Company`"] == values_df["`$Company`"]),
                    "inner",
                )

                logging.info(f"Joined attributes with values: {item_attributes.count()} rows")
            except Exception as err:
                error_msg = f"Failed to join attribute tables: {err!s}"
                logging.error(error_msg)
                raise DimensionResolutionError(error_msg) from err

            # Generate surrogate key
            try:
                from unified_etl.gold.keys import generate_surrogate_key

                item_attributes = generate_surrogate_key(
                    df=item_attributes,
                    business_keys=business_keys,
                    key_name="item_attribute_key",
                )
                logging.info("Generated surrogate keys")
            except Exception as err:
                error_msg = f"Failed to generate surrogate keys: {err!s}"
                logging.error(error_msg)
                raise DimensionResolutionError(error_msg) from err

            # Select final columns
            item_attributes = item_attributes.withColumn(
                "dimension_created_at", F.current_timestamp()
            )

            # Write the dimension table
            try:
                # Deferred import to avoid circular dependency
                from unified_etl.pipeline import write_with_schema_conflict_handling

                write_mode = "overwrite"  # Always use overwrite mode for dimensions
                write_with_schema_conflict_handling(
                    df=item_attributes,
                    table_path=construct_table_path(clean_gold_path, "dim_ItemAttribute"),
                    write_mode=write_mode,
                    spark=spark,
                )

                logging.info(
                    "Item attribute dimension created successfully",
                    row_count=item_attributes.count(),
                    incremental=incremental,
                )
                return item_attributes
            except Exception as err:
                error_msg = f"Failed to write dimension table: {err!s}"
                logging.error(error_msg)
                raise DimensionResolutionError(error_msg) from err

    except DimensionResolutionError:
        # Re-raise domain-specific exceptions
        raise
    except Exception as err:
        error_msg = f"Unexpected error in create_item_attribute_dimension: {err!s}"
        logging.error(error_msg)
        raise DimensionResolutionError(error_msg) from err


def create_item_attribute_bridge(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
) -> DataFrame:
    """
    Create a bridge table connecting items to their attributes.

    Args:
        spark: Active SparkSession
        silver_path: Path to silver layer
        gold_path: Path to gold layer

    Returns:
        DataFrame containing the item-attribute bridge table
    """
    try:
        with logging.span("create_item_attribute_bridge"):
            logging.info("Starting create_item_attribute_bridge")

            # Fix gold path construction to prevent schema/database problems
            # Extract just the last part of the gold_path if it contains multiple parts
            gold_db_parts = gold_path.split(".")
            if len(gold_db_parts) > 1:
                clean_gold_path = gold_db_parts[-1]  # Use just the last part (schema name)
                logging.debug(f"Using simplified gold_path: {clean_gold_path} from {gold_path}")
            else:
                clean_gold_path = gold_path

            # Get table configurations
            item_config = config_loader.get_table_config("Item")
            item_attr_value_mapping_config = config_loader.get_table_config(
                "ItemAttributeValueMapping"
            )

            if not item_config or not item_attr_value_mapping_config:
                logging.warning(
                    "Missing configuration for item attribute bridge tables, using default names"
                )
                item_table = "Item"
                item_attr_value_mapping_table = "ItemAttributeValueMapping"
            else:
                item_table = item_config.get("silver_target", "Item")
                item_attr_value_mapping_table = item_attr_value_mapping_config.get(
                    "silver_target", "ItemAttributeValueMapping"
                )

            # Get column configurations
            item_columns = config_loader.get_column_schema("Item")
            mapping_columns = config_loader.get_column_schema("ItemAttributeValueMapping")

            # Define business keys from configuration or use defaults
            business_keys = ["ItemNo", "ValueID", "$Company"]
            if mapping_columns:
                # Extract business keys from configuration if available
                config_keys = [
                    col
                    for col, attrs in mapping_columns.items()
                    if attrs.get("is_business_key", False)
                ]
                if config_keys:
                    business_keys = config_keys + ["$Company"]
                    logging.info(f"Using business keys from config: {business_keys}")

            # Read the tables
            try:
                items_df = spark.table(construct_table_path(silver_path, item_table))
                logging.info(f"Loaded {item_table} with {items_df.count()} rows")
            except Exception as err:
                error_msg = f"Failed to read {item_table} table: {err!s}"
                logging.error(error_msg)
                raise DimensionResolutionError(error_msg) from err

            try:
                mapping_df = spark.table(
                    construct_table_path(silver_path, item_attr_value_mapping_table)
                )
                logging.info(
                    f"Loaded {item_attr_value_mapping_table} with {mapping_df.count()} rows"
                )
            except Exception as err:
                error_msg = f"Failed to read {item_attr_value_mapping_table} table: {err!s}"
                logging.error(error_msg)
                raise DimensionResolutionError(error_msg) from err

            # Get join column names from config or use defaults
            item_no_col = "No"
            mapping_item_no_col = "ItemNo"

            if item_columns and mapping_columns:
                # Find item number column in item table
                for col, attrs in item_columns.items():
                    if attrs.get("is_primary_key", False) or col.lower() == "no":
                        item_no_col = col
                        break

                # Find item number column in mapping table
                for col, attrs in mapping_columns.items():
                    if (
                        attrs.get("is_foreign_key", False)
                        and attrs.get("references", {}).get("table") == "Item"
                    ):
                        mapping_item_no_col = col
                        break

            logging.info(f"Using join columns: {item_no_col} and {mapping_item_no_col}")

            # Join items with attribute value mappings
            try:
                bridge_df = items_df.join(
                    mapping_df,
                    (items_df[item_no_col] == mapping_df[mapping_item_no_col])
                    & (items_df["`$Company`"] == mapping_df["`$Company`"]),
                    "inner",
                )
                logging.info(f"Joined items with attribute mappings: {bridge_df.count()} rows")
            except Exception as err:
                error_msg = f"Failed to join tables: {err!s}"
                logging.error(error_msg)
                raise DimensionResolutionError(error_msg) from err

            # Generate surrogate key
            try:
                from unified_etl.gold.keys import generate_surrogate_key

                bridge_df = generate_surrogate_key(
                    df=bridge_df,
                    business_keys=business_keys,
                    key_name="item_attribute_bridge_key",
                )
                logging.info("Generated surrogate keys")
            except Exception as err:
                error_msg = f"Failed to generate surrogate keys: {err!s}"
                logging.error(error_msg)
                raise DimensionResolutionError(error_msg) from err

            # Add processing timestamp
            bridge_df = bridge_df.withColumn("bridge_created_at", F.current_timestamp())

            # Write the bridge table
            try:
                # Deferred import to avoid circular dependency
                from unified_etl.pipeline import write_with_schema_conflict_handling

                write_with_schema_conflict_handling(
                    df=bridge_df,
                    table_path=construct_table_path(clean_gold_path, "bridge_ItemAttribute"),
                    write_mode="overwrite",
                    spark=spark,
                )

                logging.info(
                    "Item attribute bridge created successfully",
                    row_count=bridge_df.count(),
                )
                return bridge_df
            except Exception as err:
                error_msg = f"Failed to write bridge table: {err!s}"
                logging.error(error_msg)
                raise DimensionResolutionError(error_msg) from err

    except DimensionResolutionError:
        # Re-raise domain-specific exceptions
        raise
    except Exception as err:
        error_msg = f"Unexpected error in create_item_attribute_bridge: {err!s}"
        logging.error(error_msg)
        raise DimensionResolutionError(error_msg) from err
