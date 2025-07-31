"""
Business Central specific Gold layer transformations.

Contains BC-specific business logic that was moved from unified-etl-core:
- Dimension bridge creation (BC's DimensionSetEntry/DimensionValue tables)
- Item attribute dimensions (BC's ItemAttribute/ItemAttributeValue)
- BC-specific dimension joins with $Company logic
- Account hierarchy building (moved from hierarchy.py)

Following CLAUDE.md: Business logic is specialized, not generic.
"""

import logging
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import LongType
from pyspark.sql.window import Window
from unified_etl_core.gold import generate_surrogate_key
from unified_etl_core.utils.base import ErrorCode
from unified_etl_core.utils.decorators import with_etl_error_handling
from unified_etl_core.utils.exceptions import (
    DimensionResolutionError,
    ETLConfigError,
    ETLProcessingError,
)


# Simple table path construction function
def construct_table_path(base_path: str, table_name: str) -> str:
    """Construct a table path from base path and table name."""
    # Handle both catalog.schema and path formats
    if "." in base_path:
        # It's a catalog.schema format
        return f"{base_path}.{table_name}"
    else:
        # It's a file path format
        return f"{base_path.rstrip('/')}/{table_name}"


@with_etl_error_handling(operation="create_bc_dimension_bridge")
def create_bc_dimension_bridge(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
    dimension_types: dict[str, str],
) -> DataFrame:
    """
    Create BC-specific dimension bridge for DimensionSetEntry/DimensionValue tables.

    Args:
        spark: REQUIRED SparkSession
        silver_path: REQUIRED path to silver layer
        gold_path: REQUIRED path to gold layer
        dimension_types: REQUIRED BC dimension type mapping (e.g., {'TEAM': 'TEAM', 'PRODUCT': 'PRODUCT'})
    """
    if not spark:
        raise ETLConfigError("SparkSession is required", code=ErrorCode.CONFIG_MISSING)
    if not silver_path:
        raise ETLConfigError("silver_path is required", code=ErrorCode.CONFIG_MISSING)
    if not gold_path:
        raise ETLConfigError("gold_path is required", code=ErrorCode.CONFIG_MISSING)
    if not dimension_types:
        raise ETLConfigError("dimension_types mapping is required", code=ErrorCode.CONFIG_MISSING)

    # Create BC dimension bridge
    # Read BC dimension framework tables
    try:
        dim_set_entries = spark.table(
            construct_table_path(silver_path, "DimensionSetEntry")
        )
        dim_values = spark.table(construct_table_path(silver_path, "DimensionValue"))
        logging.info("Loaded BC dimension framework tables")
    except Exception as e:
        raise ETLProcessingError(
            "Failed to load BC dimension tables",
            code=ErrorCode.DATA_ACCESS_ERROR,
            details={"silver_path": silver_path}
        ) from e

    # Create base bridge with BC $Company column
    base_df = dim_set_entries.select(
        F.col("$Company"), F.col("DimensionSetID")
    ).distinct()

    result_df = base_df
    logging.info(f"Base BC bridge: {base_df.count()} dimension sets")

    # Process each BC dimension type
    for dim_type, dim_code in dimension_types.items():
        logging.info(f"Processing BC dimension: {dim_type} with code: {dim_code}")

        # Validate BC dimension entries exist
        type_entries = dim_set_entries.filter(F.col("DimensionCode") == dim_code).count()
        if type_entries == 0:
            logging.warning(f"No BC entries found for dimension type {dim_type}, skipping")
            continue

        # BC-specific join logic with DimensionValueCode cast
        dim_set_entries_type = dim_set_entries.alias(f"dse_{dim_type.lower()}")
        dim_values_type = dim_values.alias(f"dv_{dim_type.lower()}")

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
                "left",
            )
            .select(
                F.col(f"dse_{dim_type.lower()}.`$Company`"),
                F.col(f"dse_{dim_type.lower()}.DimensionSetID"),
                F.col(f"dv_{dim_type.lower()}.Code").alias(f"{dim_type}Code"),
                F.col(f"dv_{dim_type.lower()}.Name").alias(f"{dim_type}Name"),
            )
        )

        # Join to result with BC $Company logic
        result_df = result_df.join(type_df, ["$Company", "DimensionSetID"], "left")

    # Add BC dimension presence flags
    for dim_type in dimension_types:
        flag_col = f"Has{dim_type}Dimension"
        code_col = f"{dim_type}Code"

        if code_col in result_df.columns:
            result_df = result_df.withColumn(
                flag_col,
                F.when(F.col(code_col).isNotNull(), F.lit(True)).otherwise(F.lit(False)),
            )

            # Log BC dimension presence stats
            dim_present = result_df.filter(F.col(flag_col)).count()
            dim_total = result_df.count()
            dim_pct = round((dim_present / dim_total * 100), 2) if dim_total > 0 else 0

            logging.info(f"BC {dim_type} dimension: {dim_present}/{dim_total} ({dim_pct}%)")

    # Generate surrogate key with BC company partitioning
    result_df = generate_surrogate_key(
        result_df,
        business_keys=["DimensionSetID"],
        key_name="DimensionBridgeKey",
        partition_columns=["$Company"],
    )

    logging.info(f"BC dimension bridge created: {result_df.count()} records")
    return result_df


def create_bc_item_attribute_dimension(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
    incremental: bool = False,
) -> DataFrame:
    """
    Create BC-specific item attribute dimension from ItemAttribute/ItemAttributeValue tables.

    Args:
        spark: REQUIRED SparkSession
        silver_path: REQUIRED path to silver layer
        gold_path: REQUIRED path to gold layer
        incremental: Whether to perform incremental loading
    """
    if not spark:
        raise DimensionResolutionError("SparkSession is required")
    if not silver_path:
        raise DimensionResolutionError("silver_path is required")
    if not gold_path:
        raise DimensionResolutionError("gold_path is required")

    # Create BC item attribute dimension
    logging.info("Creating BC item attribute dimension")

    # Read BC ItemAttribute and ItemAttributeValue tables
    try:
        attributes_df = spark.table(construct_table_path(silver_path, "ItemAttribute"))
        values_df = spark.table(construct_table_path(silver_path, "ItemAttributeValue"))
        logging.info(
            f"Loaded BC tables: {attributes_df.count()} attributes, {values_df.count()} values"
        )
    except Exception as e:
        raise ETLProcessingError(
            "Failed to read BC item attribute tables",
            code=ErrorCode.DATA_ACCESS_ERROR,
            details={"silver_path": silver_path}
        ) from e

    # Apply incremental filtering if requested
    if incremental:
        try:
            existing_dim = spark.table(construct_table_path(gold_path, "dim_ItemAttribute"))
            last_ts = existing_dim.agg(F.max("dimension_created_at")).collect()[0][0]
            if last_ts:
                logging.info(f"Incremental: filtering after {last_ts}")
                attributes_df = attributes_df.filter(
                    F.col("SystemModifiedAt") > F.lit(last_ts)
                )
                values_df = values_df.filter(F.col("SystemModifiedAt") > F.lit(last_ts))
        except Exception:
            logging.warning("Incremental filtering failed, using full load")

    # BC-specific join on ID and $Company
    try:
        item_attributes = attributes_df.join(
            values_df,
            (attributes_df["ID"] == values_df["ID"])
            & (attributes_df["$Company"] == values_df["$Company"]),
            "inner",
        )
        logging.info(f"Joined BC attributes: {item_attributes.count()} rows")
    except Exception as e:
        raise ETLProcessingError(
            "Failed to join BC attribute tables",
            code=ErrorCode.JOIN_ERROR,
            details={"table1": "ItemAttribute", "table2": "ItemAttributeValue"}
        ) from e

    # Generate surrogate key for BC with company partitioning
    business_keys = ["ID", "Value", "$Company"]
    item_attributes = generate_surrogate_key(
        df=item_attributes,
        business_keys=business_keys,
        key_name="item_attribute_key",
        partition_columns=["$Company"],
    )

    # Add BC processing timestamp
    item_attributes = item_attributes.withColumn(
        "dimension_created_at", F.current_timestamp()
    )

    logging.info(f"BC item attribute dimension created: {item_attributes.count()} records")
    return item_attributes


def create_bc_item_attribute_bridge(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
) -> DataFrame:
    """
    Create BC-specific bridge connecting items to their attributes via ItemAttributeValueMapping.

    Args:
        spark: REQUIRED SparkSession
        silver_path: REQUIRED path to silver layer
        gold_path: REQUIRED path to gold layer
    """
    if not spark:
        raise ETLConfigError("SparkSession is required", code=ErrorCode.CONFIG_MISSING)
    if not silver_path:
        raise ETLConfigError("silver_path is required", code=ErrorCode.CONFIG_MISSING)
    if not gold_path:
        raise ETLConfigError("gold_path is required", code=ErrorCode.CONFIG_MISSING)

    try:
        # Create BC item attribute bridge
        logging.info("Creating BC item attribute bridge")

        # Read BC Item and ItemAttributeValueMapping tables
        try:
            items_df = spark.table(construct_table_path(silver_path, "Item"))
            mapping_df = spark.table(
                construct_table_path(silver_path, "ItemAttributeValueMapping")
            )
            logging.info(
                f"Loaded BC bridge tables: {items_df.count()} items, {mapping_df.count()} mappings"
            )
        except Exception as e:
            raise DimensionResolutionError(f"Failed to read BC bridge tables: {e}") from e

        # BC-specific join on No/ItemNo and $Company
        try:
            bridge_df = items_df.join(
                mapping_df,
                (items_df["No"] == mapping_df["ItemNo"])
                & (items_df["$Company"] == mapping_df["$Company"]),
                "inner",
            )
            logging.info(f"BC attribute bridge joined: {bridge_df.count()} rows")
        except Exception as e:
            raise DimensionResolutionError(f"Failed to join BC bridge tables: {e}") from e

        # Generate surrogate key for BC bridge with company partitioning
        business_keys = ["ItemNo", "ValueID", "$Company"]
        bridge_df = generate_surrogate_key(
            df=bridge_df,
            business_keys=business_keys,
            key_name="item_attribute_bridge_key",
            partition_columns=["$Company"],
        )

        # Add BC processing timestamp
        bridge_df = bridge_df.withColumn("bridge_created_at", F.current_timestamp())

        logging.info(f"BC item attribute bridge created: {bridge_df.count()} records")
        return bridge_df

    except Exception as e:
        raise DimensionResolutionError(f"BC item attribute bridge creation failed: {e}") from e


@with_etl_error_handling(operation="join_bc_dimension")
def join_bc_dimension(
    spark: SparkSession,
    fact_df: DataFrame,
    dimension_name: str,
    fact_join_keys: list[str] | dict[str, str],
    gold_path: str,
    dimension_config: dict[str, Any],
) -> DataFrame:
    """
    Join fact table with BC dimension using $Company logic.

    Args:
        spark: REQUIRED SparkSession
        fact_df: REQUIRED fact DataFrame
        dimension_name: REQUIRED BC dimension name
        fact_join_keys: REQUIRED join keys mapping
        gold_path: REQUIRED path to gold layer
        dimension_config: REQUIRED BC dimension configuration
    """
    if not spark:
        raise ETLConfigError("SparkSession is required", code=ErrorCode.CONFIG_MISSING)
    if not fact_df:
        raise ETLConfigError("fact_df is required", code=ErrorCode.CONFIG_MISSING)
    if not dimension_name:
        raise ETLConfigError("dimension_name is required", code=ErrorCode.CONFIG_MISSING)
    if not fact_join_keys:
        raise ETLConfigError("fact_join_keys is required", code=ErrorCode.CONFIG_MISSING)
    if not gold_path:
        raise ETLConfigError("gold_path is required", code=ErrorCode.CONFIG_MISSING)
    if not dimension_config:
        raise ETLConfigError("dimension_config is required", code=ErrorCode.CONFIG_MISSING)

    # Join BC dimension
    # Get BC dimension configuration
    business_keys = dimension_config.get("business_keys")
    if not business_keys:
        raise ETLConfigError(
            f"business_keys required for BC dimension {dimension_name}",
            code=ErrorCode.CONFIG_MISSING,
            details={"dimension_name": dimension_name}
        )

    dim_target_table = dimension_config.get("gold_name")
    if not dim_target_table:
        raise ETLConfigError(
            f"gold_name required for BC dimension {dimension_name}",
            code=ErrorCode.CONFIG_MISSING,
            details={"dimension_name": dimension_name}
        )

    surrogate_key = f"{dimension_name}Key"

    # Handle BC Date dimension special case
    if dimension_name == "Date":
        fact_df = _handle_bc_date_dimension_join(fact_df, fact_join_keys)

    # Load BC dimension table
    clean_gold_path = gold_path.split(".")[-1] if "." in gold_path else gold_path
    dim_table_path = f"{clean_gold_path}.{dim_target_table}"

    try:
        dim_df = spark.table(dim_table_path)
        logging.info(f"Loaded BC dimension: {dim_table_path}")
    except Exception as e:
        raise ETLProcessingError(
            f"Failed to load BC dimension {dim_table_path}",
            code=ErrorCode.DATA_ACCESS_ERROR,
            details={"dimension_table": dim_table_path}
        ) from e

    # CRITICAL FIX: Alias the tables to avoid column name conflicts
    fact_alias = f"fact_{dimension_name}"
    dim_alias = f"dim_{dimension_name}"

    # Build BC-specific join conditions (includes $Company logic)
    join_conditions = _build_bc_join_conditions(
        fact_df, dim_df, fact_join_keys, business_keys, dimension_name,
        fact_alias=fact_alias, dim_alias=dim_alias
    )

    if not join_conditions:
        raise ETLConfigError(
            f"No valid join conditions for BC dimension {dimension_name}",
            code=ErrorCode.CONFIG_INVALID,
            details={"dimension_name": dimension_name}
        )

    # Perform BC dimension join
    # Build join condition by combining all conditions with AND
    join_condition = join_conditions[0]
    for condition in join_conditions[1:]:
        join_condition = join_condition & condition

    fact_df_aliased = fact_df.alias(fact_alias)
    dim_df_aliased = dim_df.alias(dim_alias)

    # Select only needed columns from BC dimension to avoid conflicts
    # Get surrogate key and exclude duplicate columns
    dim_cols_to_select = []

    # Always include the surrogate key with proper alias
    if surrogate_key in dim_df.columns:
        dim_cols_to_select.append(F.col(f"{dim_alias}.{surrogate_key}").alias(surrogate_key))

    # Get join keys to exclude
    join_keys_to_exclude = set()
    if isinstance(fact_join_keys, dict):
        # fact_join_keys is like {"No": "No", "BuyfromVendorNo": "No"}
        join_keys_to_exclude.update(fact_join_keys.values())
    elif isinstance(fact_join_keys, list):
        # If it's a list, we need to match with business_keys
        for i, _fact_key in enumerate(fact_join_keys):
            if i < len(business_keys):
                join_keys_to_exclude.add(business_keys[i])

    # Always exclude $Company and join keys
    join_keys_to_exclude.add("$Company")

    # Include dimension-specific columns but exclude ones already in fact or used in joins
    for col in dim_df.columns:
        if col not in fact_df.columns and col not in join_keys_to_exclude and col != surrogate_key:
            dim_cols_to_select.append(F.col(f"{dim_alias}.{col}").alias(col))

    # Select all columns from fact table with alias
    fact_cols = [F.col(f"{fact_alias}.{col}").alias(col) for col in fact_df.columns]

    # Join and select columns with proper aliasing
    result_df = fact_df_aliased.join(
        dim_df_aliased,
        join_condition,
        "left"
    ).select(fact_cols + dim_cols_to_select)

    # Handle null surrogate keys with BC default (0)
    if surrogate_key in result_df.columns:
        result_df = result_df.withColumn(
            surrogate_key, F.coalesce(F.col(surrogate_key), F.lit(0).cast("int"))
        )

    # Add BC-specific date dimension columns if joining Date
    if dimension_name == "Date":
        result_df = _add_bc_date_columns(result_df)

    # Log BC join statistics
    total_rows = result_df.count()
    matched_rows = result_df.filter(F.col(surrogate_key) != 0).count()
    match_pct = (matched_rows / total_rows * 100) if total_rows > 0 else 0

    logging.info(
        f"BC dimension join {dimension_name}: {matched_rows}/{total_rows} matched ({match_pct:.1f}%)"
    )
    return result_df


@with_etl_error_handling(operation="handle_bc_date_dimension_join")
def _handle_bc_date_dimension_join(
    fact_df: DataFrame, fact_join_keys: list[str] | dict[str, str]
) -> DataFrame:
    """Handle BC Date dimension special case."""
    date_col = None
    if isinstance(fact_join_keys, list) and fact_join_keys:
        date_col = fact_join_keys[0]
    elif isinstance(fact_join_keys, dict) and fact_join_keys:
        date_col = next(iter(fact_join_keys.keys()))

    if date_col and date_col in fact_df.columns:
        fact_df = fact_df.withColumn(
            "DateKey", F.date_format(F.col(date_col), "yyyyMMdd").cast("int")
        )
        logging.info(f"Generated BC DateKey from {date_col}")

    return fact_df


@with_etl_error_handling(operation="build_bc_join_conditions")
def _build_bc_join_conditions(
    fact_df: DataFrame,
    dim_df: DataFrame,
    fact_join_keys: list[str] | dict[str, str],
    business_keys: list[str],
    dimension_name: str,
    fact_alias: str | None = None,
    dim_alias: str | None = None,
) -> list:
    """Build BC-specific join conditions including $Company logic."""
    join_conditions = []

    # BC $Company join (critical for multi-tenant BC)
    fact_company_col = None
    dim_company_col = None

    for col_name in ["$Company", "$Company"]:
        if col_name in fact_df.columns:
            fact_company_col = col_name
        if col_name in dim_df.columns:
            dim_company_col = col_name

    if fact_company_col and dim_company_col:
        if fact_alias and dim_alias:
            join_conditions.append(
                F.col(f"{fact_alias}.{fact_company_col}") == F.col(f"{dim_alias}.{dim_company_col}")
            )
        else:
            join_conditions.append(fact_df[fact_company_col] == dim_df[dim_company_col])
        logging.debug(f"Added BC $Company join condition for {dimension_name}")

    # BC business key joins
    if isinstance(fact_join_keys, list):
        for i, fact_key in enumerate(fact_join_keys):
            dim_key = business_keys[i] if i < len(business_keys) else business_keys[0]

            # Special handling for Date dimension - use DateKey instead of original date column
            if dimension_name == "Date" and dim_key == "DateKey" and "DateKey" in fact_df.columns:
                fact_key = "DateKey"  # Use the generated DateKey column

            if fact_key in fact_df.columns and dim_key in dim_df.columns:
                if fact_alias and dim_alias:
                    join_conditions.append(
                        F.col(f"{fact_alias}.{fact_key}") == F.col(f"{dim_alias}.{dim_key}")
                    )
                else:
                    join_conditions.append(fact_df[fact_key] == dim_df[dim_key])

    elif isinstance(fact_join_keys, dict):
        for fact_key, dim_key in fact_join_keys.items():
            # Special handling for Date dimension - use DateKey instead of original date column
            if dimension_name == "Date" and dim_key == "DateKey" and "DateKey" in fact_df.columns:
                fact_key = "DateKey"  # Use the generated DateKey column

            if fact_key in fact_df.columns and dim_key in dim_df.columns:
                if fact_alias and dim_alias:
                    join_conditions.append(
                        F.col(f"{fact_alias}.{fact_key}") == F.col(f"{dim_alias}.{dim_key}")
                    )
                else:
                    join_conditions.append(fact_df[fact_key] == dim_df[dim_key])

    return join_conditions


@with_etl_error_handling(operation="add_bc_date_columns")
def _add_bc_date_columns(df: DataFrame) -> DataFrame:
    """Add BC-specific date dimension columns with appropriate defaults."""
    date_cols_to_add = {
        "PostingYear": F.lit(0).cast("int"),
        "FiscalYear": F.lit(0).cast("int"),
        "FiscalQuarter": F.lit("Unknown").cast("string"),
    }

    result_df = df
    for col_name, default_val in date_cols_to_add.items():
        if col_name not in result_df.columns:
            result_df = result_df.withColumn(col_name, default_val)
        else:
            result_df = result_df.withColumn(col_name, F.coalesce(F.col(col_name), default_val))

    return result_df


@with_etl_error_handling(operation="build_bc_account_hierarchy")
def build_bc_account_hierarchy(
    df: DataFrame,
    indentation_col: str | None = None,
    no_col: str = "No",
    surrogate_key_col: str | None = None,
) -> DataFrame:
    """
    Build BC-specific account hierarchy based on indentation level.

    Args:
        df: REQUIRED DataFrame with BC account data
        indentation_col: BC indentation column (auto-detected if None)
        no_col: BC account number column
        surrogate_key_col: BC surrogate key column (auto-detected if None)
    """
    if not df:
        raise ETLConfigError("DataFrame is required", code=ErrorCode.CONFIG_MISSING)

    # Build BC account hierarchy
    # Auto-detect BC columns if not provided
    if surrogate_key_col is None:
        key_cols = [col for col in df.columns if col.endswith("Key")]
        if not key_cols:
            raise ETLProcessingError(
                "No BC surrogate key column found (ending with 'Key')",
                code=ErrorCode.COLUMN_NOT_FOUND,
                details={"columns": list(df.columns)}
            )
        surrogate_key_col = key_cols[0]

    if indentation_col is None:
        indent_cols = [col for col in df.columns if "indent" in col.lower()]
        if not indent_cols:
            raise ETLProcessingError(
                "No BC indentation column found",
                code=ErrorCode.COLUMN_NOT_FOUND,
                details={"columns": list(df.columns)}
            )
        indentation_col = indent_cols[0]

    # BC company column
    company_col = "$Company"
    if company_col not in df.columns:
        raise ETLProcessingError(
            f"BC company column '{company_col}' not found",
            code=ErrorCode.COLUMN_NOT_FOUND,
            details={"column": company_col, "columns": list(df.columns)}
        )

    # Verify BC account number column
    if no_col not in df.columns:
        potential_cols = [col for col in df.columns if col.lower() in ["no", "accountno"]]
        if potential_cols:
            no_col = potential_cols[0]
        else:
            raise ETLProcessingError(
                f"BC account number column '{no_col}' not found",
                code=ErrorCode.COLUMN_NOT_FOUND,
                details={"column": no_col, "columns": list(df.columns)}
            )

    # Process BC indentation as numeric
    df_processed = df.withColumn(
        "indentation_numeric",
        F.when(
            F.col(indentation_col).cast("int").isNotNull(),
            F.col(indentation_col).cast("int"),
        ).otherwise(F.lit(0)),
    )

    # Create BC window partitioned by company
    window_order = Window.partitionBy(company_col).orderBy(no_col)
    df_with_row_num = df_processed.withColumn("row_num", F.row_number().over(window_order))

    # BC-specific parent finding logic
    window_unbounded = (
        Window.partitionBy(company_col)
        .orderBy("row_num")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    df_with_prev = df_with_row_num.withColumn(
        "prev_rows",
        F.collect_list(F.struct("row_num", "indentation_numeric", surrogate_key_col)).over(
            window_unbounded
        ),
    )

    # BC UDF for parent key finding
    @F.udf(returnType=LongType())
    def find_bc_parent_key(current_indent, prev_rows):
        if current_indent <= 0 or not prev_rows:
            return None

        target_indent = current_indent - 1
        for i in range(len(prev_rows) - 2, -1, -1):
            row = prev_rows[i]
            if row.indentation_numeric == target_indent:
                return row[2]  # Return surrogate key
        return None

    # Apply BC parent key logic
    df_with_parent = df_with_prev.withColumn(
        "ParentKey", find_bc_parent_key(F.col("indentation_numeric"), F.col("prev_rows"))
    )

    # Find BC parent accounts
    df_with_parent.createOrReplaceTempView("bc_account_hierarchy")
    parent_keys_df = df_with_parent.sparkSession.sql("""
        SELECT DISTINCT ParentKey
        FROM bc_account_hierarchy
        WHERE ParentKey IS NOT NULL
    """)

    # Mark BC parent accounts
    df_with_is_parent = df_with_parent.join(
        parent_keys_df.withColumnRenamed("ParentKey", "is_parent_key"),
        df_with_parent[surrogate_key_col] == F.col("is_parent_key"),
        "left",
    )

    df_with_is_parent = df_with_is_parent.withColumn(
        "IsParent",
        F.when(F.col("is_parent_key").isNotNull(), F.lit(True)).otherwise(F.lit(False)),
    )

    # Add BC hierarchy attributes
    df_final = df_with_is_parent.withColumn(
        "HierarchyLevel", F.col("indentation_numeric")
    ).withColumn(
        "NodePath",
        F.when(F.col("IsParent"), F.concat(F.col(no_col), F.lit("→*"))).otherwise(
            F.col(no_col)
        ),
    )

    # Clean up temporary columns
    result_df = df_final.drop(
        "row_num", "prev_rows", "is_parent_key", "indentation_numeric"
    )

    # Log BC hierarchy statistics
    parent_count = result_df.filter(F.col("IsParent")).count()
    child_count = result_df.filter(F.col("ParentKey").isNotNull()).count()
    total_count = result_df.count()

    logging.info(
        f"BC account hierarchy: {parent_count} parents, {child_count} children, {total_count} total"
    )
    return result_df
