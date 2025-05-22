# unified_etl/gold/utils.py


import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame, SparkSession

from unified_etl.utils import config_loader, logging
from unified_etl.utils.exceptions import DimensionJoinError


def join_dimension(
    spark: SparkSession,
    fact_df: DataFrame,
    dimension_name: str,
    fact_join_keys: list[str] | dict[str, str],
    gold_path: str,
) -> DataFrame:
    """
    Join a fact table with a dimension table using configuration-driven approach.

    Args:
        spark: Active SparkSession
        fact_df: Fact DataFrame to join with dimension
        dimension_name: Name of the dimension (e.g., 'GLAccount', 'Customer')
        fact_join_keys: List of keys if same names in fact and dimension, or
                       Dict mapping fact column names to dimension column names
        gold_path: Path to gold layer

    Returns:
        DataFrame with dimension join applied

    Raises:
        DimensionJoinError: If dimension join fails
    """
    try:
        with logging.span("join_dimension", dimension_name=dimension_name):
            # Get dimension configuration
            dim_config = config_loader.get_table_config(dimension_name)

            # --- Handle Date dimension special case ---
            if dimension_name == "Date":
                # Automatically generate DateKey from date columns when joining with Date dimension
                date_col = None
                if isinstance(fact_join_keys, list) and len(fact_join_keys) > 0:
                    date_col = fact_join_keys[0]
                elif isinstance(fact_join_keys, dict) and len(fact_join_keys) > 0:
                    date_col = next(iter(fact_join_keys.keys()))

                if date_col and date_col in fact_df.columns:
                    # Generate DateKey in YYYYMMDD format as integer
                    fact_df = fact_df.withColumn(
                        "DateKey", F.date_format(F.col(date_col), "yyyyMMdd").cast("int")
                    )
                    logging.info(
                        f"Automatically generated DateKey from {date_col} for Date dimension join"
                    )

                    # If fact_join_keys is a list, add DateKey to it
                    if isinstance(fact_join_keys, list):
                        fact_join_keys = ["DateKey"]
                    # If fact_join_keys is a dict, update it to use DateKey
                    elif isinstance(fact_join_keys, dict):
                        # Determine the target key in the dimension
                        dim_key = (
                            next(iter(fact_join_keys.values())) if fact_join_keys else "DateKey"
                        )
                        fact_join_keys = {"DateKey": dim_key}

            if not dim_config:
                logging.warning(
                    f"No configuration found for dimension {dimension_name}. Skipping join."
                )
                # Add null surrogate key column to prevent downstream errors
                null_key_name = f"{dimension_name}Key"
                if null_key_name not in fact_df.columns:
                    fact_df = fact_df.withColumn(null_key_name, F.lit(None).cast("integer"))
                return fact_df

            # Extract business keys and target table name from config
            business_keys = dim_config.get("business_keys", [])  # Default to empty list
            if not business_keys:
                logging.error(
                    f"No business_keys defined in config for dimension {dimension_name}. Cannot join."
                )
                # Add null surrogate key and return
                null_key_name = f"{dimension_name}Key"
                if null_key_name not in fact_df.columns:
                    fact_df = fact_df.withColumn(null_key_name, F.lit(0).cast("integer"))
                return fact_df

            # Use gold_name first, fall back to gold_target for consistency with pipeline.py
            dim_target_table = dim_config.get("gold_name")
            if not dim_target_table:
                dim_target_table = dim_config.get("gold_target", f"dim_{dimension_name}")
                if dim_config.get("gold_target"):
                    logging.warning(
                        f"Using legacy 'gold_target' key for {dimension_name}. Please migrate to 'gold_name'."
                    )

            surrogate_key = f"{dimension_name}Key"

            # Fix gold path construction to prevent schema/database problems
            # Extract just the schema part of the gold_path if it contains multiple parts
            gold_db_parts = gold_path.split(".")
            if len(gold_db_parts) > 1:
                # Use just the schema name, not the full path
                clean_gold_path = gold_db_parts[-1]  # Use just the last part (schema name)
                logging.debug(f"Using simplified gold_path: {clean_gold_path} from {gold_path}")
            else:
                clean_gold_path = gold_path

            # Construct full dimension table path - do not include the database name
            # This prevents issues like Wise_Fabric_TEST.LH.LH
            dim_table_path = f"{clean_gold_path}.{dim_target_table}"
            logging.debug(f"Constructed dimension table path: {dim_table_path}")

            # Read dimension table
            dim_df = None
            try:
                dim_df = spark.table(dim_table_path)
                logging.info(f"Successfully loaded dimension table {dim_table_path}")
            except Exception as e:
                logging.error(
                    f"Failed to load dimension table {dim_table_path}: {e!s}. Skipping join."
                )
                # --- Add fallback keys if join fails ---
                null_key_name = f"{dimension_name}Key"
                if null_key_name not in fact_df.columns:
                    fact_df = fact_df.withColumn(
                        null_key_name, F.lit(0).cast("integer")
                    )  # Default key 0

                # Special fallback for Date dimension columns
                if dimension_name == "Date":
                    date_col = None
                    if isinstance(fact_join_keys, list) and len(fact_join_keys) > 0:
                        date_col = fact_join_keys[0]
                    elif isinstance(fact_join_keys, dict) and len(fact_join_keys) > 0:
                        date_col = next(iter(fact_join_keys.keys()), None)

                    if date_col and date_col in fact_df.columns:
                        fact_df = fact_df.withColumn(
                            "DateKey", F.date_format(F.col(date_col), "yyyyMMdd").cast("int")
                        )
                        fact_df = fact_df.withColumn("PostingYear", F.year(F.col(date_col)))
                        fact_df = fact_df.withColumn(
                            "FiscalYear", F.lit(None).cast("int")
                        )  # Add null fiscal cols
                        fact_df = fact_df.withColumn("FiscalQuarter", F.lit(None).cast("string"))
                        logging.info(
                            f"Added fallback Date keys/cols from {date_col} after read failure."
                        )
                    else:  # Add default nulls if date col not found either
                        fact_df = fact_df.withColumn("DateKey", F.lit(0).cast("int"))
                        fact_df = fact_df.withColumn("PostingYear", F.lit(0).cast("int"))
                        fact_df = fact_df.withColumn("FiscalYear", F.lit(0).cast("int"))
                        fact_df = fact_df.withColumn("FiscalQuarter", F.lit(None).cast("string"))

                return fact_df  # Return fact_df with null/default keys

            # Select only necessary columns from dimension table
            select_cols = []
            dim_aliases = {}  # To track aliases for joining

            # --- Add Company Column Handling ---
            dim_company_alias = None
            fact_company_col = None
            # Find the actual company column name in fact_df (might be '$Company' or '`$Company`')
            for col_name in ["$Company", "`$Company`"]:
                if col_name in fact_df.columns:
                    fact_company_col = col_name
                    break
            # Find the actual company column name in dim_df
            dim_company_col = None
            for col_name in ["$Company", "`$Company`"]:
                if col_name in dim_df.columns:
                    dim_company_col = col_name
                    break

            if dim_company_col and fact_company_col:
                dim_company_alias = (
                    f"dim_{dimension_name}_company"  # Make alias unique per dimension
                )
                select_cols.append(F.col(dim_company_col).alias(dim_company_alias))
                dim_aliases[dim_company_col] = dim_company_alias

            # Add business keys (including DateKey for Date dimension)
            keys_to_select = business_keys
            if dimension_name == "Date" and "DateKey" in dim_df.columns:
                keys_to_select = ["DateKey"]  # For Date dim, we only join on DateKey

            for bk in keys_to_select:
                if bk in dim_df.columns:
                    alias = f"dim_{dimension_name}_{bk}"  # Make alias unique
                    select_cols.append(F.col(bk).alias(alias))
                    dim_aliases[bk] = alias
                else:
                    # Handle missing business key in dimension table (error or log)
                    logging.error(f"Business key '{bk}' not found in dimension {dim_table_path}")
                    # Add null surrogate key and return
                    null_key_name = f"{dimension_name}Key"
                    if null_key_name not in fact_df.columns:
                        fact_df = fact_df.withColumn(null_key_name, F.lit(0).cast("integer"))
                    return fact_df

            # Add surrogate key
            if surrogate_key in dim_df.columns:
                select_cols.append(F.col(surrogate_key))  # Select the target surrogate key
            else:
                # Handle missing surrogate key (error or log)
                logging.error(
                    f"Surrogate key '{surrogate_key}' not found in dimension {dim_table_path}"
                )
                # Add null surrogate key and return
                null_key_name = f"{dimension_name}Key"
                if null_key_name not in fact_df.columns:
                    fact_df = fact_df.withColumn(null_key_name, F.lit(0).cast("integer"))
                return fact_df

            # Add other required columns from Date Dimension
            date_cols_to_keep = []
            if dimension_name == "Date":
                required_date_cols = [
                    "PostingYear",
                    "FiscalYear",
                    "FiscalQuarter",  # Add others if needed
                ]
                for date_col_name in required_date_cols:
                    if date_col_name in dim_df.columns:
                        select_cols.append(F.col(date_col_name))
                        date_cols_to_keep.append(date_col_name)  # Keep track
                    else:
                        # Add fallback null column
                        default_val = (
                            F.lit(0).cast("int")
                            if "Year" in date_col_name
                            else F.lit("Unknown").cast("string")
                        )
                        select_cols.append(default_val.alias(date_col_name))
                        date_cols_to_keep.append(date_col_name)  # Keep track

            dim_select_df = dim_df.select(*select_cols).distinct()  # Select distinct dimension rows

            # --- Construct Join Condition ---
            join_conditions = []

            # Company join
            if dim_company_alias and fact_company_col:
                join_conditions.append(
                    fact_df[fact_company_col] == dim_select_df[dim_company_alias]
                )

            # Business key joins (handle list or dict for fact_join_keys)
            if isinstance(fact_join_keys, list):
                # Handles the Date dimension case where fact_join_keys becomes ["DateKey"]
                for i, fact_key in enumerate(fact_join_keys):
                    # Use the key we decided to select from the dimension
                    dim_key = keys_to_select[i] if i < len(keys_to_select) else None
                    dim_key_alias = dim_aliases.get(dim_key)
                    if fact_key in fact_df.columns and dim_key_alias:
                        join_conditions.append(fact_df[fact_key] == dim_select_df[dim_key_alias])
                    else:
                        # Handle missing join key error
                        logging.error(
                            f"Join key issue: Fact key '{fact_key}' or Dim alias for '{dim_key}' not found for {dimension_name}"
                        )
                        # Add null surrogate key and return
                        null_key_name = f"{dimension_name}Key"
                        if null_key_name not in fact_df.columns:
                            fact_df = fact_df.withColumn(null_key_name, F.lit(0).cast("integer"))
                        return fact_df
            elif isinstance(fact_join_keys, dict):
                for fact_key, dim_business_key in fact_join_keys.items():
                    # Use the specific business key name from the mapping
                    dim_key_alias = dim_aliases.get(dim_business_key)
                    if fact_key in fact_df.columns and dim_key_alias:
                        join_conditions.append(fact_df[fact_key] == dim_select_df[dim_key_alias])
                    else:
                        # Handle missing join key error
                        logging.error(
                            f"Join key issue: Fact key '{fact_key}' or Dim alias for '{dim_business_key}' not found for {dimension_name}"
                        )
                        # Add null surrogate key and return
                        null_key_name = f"{dimension_name}Key"
                        if null_key_name not in fact_df.columns:
                            fact_df = fact_df.withColumn(null_key_name, F.lit(0).cast("integer"))
                        return fact_df

            # --- Perform Join ---
            if not join_conditions:
                # Handle no join conditions error
                logging.error(
                    f"No valid join conditions constructed for dimension {dimension_name}"
                )
                # Add null surrogate key and return
                null_key_name = f"{dimension_name}Key"
                if null_key_name not in fact_df.columns:
                    fact_df = fact_df.withColumn(null_key_name, F.lit(0).cast("integer"))
                return fact_df

            join_condition = F.reduce(lambda x, y: x & y, join_conditions)

            result_df = fact_df.join(dim_select_df, join_condition, "left")

            # --- Clean up columns ---
            # Drop the aliased dimension join keys
            cols_to_drop_aliases = list(dim_aliases.values())
            result_df = result_df.drop(*cols_to_drop_aliases)

            # Explicitly drop the fact-side DateKey if it was auto-generated and the Date dimension was joined
            if (
                dimension_name == "Date"
                and "DateKey" in fact_df.columns
                and surrogate_key != "DateKey"
                and "DateKey" in result_df.columns
            ):
                logging.debug(
                    f"Dropping auto-generated fact-side DateKey after joining {dimension_name}"
                )
                result_df = result_df.drop("DateKey")  # Drop the original one from fact_df

            # --- Handle Null Surrogate Keys ---
            # Coalesce null surrogate keys to 0 (or -1 if preferred)
            if surrogate_key in result_df.columns:
                result_df = result_df.withColumn(
                    surrogate_key, F.coalesce(F.col(surrogate_key), F.lit(0).cast("int"))
                )
            else:
                # If the surrogate key column itself is missing after the join (shouldn't happen if selected), add it as 0
                result_df = result_df.withColumn(surrogate_key, F.lit(0).cast("int"))

            # Coalesce null date dimension columns specifically
            if dimension_name == "Date":
                for date_col_name in date_cols_to_keep:
                    # Use appropriate defaults (0 for years, 'Unknown' for quarter)
                    default_val = (
                        F.lit(0).cast("int")
                        if "Year" in date_col_name
                        else F.lit("Unknown").cast("string")
                    )
                    if date_col_name in result_df.columns:
                        result_df = result_df.withColumn(
                            date_col_name, F.coalesce(F.col(date_col_name), default_val)
                        )
                    else:
                        # If the column is somehow missing even after selection, add it with default
                        result_df = result_df.withColumn(date_col_name, default_val)

            # --- Log Join Statistics ---
            total_rows = result_df.count()
            matched_rows = result_df.filter(F.col(surrogate_key).isNotNull()).count()
            match_percentage = (matched_rows / total_rows * 100) if total_rows > 0 else 0

            logging.info(
                f"Dimension join statistics for {dimension_name}",
                total_rows=total_rows,
                matched_rows=matched_rows,
                match_percentage=f"{match_percentage:.2f}%",
            )

            return result_df

    except Exception as e:
        error_msg = f"Failed to join dimension {dimension_name}: {e!s}"
        logging.error(error_msg, exc_info=True)

        # Ensure fallback keys are added on any exception during join
        null_key_name = f"{dimension_name}Key"
        if null_key_name not in fact_df.columns:
            fact_df = fact_df.withColumn(null_key_name, F.lit(0).cast("integer"))  # Default key 0

        if dimension_name == "Date":
            # Add default null date columns
            fact_df = fact_df.withColumn("DateKey", F.lit(0).cast("int"))
            fact_df = fact_df.withColumn("PostingYear", F.lit(0).cast("int"))
            fact_df = fact_df.withColumn("FiscalYear", F.lit(0).cast("int"))
            fact_df = fact_df.withColumn("FiscalQuarter", F.lit(None).cast("string"))

        raise DimensionJoinError(error_msg) from e
