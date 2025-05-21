from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Optional, List, Dict, Any
from datetime import date, datetime, timedelta
import re

from core_etl.logging_utils import etl_logger
from core_etl.config_utils import get_bc_global_setting, get_bc_table_config
from core_etl.delta_writer import write_delta_table

# --- Date Dimension ---
def generate_date_dimension(
    spark: SparkSession,
    start_date: date,
    end_date: date,
    gold_schema_name: str, # e.g., "gold.bc"
    target_table_name: Optional[str] = None, # e.g., "dim_Date"
    write_to_table: bool = True # Flag to control if function writes table or just returns DF
) -> Optional[DataFrame]:
    """
    Creates a date dimension table.
    Similar to BCPipeline.create_date_dimension but as a standalone function.
    """
    fiscal_year_start_month = get_bc_global_setting("fiscal_year_start_month") or 1
    etl_logger.info(f"Generating Date dimension from {start_date} to {end_date} with fiscal year starting in month {fiscal_year_start_month}")

    current_date_val = start_date
    dates_list = []
    while current_date_val <= end_date:
        dates_list.append(current_date_val)
        current_date_val += timedelta(days=1)

    date_df = spark.createDataFrame([(d,) for d in dates_list], ["Date"])

    date_df = (
        date_df.withColumn("DateKey", F.year("Date") * 10000 + F.month("Date") * 100 + F.dayofmonth("Date"))
        .withColumn("Year", F.year("Date"))
        .withColumn("Quarter", F.quarter("Date"))
        .withColumn("Month", F.month("Date"))
        .withColumn("DayOfMonth", F.dayofmonth("Date"))
        .withColumn("DayOfWeek", F.dayofweek("Date"))
        .withColumn("DayName", F.date_format("Date", "EEEE"))
        .withColumn("MonthName", F.date_format("Date", "MMMM"))
        .withColumn("WeekOfYear", F.weekofyear("Date"))
        .withColumn("IsWeekend", F.when(F.dayofweek("Date").isin([1, 7]), True).otherwise(False))
    )

    date_df = date_df.withColumn(
        "FiscalYear",
        F.when(F.col("Month") >= fiscal_year_start_month, F.col("Year"))
        .otherwise(F.col("Year") - 1)
    )
    date_df = date_df.withColumn(
        "FiscalMonth",
        F.when(F.col("Month") >= fiscal_year_start_month, F.col("Month") - fiscal_year_start_month + 1)
        .otherwise(F.col("Month") + 12 - fiscal_year_start_month + 1)
    )
    date_df = date_df.withColumn(
        "FiscalQuarter",
        ((F.col("FiscalMonth") - 1) / 3).cast("integer") + 1
    )

    if write_to_table:
        date_dim_config_name = "Date" 
        date_dim_config = get_bc_table_config(date_dim_config_name)
        
        final_target_table_name = target_table_name
        if not final_target_table_name:
            final_target_table_name = date_dim_config.get("gold_target_name", "dim_Date") if date_dim_config else "dim_Date"
        
        date_dim_partition_cols = date_dim_config.get("partition_columns_gold") if date_dim_config else None
        date_dim_path = f"{gold_schema_name}.{final_target_table_name}"
        
        try:
            write_delta_table(
                df=date_df,
                table_path=date_dim_path,
                mode="overwrite",
                partition_by_cols=date_dim_partition_cols,
                overwrite_schema_flag=True
            )
            etl_logger.info(f"Date dimension created and saved to {date_dim_path}")
        except Exception as e:
            etl_logger.error(f"Failed to save Date dimension to {date_dim_path}: {e}", exc_info=True)
            return None # Signal error if write fails
    return date_df

# --- Surrogate Key Generation ---
def generate_surrogate_key(
    spark: SparkSession, # Added SparkSession for consistency, though not strictly needed if df is passed
    df: DataFrame,
    table_config_name: str, # Clean name of the table to fetch NKs from config
    key_name: Optional[str] = None
) -> DataFrame:
    """
    Generates a surrogate key for a DataFrame based on natural keys from configuration.
    Similar to BCPipeline.generate_surrogate_key but as a standalone function.
    """
    config = get_bc_table_config(table_config_name)
    if not config:
        etl_logger.error(f"No configuration found for table {table_config_name} to generate surrogate key. Returning original DF.")
        return df 

    final_key_name = key_name if key_name else f"SK_{table_config_name}"

    natural_keys_for_table = config.get("natural_keys", [])
    if not natural_keys_for_table:
        etl_logger.error(f"No natural keys defined in config for table {table_config_name} to generate surrogate key '{final_key_name}'.")
        return df.withColumn(final_key_name, F.lit(None).cast("long"))

    actual_natural_keys = [k for k in natural_keys_for_table if k in df.columns]
    if len(actual_natural_keys) != len(natural_keys_for_table):
        missing_keys = [k for k in natural_keys_for_table if k not in df.columns]
        etl_logger.warning(f"Not all configured natural keys for table {table_config_name} found in DataFrame. Missing: {missing_keys}. Using available: {actual_natural_keys} for SK '{final_key_name}'.")
        if not actual_natural_keys:
            etl_logger.error(f"No configured natural keys for {table_config_name} are present in the DataFrame. Cannot generate SK '{final_key_name}'.")
            return df.withColumn(final_key_name, F.lit(None).cast("long"))
    
    order_by_cols = [F.col(k) for k in actual_natural_keys]
    
    company_col = "Company" 
    window_spec = Window.orderBy(*order_by_cols) 

    if company_col in df.columns and company_col not in actual_natural_keys:
         etl_logger.debug(f"Generating SK '{final_key_name}' for {table_config_name} partitioned by {company_col}, ordered by {actual_natural_keys}.")
         window_spec = Window.partitionBy(F.col(company_col)).orderBy(*order_by_cols)
    else:
         etl_logger.debug(f"Generating SK '{final_key_name}' for {table_config_name} ordered by {actual_natural_keys} (Company partitioning not applied or Company is in NKs).")
            
    result_df = df.withColumn(final_key_name, F.row_number().over(window_spec))
    etl_logger.info(f"Generated surrogate key '{final_key_name}' for table '{table_config_name}' using keys: {actual_natural_keys}")
    return result_df


# --- Dimension Bridge ---
def create_dimension_bridge(
    spark: SparkSession,
    silver_dim_set_entry_df: DataFrame, # DataFrame for DimensionSetEntry from Silver
    silver_dim_value_df: DataFrame,     # DataFrame for DimensionValue from Silver
    gold_schema_name: str,              # e.g., "gold.bc"
    dimension_types_override: Optional[Dict[str, str]] = None,
    target_table_name: Optional[str] = None, # e.g., "dim_DimensionBridge"
    write_to_table: bool = True
) -> Optional[DataFrame]:
    """
    Creates the Dimension Bridge table from DimensionSetEntry and DimensionValue.
    Similar to BCPipeline.create_dimension_bridge but as a standalone function.
    """
    etl_logger.info("Creating Dimension Bridge table (standalone function)")

    if silver_dim_set_entry_df is None or silver_dim_value_df is None:
        etl_logger.error("DimensionSetEntry or DimensionValue DataFrame is None. Cannot create bridge.")
        return None

    dim_set_entry_df = silver_dim_set_entry_df.alias("dse")
    dim_value_df = silver_dim_value_df.alias("dv")

    dimension_types_map = dimension_types_override
    if dimension_types_map is None:
        dimension_types_map = get_bc_global_setting("dimension_pivot_type_mapping")
    if dimension_types_map is None:
         dimension_types_map = { 
            "DEPARTMENT": "DepartmentName", "PROJECT": "ProjectName", 
            "CUSTOMERGROUP": "CustomerGroupName", "SALESPERSON": "SalespersonName",
         }
         etl_logger.debug(f"Using default dimension_types_map for bridge: {dimension_types_map}")

    join_conditions_list = [
        F.col("dse.DimensionCode") == F.col("dv.DimensionCode"),
        F.col("dse.DimensionValueCode") == F.col("dv.Code") 
    ]
    company_col = "Company"
    company_selected_alias = None
    if company_col in dim_set_entry_df.columns and company_col in dim_value_df.columns:
        join_conditions_list.append(F.col(f"dse.{company_col}") == F.col(f"dv.{company_col}"))
        # Select company from one table to avoid ambiguity. Alias it for clarity in bridge_df.
        company_selected_alias = F.col(f"dse.{company_col}").alias(company_col)
    else:
        etl_logger.warning(f"'{company_col}' column not found in one or both DimensionSetEntry/DimensionValue. Joining without it.")

    bridge_df_select_exprs = [
        F.col("dse.DimensionSetID"),
        F.col("dv.DimensionCode").alias("PivotDimensionCode"), # Alias to avoid clash if 'DimensionCode' is pivoted
        F.col("dv.Code").alias("DimensionValueCode"),
        F.col("dv.Name").alias("DimensionValueName")
    ]
    if company_selected_alias is not None:
        bridge_df_select_exprs.insert(1, company_selected_alias)


    bridge_df = dim_set_entry_df.join(
        dim_value_df,
        F.expr(" AND ".join([str(cond._jc) for cond in join_conditions_list])),
        "inner"
    ).select(*bridge_df_select_exprs)

    if "PivotDimensionCode" not in bridge_df.columns: # Check for the aliased column
        etl_logger.error("'PivotDimensionCode' (aliased dv.DimensionCode) not found in bridge_df after join. Cannot pivot.")
        return None

    distinct_dimension_codes = [row.PivotDimensionCode for row in bridge_df.select("PivotDimensionCode").distinct().collect() if row.PivotDimensionCode]
    
    pivot_expressions = []
    for dim_code_val in distinct_dimension_codes:
        col_name = dimension_types_map.get(dim_code_val.upper(), dim_code_val.upper().replace(" ", "_"))
        safe_col_name = "Dim_" + re.sub(r'[^a-zA-Z0-9_]', '', col_name)
        pivot_expressions.append(
            F.max(F.when(F.col("PivotDimensionCode") == dim_code_val, F.col("DimensionValueName"))).alias(safe_col_name)
        )
    
    group_by_cols = ["DimensionSetID"]
    if company_selected_alias is not None: # If 'Company' was selected
        group_by_cols.append(company_col) 
            
    if not pivot_expressions:
         etl_logger.warning("No dimension codes found to pivot for DimensionBridge.")
         if company_selected_alias is not None:
            final_bridge_df = bridge_df.select("DimensionSetID", company_col).distinct()
         else:
            final_bridge_df = bridge_df.select("DimensionSetID").distinct()
    else:
        final_bridge_df = bridge_df.groupBy(*group_by_cols).agg(*pivot_expressions)
    
    # Generate SK for the bridge table itself.
    bridge_config_name = "DimensionBridge" # Config name for DimensionBridge table
    final_bridge_df = generate_surrogate_key(spark, final_bridge_df, bridge_config_name, key_name="DimensionBridgeKey")
    
    if write_to_table:
        dim_bridge_config = get_bc_table_config(bridge_config_name)
        final_target_table_name = target_table_name
        if not final_target_table_name:
            final_target_table_name = dim_bridge_config.get("gold_target_name", "dim_DimensionBridge") if dim_bridge_config else "dim_DimensionBridge"
        
        gold_bridge_partition_cols = dim_bridge_config.get("partition_columns_gold") if dim_bridge_config else None
        bridge_table_path = f"{gold_schema_name}.{final_target_table_name}"
        
        try:
            write_delta_table(
                df=final_bridge_df,
                table_path=bridge_table_path,
                mode="overwrite",
                partition_by_cols=gold_bridge_partition_cols,
                overwrite_schema_flag=True
            )
            etl_logger.info(f"Dimension Bridge table created and saved to {bridge_table_path}")
        except Exception as e:
            etl_logger.error(f"Failed to save Dimension Bridge to {bridge_table_path}: {e}", exc_info=True)
            return None
            
    return final_bridge_df


# --- Account Hierarchy ---
def build_account_hierarchy(
    spark: SparkSession,
    gl_account_dim_df: DataFrame, # Input: GLAccount dimension with parent-child info
    gold_schema_name: str,
    target_table_name: Optional[str] = None, # e.g., "dim_GLAccountHierarchy"
    write_to_table: bool = True
    # config: Optional[Dict[str, Any]] = None # For any specific hierarchy configs
) -> Optional[DataFrame]:
    """
    Builds an Account Hierarchy from the GLAccount dimension.
    Placeholder function - actual hierarchy logic needs to be implemented based on BC_ETL.
    """
    etl_logger.info("Starting creation of Account Hierarchy (placeholder function).")
    if gl_account_dim_df is None:
        etl_logger.warning("GL Account dimension is None. Cannot build hierarchy.")
        return None

    # Actual hierarchy logic from BC_ETL would go here.
    # This often involves recursive CTEs (if Spark SQL) or iterative DataFrame operations.
    # For placeholder:
    # Assume gl_account_dim_df has 'No' (AccountNo) and 'ParentAccountNo' (or similar, e.g., 'Totaling')
    
    # Example: Check for necessary columns from the GLAccount dimension model
    # These names should match the Pydantic model for GLAccount
    account_no_col = "No" 
    parent_col_candidate_1 = "ParentAccountNo" # Hypothetical
    parent_col_candidate_2 = "Totaling"       # Common in BC for G/L Account hierarchy
    
    parent_col = None
    if parent_col_candidate_1 in gl_account_dim_df.columns:
        parent_col = parent_col_candidate_1
    elif parent_col_candidate_2 in gl_account_dim_df.columns:
        parent_col = parent_col_candidate_2

    if account_no_col not in gl_account_dim_df.columns or parent_col is None:
        etl_logger.error(f"GL Account dimension must contain '{account_no_col}' and a parent account column (e.g., '{parent_col_candidate_1}' or '{parent_col_candidate_2}') for hierarchy building.")
        return None
    
    etl_logger.info(f"Using '{parent_col}' as parent column for GL Account hierarchy.")

    # This is a highly simplified example and NOT a full hierarchy build.
    # A real implementation would use graph algorithms or iterative joins to determine levels, paths, etc.
    # For now, just select some key columns and add a placeholder level.
    hierarchy_df = gl_account_dim_df.select(
        F.col(account_no_col).alias("AccountNo"),
        F.col(parent_col).alias("ParentAccountNo"),
        F.col("Name").alias("AccountName"),
        F.col("SK_GLAccount").alias("GLAccountKey") # Assuming SK was generated
    ).withColumn("HierarchyLevel", F.lit(1)) # Placeholder level
    # Add other relevant attributes like AccountType, IncomeBalance, etc.

    if write_to_table:
        hierarchy_config_name = "GLAccountHierarchy" # Logical name for config
        hierarchy_config = get_bc_table_config(hierarchy_config_name)
        
        final_target_table_name = target_table_name
        if not final_target_table_name:
            final_target_table_name = hierarchy_config.get("gold_target_name", "dim_GLAccountHierarchy") if hierarchy_config else "dim_GLAccountHierarchy"
        
        partition_cols = hierarchy_config.get("partition_columns_gold") if hierarchy_config else None
        table_path = f"{gold_schema_name}.{final_target_table_name}"
        
        try:
            write_delta_table(
                df=hierarchy_df,
                table_path=table_path,
                mode="overwrite", # Hierarchies are typically overwritten
                partition_by_cols=partition_cols,
                overwrite_schema_flag=True
            )
            etl_logger.info(f"Account Hierarchy table (placeholder) saved to {table_path}")
        except Exception as e:
            etl_logger.error(f"Failed to save Account Hierarchy table (placeholder) to {table_path}: {e}", exc_info=True)
            return None
            
    etl_logger.info("Account Hierarchy processing complete (placeholder).")
    return hierarchy_df
