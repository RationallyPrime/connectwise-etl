import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Type

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from sparkdantic import SparkModel

# Import core utilities
from core_etl.logging_utils import etl_logger
from core_etl.config_utils import get_bc_table_config, get_bc_global_setting
from core_etl.delta_writer import write_delta_table
from core_etl.watermark_manager import get_watermark, update_watermark
# Spark session is expected to be passed by the caller, initialized using core_etl.spark_utils.get_spark_session()

# Import SCD Type 2 handling function (assuming it will be created here)
from fabric_api.silver.scd import apply_scd_type_2

# Updated Pydantic model import path
try:
    from fabric_api.models.bc_models import models as bc_all_models
except ImportError:
    etl_logger.warning("BC models from fabric_api.models.bc_models could not be imported. Ensure they are generated correctly.")
    bc_all_models = None # Placeholder


class BCPipeline:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        # Schemas can be made configurable via get_bc_global_setting if needed
        self.bronze_schema_name = get_bc_global_setting("bronze_schema_name") if get_bc_global_setting("bronze_schema_name") else "bronze.bc"
        self.silver_schema_name = get_bc_global_setting("silver_schema_name") if get_bc_global_setting("silver_schema_name") else "silver.bc"
        self.gold_schema_name = get_bc_global_setting("gold_schema_name") if get_bc_global_setting("gold_schema_name") else "gold.bc"
        
        self.schemas = {
            "bronze": self.bronze_schema_name,
            "silver": self.silver_schema_name,
            "gold": self.gold_schema_name,
        }
        self._create_schemas()

        self.model_mapping: Dict[str, Type[SparkModel]] = {}
        if bc_all_models:
            for model_name in dir(bc_all_models):
                model_class = getattr(bc_all_models, model_name)
                if isinstance(model_class, type) and issubclass(model_class, SparkModel) and model_class is not SparkModel:
                    self.model_mapping[model_name] = model_class
        
        if not self.model_mapping:
            etl_logger.warning("model_mapping is empty. BC Pydantic models might not be loaded correctly from fabric_api.models.bc_models.")

        # dimension_tables, fact_tables, and natural_keys are now sourced from YAML config via get_bc_table_config
        # No longer hardcoded here.

    def _create_schemas(self):
        for schema_name in self.schemas.values():
            try:
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                etl_logger.info(f"Ensured schema exists: {schema_name}")
            except Exception as e:
                etl_logger.error(f"Error creating schema {schema_name}: {e}, continuing...")

    def bronze_to_silver(
        self, 
        table_name: str, # This is the raw table name from bronze, e.g., Customer20
        run_incremental: bool = False # Parameter name changed for clarity
    ) -> Optional[DataFrame]:
        etl_logger.info(f"Processing {table_name} from Bronze to Silver. Incremental run: {run_incremental}")

        # 1. Get table configuration
        # Strip numeric suffix from table name to get clean_table_name for config lookup
        table_suffix_match = re.search(r'(\d+)$', table_name)
        table_numeric_suffix = table_suffix_match.group(1) if table_suffix_match else None
        clean_table_name = re.sub(r'\d+$', '', table_name) if table_numeric_suffix else table_name
        
        config = get_bc_table_config(clean_table_name)
        if not config:
            etl_logger.error(f"No configuration found for table {clean_table_name} (derived from {table_name}). Skipping.")
            return None

        watermark_column = config.get("incremental_column")
        scd_type = config.get("scd_type", "1") # Default to SCD Type 1
        natural_keys_config = config.get("natural_keys", []) # Used for SCD1 merge and general deduplication if not SCD2
        business_keys_config = config.get("business_keys", []) # Specifically for SCD2
        silver_target_table_name = config.get("silver_target_name", clean_table_name)
        silver_partition_columns = config.get("partition_columns_silver")

        # 2. Read bronze table
        bronze_table_path = f"{self.schemas['bronze']}.{table_name}"
        try:
            bronze_df = self.spark.table(bronze_table_path)
        except Exception as e:
            error_msg = str(e)
            if "doesn't exist" in error_msg or "not found" in error_msg:
                etl_logger.error(f"Table {bronze_table_path} not found or broken - skipping")
            else:
                etl_logger.error(f"Failed to read bronze table {bronze_table_path}: {error_msg}")
            return None
        
        etl_logger.info(f"Clean table name for processing: {clean_table_name}, Original table: {table_name}, Suffix: {table_numeric_suffix}")

        # 3. Rename columns: strip table's numeric suffix, standardize $Company
        current_columns = bronze_df.columns
        renamed_cols_expr = []
        for col_name in current_columns:
            new_col_name = col_name
            if table_numeric_suffix and new_col_name.endswith(table_numeric_suffix):
                new_col_name = new_col_name[:-len(table_numeric_suffix)]
                etl_logger.debug(f"Renaming column {col_name} to {new_col_name} for {clean_table_name}")
            
            if new_col_name == "$Company":
                new_col_name = "Company"
                etl_logger.debug(f"Renaming column $Company to Company for {clean_table_name}")

            renamed_cols_expr.append(F.col(col_name).alias(new_col_name))
        
        source_df = bronze_df.select(*renamed_cols_expr)

        # 4. Get Pydantic model and schema for validation and selection
        model_class = self.model_mapping.get(clean_table_name)
        validated_df = source_df # Start with source_df
        
        if model_class:
            etl_logger.info(f"Using Pydantic model {model_class.__name__} for {clean_table_name}")
            try:
                model_schema = model_class.model_spark_schema()
                model_field_names = set(model_schema.fieldNames())
                df_field_names = set(source_df.columns)
                
                columns_to_select = [mf for mf in model_field_names if mf in df_field_names]
                missing_in_df = [mf for mf in model_field_names if mf not in df_field_names]
                                
                if missing_in_df:
                    etl_logger.debug(f"Fields in model {model_class.__name__} but not in DataFrame for {clean_table_name}: {missing_in_df}")

                if not columns_to_select:
                    etl_logger.warning(f"No common columns found between DataFrame and model {model_class.__name__} for {clean_table_name}. Using DataFrame schema as is for further processing.")
                    # validated_df remains source_df
                else:
                    selected_df = source_df.select(*columns_to_select)
                    casted_df = selected_df
                    for field in model_schema.fields:
                        if field.name in selected_df.columns: # Ensure column exists before casting
                            casted_df = casted_df.withColumn(field.name, F.col(field.name).cast(field.dataType))
                    validated_df = casted_df
                    etl_logger.info(f"Schema validation and casting applied for {clean_table_name} using {model_class.__name__}")
            except Exception as e:
                etl_logger.warning(f"Could not fully apply Pydantic model schema for {clean_table_name} using {model_class.__name__}: {e}. Proceeding with best effort using available columns.")
                # validated_df might be source_df or partially processed
        else:
            etl_logger.warning(f"No Pydantic model found for {clean_table_name}. Proceeding without schema validation against Pydantic model.")

        # 5. Apply incremental filter if enabled
        current_max_watermark_val = None
        if run_incremental and watermark_column:
            if watermark_column not in validated_df.columns:
                etl_logger.warning(f"Watermark column '{watermark_column}' not found in DataFrame for {clean_table_name}. Cannot apply incremental filter.")
            else:
                last_watermark_str = get_watermark(self.spark, f"silver.{silver_target_table_name}") # Use target table name for watermark key
                if last_watermark_str:
                    etl_logger.info(f"Last watermark for {silver_target_table_name}: {last_watermark_str}")
                    # Ensure watermark column type is compatible for comparison
                    # Pydantic model casting should have handled this if watermark_column is in the model.
                    # If not, an explicit cast might be needed based on expected data type.
                    # For simplicity, assume it's a timestamp or string that can be directly compared.
                    validated_df = validated_df.filter(F.col(watermark_column) > F.lit(last_watermark_str)) 
                
                # Calculate new watermark value from the potentially filtered dataframe
                # Ensure the column is not all nulls before trying to get max.
                if watermark_column in validated_df.columns and validated_df.select(watermark_column).na.drop().count() > 0:
                    current_max_watermark_val = validated_df.agg(F.max(watermark_column)).collect()[0][0]
                    etl_logger.info(f"New potential watermark for {silver_target_table_name} from this batch: {current_max_watermark_val}")
                else:
                    etl_logger.info(f"No new data or no valid watermark values in {watermark_column} for {silver_target_table_name} in this batch.")


        # 6. Add generic processing timestamp. Specific SCD handling will manage SilverCreatedAt/SilverModifiedAt/EffectiveDate etc.
        df_with_processed_ts = validated_df.withColumn("SilverProcessedAt", F.current_timestamp())

        # 7. Handle SCD Type specific logic and Write to Silver
        silver_table_full_path = f"{self.schemas['silver']}.{silver_target_table_name}"
        write_mode = "overwrite" # Default, will be changed by SCD logic if needed
        df_to_write = df_with_processed_ts
        scd1_merge_params = None # For SCD1 merge keys

        if scd_type == "2":
            etl_logger.info(f"Applying SCD Type 2 processing for {clean_table_name} (target: {silver_target_table_name})")
            if not business_keys_config:
                etl_logger.error(f"SCD Type 2 specified for {clean_table_name}, but 'business_keys' are not defined in config. Skipping SCD2 processing.")
                return None # Or raise error, or fallback
            
            # Define SCD2 control columns (could be made configurable)
            effective_date_col = "SilverEffectiveDate"
            end_date_col = "SilverEndDate"
            current_flag_col = "IsSilverCurrent"
            
            etl_logger.info(f"Using Business Keys for SCD2: {business_keys_config}")
            etl_logger.info(f"SCD2 Control Columns: EffectiveDate='{effective_date_col}', EndDate='{end_date_col}', CurrentFlag='{current_flag_col}'")

            # The `apply_scd_type_2` function needs the target silver table to check existing records
            # It will read the existing silver table, compare, and return a DataFrame of records to append.
            # This DataFrame includes new records, and old records that need their end dates/current flags updated.
            # However, the current signature of apply_scd_type_2 is:
            # apply_scd_type_2(df: DataFrame, business_keys: list[str], effective_date_col: str, end_date_col: str, current_flag_col: str) -> DataFrame
            # This implies it takes the *source* df and handles the merge/comparison internally by reading the target table.
            # Let's assume apply_scd_type_2 needs the spark session and full silver table path to read the target.
            
            # The df_with_processed_ts contains only the *new or changed data from source for this batch*.
            # apply_scd_type_2 will need to:
            #   1. Read the existing silver table (`silver_table_full_path`).
            #   2. Join `df_with_processed_ts` with existing silver data on business keys.
            #   3. Identify new records, changed records, unchanged records.
            #   4. For changed records: generate a new version (effective date now, end date far future, current true)
            #      AND identify the old version in silver to be closed (update its end date, set current false).
            #   5. For new records: generate a new version (effective date now, end date far future, current true).
            #   6. Return a DataFrame of *all rows that need to be written* to the silver table.
            #      This implies `apply_scd_type_2` might return more rows than it received if it's also returning the updated old versions.
            #      OR, it might perform the updates on the target table directly (less likely for a function named "apply_")
            #      The prompt says "returns a DataFrame containing all records to be appended". This means it prepares the new/updated records.
            #      The `write_delta_table` with mode="append" will just add these.
            #      This approach requires the target table to be a Delta table that supports schema evolution and potentially complex appends if not careful.
            #      A more robust `apply_scd_type_2` might actually perform the MERGE itself using DeltaTable APIs.
            #      Given the constraints, we assume `apply_scd_type_2` returns a DataFrame that should simply be appended.

            df_to_write = apply_scd_type_2(
                spark=self.spark, # Assuming apply_scd_type_2 needs spark
                source_df=df_with_processed_ts, # New/changed data from this batch
                target_table_path=silver_table_full_path, # Full path to existing silver delta table
                business_keys=business_keys_config,
                effective_date_col=effective_date_col,
                end_date_col=end_date_col,
                current_flag_col=current_flag_col
                # `apply_scd_type_2` is responsible for SilverCreatedAt/SilverProcessedAt on the records it outputs
            )
            if df_to_write is None:
                etl_logger.error(f"apply_scd_type_2 returned None for {clean_table_name}. Skipping write.")
                return None

            write_mode = "append" # Always append for SCD Type 2 history
            etl_logger.info(f"SCD Type 2 processing for {silver_target_table_name} will use append mode.")

        elif run_incremental: # SCD Type 1 or other incremental types
            # Keys for SCD1 merge operation.
            scd1_merge_keys = natural_keys_config[:] # Use natural_keys for SCD1, not business_keys
            if "Company" not in scd1_merge_keys and "Company" in df_to_write.columns:
                etl_logger.debug(f"Adding 'Company' to SCD1 merge keys for {clean_table_name} if not already present.")
                scd1_merge_keys.insert(0, "Company")
            
            scd1_merge_keys = [k for k in scd1_merge_keys if k in df_to_write.columns]

            if scd_type == "1" and scd1_merge_keys:
                write_mode = "merge" 
                scd1_merge_params = scd1_merge_keys
                # df_to_write already has SilverProcessedAt.
                # write_delta_table (if mode='merge') should handle setting SilverCreatedAt for new records.
                etl_logger.info(f"Preparing SCD1 merge for {silver_target_table_name} with keys: {scd1_merge_params}")
            else: # Incremental append for non-SCD1 or if keys are missing
                if scd_type == "1" and not scd1_merge_keys:
                     etl_logger.warning(f"SCD Type 1 specified for {clean_table_name} but no valid natural keys found for merge. Falling back to append.")
                write_mode = "append"
                etl_logger.info(f"Preparing incremental append for {silver_target_table_name} (SCD type is '{scd_type}')")
        else: # Full load (non-incremental)
            write_mode = "overwrite"
            # For full overwrite, SilverCreatedAt would typically be set for all records during this load.
            df_to_write = df_to_write.withColumn("SilverCreatedAt", F.current_timestamp())
            etl_logger.info(f"Preparing full overwrite for {silver_target_table_name}")
        
        # Perform the write operation
        try:
            write_delta_table(
                df=df_to_write,
                table_path=silver_table_full_path,
                mode=write_mode,
                partition_by_cols=silver_partition_columns,
                merge_schema_flag=True, 
                overwrite_schema_flag=(write_mode == "overwrite"),
                scd1_keys=scd1_merge_params 
            )
            etl_logger.info(f"Successfully wrote to Silver table: {silver_table_full_path} with mode '{write_mode}'")

            # Update watermark if incremental and successful write (applies to both SCD1 and SCD2 source batch handling)
            if run_incremental and watermark_column and current_max_watermark_val is not None:
                update_watermark(self.spark, f"silver.{silver_target_table_name}", str(current_max_watermark_val))
                etl_logger.info(f"Successfully updated watermark for {silver_target_table_name} to {current_max_watermark_val}")

        except Exception as e:
            etl_logger.error(f"Failed to write or update watermark for Silver table {silver_target_table_name}: {e}", exc_info=True)
            return None # Signal failure
        
        self._log_dataframe_info(df_to_write, f"Silver data preview for {silver_target_table_name} (mode: {write_mode})")
        
        # Attach clean_table_name for orchestrator use
        # df_to_write might be significantly different in schema for SCD2 (new date/flag cols)
        # For consistency, we might want to return the original df_with_processed_ts if downstream processes
        # are not expecting the SCD2-specific columns. However, returning the actual written data is usually more correct.
        # The _processed_clean_table_name attribute is for the orchestrator to know which *entity* was processed.
        df_to_write._processed_clean_table_name = clean_table_name # type: ignore[attr-defined] 
        return df_to_write


    def generate_surrogate_key(self, df: DataFrame, table_config_name: str, key_name: Optional[str] = None) -> DataFrame:
        config = get_bc_table_config(table_config_name)
        if not config:
            etl_logger.error(f"No configuration found for table {table_config_name} to generate surrogate key. Skipping SK generation.")
            return df # Or add a null SK column: df.withColumn(key_name or f"SK_{table_config_name}", F.lit(None).cast("long"))

        if key_name is None:
            key_name = f"SK_{table_config_name}" # Default SK name

        natural_keys_for_table = config.get("natural_keys", [])
        if not natural_keys_for_table:
            etl_logger.error(f"No natural keys defined in config for table {table_config_name} to generate surrogate key '{key_name}'.")
            return df.withColumn(key_name, F.lit(None).cast("long"))

        actual_natural_keys = [k for k in natural_keys_for_table if k in df.columns]
        if len(actual_natural_keys) != len(natural_keys_for_table):
            missing_keys = [k for k in natural_keys_for_table if k not in df.columns]
            etl_logger.warning(f"Not all configured natural keys for table {table_config_name} found in DataFrame. Missing: {missing_keys}. Using available: {actual_natural_keys} for SK '{key_name}'.")
            if not actual_natural_keys:
                etl_logger.error(f"No configured natural keys for {table_config_name} are present in the DataFrame. Cannot generate surrogate key '{key_name}'.")
                return df.withColumn(key_name, F.lit(None).cast("long"))
        
        order_by_cols = [F.col(k) for k in actual_natural_keys]
        
        # Standardized company column name from Pydantic models
        company_col = "Company" 
        window_spec = Window.orderBy(*order_by_cols) # Default: no company partition

        # Check if 'Company' should be part of partitioning for the SK.
        # This depends on whether the natural keys are unique across companies or within a company.
        # For most BC tables, natural keys are unique within a company.
        # The config for natural_keys should include "Company" if it's part of the composite key.
        # If "Company" is present in df.columns AND it's NOT listed in natural_keys_for_table,
        # it implies we might need to partition by it before applying row_number over the other natural keys.
        # However, if "Company" IS part of natural_keys_for_table, it's already included in order_by_cols.
        
        # A common pattern is to partition by Company if it exists and is not already the sole natural key.
        if company_col in df.columns:
            if company_col not in actual_natural_keys : # If Company is present but not part of NKs used for ordering
                 etl_logger.debug(f"Generating surrogate key '{key_name}' for {table_config_name} partitioned by {company_col} and ordered by {actual_natural_keys}.")
                 window_spec = Window.partitionBy(F.col(company_col)).orderBy(*order_by_cols)
            else: # Company is part of natural keys, so it's in order_by_cols
                 etl_logger.debug(f"Generating surrogate key '{key_name}' for {table_config_name} ordered by {actual_natural_keys} (which includes Company).")
                 # window_spec remains Window.orderBy(*order_by_cols)
        else: # Company column not present
            etl_logger.debug(f"'Company' column not found in {table_config_name}. Surrogate key '{key_name}' will be generated based on ordering by {actual_natural_keys} without company partitioning.")
            
        result_df = df.withColumn(key_name, F.row_number().over(window_spec))
        etl_logger.info(f"Generated surrogate key '{key_name}' for table '{table_config_name}' using keys: {actual_natural_keys}")
        return result_df

    def create_date_dimension(self, start_date: date, end_date: date) -> DataFrame:
        fiscal_year_start_month = get_bc_global_setting("fiscal_year_start_month") or 1 # Default to January
        etl_logger.info(f"Creating Date dimension from {start_date} to {end_date} with fiscal year starting in month {fiscal_year_start_month}")
        
        current_date_val = start_date
        dates_list = []
        while current_date_val <= end_date:
            dates_list.append(current_date_val)
            current_date_val += timedelta(days=1)

        date_df = self.spark.createDataFrame([(d,) for d in dates_list], ["Date"])

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
            ( (F.col("FiscalMonth") - 1) / 3).cast("integer") + 1
        )
        
        # Configuration for date dimension target name (though "dim_Date" is standard)
        date_dim_config_name = "Date" # Logical name for config lookup
        date_dim_config = get_bc_table_config(date_dim_config_name)
        date_dim_target_name = date_dim_config.get("gold_target_name", "dim_Date") if date_dim_config else "dim_Date"
        date_dim_partition_cols = date_dim_config.get("partition_columns_gold") if date_dim_config else None

        date_dim_path = f"{self.schemas['gold']}.{date_dim_target_name}"
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
            # Depending on policy, might re-raise or return None
            raise # Re-raise to signal failure in run_pipeline
        return date_df

    def create_dimension_bridge(self, dimension_types_override: Optional[Dict[str, str]] = None) -> Optional[DataFrame]:
        etl_logger.info("Creating Dimension Bridge table (dim_DimensionBridge)")

        # Config names for source and target tables
        dim_set_entry_config_name = "DimensionSetEntry"
        dim_value_config_name = "DimensionValue"
        dim_bridge_config_name = "DimensionBridge"

        dim_set_entry_config = get_bc_table_config(dim_set_entry_config_name)
        dim_value_config = get_bc_table_config(dim_value_config_name)
        dim_bridge_config = get_bc_table_config(dim_bridge_config_name)

        if not (dim_set_entry_config and dim_value_config and dim_bridge_config):
            etl_logger.error("Missing configuration for DimensionSetEntry, DimensionValue, or DimensionBridge. Cannot create bridge.")
            return None

        # Get Silver layer table names from config (or use clean name as default)
        silver_dim_set_entry_name = dim_set_entry_config.get("silver_target_name", dim_set_entry_config_name)
        silver_dim_value_name = dim_value_config.get("silver_target_name", dim_value_config_name)
        
        dim_set_entry_path = f"{self.schemas['silver']}.{silver_dim_set_entry_name}"
        dim_value_path = f"{self.schemas['silver']}.{silver_dim_value_name}"

        try:
            dim_set_entry_df = self.spark.table(dim_set_entry_path)
            dim_value_df = self.spark.table(dim_value_path)
        except Exception as e:
            etl_logger.error(f"Failed to read {dim_set_entry_path} or {dim_value_path} tables: {e}", exc_info=True)
            return None

        # Use dimension_types_override if provided, else try from global settings, else default
        dimension_types_map = dimension_types_override
        if dimension_types_map is None:
            dimension_types_map = get_bc_global_setting("dimension_pivot_type_mapping")
        if dimension_types_map is None: # Still None, use hardcoded as last resort
             dimension_types_map = { 
                "DEPARTMENT": "DepartmentName", "PROJECT": "ProjectName", 
                "CUSTOMERGROUP": "CustomerGroupName", "SALESPERSON": "SalespersonName",
             }
             etl_logger.debug(f"Using default dimension_types_map for bridge: {dimension_types_map}")


        join_conditions_list = [
            dim_set_entry_df["DimensionCode"] == dim_value_df["DimensionCode"],
            dim_set_entry_df["DimensionValueCode"] == dim_value_df["Code"] # 'Code' is from DimensionValue model
        ]
        company_columns_for_select = []
        # Standardized 'Company' column name
        company_col = "Company" 
        if company_col in dim_set_entry_df.columns and company_col in dim_value_df.columns:
            join_conditions_list.append(dim_set_entry_df[company_col] == dim_value_df[company_col])
            # Select company from one table to avoid ambiguity (e.g., dim_set_entry_df[company_col])
            company_columns_for_select = [dim_set_entry_df[company_col]] 
        else:
            etl_logger.warning(f"'{company_col}' column not found in one or both {silver_dim_set_entry_name}/{silver_dim_value_name}. Joining without it.")

        bridge_df = dim_set_entry_df.join(
            dim_value_df,
            F.expr(" AND ".join([str(cond._jc) for cond in join_conditions_list])),
            "inner"
        ).select(
            dim_set_entry_df["DimensionSetID"],
            *company_columns_for_select, # Will be empty list if company_col not selected
            dim_value_df["DimensionCode"],
            dim_value_df["Code"].alias("DimensionValueCode"), # From DimensionValue model
            dim_value_df["Name"].alias("DimensionValueName")  # From DimensionValue model
        )

        if "DimensionCode" not in bridge_df.columns:
            etl_logger.error("DimensionCode column not found in bridge_df after join. Cannot pivot.")
            return None

        distinct_dimension_codes = [row.DimensionCode for row in bridge_df.select("DimensionCode").distinct().collect() if row.DimensionCode]
        
        pivot_expressions = []
        for dim_code_val in distinct_dimension_codes:
            # Use configured map, fall back to cleaning the dim_code_val itself
            col_name = dimension_types_map.get(dim_code_val.upper(), dim_code_val.upper().replace(" ", "_"))
            safe_col_name = "Dim_" + re.sub(r'[^a-zA-Z0-9_]', '', col_name)
            pivot_expressions.append(
                F.max(F.when(F.col("DimensionCode") == dim_code_val, F.col("DimensionValueName"))).alias(safe_col_name)
            )
        
        group_by_cols = ["DimensionSetID"]
        if company_columns_for_select: # If 'Company' was selected
            group_by_cols.append(company_col) 
            
        if not pivot_expressions:
             etl_logger.warning("No dimension codes found to pivot for DimensionBridge. Result will only have DimensionSetID (and Company if present).")
             if company_columns_for_select:
                final_bridge_df = bridge_df.select("DimensionSetID", company_col).distinct()
             else:
                final_bridge_df = bridge_df.select("DimensionSetID").distinct()
        else:
            final_bridge_df = bridge_df.groupBy(*group_by_cols).agg(*pivot_expressions)
        
        # Generate SK for the bridge table itself. Config name "DimensionBridge"
        final_bridge_df = self.generate_surrogate_key(final_bridge_df, dim_bridge_config_name, key_name="DimensionBridgeKey")
        
        gold_bridge_target_name = dim_bridge_config.get("gold_target_name", "dim_DimensionBridge")
        gold_bridge_partition_cols = dim_bridge_config.get("partition_columns_gold")
        bridge_table_path = f"{self.schemas['gold']}.{gold_bridge_target_name}"
        
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
            return None # Signal error
            
        self._log_dataframe_info(final_bridge_df, gold_bridge_target_name)
        return final_bridge_df

    def silver_to_gold_dimension(self, clean_silver_table_name: str) -> Optional[DataFrame]:
        etl_logger.info(f"Processing dimension table {clean_silver_table_name} from Silver to Gold")
        
        config = get_bc_table_config(clean_silver_table_name)
        if not config:
            etl_logger.error(f"No configuration found for dimension table {clean_silver_table_name}. Skipping.")
            return None

        # Silver source name might differ from clean_silver_table_name if specified in config
        silver_source_name = config.get("silver_target_name", clean_silver_table_name)
        silver_table_path = f"{self.schemas['silver']}.{silver_source_name}"
        
        gold_dim_target_name = config.get("gold_target_name", f"dim_{clean_silver_table_name}")
        gold_dim_partition_cols = config.get("partition_columns_gold")
        # scd_type_dim = config.get("scd_type_gold", "1") # For future SCD2/SCD1 handling in Gold

        try:
            silver_df = self.spark.table(silver_table_path)
        except Exception as e:
            etl_logger.error(f"Failed to read Silver table {silver_table_path}: {e}", exc_info=True)
            return None

        # Use clean_silver_table_name for SK generation config lookup
        dim_df = self.generate_surrogate_key(silver_df, clean_silver_table_name) 

        # Custom transformations for specific dimensions
        if clean_silver_table_name == "GLAccount":
            if "AccountCategory" in dim_df.columns:
                dim_df = dim_df.withColumn(
                    "AccountCategoryName",
                    F.when(F.col("AccountCategory") == 1, F.lit("Assets"))
                     .when(F.col("AccountCategory") == 2, F.lit("Liabilities"))
                     .when(F.col("AccountCategory") == 3, F.lit("Equity"))
                     .when(F.col("AccountCategory") == 4, F.lit("Income"))
                     .when(F.col("AccountCategory") == 5, F.lit("Expense"))
                     .otherwise(F.lit("Unknown"))
                )
        elif clean_silver_table_name == "Customer":
            address_cols = ["Address", "Address2", "City", "PostCode", "County", "CountryRegionCode"]
            actual_address_cols = [col for col in address_cols if col in dim_df.columns]
            if actual_address_cols:
                dim_df = dim_df.withColumn("FullAddress", F.concat_ws(", ", *[F.col(c) for c in actual_address_cols if c in dim_df.columns]))
            if "CustomerPostingGroup" in dim_df.columns:
                 dim_df = dim_df.withColumn("CustomerGroupSegment", F.col("CustomerPostingGroup"))
        
        # Placeholder for SCD Type 2 logic if this dimension requires it (e.g. if config indicates scd_type_gold == "2")
        # scd_type_gold = config.get("scd_type_gold", "1") 
        # if scd_type_gold == "2":
        #     etl_logger.info(f"SCD Type 2 processing for {gold_dim_target_name} is not yet implemented. Performing SCD1 overwrite.")
        #     # dim_df = self._handle_scd2_dimension(dim_df, gold_table_full_path, natural_keys_for_scd2_from_config)

        gold_table_full_path = f"{self.schemas['gold']}.{gold_dim_target_name}"
        try:
            # For dimensions, typically overwrite mode unless SCD2 is implemented with merges
            write_delta_table(
                df=dim_df,
                table_path=gold_table_full_path,
                mode="overwrite", # For SCD1 dims or initial load of SCD2
                partition_by_cols=gold_dim_partition_cols,
                overwrite_schema_flag=True 
            )
            etl_logger.info(f"Dimension table {gold_dim_target_name} saved to {gold_table_full_path}")
        except Exception as e:
            etl_logger.error(f"Failed to save Gold dimension table {gold_table_full_path}: {e}", exc_info=True)
            return None
            
        self._log_dataframe_info(dim_df, gold_dim_target_name)
        return dim_df

import importlib # Added for dynamic imports

# ... other imports ...

class BCPipeline:
    # ... __init__ and other methods ...

    def silver_to_gold_fact(
        self, 
        clean_silver_table_name: str, # This is the clean name of the silver table, e.g., "GLEntry"
        is_full_refresh: bool = False, # Parameter from run_pipeline to control write mode
        # min_year_filter is now fetched from global settings inside
    ) -> Optional[DataFrame]:
        etl_logger.info(f"Processing fact from silver table: {clean_silver_table_name}. Full refresh: {is_full_refresh}")
        
        table_config = get_bc_table_config(clean_silver_table_name)
        if not table_config:
            etl_logger.error(f"No configuration found for silver table '{clean_silver_table_name}' to process to Gold Fact. Skipping.")
            return None

        if table_config.get("table_type") != "fact":
            etl_logger.warning(f"Table '{clean_silver_table_name}' is not configured as 'fact' type. Skipping fact processing.")
            return None

        # Determine the target fact function and gold table name from config
        target_fact_function_name = table_config.get("gold_fact_function_name")
        if not target_fact_function_name:
            fact_type = table_config.get("gold_target_fact_type")
            if not fact_type:
                etl_logger.error(f"Configuration for '{clean_silver_table_name}' must specify either 'gold_fact_function_name' or 'gold_target_fact_type'. Skipping.")
                return None
            target_fact_function_name = f"create_{fact_type.lower()}_fact"
        
        gold_target_table_name = table_config.get("gold_target_name", f"fact_{clean_silver_table_name}")
        primary_silver_source_name = table_config.get("silver_target_name", clean_silver_table_name) # Actual silver table name
        
        etl_logger.info(f"Target fact function: '{target_fact_function_name}', Gold table: '{gold_target_table_name}', Primary Silver source: '{primary_silver_source_name}'")

        # 1. Read primary silver source DataFrame
        try:
            primary_silver_df = self.spark.table(f"{self.schemas['silver']}.{primary_silver_source_name}")
        except Exception as e:
            etl_logger.error(f"Failed to read primary Silver source table {self.schemas['silver']}.{primary_silver_source_name}: {e}", exc_info=True)
            return None

        # 2. Load common dimensions needed by many/all facts
        common_dims_to_load = {
            "date_dim_df": get_bc_table_config("Date").get("gold_target_name", "dim_Date") if get_bc_table_config("Date") else "dim_Date",
            "company_dim_df": get_bc_table_config("CompanyInformation").get("gold_target_name", "dim_CompanyInformation") if get_bc_table_config("CompanyInformation") else "dim_CompanyInformation",
            "dimension_bridge_df": get_bc_table_config("DimensionBridge").get("gold_target_name", "dim_DimensionBridge") if get_bc_table_config("DimensionBridge") else "dim_DimensionBridge",
        }
        loaded_dims = {}
        for df_arg_name, dim_table_gold_name in common_dims_to_load.items():
            try:
                # Ensure dim_table_gold_name is not None before attempting to load
                if dim_table_gold_name:
                    loaded_dims[df_arg_name] = self.spark.table(f"{self.schemas['gold']}.{dim_table_gold_name}")
                    etl_logger.info(f"Loaded common Gold dimension: {dim_table_gold_name} for {df_arg_name}")
                else:
                    etl_logger.warning(f"Configuration for common dimension key '{df_arg_name}' resulted in a None table name. Passing None.")
                    loaded_dims[df_arg_name] = None
            except Exception as e:
                etl_logger.warning(f"Could not load common Gold dimension {dim_table_gold_name}: {e}. Passing None for {df_arg_name}.", exc_info=False)
                loaded_dims[df_arg_name] = None
        
        # 3. Load specific dimensions based on fact function needs
        specific_dims_map = {
            "create_finance_fact": {
                "gl_account_dim_df": get_bc_table_config("GLAccount").get("gold_target_name", "dim_GLAccount") if get_bc_table_config("GLAccount") else "dim_GLAccount",
            },
            "create_sales_fact": {
                "customer_dim_df": get_bc_table_config("Customer").get("gold_target_name", "dim_Customer") if get_bc_table_config("Customer") else "dim_Customer",
                "item_dim_df": get_bc_table_config("Item").get("gold_target_name", "dim_Item") if get_bc_table_config("Item") else "dim_Item",
            },
            "create_purchase_fact": {
                "vendor_dim_df": get_bc_table_config("Vendor").get("gold_target_name", "dim_Vendor") if get_bc_table_config("Vendor") else "dim_Vendor",
                "item_dim_df": get_bc_table_config("Item").get("gold_target_name", "dim_Item") if get_bc_table_config("Item") else "dim_Item",
            },
            "create_inventory_fact": {
                "item_dim_df": get_bc_table_config("Item").get("gold_target_name", "dim_Item") if get_bc_table_config("Item") else "dim_Item",
                "location_dim_df": get_bc_table_config("Location").get("gold_target_name", "dim_Location") if get_bc_table_config("Location") else "dim_Location", 
            },
            "create_accounts_receivable_fact": {
                 "customer_dim_df": get_bc_table_config("Customer").get("gold_target_name", "dim_Customer") if get_bc_table_config("Customer") else "dim_Customer",
            },
            "create_accounts_payable_fact": {
                 "vendor_dim_df": get_bc_table_config("Vendor").get("gold_target_name", "dim_Vendor") if get_bc_table_config("Vendor") else "dim_Vendor",
            }
        }

        if target_fact_function_name in specific_dims_map:
            for df_arg_name, dim_table_gold_name in specific_dims_map[target_fact_function_name].items():
                if df_arg_name not in loaded_dims: 
                    try:
                        if dim_table_gold_name:
                            loaded_dims[df_arg_name] = self.spark.table(f"{self.schemas['gold']}.{dim_table_gold_name}")
                            etl_logger.info(f"Loaded specific Gold dimension for {target_fact_function_name}: {dim_table_gold_name} as {df_arg_name}")
                        else:
                            etl_logger.warning(f"Configuration for specific dimension key '{df_arg_name}' for '{target_fact_function_name}' resulted in a None table name. Passing None.")
                            loaded_dims[df_arg_name] = None
                    except Exception as e:
                        etl_logger.warning(f"Could not load specific Gold dimension {dim_table_gold_name} for {target_fact_function_name}: {e}. Passing None for {df_arg_name}.", exc_info=False)
                        loaded_dims[df_arg_name] = None
        
        # 4. Dynamically import and call the fact creation function
        try:
            bc_facts_module = importlib.import_module("fabric_api.gold.bc_facts")
            fact_function = getattr(bc_facts_module, target_fact_function_name)
        except (ImportError, AttributeError) as e:
            etl_logger.error(f"Could not find or import fact function '{target_fact_function_name}' from 'fabric_api.gold.bc_facts': {e}", exc_info=True)
            return None

        # 5. Get global settings and prepare parameters for the fact function
        min_year_filter_gold = get_bc_global_setting("min_year_filter_gold")
        fiscal_year_start_month = get_bc_global_setting("fiscal_year_start_month") or 1
        
        fact_function_params = {
            "spark": self.spark,
            "primary_silver_df": primary_silver_df,
            **loaded_dims, 
            "silver_db_name": self.schemas['silver'],
            "gold_db_name": self.schemas['gold'],
            "min_year_filter": min_year_filter_gold, # Use the specific gold filter
            "fiscal_year_start_month": fiscal_year_start_month,
            "is_incremental": not is_full_refresh, 
        }
        
        etl_logger.info(f"Calling fact function '{target_fact_function_name}' with available parameters.")
        gold_df = fact_function(**fact_function_params)

        # 6. Write the output DataFrame
        if gold_df:
            gold_table_full_path = f"{self.schemas['gold']}.{gold_target_table_name}"
            write_mode = "append" if not is_full_refresh else "overwrite" 
            gold_fact_partition_cols = table_config.get("partition_columns_gold") 
            
            etl_logger.info(f"Writing gold fact table {gold_table_full_path}. Mode: {write_mode}, Partitions: {gold_fact_partition_cols}")
            write_delta_table(
                df=gold_df,
                table_path=gold_table_full_path,
                mode=write_mode,
                partition_by_cols=gold_fact_partition_cols,
                merge_schema_flag=True, 
                overwrite_schema_flag=(write_mode == "overwrite")
            )
            etl_logger.info(f"Successfully wrote gold fact table: {gold_table_full_path}")
            self._log_dataframe_info(gold_df, gold_target_table_name)
            return gold_df
        else:
            etl_logger.warning(f"Gold fact DataFrame was not generated by '{target_fact_function_name}' for source '{clean_silver_table_name}'.")
            return None

    def run_pipeline(
        self,
        raw_bronze_tables_list: Optional[List[str]] = None, 
        run_incremental_silver: bool = False, 
        # gold_min_year_filter: Optional[int] = None, # Removed
        dimension_pivot_types_override: Optional[Dict[str, str]] = None,
        is_full_refresh_gold: bool = False, # Added
    ) -> Dict[str, Any]:
        etl_logger.info(f"Starting BC Medallion Pipeline. Incremental Silver: {run_incremental_silver}, Full Refresh Gold: {is_full_refresh_gold}")
        results: Dict[str, Any] = {"status": "started", "phases": {}}

        # Phase 1: Create Date Dimension
        etl_logger.info("Phase 1: Creating Date Dimension")
        results["phases"]["date_dimension"] = {"status": "pending", "details": {}}
        try:
            start_year_setting = get_bc_global_setting("date_dimension_start_year")
            min_year_gold_filter = get_bc_global_setting("min_year_filter_gold") # For consistency if facts use it
            
            default_start_year = 2015 
            # Prefer min_year_gold_filter if set, then specific date_dimension_start_year, then default
            start_year = min_year_gold_filter if min_year_gold_filter else (start_year_setting if start_year_setting else default_start_year)
            
            end_year_offset = get_bc_global_setting("date_dimension_end_year_offset") or 5 
            end_year = datetime.now().year + end_year_offset
            
            start_date_val = date(start_year, 1, 1)
            end_date_val = date(end_year, 12, 31)
            
            date_dim_df = self.create_date_dimension(start_date_val, end_date_val) 
            
            if date_dim_df is not None: 
                date_dim_config = get_bc_table_config("Date")
                date_dim_gold_name = date_dim_config.get("gold_target_name", "dim_Date") if date_dim_config else "dim_Date"
                results["phases"]["date_dimension"]["status"] = "success"
                results["phases"]["date_dimension"]["details"]["table_written"] = f"{self.schemas['gold']}.{date_dim_gold_name}"
                etl_logger.info(f"Date dimension created successfully: {self.schemas['gold']}.{date_dim_gold_name}")
        except Exception as e:
            etl_logger.error(f"Error in Phase 1 (Create Date Dimension): {e}", exc_info=True)
            results["phases"]["date_dimension"]["status"] = "error"
            results["phases"]["date_dimension"]["details"]["error_message"] = str(e)
        
        # Phase 2: Process Bronze to Silver
        etl_logger.info("Phase 2: Processing Bronze to Silver")
        results["phases"]["bronze_to_silver"] = {"status": "pending", "processed_tables": {}}
        silver_tables_processed_clean_names: List[str] = [] 

        active_bronze_tables = raw_bronze_tables_list
        if not active_bronze_tables: 
            try:
                etl_logger.info(f"No specific tables provided, discovering tables from schema: {self.schemas['bronze']}")
                bronze_spark_tables = self.spark.sql(f"SHOW TABLES IN {self.schemas['bronze']}").collect()
                active_bronze_tables = [t.tableName for t in bronze_spark_tables if not t.tableName.startswith("vw_")] 
                etl_logger.info(f"Discovered {len(active_bronze_tables)} tables in {self.schemas['bronze']}: {active_bronze_tables}")
            except Exception as e:
                etl_logger.error(f"Error discovering tables in {self.schemas['bronze']}: {e}", exc_info=True)
                results["phases"]["bronze_to_silver"]["status"] = "error"
                results["phases"]["bronze_to_silver"]["error_message"] = f"Failed to discover tables: {e}"
                active_bronze_tables = [] 

        for raw_table_name in active_bronze_tables:
            clean_name_for_config = re.sub(r'\d+$', '', raw_table_name)
            table_config = get_bc_table_config(clean_name_for_config)
            if not (clean_name_for_config in self.model_mapping or table_config):
                 etl_logger.info(f"Skipping table {raw_table_name} (no Pydantic model, no explicit config).")
                 continue

            table_detail_report = {"source_bronze_table": raw_table_name, "status": "pending"}
            try:
                silver_df = self.bronze_to_silver(raw_table_name, run_incremental=run_incremental_silver)
                if silver_df:
                    processed_clean_name = getattr(silver_df, "_processed_clean_table_name", clean_name_for_config)
                    silver_target_name = table_config.get("silver_target_name", processed_clean_name) if table_config else processed_clean_name
                    silver_tables_processed_clean_names.append(processed_clean_name)
                    table_detail_report["status"] = "success"
                    table_detail_report["silver_table"] = f"{self.schemas['silver']}.{silver_target_name}"
                    etl_logger.info(f"Successfully processed {raw_table_name} to Silver table {silver_target_name}")
                else:
                    table_detail_report["status"] = "skipped_or_error"
                    table_detail_report["reason"] = "bronze_to_silver returned None or error (see logs)."
            except Exception as e:
                etl_logger.error(f"Major failure in bronze_to_silver for {raw_table_name}: {e}", exc_info=True)
                table_detail_report["status"] = "error"; table_detail_report["error_message"] = str(e)
            results["phases"]["bronze_to_silver"]["processed_tables"][raw_table_name] = table_detail_report
        results["phases"]["bronze_to_silver"]["status"] = "completed" if silver_tables_processed_clean_names else "completed_with_no_successes"


        # Phase 3: Process Silver to Gold - Dimensions
        etl_logger.info("Phase 3: Processing Silver to Gold - Dimensions")
        results["phases"]["silver_to_gold_dimensions"] = {"status": "pending", "processed_tables": {}}
        processed_gold_dimensions_map: Dict[str, str] = {} 
        for clean_silver_name in silver_tables_processed_clean_names: 
            config = get_bc_table_config(clean_silver_name)
            if config and config.get("table_type") == "dimension":
                table_detail_report = {"source_silver_table": clean_silver_name, "status": "pending"}
                try:
                    gold_dim_df = self.silver_to_gold_dimension(clean_silver_name) 
                    if gold_dim_df:
                        gold_dim_target_name = config.get("gold_target_name", f"dim_{clean_silver_name}")
                        table_detail_report["status"] = "success"
                        table_detail_report["gold_table"] = f"{self.schemas['gold']}.{gold_dim_target_name}"
                        processed_gold_dimensions_map[clean_silver_name] = gold_dim_target_name
                    else: table_detail_report["status"] = "skipped_or_error"
                except Exception as e:
                    etl_logger.error(f"Major failure processing dimension {clean_silver_name} to Gold: {e}", exc_info=True)
                    table_detail_report["status"] = "error"; table_detail_report["error_message"] = str(e)
                results["phases"]["silver_to_gold_dimensions"]["processed_tables"][clean_silver_name] = table_detail_report
        results["phases"]["silver_to_gold_dimensions"]["status"] = "completed"


        # Phase 4: Create Dimension Bridge
        etl_logger.info("Phase 4: Creating Dimension Bridge")
        results["phases"]["dimension_bridge"] = {"status": "pending", "details": {}}
        dim_set_entry_conf_name = "DimensionSetEntry"; dim_value_conf_name = "DimensionValue"
        can_create_bridge = all(name in silver_tables_processed_clean_names for name in [dim_set_entry_conf_name, dim_value_conf_name])
        
        if can_create_bridge:
            try:
                bridge_df = self.create_dimension_bridge(dimension_types_override=dimension_pivot_types_override) 
                if bridge_df:
                    bridge_conf = get_bc_table_config("DimensionBridge")
                    bridge_gold_name = bridge_conf.get("gold_target_name", "dim_DimensionBridge") if bridge_conf else "dim_DimensionBridge"
                    results["phases"]["dimension_bridge"]["status"] = "success"
                    results["phases"]["dimension_bridge"]["details"]["table_written"] = f"{self.schemas['gold']}.{bridge_gold_name}"
                    processed_gold_dimensions_map["DimensionBridge"] = bridge_gold_name 
                else: results["phases"]["dimension_bridge"]["status"] = "skipped_or_error"
            except Exception as e:
                etl_logger.error(f"Major failure creating dimension bridge: {e}", exc_info=True)
                results["phases"]["dimension_bridge"]["status"] = "error"; results["phases"]["dimension_bridge"]["details"]["error_message"] = str(e)
        else:
            missing_reqs = [name for name in [dim_set_entry_conf_name, dim_value_conf_name] if name not in silver_tables_processed_clean_names]
            results["phases"]["dimension_bridge"]["status"] = "skipped"; results["phases"]["dimension_bridge"]["details"]["reason"] = f"Required source tables not in Silver: {missing_reqs}"
            etl_logger.warning(f"Skipping dimension bridge creation, missing Silver tables: {missing_reqs}")

        # Phase 5: Process Silver to Gold - Facts
        etl_logger.info("Phase 5: Processing Silver to Gold - Facts")
        results["phases"]["silver_to_gold_facts"] = {"status": "pending", "processed_tables": {}}
        for clean_silver_name in silver_tables_processed_clean_names: 
            config = get_bc_table_config(clean_silver_name)
            if config and config.get("table_type") == "fact":
                if clean_silver_name == dim_set_entry_conf_name and results["phases"]["dimension_bridge"]["status"] == "success":
                    etl_logger.info(f"Skipping {clean_silver_name} in facts processing as it's input to dim_DimensionBridge.")
                    continue

                table_detail_report = {"source_silver_table": clean_silver_name, "status": "pending"}
                try:
                    etl_logger.info(f"Processing fact: {clean_silver_name} from Silver to Gold with is_full_refresh_gold={is_full_refresh_gold}")
                    gold_fact_df = self.silver_to_gold_fact(
                        clean_silver_table_name=clean_silver_name,
                        is_full_refresh=is_full_refresh_gold 
                    )
                    if gold_fact_df:
                        gold_fact_target_name = config.get("gold_target_name", f"fact_{clean_silver_name}")
                        table_detail_report["status"] = "success"
                        table_detail_report["gold_table"] = f"{self.schemas['gold']}.{gold_fact_target_name}"
                    else: table_detail_report["status"] = "skipped_or_error"
                except Exception as e:
                    etl_logger.error(f"Major failure processing fact {clean_silver_name} to Gold: {e}", exc_info=True)
                    table_detail_report["status"] = "error"; table_detail_report["error_message"] = str(e)
                results["phases"]["silver_to_gold_facts"]["processed_tables"][clean_silver_name] = table_detail_report
        results["phases"]["silver_to_gold_facts"]["status"] = "completed"
        
        results["status"] = "completed"
        etl_logger.info("BC Medallion Pipeline full run attempt finished.")
        return results

    def _log_dataframe_info(self, df: DataFrame, name: str):
        try:
            count = -1 
            # count = df.count() # Potentially expensive
            etl_logger.info(f"DataFrame '{name}': Schema below. Count (if calculated): {count}")
            df.printSchema()
            # df.show(5, truncate=False) # Potentially verbose
        except Exception as e:
            etl_logger.warning(f"Could not log full info for DataFrame {name}: {e}")
