import logging
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Type

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from sparkdantic import SparkModel

# Attempt to import all models from the newly generated bc_models
# This might be a long list, so handle potential import errors gracefully for now
# if the models haven't been generated in the exact expected way yet.
try:
    from fabric_api.bc_models import models as bc_all_models # Assuming __init__ exposes them
    # Or, if models are directly in bc_models.models:
    # from fabric_api.bc_models.models import * 
except ImportError:
    logging.warning("BC models could not be imported. Ensure they are generated correctly.")
    bc_all_models = None # Placeholder

logger = logging.getLogger(__name__)

class BCPipeline:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.schemas = {
            "bronze": "bronze.bc",
            "silver": "silver.bc",
            "gold": "gold.bc",
        }
        self._create_schemas()

        self.model_mapping: Dict[str, Type[SparkModel]] = {}
        if bc_all_models:
            # Dynamically populate model_mapping from imported models
            # Assumes model class names match clean table names
            for model_name in dir(bc_all_models):
                model_class = getattr(bc_all_models, model_name)
                if isinstance(model_class, type) and issubclass(model_class, SparkModel) and model_class is not SparkModel:
                    # Store by class name (which should be the clean table name)
                    self.model_mapping[model_name] = model_class
        
        if not self.model_mapping:
            logger.warning("model_mapping is empty. BC Pydantic models might not be loaded correctly.")

        # Define natural keys using CamelCase field names as per new models
        # These should match the Python attribute names in the generated Pydantic models
        self.natural_keys = {
            "AccountingPeriod": ["StartingDate"],
            "CompanyInformation": ["Name"], # Assuming 'Name' is the PK for CompanyInformation
            "Currency": ["Code"],
            "CustLedgerEntry": ["EntryNo"],
            "Customer": ["No"],
            "DetailedCustLedgEntry": ["EntryNo"],
            "DetailedVendorLedgEntry": ["EntryNo"],
            "Dimension": ["Code"],
            "DimensionSetEntry": ["DimensionSetID", "DimensionCode"], # Check if 'Company' is needed
            "DimensionValue": ["DimensionCode", "Code"], # Check if 'Company' is needed
            "GLAccount": ["No"],
            "GLEntry": ["EntryNo"],
            "GeneralLedgerSetup": ["PrimaryKey"], # This table is often a singleton
            "Item": ["No"],
            "Job": ["No"],
            "JobLedgerEntry": ["EntryNo"],
            "Resource": ["No"],
            "SalesInvoiceHeader": ["No"],
            "SalesInvoiceLine": ["DocumentNo", "LineNo"],
            "Vendor": ["No"],
            "VendorLedgerEntry": ["EntryNo"],
        }

        self.dimension_tables = [
            "AccountingPeriod", "GLAccount", "Customer", "Vendor", "Item",
            "Dimension", "DimensionValue", "Currency", "CompanyInformation",
            "Job", "Resource", "GeneralLedgerSetup",
        ]

        self.fact_tables = [
            "GLEntry", "CustLedgerEntry", "VendorLedgerEntry", "JobLedgerEntry",
            "SalesInvoiceHeader", "SalesInvoiceLine", "DetailedCustLedgEntry",
            "DetailedVendorLedgEntry", "DimensionSetEntry", # DimensionSetEntry can also be seen as a fact or bridge component
        ]

    def _create_schemas(self):
        for schema_name in self.schemas.values():
            try:
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                logger.info(f"Ensured schema exists: {schema_name}")
            except Exception as e:
                logger.error(f"Error creating schema {schema_name}: {e}, continuing...")

    def bronze_to_silver(
        self, 
        table_name: str, 
        incremental: bool = False, 
        watermark_column: str = "SystemModifiedAt" # Assuming SystemModifiedAt is CamelCase from model
    ) -> Optional[DataFrame]:
        logger.info(f"Processing {table_name} from Bronze to Silver")

        # 1. Read bronze table
        bronze_table_path = f"{self.schemas['bronze']}.{table_name}"
        try:
            bronze_df = self.spark.table(bronze_table_path)
        except Exception as e:
            error_msg = str(e)
            if "doesn't exist" in error_msg or "not found" in error_msg:
                logger.error(f"Table {bronze_table_path} not found or broken - skipping")
            else:
                logger.error(f"Failed to read bronze table {bronze_table_path}: {error_msg}")
            return None

        # 2. Strip numeric suffix from table name and extract suffix
        table_suffix_match = re.search(r'(\d+)$', table_name)
        table_numeric_suffix = table_suffix_match.group(1) if table_suffix_match else None
        clean_table_name = re.sub(r'\d+$', '', table_name) if table_numeric_suffix else table_name
        
        logger.info(f"Clean table name: {clean_table_name}, Table suffix: {table_numeric_suffix}")

        # 3. Rename columns: strip table's numeric suffix, standardize $Company
        current_columns = bronze_df.columns
        renamed_cols_expr = []
        for col_name in current_columns:
            new_col_name = col_name
            if table_numeric_suffix and new_col_name.endswith(table_numeric_suffix):
                new_col_name = new_col_name[:-len(table_numeric_suffix)]
                logger.debug(f"Renaming column {col_name} to {new_col_name}")
            
            if new_col_name == "$Company":
                new_col_name = "Company" # Standardize to 'Company'
                logger.debug(f"Renaming column $Company to Company")

            renamed_cols_expr.append(F.col(col_name).alias(new_col_name))
        
        transformed_df = bronze_df.select(*renamed_cols_expr)

        # 4. Get Pydantic model and schema
        model_class = self.model_mapping.get(clean_table_name)
        validated_df = transformed_df
        
        if model_class:
            logger.info(f"Using Pydantic model {model_class.__name__} for {clean_table_name}")
            try:
                model_schema = model_class.model_spark_schema()
                
                # Select only columns present in the model schema from the transformed_df
                # Ensure field names in model_schema are used for selection
                model_field_names = set(model_schema.fieldNames())
                df_field_names = set(transformed_df.columns)
                
                columns_to_select = []
                missing_in_df = []
                
                for model_field_name in model_field_names:
                    if model_field_name in df_field_names:
                        columns_to_select.append(model_field_name)
                    else:
                        # This can happen if a field in model is not in source, which is fine
                        missing_in_df.append(model_field_name)
                
                if missing_in_df:
                    logger.debug(f"Fields in model but not in DataFrame for {clean_table_name}: {missing_in_df}")

                if not columns_to_select:
                    logger.warning(f"No common columns found between DataFrame and model for {clean_table_name}. Skipping schema validation.")
                    validated_df = transformed_df # Use transformed_df if no common columns
                else:
                    selected_df = transformed_df.select(*columns_to_select)
                
                    # Cast columns to model schema types
                    casted_df = selected_df
                    for field in model_schema.fields:
                        if field.name in selected_df.columns:
                            casted_df = casted_df.withColumn(field.name, F.col(field.name).cast(field.dataType))
                    validated_df = casted_df
                    logger.info(f"Schema validation and casting applied for {clean_table_name} using {model_class.__name__}")

            except Exception as e:
                logger.warning(f"Could not fully apply Pydantic model schema for {clean_table_name} using {model_class.__name__}: {e}. Proceeding with best effort.")
                # validated_df remains transformed_df if error during schema application
        else:
            logger.warning(f"No Pydantic model found for {clean_table_name}. Proceeding without schema validation.")

        # 5. Apply incremental filter if enabled (using original watermark_column name)
        # The watermark column should ideally be defined in the model and thus use CamelCase.
        if incremental and watermark_column in validated_df.columns: # Check against validated_df columns
            # This part might need adjustment if watermark columns also have suffixes in bronze
            # For now, assume watermark_column is its final CamelCase name from model
            # Or, it's a system column that doesn't get renamed.
            
            # Get last watermark (would normally come from a metadata table)
            # Using a fixed lookback for example purposes
            seven_days_ago = datetime.now() - timedelta(days=7)
            # Ensure the watermark column is of a comparable type (timestamp or date)
            # This relies on the model schema casting if watermark_column is part of the model
            validated_df = validated_df.filter(F.col(watermark_column) >= F.lit(seven_days_ago).cast("timestamp"))
            logger.info(f"Applied incremental filter on {watermark_column} for {clean_table_name}")


        # 6. Add processing metadata
        silver_df = validated_df.withColumn("SilverProcessedAt", F.current_timestamp())

        # 7. Apply data quality checks (Drop duplicates)
        # Natural keys should use CamelCase field names from models, including 'Company'
        if clean_table_name in self.natural_keys:
            nk_columns = self.natural_keys[clean_table_name]
            # Check if 'Company' is part of natural keys or should be added if present in df
            columns_for_dedup = nk_columns[:] # Make a copy
            if "Company" not in columns_for_dedup and "Company" in silver_df.columns:
                 # Check if 'Company' should be part of the composite key for this table
                 # This logic might need refinement based on specific table needs.
                 # For many BC tables, primary keys are often per company.
                 # For simplicity, if 'Company' column exists, add it to NK for deduplication.
                logger.debug(f"Adding 'Company' to natural keys for deduplication of {clean_table_name}")
                columns_for_dedup.insert(0, "Company") # Add Company as the first key for partitioning

            # Ensure all nk_columns for deduplication actually exist in the DataFrame
            final_dedup_cols = [col for col in columns_for_dedup if col in silver_df.columns]
            if final_dedup_cols:
                if len(final_dedup_cols) != len(columns_for_dedup):
                    logger.warning(f"Not all natural key columns found for {clean_table_name} for deduplication. Original: {columns_for_dedup}, Found: {final_dedup_cols}")
                
                logger.info(f"Dropping duplicates for {clean_table_name} based on keys: {final_dedup_cols}")
                silver_df = silver_df.dropDuplicates(final_dedup_cols)
            else:
                logger.warning(f"No natural key columns found in DataFrame for {clean_table_name}. Skipping deduplication.")
        
        self._log_dataframe_info(silver_df, f"Silver {clean_table_name}")

        # Attach clean_table_name for the caller (e.g., orchestrator)
        # This is a common pattern but not standard DataFrame API. Consider a wrapper class or returning a tuple.
        silver_df._table_name = clean_table_name # type: ignore[attr-defined]
        return silver_df

    def generate_surrogate_key(self, df: DataFrame, table_name: str, key_name: Optional[str] = None) -> DataFrame:
        if key_name is None:
            key_name = f"SK_{table_name}"

        natural_keys_for_table = self.natural_keys.get(table_name, [])
        if not natural_keys_for_table:
            logger.error(f"No natural keys defined for table {table_name} to generate surrogate key.")
            return df.withColumn(key_name, F.lit(None).cast("long")) # Add an empty key column

        # Ensure all natural key columns exist in the DataFrame
        actual_natural_keys = [k for k in natural_keys_for_table if k in df.columns]
        if len(actual_natural_keys) != len(natural_keys_for_table):
            missing_keys = [k for k in natural_keys_for_table if k not in df.columns]
            logger.warning(f"Not all defined natural keys for table {table_name} found in DataFrame. Missing: {missing_keys}. Using available: {actual_natural_keys}")
            if not actual_natural_keys: # If no NKs are present at all
                logger.error(f"No natural keys for {table_name} are present in the DataFrame. Cannot generate surrogate key.")
                return df.withColumn(key_name, F.lit(None).cast("long"))
        
        order_by_cols = [F.col(k) for k in actual_natural_keys]
        
        company_col = "Company"
        if company_col in df.columns and company_col not in actual_natural_keys:
            window_spec = Window.partitionBy(F.col(company_col)).orderBy(*order_by_cols)
            logger.debug(f"Generating surrogate key for {table_name} partitioned by {company_col}.")
        else:
            window_spec = Window.orderBy(*order_by_cols)
            if company_col not in df.columns:
                 logger.debug(f"'Company' column not found in {table_name}. Surrogate key will be generated without company partitioning.")
            else: # Company is part of natural keys
                 logger.debug(f"'Company' is part of natural keys for {table_name}. Surrogate key generation will include it in ordering.")
        
        result_df = df.withColumn(key_name, F.row_number().over(window_spec))
        logger.info(f"Generated surrogate key '{key_name}' for table '{table_name}' using keys: {actual_natural_keys}")
        return result_df

    def create_date_dimension(self, start_date: date, end_date: date, fiscal_year_start_month: int = 1) -> DataFrame:
        logger.info(f"Creating Date dimension from {start_date} to {end_date} with fiscal year starting in month {fiscal_year_start_month}")
        
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
            .withColumn("DayOfWeek", F.dayofweek("Date")) # Sunday=1, Saturday=7
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
        
        date_dim_path = f"{self.schemas['gold']}.dim_Date"
        try:
            date_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(date_dim_path)
            logger.info(f"Date dimension created and saved to {date_dim_path}")
        except Exception as e:
            logger.error(f"Failed to save Date dimension to {date_dim_path}: {e}")
        return date_df

    def create_dimension_bridge(self, dimension_types: Optional[Dict[str, str]] = None) -> Optional[DataFrame]:
        logger.info("Creating Dimension Bridge table (dim_DimensionBridge)")
        if dimension_types is None:
            dimension_types = { # Using placeholder names, actual DimensionCode values from data are needed
                "DEPARTMENT": "DepartmentName",
                "PROJECT": "ProjectName",
                "CUSTOMERGROUP": "CustomerGroupName",
                "SALESPERSON": "SalespersonName",
            }

        dim_set_entry_path = f"{self.schemas['silver']}.DimensionSetEntry"
        dim_value_path = f"{self.schemas['silver']}.DimensionValue"

        try:
            dim_set_entry_df = self.spark.table(dim_set_entry_path)
            dim_value_df = self.spark.table(dim_value_path)
        except Exception as e:
            logger.error(f"Failed to read DimensionSetEntry or DimensionValue tables: {e}")
            return None

        join_conditions_list = [
            dim_set_entry_df["DimensionCode"] == dim_value_df["DimensionCode"],
            dim_set_entry_df["DimensionValueCode"] == dim_value_df["Code"]
        ]
        company_columns_for_select = []
        if "Company" in dim_set_entry_df.columns and "Company" in dim_value_df.columns:
            join_conditions_list.append(dim_set_entry_df["Company"] == dim_value_df["Company"])
            company_columns_for_select = [dim_set_entry_df["Company"]] # Ensure Company is selected
        else:
            logger.warning("'Company' column not found in one or both DimensionSetEntry/DimensionValue. Joining without it.")

        bridge_df = dim_set_entry_df.join(
            dim_value_df,
            F.expr(" AND ".join([str(cond._jc) for cond in join_conditions_list])), # type: ignore[attr-defined]
            "inner"
        ).select(
            dim_set_entry_df["DimensionSetID"],
            *company_columns_for_select,
            dim_value_df["DimensionCode"],
            dim_value_df["Code"].alias("DimensionValueCode"),
            dim_value_df["Name"].alias("DimensionValueName")
        )

        if "DimensionCode" not in bridge_df.columns:
            logger.error("DimensionCode column not found in bridge_df after join. Cannot pivot.")
            return None

        distinct_dimension_codes = [row.DimensionCode for row in bridge_df.select("DimensionCode").distinct().collect() if row.DimensionCode]
        
        pivot_expressions = []
        for dim_code_val in distinct_dimension_codes:
            col_name = dimension_types.get(dim_code_val.upper(), dim_code_val.upper().replace(" ", "_")) # Use upper for matching keys
            safe_col_name = "Dim_" + re.sub(r'[^a-zA-Z0-9_]', '', col_name) # Prefix to ensure valid and distinct
            pivot_expressions.append(
                F.max(F.when(F.col("DimensionCode") == dim_code_val, F.col("DimensionValueName"))).alias(safe_col_name)
            )
        
        group_by_cols = ["DimensionSetID"]
        if "Company" in bridge_df.columns:
            group_by_cols.append("Company")
            
        if not pivot_expressions:
             logger.warning("No dimension codes found to pivot for DimensionBridge.")
             # Create a DataFrame with just keys if no pivot expressions
             if "Company" in bridge_df.columns:
                final_bridge_df = bridge_df.select("DimensionSetID", "Company").distinct()
             else:
                final_bridge_df = bridge_df.select("DimensionSetID").distinct()
        else:
            final_bridge_df = bridge_df.groupBy(*group_by_cols).agg(*pivot_expressions)

        final_bridge_df = self.generate_surrogate_key(final_bridge_df, "DimensionBridge", key_name="DimensionBridgeKey")
        
        bridge_table_path = f"{self.schemas['gold']}.dim_DimensionBridge"
        try:
            final_bridge_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(bridge_table_path)
            logger.info(f"Dimension Bridge table created and saved to {bridge_table_path}")
        except Exception as e:
            logger.error(f"Failed to save Dimension Bridge to {bridge_table_path}: {e}")
            
        self._log_dataframe_info(final_bridge_df, "dim_DimensionBridge")
        return final_bridge_df

    def silver_to_gold_dimension(self, table_name: str) -> Optional[DataFrame]:
        logger.info(f"Processing dimension table {table_name} from Silver to Gold")
        silver_table_path = f"{self.schemas['silver']}.{table_name}"
        
        try:
            silver_df = self.spark.table(silver_table_path)
        except Exception as e:
            logger.error(f"Failed to read Silver table {silver_table_path}: {e}")
            return None

        dim_df = self.generate_surrogate_key(silver_df, table_name) # SK name will be SK_{table_name}

        if table_name == "GLAccount":
            if "AccountCategory" in dim_df.columns: # Field name from model
                dim_df = dim_df.withColumn(
                    "AccountCategoryName", # New derived column
                    F.when(F.col("AccountCategory") == 1, F.lit("Assets"))
                     .when(F.col("AccountCategory") == 2, F.lit("Liabilities"))
                     .when(F.col("AccountCategory") == 3, F.lit("Equity"))
                     .when(F.col("AccountCategory") == 4, F.lit("Income"))
                     .when(F.col("AccountCategory") == 5, F.lit("Expense"))
                     .otherwise(F.lit("Unknown"))
                )
        elif table_name == "Customer":
            address_cols = ["Address", "Address2", "City", "PostCode", "County", "CountryRegionCode"]
            actual_address_cols = [col for col in address_cols if col in dim_df.columns]
            if actual_address_cols: # Check if any address columns are actually present
                dim_df = dim_df.withColumn("FullAddress", F.concat_ws(", ", *[F.col(c) for c in actual_address_cols]))
            
            if "CustomerPostingGroup" in dim_df.columns: # Field name from model
                 dim_df = dim_df.withColumn("CustomerGroupSegment", F.col("CustomerPostingGroup"))

        gold_table_path = f"{self.schemas['gold']}.dim_{table_name}"
        try:
            dim_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_table_path)
            logger.info(f"Dimension table dim_{table_name} saved to {gold_table_path}")
        except Exception as e:
            logger.error(f"Failed to save Gold dimension table {gold_table_path}: {e}")
            return None
            
        self._log_dataframe_info(dim_df, f"dim_{table_name}")
        return dim_df

    def silver_to_gold_fact(self, table_name: str, min_year: Optional[int] = None) -> Optional[DataFrame]:
        logger.info(f"Processing fact table {table_name} from Silver to Gold")
        silver_table_path = f"{self.schemas['silver']}.{table_name}"
        
        try:
            fact_df = self.spark.table(silver_table_path)
        except Exception as e:
            logger.error(f"Failed to read Silver table {silver_table_path}: {e}")
            return None

        if min_year and "PostingDate" in fact_df.columns:
            fact_df = fact_df.filter(F.year(F.col("PostingDate")) >= min_year)
            logger.info(f"Filtered {table_name} for year >= {min_year}")

        date_dim_path = f"{self.schemas['gold']}.dim_Date"
        try:
            date_dim_df = self.spark.table(date_dim_path)
            if "PostingDate" in fact_df.columns and "Date" in date_dim_df.columns and "DateKey" in date_dim_df.columns:
                fact_df = fact_df.join(date_dim_df, fact_df["PostingDate"] == date_dim_df["Date"], "left") \
                                 .select(fact_df["*"], date_dim_df["DateKey"].alias("PostingDateKey"))
                logger.info(f"Joined {table_name} with dim_Date")
            else:
                logger.warning(f"Could not join {table_name} with dim_Date due to missing PostingDate/Date/DateKey columns.")
        except Exception as e:
            logger.warning(f"dim_Date table not found or error during join: {e}. Proceeding without Date dim join.")

        bridge_dim_path = f"{self.schemas['gold']}.dim_DimensionBridge"
        company_col = "Company" # Standardized
        dim_set_id_col = "DimensionSetID" # Standardized

        try:
            bridge_df = self.spark.table(bridge_dim_path)
            join_cols_bridge = [dim_set_id_col, company_col]
            
            fact_has_join_cols = all(col in fact_df.columns for col in join_cols_bridge)
            bridge_has_join_cols = all(col in bridge_df.columns for col in join_cols_bridge)
            bridge_has_sk = "DimensionBridgeKey" in bridge_df.columns

            if fact_has_join_cols and bridge_has_join_cols and bridge_has_sk:
                bridge_join_df = bridge_df.select(*join_cols_bridge, "DimensionBridgeKey")
                fact_df = fact_df.join(bridge_join_df, on=join_cols_bridge, how="left")
                logger.info(f"Joined {table_name} with dim_DimensionBridge")
            else:
                missing_fact_cols = [col for col in join_cols_bridge if col not in fact_df.columns]
                missing_bridge_cols = [col for col in join_cols_bridge if col not in bridge_df.columns]
                if not bridge_has_sk: missing_bridge_cols.append("DimensionBridgeKey (SK)")
                logger.warning(f"Could not join {table_name} with dim_DimensionBridge. Missing fact cols: {missing_fact_cols}, bridge cols: {missing_bridge_cols}.")
        except Exception as e:
            logger.warning(f"dim_DimensionBridge table not found or error during join: {e}. Proceeding without Dimension Bridge join.")

        # Dimension Joins (example for GLEntry)
        if table_name == "GLEntry":
            if "GLAccountNo" in fact_df.columns and company_col in fact_df.columns:
                try:
                    gl_account_dim = self.spark.table(f"{self.schemas['gold']}.dim_GLAccount")
                    if "SK_GLAccount" in gl_account_dim.columns and "No" in gl_account_dim.columns and company_col in gl_account_dim.columns:
                        fact_df = fact_df.join(
                            gl_account_dim.select(F.col("SK_GLAccount").alias("GLAccountKey"), F.col("No"), F.col(company_col)),
                            (fact_df["GLAccountNo"] == gl_account_dim["No"]) & (fact_df[company_col] == gl_account_dim[company_col]), "left"
                        ).drop(gl_account_dim[company_col]).drop(gl_account_dim["No"])
                    else: logger.warning(f"dim_GLAccount is missing required columns for join with {table_name}.")
                except Exception as e: logger.warning(f"Error joining {table_name} with dim_GLAccount: {e}")
            if "Amount" in fact_df.columns: # Measure from model
                fact_df = fact_df.withColumn("AmountSign", F.signum(F.col("Amount")))
        
        elif table_name == "CustLedgerEntry":
            if "CustomerNo" in fact_df.columns and company_col in fact_df.columns: # Field names from model
                try:
                    customer_dim = self.spark.table(f"{self.schemas['gold']}.dim_Customer")
                    if "SK_Customer" in customer_dim.columns and "No" in customer_dim.columns and company_col in customer_dim.columns:
                        fact_df = fact_df.join(
                            customer_dim.select(F.col("SK_Customer").alias("CustomerKey"), F.col("No"), F.col(company_col)),
                            (fact_df["CustomerNo"] == customer_dim["No"]) & (fact_df[company_col] == customer_dim[company_col]), "left"
                        ).drop(customer_dim[company_col]).drop(customer_dim["No"])
                    else: logger.warning(f"dim_Customer is missing required columns for join with {table_name}.")
                except Exception as e: logger.warning(f"Error joining {table_name} with dim_Customer: {e}")

        elif table_name == "VendorLedgerEntry":
            if "VendorNo" in fact_df.columns and company_col in fact_df.columns: # Field names from model
                try:
                    vendor_dim = self.spark.table(f"{self.schemas['gold']}.dim_Vendor")
                    if "SK_Vendor" in vendor_dim.columns and "No" in vendor_dim.columns and company_col in vendor_dim.columns:
                        fact_df = fact_df.join(
                            vendor_dim.select(F.col("SK_Vendor").alias("VendorKey"), F.col("No"), F.col(company_col)),
                            (fact_df["VendorNo"] == vendor_dim["No"]) & (fact_df[company_col] == vendor_dim[company_col]), "left"
                        ).drop(vendor_dim[company_col]).drop(vendor_dim["No"])
                    else: logger.warning(f"dim_Vendor is missing required columns for join with {table_name}.")
                except Exception as e: logger.warning(f"Error joining {table_name} with dim_Vendor: {e}")
        
        fact_df = fact_df.withColumn("GoldFactProcessedAt", F.current_timestamp())
        
        gold_table_path = f"{self.schemas['gold']}.fact_{table_name}"
        try:
            fact_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_table_path)
            logger.info(f"Fact table fact_{table_name} saved to {gold_table_path}")
        except Exception as e:
            logger.error(f"Failed to save Gold fact table {gold_table_path}: {e}")
            return None

        self._log_dataframe_info(fact_df, f"fact_{table_name}")
        return fact_df

    def run_pipeline(
        self,
        tables: Optional[List[str]] = None, # List of raw table names from bronze (e.g., Customer20)
        incremental: bool = False,
        min_year: Optional[int] = None,
        dimension_types: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        logger.info("Starting BC Medallion Pipeline full run")
        results: Dict[str, Any] = {"status": "started", "phases": {}}

        # Phase 1: Create Date Dimension
        logger.info("Phase 1: Creating Date Dimension")
        results["phases"]["date_dimension"] = {"status": "pending", "details": {}}
        try:
            start_date_val = date(min_year if min_year else 2020, 1, 1)
            # Ensure end_date is far enough in the future.
            current_year = datetime.now().year
            end_date_val = date(current_year + 5, 12, 31) 
            
            date_dim_df = self.create_date_dimension(start_date_val, end_date_val)
            # create_date_dimension already writes the table
            # date_table_path = f"{self.schemas['gold']}.dim_Date" 
            # date_dim_df.write.mode("overwrite").saveAsTable(date_table_path)
            if date_dim_df: # Check if DataFrame was returned
                results["phases"]["date_dimension"]["status"] = "success"
                results["phases"]["date_dimension"]["details"]["table_written"] = f"{self.schemas['gold']}.dim_Date"
                logger.info(f"Date dimension created successfully at {self.schemas['gold']}.dim_Date")
            else:
                results["phases"]["date_dimension"]["status"] = "error"
                results["phases"]["date_dimension"]["details"]["error_message"] = "create_date_dimension returned None"

        except Exception as e:
            logger.error(f"Error in Phase 1 (Create Date Dimension): {e}", exc_info=True)
            results["phases"]["date_dimension"]["status"] = "error"
            results["phases"]["date_dimension"]["details"]["error_message"] = str(e)
        
        # Phase 2: Process Bronze to Silver
        logger.info("Phase 2: Processing Bronze to Silver")
        results["phases"]["bronze_to_silver"] = {"status": "pending", "details": {}}
        silver_tables_created: List[str] = [] # Stores clean table names

        if not tables:
            try:
                logger.info(f"No specific tables provided, discovering tables from schema: {self.schemas['bronze']}")
                bronze_spark_tables = self.spark.sql(f"SHOW TABLES IN {self.schemas['bronze']}").collect()
                tables = [t.tableName for t in bronze_spark_tables if not t.tableName.startswith("vw_")] # Exclude views
                logger.info(f"Discovered {len(tables)} tables in {self.schemas['bronze']}: {tables}")
                if not tables:
                    logger.warning(f"No tables found in {self.schemas['bronze']}. Skipping Bronze to Silver.")
            except Exception as e:
                logger.error(f"Error discovering tables in {self.schemas['bronze']}: {e}", exc_info=True)
                results["phases"]["bronze_to_silver"]["status"] = "error"
                results["phases"]["bronze_to_silver"]["details"]["error_message"] = f"Failed to discover tables: {e}"
                tables = [] 

        for table_name in tables:
            clean_name_for_check = re.sub(r'\d+$', '', table_name)
            # Basic filter: BC table names usually start with an uppercase letter (Pydantic model name)
            # and often have a numeric suffix from source, or their clean name is in our model_mapping.
            is_likely_bc_table = table_name[0].isupper() and \
                                 (re.search(r'\d+$', table_name) or clean_name_for_check in self.model_mapping)

            if not is_likely_bc_table:
                 logger.info(f"Skipping table {table_name} as it does not appear to be a BC source table.")
                 continue

            table_detail = {"source_table": table_name, "status": "pending"}
            try:
                logger.info(f"Processing table: {table_name} for Bronze to Silver")
                silver_df = self.bronze_to_silver(table_name, incremental=incremental)
                if silver_df:
                    clean_silver_table_name = getattr(silver_df, "_table_name", clean_name_for_check)
                    silver_table_path = f"{self.schemas['silver']}.{clean_silver_table_name}"
                    
                    write_mode = "append" if incremental else "overwrite"
                    # Using mergeSchema to handle schema evolution, common in Silver
                    silver_df.write.format("delta").mode(write_mode).option("mergeSchema", "true").saveAsTable(silver_table_path)
                    
                    silver_tables_created.append(clean_silver_table_name)
                    table_detail["status"] = "success"
                    table_detail["silver_table"] = silver_table_path
                    # table_detail["rows_written"] = silver_df.count() # Can be expensive, enable if needed
                    logger.info(f"Successfully processed {table_name} to {silver_table_path}")
                else:
                    logger.warning(f"No DataFrame returned from bronze_to_silver for {table_name}. Skipping write.")
                    table_detail["status"] = "skipped"
                    table_detail["reason"] = "No data returned from bronze_to_silver"
            except Exception as e:
                logger.error(f"Failed to process {table_name} from Bronze to Silver: {e}", exc_info=True)
                table_detail["status"] = "error"
                table_detail["error_message"] = str(e)
            results["phases"]["bronze_to_silver"]["details"][table_name] = table_detail
        
        if not any(td["status"] == "success" for td in results["phases"]["bronze_to_silver"]["details"].values()):
            results["phases"]["bronze_to_silver"]["status"] = "warning_or_error_no_success"
        else:
            results["phases"]["bronze_to_silver"]["status"] = "completed"


        # Phase 3: Process Silver to Gold - Dimensions
        logger.info("Phase 3: Processing Silver to Gold - Dimensions")
        results["phases"]["silver_to_gold_dimensions"] = {"status": "pending", "details": {}}
        for dim_table_name in self.dimension_tables:
            table_detail = {"source_table": dim_table_name, "status": "pending"}
            if dim_table_name in silver_tables_created:
                try:
                    logger.info(f"Processing dimension: {dim_table_name} for Silver to Gold")
                    gold_dim_df = self.silver_to_gold_dimension(dim_table_name)
                    # silver_to_gold_dimension already writes the table
                    if gold_dim_df:
                        table_detail["status"] = "success"
                        table_detail["gold_table"] = f"{self.schemas['gold']}.dim_{dim_table_name}"
                        # table_detail["rows_written"] = gold_dim_df.count()
                        logger.info(f"Successfully processed dimension {dim_table_name}")
                    else:
                        table_detail["status"] = "skipped"
                        table_detail["reason"] = "No data returned from silver_to_gold_dimension"
                except Exception as e:
                    logger.error(f"Failed to process dimension {dim_table_name}: {e}", exc_info=True)
                    table_detail["status"] = "error"
                    table_detail["error_message"] = str(e)
            else:
                table_detail["status"] = "skipped"
                table_detail["reason"] = f"Not created in Silver layer: {dim_table_name}"
            results["phases"]["silver_to_gold_dimensions"]["details"][dim_table_name] = table_detail
        results["phases"]["silver_to_gold_dimensions"]["status"] = "completed"


        # Phase 4: Create Dimension Bridge
        logger.info("Phase 4: Creating Dimension Bridge")
        results["phases"]["dimension_bridge"] = {"status": "pending", "details": {}}
        if all(dt in silver_tables_created for dt in ["DimensionSetEntry", "DimensionValue"]):
            try:
                logger.info("All required tables for dimension bridge are present. Creating bridge.")
                bridge_df = self.create_dimension_bridge(dimension_types=dimension_types)
                # create_dimension_bridge already writes the table
                if bridge_df:
                    results["phases"]["dimension_bridge"]["status"] = "success"
                    results["phases"]["dimension_bridge"]["details"]["table_written"] = f"{self.schemas['gold']}.dim_DimensionBridge"
                    # results["phases"]["dimension_bridge"]["details"]["rows_written"] = bridge_df.count()
                    logger.info(f"Dimension bridge created successfully.")
                else:
                    results["phases"]["dimension_bridge"]["status"] = "skipped"
                    results["phases"]["dimension_bridge"]["details"]["reason"] = "No data returned from create_dimension_bridge"
            except Exception as e:
                logger.error(f"Failed to create dimension bridge: {e}", exc_info=True)
                results["phases"]["dimension_bridge"]["status"] = "error"
                results["phases"]["dimension_bridge"]["details"]["error_message"] = str(e)
        else:
            results["phases"]["dimension_bridge"]["status"] = "skipped"
            missing_reqs = [req for req in ["DimensionSetEntry", "DimensionValue"] if req not in silver_tables_created]
            results["phases"]["dimension_bridge"]["details"]["reason"] = f"Required tables not created in Silver: {missing_reqs}"
            logger.warning(f"Skipping dimension bridge creation, required tables not in silver: {missing_reqs}")

        # Phase 5: Process Silver to Gold - Facts
        logger.info("Phase 5: Processing Silver to Gold - Facts")
        results["phases"]["silver_to_gold_facts"] = {"status": "pending", "details": {}}
        for fact_table_name in self.fact_tables:
            if fact_table_name == "DimensionSetEntry" and results["phases"]["dimension_bridge"]["status"] == "success":
                 logger.info(f"Skipping {fact_table_name} in facts processing as it's part of dimension bridge.")
                 continue

            table_detail = {"source_table": fact_table_name, "status": "pending"}
            if fact_table_name in silver_tables_created:
                try:
                    logger.info(f"Processing fact: {fact_table_name} for Silver to Gold")
                    gold_fact_df = self.silver_to_gold_fact(fact_table_name, min_year=min_year)
                    # silver_to_gold_fact already writes the table
                    if gold_fact_df:
                        table_detail["status"] = "success"
                        table_detail["gold_table"] = f"{self.schemas['gold']}.fact_{fact_table_name}"
                        # table_detail["rows_written"] = gold_fact_df.count()
                        logger.info(f"Successfully processed fact {fact_table_name}")
                    else:
                        table_detail["status"] = "skipped"
                        table_detail["reason"] = "No data returned from silver_to_gold_fact"
                except Exception as e:
                    logger.error(f"Failed to process fact {fact_table_name}: {e}", exc_info=True)
                    table_detail["status"] = "error"
                    table_detail["error_message"] = str(e)
            else:
                table_detail["status"] = "skipped"
                table_detail["reason"] = f"Not created in Silver layer: {fact_table_name}"
            results["phases"]["silver_to_gold_facts"]["details"][fact_table_name] = table_detail
        results["phases"]["silver_to_gold_facts"]["status"] = "completed"
        
        results["status"] = "completed"
        logger.info("BC Medallion Pipeline full run completed.")
        return results

    # Helper for logging (can be moved to a util if used elsewhere)
    def _log_dataframe_info(self, df: DataFrame, name: str):
        logger.info(f"DataFrame {name}: Count={df.count()}, Schema:")
        df.printSchema()
