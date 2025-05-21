from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Optional

from core_etl.logging_utils import etl_logger
# from core_etl.delta_writer import write_delta_table # Not used directly here, BCPipeline handles write
# from core_etl.config_utils import get_bc_table_config # For specific sub-configs if needed

def create_finance_fact(
    spark: SparkSession,
    primary_silver_df: DataFrame, # e.g., GLEntry
    date_dim_df: Optional[DataFrame],
    gl_account_dim_df: Optional[DataFrame],
    dimension_bridge_df: Optional[DataFrame],
    company_dim_df: Optional[DataFrame], # Assuming a company dimension
    # --- Parameters passed from BCPipeline ---
    silver_db_name: str, # Example: "silver.bc"
    gold_db_name: str,   # Example: "gold.bc"
    min_year_filter: Optional[int] = None,
    fiscal_year_start_month: int = 1,
    is_incremental: bool = False,
    # last_processed_time: Optional[str] = None, # For incremental logic within the fact if needed
) -> Optional[DataFrame]:
    """
    Creates the Finance Fact table from GLEntry and related dimensions.
    The BCPipeline.silver_to_gold_fact method is responsible for reading source DataFrames
    and writing the output of this function.
    """
    etl_logger.info("Starting creation of Finance Fact table (create_finance_fact function).")

    if primary_silver_df is None:
        etl_logger.warning("Primary silver DataFrame (e.g., GLEntry) is None for Finance Fact. Skipping.")
        return None

    fact_df = primary_silver_df.alias("fact_df") # Alias for clarity

    # Filter by min_year if provided (usually on PostingDate)
    # This might be done in BCPipeline before calling, or here if fact_function needs to control it.
    # For now, assume BCPipeline might pre-filter primary_silver_df.
    # If not, this function might need a `posting_date_col` parameter.
    if min_year_filter and "PostingDate" in fact_df.columns:
        fact_df = fact_df.filter(F.year(F.col("PostingDate")) >= min_year_filter)
        etl_logger.info(f"Finance Fact: Applied min_year_filter >= {min_year_filter}")


    # Join with Date Dimension (on PostingDate)
    if date_dim_df:
        date_dim_df_aliased = date_dim_df.alias("dd")
        fact_df = fact_df.join(
            date_dim_df_aliased,
            F.to_date(fact_df["PostingDate"]) == F.to_date(F.col("dd.Date")), # Ensure robust date join
            "left"
        ).select("fact_df.*", F.col("dd.DateKey").alias("PostingDateKey"))
    else:
        etl_logger.warning("Date dimension not provided. Finance fact will be missing PostingDateKey.")
        fact_df = fact_df.withColumn("PostingDateKey", F.lit(None).cast("int"))


    # Join with GLAccount Dimension
    if gl_account_dim_df:
        gl_account_dim_df_aliased = gl_account_dim_df.alias("gad")
        # Assuming SK_GLAccount and Company are in gl_account_dim_df, and No, Company in fact_df (GLEntry)
        fact_df = fact_df.join(
            gl_account_dim_df_aliased,
            (fact_df["GLAccountNo"] == F.col("gad.No")) & (fact_df["Company"] == F.col("gad.Company")), # Ensure Company join for multi-company Dims
            "left"
        ).select("fact_df.*", F.col("gad.SK_GLAccount").alias("GLAccountKey"))
    else:
        etl_logger.warning("GLAccount dimension not provided. Finance fact will be missing GLAccountKey.")
        fact_df = fact_df.withColumn("GLAccountKey", F.lit(None).cast("long"))

    # Join with Dimension Bridge
    # Ensure join keys ('DimensionSetID', 'Company') are correct based on your bridge and fact data.
    if dimension_bridge_df:
        dimension_bridge_df_aliased = dimension_bridge_df.alias("dbd")
        join_keys_bridge = ["DimensionSetID"]
        if "Company" in fact_df.columns and "Company" in dimension_bridge_df_aliased.columns:
            join_keys_bridge.append("Company")
        
        fact_df = fact_df.join(
            dimension_bridge_df_aliased,
            join_keys_bridge,
            "left"
        ).select("fact_df.*", F.col("dbd.DimensionBridgeKey")) # Assuming SK is named DimensionBridgeKey
    else:
        etl_logger.warning("Dimension Bridge not provided. Finance fact will be missing DimensionBridgeKey.")
        fact_df = fact_df.withColumn("DimensionBridgeKey", F.lit(None).cast("long"))
    
    # Join with Company Dimension (example)
    if company_dim_df:
        company_dim_df_aliased = company_dim_df.alias("cd")
        # Assuming company_dim_df has SK_CompanyInformation and 'Name' (matching fact_df's 'Company')
        fact_df = fact_df.join(
            company_dim_df_aliased,
            fact_df["Company"] == F.col("cd.Name"), 
            "left"
        ).select("fact_df.*", F.col("cd.SK_CompanyInformation").alias("CompanyKey"))
    else:
        etl_logger.warning("Company dimension not provided. Finance fact will be missing CompanyKey.")
        fact_df = fact_df.withColumn("CompanyKey", F.lit(None).cast("long"))


    # Add other relevant fact measures, transformations
    # Example: Ensure Amount is present and aliased if needed
    if "Amount" in fact_df.columns:
        fact_df = fact_df.withColumn("AmountLCY", F.col("Amount")) # Example, assuming Amount is LCY from GLEntry
    else:
        etl_logger.warning("Amount column not found in primary_silver_df for Finance Fact.")
        fact_df = fact_df.withColumn("AmountLCY", F.lit(0.0).cast("decimal(38,18)"))


    fact_df = fact_df.withColumn("GoldFactRowProcessedAt", F.current_timestamp()) # Renamed to avoid clash if BCPipeline adds its own
    
    etl_logger.info("Finance Fact table (create_finance_fact) processing complete.")
    
    # Select final columns - this should be standardized or based on a model
    # For now, returning all accumulated columns.
    # Example: final_columns = ["PostingDateKey", "GLAccountKey", ..., "AmountLCY", "GoldFactRowProcessedAt"]
    # fact_df = fact_df.select(*final_columns)
    
    return fact_df
