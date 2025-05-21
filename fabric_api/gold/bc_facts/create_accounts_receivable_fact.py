from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Optional

from core_etl.logging_utils import etl_logger

def create_accounts_receivable_fact(
    spark: SparkSession,
    primary_silver_df: DataFrame, # e.g., CustLedgerEntry
    date_dim_df: Optional[DataFrame], # For PostingDate, DocumentDate, DueDate
    customer_dim_df: Optional[DataFrame],
    company_dim_df: Optional[DataFrame],
    dimension_bridge_df: Optional[DataFrame], # If CustLedgerEntry has DimensionSetID
    # --- Parameters from BCPipeline ---
    silver_db_name: str,
    gold_db_name: str,
    min_year_filter: Optional[int] = None,
    # ... other relevant parameters
) -> Optional[DataFrame]:
    """
    Creates the Accounts Receivable Fact table from CustLedgerEntry and related dimensions.
    BCPipeline.silver_to_gold_fact is responsible for reading sources and writing output.
    """
    etl_logger.info("Starting creation of Accounts Receivable Fact table (create_accounts_receivable_fact function).")

    if primary_silver_df is None:
        etl_logger.warning("Primary silver DataFrame (e.g., CustLedgerEntry) is None for AR Fact. Skipping.")
        return None

    fact_df = primary_silver_df.alias("fact_df")

    if min_year_filter and "PostingDate" in fact_df.columns:
        fact_df = fact_df.filter(F.year(F.col("PostingDate")) >= min_year_filter)
        etl_logger.info(f"AR Fact: Applied min_year_filter >= {min_year_filter} on PostingDate.")

    # Join with Date Dimension for multiple date keys
    date_columns_to_key = {
        "PostingDate": "PostingDateKey",
        "DocumentDate": "DocumentDateKey",
        "DueDate": "DueDateKey"
    }
    if date_dim_df:
        date_dim_df_aliased = date_dim_df.alias("dd")
        for date_col, key_col_name in date_columns_to_key.items():
            if date_col in fact_df.columns:
                # Create a new alias for date_dim_df for each join to avoid ambiguity if joining multiple times on different dates
                # However, it's more efficient to join once and select multiple keys if source dates are different rows or can be pivoted.
                # For typical fact table, we join each date column to the date dimension.
                # To avoid multiple joins to the same dimension, if performance is an issue, consider looking up keys after an initial join or using UDFs.
                # Simple approach: multiple joins or ensure your date_dim can be used multiple times with aliases.
                # For this example, we'll create a separate aliased join for each date type.
                
                # To select specific columns and avoid ambiguity if a fact_df column has same name as in date_dim_df (e.g. "Year")
                current_fact_cols = fact_df.columns 
                select_exprs = [F.col(c) for c in current_fact_cols] + [F.col(f"dd_join_{date_col}.DateKey").alias(key_col_name)]

                fact_df = fact_df.join(
                    date_dim_df.alias(f"dd_join_{date_col}"),
                    F.to_date(fact_df[date_col]) == F.to_date(F.col(f"dd_join_{date_col}.Date")),
                    "left"
                ).select(*select_exprs)
                etl_logger.info(f"AR Fact: Joined with Date Dimension for {date_col} as {key_col_name}.")
            else:
                etl_logger.warning(f"AR Fact: Date column '{date_col}' not found. Missing {key_col_name}.")
                fact_df = fact_df.withColumn(key_col_name, F.lit(None).cast("int"))
    else:
        etl_logger.warning("Date dimension not provided for AR Fact. Date keys will be missing.")
        for key_col_name in date_columns_to_key.values():
            fact_df = fact_df.withColumn(key_col_name, F.lit(None).cast("int"))


    # Join with Customer Dimension
    if customer_dim_df and "CustomerNo" in fact_df.columns:
        customer_dim_df_aliased = customer_dim_df.alias("cdim")
        join_keys_customer = [fact_df["CustomerNo"] == F.col("cdim.No")]
        if "Company" in fact_df.columns and "Company" in cdim.columns:
             join_keys_customer.append(fact_df["Company"] == F.col("cdim.Company"))

        fact_df = fact_df.join(
            customer_dim_df_aliased,
            F.expr(" AND ".join([str(cond._jc) for cond in join_keys_customer])),
            "left"
        ).select("fact_df.*", F.col("cdim.SK_Customer").alias("CustomerKey"))
    else:
        etl_logger.warning("Customer dimension or 'CustomerNo' not available for AR Fact. Missing CustomerKey.")
        fact_df = fact_df.withColumn("CustomerKey", F.lit(None).cast("long"))

    # Join with Dimension Bridge (if applicable)
    if dimension_bridge_df and "DimensionSetID" in fact_df.columns:
        dimension_bridge_df_aliased = dimension_bridge_df.alias("dbd")
        join_keys_bridge = [fact_df["DimensionSetID"] == F.col("dbd.DimensionSetID")]
        if "Company" in fact_df.columns and "Company" in dbd.columns:
             join_keys_bridge.append(fact_df["Company"] == F.col("dbd.Company"))
        
        fact_df = fact_df.join(
            dimension_bridge_df_aliased,
            F.expr(" AND ".join([str(cond._jc) for cond in join_keys_bridge])),
            "left"
        ).select("fact_df.*", F.col("dbd.DimensionBridgeKey"))
    else:
        etl_logger.warning("Dimension Bridge or 'DimensionSetID' not available for AR Fact. Missing DimensionBridgeKey.")
        fact_df = fact_df.withColumn("DimensionBridgeKey", F.lit(None).cast("long"))

    # Join with Company Dimension
    if company_dim_df and "Company" in fact_df.columns:
        company_dim_df_aliased = company_dim_df.alias("compdim")
        fact_df = fact_df.join(
            company_dim_df_aliased,
            fact_df["Company"] == F.col("compdim.Name"), 
            "left"
        ).select("fact_df.*", F.col("compdim.SK_CompanyInformation").alias("CompanyKey"))
    else:
        etl_logger.warning("Company dimension or 'Company' column not available for AR Fact. Missing CompanyKey.")
        fact_df = fact_df.withColumn("CompanyKey", F.lit(None).cast("long"))

    # Example measures from CustLedgerEntry
    # Amount, RemainingAmount, etc. are typically in LCY in CustLedgerEntry
    if "Amount" in fact_df.columns: # Original amount of the ledger entry
        fact_df = fact_df.withColumn("OriginalAmountLCY", F.col("Amount"))
    if "RemainingAmount" in fact_df.columns: # Remaining amount of the ledger entry
        fact_df = fact_df.withColumn("RemainingAmountLCY", F.col("RemainingAmount"))
    
    # Calculate Age of Invoice (example)
    if "DueDate" in fact_df.columns:
        fact_df = fact_df.withColumn("DaysPastDue", F.datediff(F.current_date(), F.to_date(F.col("DueDate"))))
        fact_df = fact_df.withColumn("IsOverdue", F.when(F.col("DaysPastDue") > 0, True).otherwise(False))

    fact_df = fact_df.withColumn("GoldFactRowProcessedAt", F.current_timestamp())
    
    etl_logger.info("Accounts Receivable Fact table (create_accounts_receivable_fact) processing complete.")
    return fact_df
