from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Optional

from core_etl.logging_utils import etl_logger

def create_sales_fact(
    spark: SparkSession,
    primary_silver_df: DataFrame, # e.g., SalesInvoiceLine or a pre-joined header/line table
    date_dim_df: Optional[DataFrame],
    customer_dim_df: Optional[DataFrame],
    item_dim_df: Optional[DataFrame],
    dimension_bridge_df: Optional[DataFrame],
    company_dim_df: Optional[DataFrame],
    # --- Parameters from BCPipeline ---
    silver_db_name: str,
    gold_db_name: str,
    min_year_filter: Optional[int] = None,
    # ... other relevant parameters
) -> Optional[DataFrame]:
    """
    Creates the Sales Fact table.
    BCPipeline.silver_to_gold_fact is responsible for reading sources and writing output.
    """
    etl_logger.info("Starting creation of Sales Fact table (create_sales_fact function).")

    if primary_silver_df is None:
        etl_logger.warning("Primary silver DataFrame (e.g., SalesInvoiceLine) is None for Sales Fact. Skipping.")
        return None

    fact_df = primary_silver_df.alias("fact_df")

    if min_year_filter and "PostingDate" in fact_df.columns: # Assuming PostingDate exists on the primary_silver_df
        fact_df = fact_df.filter(F.year(F.col("PostingDate")) >= min_year_filter)
        etl_logger.info(f"Sales Fact: Applied min_year_filter >= {min_year_filter}")

    # Join with Date Dimension (on PostingDate or DocumentDate)
    date_join_col = "PostingDate" if "PostingDate" in fact_df.columns else "DocumentDate"
    if date_dim_df and date_join_col in fact_df.columns:
        date_dim_df_aliased = date_dim_df.alias("dd")
        fact_df = fact_df.join(
            date_dim_df_aliased,
            F.to_date(fact_df[date_join_col]) == F.to_date(F.col("dd.Date")),
            "left"
        ).select("fact_df.*", F.col("dd.DateKey").alias(f"{date_join_col}Key"))
    else:
        etl_logger.warning(f"Date dimension or '{date_join_col}' not available. Sales fact will be missing {date_join_col}Key.")
        fact_df = fact_df.withColumn(f"{date_join_col}Key", F.lit(None).cast("int"))

    # Join with Customer Dimension
    # Assuming primary_silver_df has 'CustomerNo' and 'Company' (if company specific)
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
        etl_logger.warning("Customer dimension or 'CustomerNo' not available. Sales fact will be missing CustomerKey.")
        fact_df = fact_df.withColumn("CustomerKey", F.lit(None).cast("long"))

    # Join with Item Dimension
    if item_dim_df and "ItemNo" in fact_df.columns: # Assuming 'ItemNo' from SalesInvoiceLine
        item_dim_df_aliased = item_dim_df.alias("idim")
        join_keys_item = [fact_df["ItemNo"] == F.col("idim.No")]
        if "Company" in fact_df.columns and "Company" in idim.columns: # Items can be company specific or shared
             join_keys_item.append(fact_df["Company"] == F.col("idim.Company"))

        fact_df = fact_df.join(
            item_dim_df_aliased,
            F.expr(" AND ".join([str(cond._jc) for cond in join_keys_item])),
            "left"
        ).select("fact_df.*", F.col("idim.SK_Item").alias("ItemKey"))
    else:
        etl_logger.warning("Item dimension or 'ItemNo' not available. Sales fact will be missing ItemKey.")
        fact_df = fact_df.withColumn("ItemKey", F.lit(None).cast("long"))

    # Join with Dimension Bridge
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
        etl_logger.warning("Dimension Bridge or 'DimensionSetID' not available. Sales fact will be missing DimensionBridgeKey.")
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
        etl_logger.warning("Company dimension or 'Company' column not available. Sales fact will be missing CompanyKey.")
        fact_df = fact_df.withColumn("CompanyKey", F.lit(None).cast("long"))

    # Example measures (these would be based on SalesInvoiceLine fields)
    if "Quantity" in fact_df.columns:
        fact_df = fact_df.withColumn("SalesQuantity", F.col("Quantity"))
    if "Amount" in fact_df.columns: # Amount excluding tax
        fact_df = fact_df.withColumn("SalesAmountLCY", F.col("Amount"))
    if "AmountIncludingVAT" in fact_df.columns: # BC specific field name
        fact_df = fact_df.withColumn("SalesAmountInclTaxLCY", F.col("AmountIncludingVAT"))
    
    fact_df = fact_df.withColumn("GoldFactRowProcessedAt", F.current_timestamp())
    
    etl_logger.info("Sales Fact table (create_sales_fact) processing complete.")
    return fact_df
