from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Optional

from core_etl.logging_utils import etl_logger

def create_inventory_fact(
    spark: SparkSession,
    primary_silver_df: DataFrame, # e.g., ItemLedgerEntry
    date_dim_df: Optional[DataFrame],
    item_dim_df: Optional[DataFrame],
    location_dim_df: Optional[DataFrame], # Assuming a Location dimension
    dimension_bridge_df: Optional[DataFrame], # If ILE has DimensionSetID
    company_dim_df: Optional[DataFrame],
    # --- Parameters from BCPipeline ---
    silver_db_name: str,
    gold_db_name: str,
    min_year_filter: Optional[int] = None,
    # ... other relevant parameters
) -> Optional[DataFrame]:
    """
    Creates the Inventory Fact table from ItemLedgerEntry and related dimensions.
    BCPipeline.silver_to_gold_fact is responsible for reading sources and writing output.
    """
    etl_logger.info("Starting creation of Inventory Fact table (create_inventory_fact function).")

    if primary_silver_df is None:
        etl_logger.warning("Primary silver DataFrame (e.g., ItemLedgerEntry) is None for Inventory Fact. Skipping.")
        return None

    fact_df = primary_silver_df.alias("fact_df")

    if min_year_filter and "PostingDate" in fact_df.columns:
        fact_df = fact_df.filter(F.year(F.col("PostingDate")) >= min_year_filter)
        etl_logger.info(f"Inventory Fact: Applied min_year_filter >= {min_year_filter}")

    # Join with Date Dimension (on PostingDate)
    if date_dim_df and "PostingDate" in fact_df.columns:
        date_dim_df_aliased = date_dim_df.alias("dd")
        fact_df = fact_df.join(
            date_dim_df_aliased,
            F.to_date(fact_df["PostingDate"]) == F.to_date(F.col("dd.Date")),
            "left"
        ).select("fact_df.*", F.col("dd.DateKey").alias("PostingDateKey"))
    else:
        etl_logger.warning("Date dimension or 'PostingDate' not available for Inventory Fact. Missing PostingDateKey.")
        fact_df = fact_df.withColumn("PostingDateKey", F.lit(None).cast("int"))

    # Join with Item Dimension
    if item_dim_df and "ItemNo" in fact_df.columns:
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
        etl_logger.warning("Item dimension or 'ItemNo' not available for Inventory Fact. Missing ItemKey.")
        fact_df = fact_df.withColumn("ItemKey", F.lit(None).cast("long"))

    # Join with Location Dimension (assuming ILE has LocationCode)
    if location_dim_df and "LocationCode" in fact_df.columns:
        location_dim_df_aliased = location_dim_df.alias("ldim")
        # Location dimension might be simpler, potentially not company-specific or using a different key
        fact_df = fact_df.join(
            location_dim_df_aliased,
            fact_df["LocationCode"] == F.col("ldim.Code"), # Assuming Location dim has 'Code' as key
            "left"
        ).select("fact_df.*", F.col("ldim.SK_Location").alias("LocationKey")) # SK_Location is hypothetical
    else:
        etl_logger.warning("Location dimension or 'LocationCode' not available for Inventory Fact. Missing LocationKey.")
        fact_df = fact_df.withColumn("LocationKey", F.lit(None).cast("long"))

    # Join with Dimension Bridge (if ItemLedgerEntry has DimensionSetID)
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
        etl_logger.warning("Dimension Bridge or 'DimensionSetID' not available for Inventory Fact. Missing DimensionBridgeKey.")
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
        etl_logger.warning("Company dimension or 'Company' column not available for Inventory Fact. Missing CompanyKey.")
        fact_df = fact_df.withColumn("CompanyKey", F.lit(None).cast("long"))

    # Example measures from ItemLedgerEntry
    if "Quantity" in fact_df.columns:
        fact_df = fact_df.withColumn("InventoryQuantityChange", F.col("Quantity"))
    if "CostAmountActual" in fact_df.columns:
        fact_df = fact_df.withColumn("InventoryCostAmountActual", F.col("CostAmountActual"))
    if "SalesAmountActual" in fact_df.columns: # If ILE contains sales value for COGS entries
        fact_df = fact_df.withColumn("InventorySalesAmountActual", F.col("SalesAmountActual"))
    
    fact_df = fact_df.withColumn("GoldFactRowProcessedAt", F.current_timestamp())
    
    etl_logger.info("Inventory Fact table (create_inventory_fact) processing complete.")
    return fact_df
