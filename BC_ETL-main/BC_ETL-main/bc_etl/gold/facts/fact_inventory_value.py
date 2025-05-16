"""
Inventory Value fact table creation module.
Handles the transformation of ValueEntry data into the fact_InventoryValue gold table.
"""

from datetime import datetime

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from bc_etl.gold.utils import join_dimension
from bc_etl.utils import logging
from bc_etl.utils.config_utils import extract_table_config
from bc_etl.utils.dataframe_utils import add_audit_columns, year_filter
from bc_etl.utils.decorators import fact_table_handler
from bc_etl.utils.schema_utils import create_dimension_flags, enforce_schema
from bc_etl.utils.table_utils import read_table_from_config

# Define Target Schema for inventory value fact table
FACT_INVENTORY_VALUE_SCHEMA = StructType(
    [
        StructField("$Company", StringType(), True),
        StructField("EntryNo", IntegerType(), True),
        StructField("ItemNo", StringType(), True),
        StructField("ItemKey", IntegerType(), True),  # From dim join
        StructField("LocationCode", StringType(), True),
        StructField("LocationKey", IntegerType(), True),  # From dim join
        StructField("PostingDate", DateType(), True),
        StructField("DateKey", IntegerType(), True),  # From dim_Date join
        StructField("PostingYear", IntegerType(), True),  # Partition Key, from dim_Date join
        StructField("FiscalYear", IntegerType(), True),  # From dim_Date join
        StructField("FiscalQuarter", StringType(), True),  # From dim_Date join
        StructField("DimensionBridgeKey", IntegerType(), True),  # From bridge join
        StructField("TeamCode", StringType(), True),  # From bridge or global dims
        StructField("TeamName", StringType(), True),  # From bridge or global dims
        StructField("ProductCode", StringType(), True),  # From bridge or global dims
        StructField("ProductName", StringType(), True),  # From bridge or global dims
        StructField("HasTeamDimension", BooleanType(), True),  # Derived/From bridge
        StructField("HasProductDimension", BooleanType(), True),  # Derived/From bridge
        StructField("ValuedQuantity", DecimalType(18, 6), True),  # Quantity valued
        StructField("InvoicedQuantity", DecimalType(18, 6), True),  # Quantity invoiced
        StructField("CostAmountActual", DecimalType(18, 6), True),  # Actual cost
        StructField("CostAmountExpected", DecimalType(18, 6), True),  # Expected cost
        StructField("CostPerUnit", DecimalType(18, 6), True),  # Cost per unit
        StructField("DocumentNo", StringType(), True),  # Document number
        StructField("DocumentType", IntegerType(), True),  # Document type
        StructField("DocumentTypeText", StringType(), True),  # Human-readable document type
        StructField("ItemLedgerEntryType", IntegerType(), True),  # Entry type
        StructField("ItemLedgerEntryTypeText", StringType(), True),  # Human-readable entry type
        StructField("SourceType", IntegerType(), True),  # Source type
        StructField("SourceNo", StringType(), True),  # Source number
        StructField("InventoryPostingGroup", StringType(), True),  # Posting group
    ]
)


@fact_table_handler(error_msg="Failed to create inventory value fact table")
def create_inventory_value_fact(
    spark: SparkSession,
    source_table: str = "",
    silver_path: str = "",
    gold_path: str = "",
    min_year: int | None = None,
    incremental: bool = False,
    last_processed_time: datetime | None = None,
    fiscal_year_start: int = 1,
) -> DataFrame:
    """
    Create inventory value fact table from Value Entry table.

    Args:
        spark: Spark session
        source_table: Optional specific table name to process
        silver_path: Path to silver layer
        gold_path: Path to gold layer
        min_year: Optional minimum year to include
        incremental: Whether to perform incremental loading
        last_processed_time: Timestamp of last successful processing
        fiscal_year_start: The month (1-12) when fiscal year starts

    Returns:
        DataFrame containing the inventory value fact table
    """
    with logging.span("create_inventory_value_fact"):
        # Use source_table if provided, otherwise use default
        table_name = source_table or "ValueEntry"

        # Read source data
        source_df, source_table_name_used = read_table_from_config(
            spark=spark, table_base_name=table_name, db_path=silver_path, required=True
        )

        # Return empty DataFrame with schema if source is None
        if source_df is None:
            logging.error(f"Failed to read source table {table_name}")
            return enforce_schema(
                df=spark.createDataFrame([], FACT_INVENTORY_VALUE_SCHEMA),
                target_schema=FACT_INVENTORY_VALUE_SCHEMA,
                cast_columns=False,
                add_missing=False,
            )

        # Apply incremental filtering AFTER reading, if needed
        if incremental and last_processed_time:
            # Get the incremental column from table config
            table_config = extract_table_config(table_name)
            incremental_column = table_config.get("incremental_column")

            if incremental_column and incremental_column in source_df.columns:
                source_df = source_df.filter(F.col(incremental_column) > F.lit(last_processed_time))
                logging.info(
                    f"Applied incremental filter to {table_name} using column '{incremental_column}'"
                )
            else:
                logging.warning(
                    f"No configured incremental column found for {table_name}. Processing all data."
                )

        # Apply year filter if min_year is provided
        if min_year is not None:
            source_df = year_filter(
                df=source_df,
                min_year=min_year,
                date_column="PostingDate",
                year_column="PostingYear",
                add_year_column=True,
            )

        # Initial fact table
        fact_df = source_df

        # Join with Item dimension
        fact_df = join_dimension(
            spark=spark,
            fact_df=fact_df,
            dimension_name="Item",
            fact_join_keys=["ItemNo"],
            gold_path=gold_path,
        )

        # Join with Location dimension if LocationCode exists
        if "LocationCode" in fact_df.columns:
            fact_df = join_dimension(
                spark=spark,
                fact_df=fact_df,
                dimension_name="Location",
                fact_join_keys=["LocationCode"],
                gold_path=gold_path,
            )

        # Join with Date dimension
        fact_df = join_dimension(
            spark=spark,
            fact_df=fact_df,
            dimension_name="Date",
            fact_join_keys=["PostingDate"],
            gold_path=gold_path,
        )

        # Join with DimensionBridge
        fact_df = join_dimension(
            spark=spark,
            fact_df=fact_df,
            dimension_name="DimensionBridge",
            fact_join_keys={"EntryNo": "EntryNo", "SourceTable": "ValueEntry"},
            gold_path=gold_path,
        )

        # Add dimension flags
        fact_df = create_dimension_flags(
            fact_df, dimension_prefix_pairs=[("Team", "Team"), ("Product", "Product")]
        )

        # Ensure schema compliance
        fact_df = enforce_schema(
            df=fact_df,
            target_schema=FACT_INVENTORY_VALUE_SCHEMA,
            cast_columns=True,
            add_missing=True,
        )

        # Add document type text
        fact_df = fact_df.withColumn(
            "DocumentTypeText",
            F.when(F.col("DocumentType") == 0, "Purchase")
            .when(F.col("DocumentType") == 1, "Sales")
            .when(F.col("DocumentType") == 2, "Positive Adjmt.")
            .when(F.col("DocumentType") == 3, "Negative Adjmt.")
            .when(F.col("DocumentType") == 4, "Transfer")
            .when(F.col("DocumentType") == 5, "Consumption")
            .when(F.col("DocumentType") == 6, "Output")
            .otherwise("Unknown"),
        )

        # Add item ledger entry type text
        fact_df = fact_df.withColumn(
            "ItemLedgerEntryTypeText",
            F.when(F.col("ItemLedgerEntryType") == 0, "Purchase")
            .when(F.col("ItemLedgerEntryType") == 1, "Sale")
            .when(F.col("ItemLedgerEntryType") == 2, "Positive Adjmt.")
            .when(F.col("ItemLedgerEntryType") == 3, "Negative Adjmt.")
            .when(F.col("ItemLedgerEntryType") == 4, "Transfer")
            .when(F.col("ItemLedgerEntryType") == 5, "Consumption")
            .when(F.col("ItemLedgerEntryType") == 6, "Output")
            .otherwise("Unknown"),
        )

        # Calculate cost per unit where quantity is not zero
        fact_df = fact_df.withColumn(
            "CostPerUnit",
            F.when(
                F.col("ValuedQuantity").isNotNull() & (F.col("ValuedQuantity") != 0),
                F.col("CostAmountActual") / F.col("ValuedQuantity"),
            ).otherwise(F.lit(None).cast(DecimalType(18, 6))),
        )

        # Add audit columns
        fact_df = add_audit_columns(fact_df, layer="gold")

        logging.info(f"Created inventory value fact with {fact_df.count()} rows")
        return fact_df
