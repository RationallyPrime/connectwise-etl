"""
Accounts Receivable fact table creation module.
Handles the transformation of CustLedgerEntry data into the fact_AccountsReceivable gold table.
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
from bc_etl.utils.dataframe_utils import add_audit_columns, year_filter
from bc_etl.utils.decorators import fact_table_handler
from bc_etl.utils.exceptions import FactTableError
from bc_etl.utils.schema_utils import create_dimension_flags, enforce_schema
from bc_etl.utils.table_utils import read_table_from_config
from bc_etl.utils.watermark_manager import manage_watermark

# Define Target Schema for accounts receivable fact table
FACT_ACCOUNTS_RECEIVABLE_SCHEMA = StructType(
    [
        StructField("$Company", StringType(), True),
        StructField("EntryNo", IntegerType(), True),
        StructField("CustomerNo", StringType(), True),
        StructField("CustomerKey", IntegerType(), True),  # From dim join
        StructField("DocumentNo", StringType(), True),
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
        StructField("Amount", DecimalType(18, 6), True),  # Original amount
        StructField("RemainingAmount", DecimalType(18, 6), True),  # Current outstanding amount
        StructField("OriginalAmountLCY", DecimalType(18, 6), True),  # Amount in local currency
        StructField("RemainingAmountLCY", DecimalType(18, 6), True),  # Remaining in local currency
        StructField("CurrencyCode", StringType(), True),  # Transaction currency
        StructField("DueDate", DateType(), True),  # When payment is due
        StructField("DocumentType", IntegerType(), True),  # Invoice, Credit Memo, etc.
        StructField("DocumentTypeText", StringType(), True),  # Human-readable document type
        StructField("Open", BooleanType(), True),  # Whether entry is still open
        StructField("CustomerPostingGroup", StringType(), True),  # Posting group
        StructField("DaysOverdue", IntegerType(), True),  # Calculated field
        StructField("AgingBucket", StringType(), True),  # Calculated field (e.g., "30-60 days")
    ]
)


@fact_table_handler(error_msg="Failed to create accounts receivable fact table")
def create_accounts_receivable_fact(
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
    Create accounts receivable fact table from Customer Ledger Entry table.

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
        DataFrame containing the accounts receivable fact table

    Raises:
        FactTableError: If required columns are missing or processing fails
    """
    with logging.span("create_accounts_receivable_fact"):
        logging.info("Starting create_accounts_receivable_fact")
        logging.info(f"Incremental: {incremental}, Last processed time: {last_processed_time}")

        # Get table name to use - either from parameter or config
        table_base_name = source_table or "CustLedgerEntry"

        # Manage watermark for incremental processing
        if incremental:
            last_processed_time = manage_watermark(
                spark=spark,
                table_name=table_base_name,
                is_incremental=incremental,
                last_processed_time=last_processed_time,
                default_lookback_days=90,  # Default to 90 days for AR entries
            )

        # Read Customer Ledger Entry table using table_utils
        customer_df, customer_table = read_table_from_config(
            spark=spark,
            table_base_name=table_base_name,
            db_path=silver_path,
            required=True,
        )

        # Ensure customer_df is not None before proceeding
        if customer_df is None:
            raise FactTableError(f"Failed to read {table_base_name} table")

        # Filter by year if min_year is provided
        if min_year is not None:
            customer_df = year_filter(
                df=customer_df,
                min_year=min_year,
                date_column="PostingDate",
                year_column="PostingYear",
                add_year_column=True,
            )

        # Join with Customer dimension
        fact_df = join_dimension(
            spark=spark,
            fact_df=customer_df,
            dimension_name="Customer",
            fact_join_keys=["CustomerNo"],
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

        # Join with Dimension Bridge table for team and product dimensions
        try:
            bridge_df = spark.table(f"{gold_path}.dim_DimensionBridge")

            # Join with bridge table
            fact_df = fact_df.join(
                bridge_df,
                (fact_df["$Company"] == bridge_df["$Company"])
                & (fact_df["CustomerNo"] == bridge_df["CustomerNo"])
                & (bridge_df["SourceTable"] == "Customer"),
                "left",
            )

            # Select needed columns from bridge
            bridge_cols = [
                "DimensionBridgeKey",
                "TeamCode",
                "TeamName",
                "ProductCode",
                "ProductName",
            ]

            # Keep only needed columns from bridge
            for col in bridge_cols:
                if col in bridge_df.columns and col not in fact_df.columns:
                    fact_df = fact_df.withColumn(col, F.col(col))
                elif col not in bridge_df.columns:
                    # Add missing columns with null values
                    fact_df = fact_df.withColumn(
                        col,
                        F.lit(None).cast("string" if "Name" in col or "Code" in col else "integer"),
                    )

        except Exception as e:
            logging.warning(f"Failed to join with dimension bridge: {e}")
            # Add missing columns if bridge join fails
            fact_df = fact_df.withColumn("DimensionBridgeKey", F.lit(None).cast("integer"))
            fact_df = fact_df.withColumn("TeamCode", F.lit(None).cast("string"))
            fact_df = fact_df.withColumn("TeamName", F.lit(None).cast("string"))
            fact_df = fact_df.withColumn("ProductCode", F.lit(None).cast("string"))
            fact_df = fact_df.withColumn("ProductName", F.lit(None).cast("string"))

        # Add dimension flags
        fact_df = create_dimension_flags(
            fact_df, dimension_prefix_pairs=[("Team", "Team"), ("Product", "Product")]
        )

        # Calculate days overdue and aging bucket
        current_date = datetime.now().date()
        fact_df = fact_df.withColumn(
            "DaysOverdue",
            F.when(
                (F.col("Open") is True) & (F.col("DueDate") < F.lit(current_date)),
                F.datediff(F.lit(current_date), F.col("DueDate")),
            ).otherwise(0),
        )

        # Add aging bucket
        fact_df = fact_df.withColumn(
            "AgingBucket",
            F.when(F.col("DaysOverdue") <= 0, "Current")
            .when(F.col("DaysOverdue") <= 30, "1-30 days")
            .when(F.col("DaysOverdue") <= 60, "31-60 days")
            .when(F.col("DaysOverdue") <= 90, "61-90 days")
            .otherwise("90+ days"),
        )

        # Add document type text
        fact_df = fact_df.withColumn(
            "DocumentTypeText",
            F.when(F.col("DocumentType") == 0, "Payment")
            .when(F.col("DocumentType") == 1, "Invoice")
            .when(F.col("DocumentType") == 2, "Credit Memo")
            .when(F.col("DocumentType") == 3, "Finance Charge Memo")
            .when(F.col("DocumentType") == 4, "Reminder")
            .when(F.col("DocumentType") == 5, "Refund")
            .otherwise("Unknown"),
        )

        # Enforce schema to ensure all required columns exist with correct types
        result_df = enforce_schema(
            df=fact_df,
            target_schema=FACT_ACCOUNTS_RECEIVABLE_SCHEMA,
            cast_columns=True,
            add_missing=True,
        )

        # Add audit columns
        result_df = add_audit_columns(result_df, layer="gold")

        # Update watermark if incremental processing
        if incremental and last_processed_time:
            manage_watermark(
                spark=spark,
                table_name=table_base_name,
                is_incremental=incremental,
                last_processed_time=last_processed_time,
                df=result_df,
                df_timestamp_col="PostingDate",
            )

        logging.info(f"Created accounts receivable fact with {result_df.count()} rows")
        return result_df
