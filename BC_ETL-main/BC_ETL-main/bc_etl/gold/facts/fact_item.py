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
from bc_etl.utils.dataframe_utils import add_audit_columns, add_sign_columns, year_filter
from bc_etl.utils.decorators import fact_table_handler
from bc_etl.utils.schema_utils import create_dimension_flags, enforce_schema
from bc_etl.utils.table_utils import read_table_from_config

# Define Target Schema for item fact table
# It's crucial this schema matches EXACTLY across all functions writing to the same fact table
FACT_ITEM_SCHEMA = StructType(
    [
        StructField("$Company", StringType(), True),
        StructField("ItemNo", StringType(), True),
        StructField("ItemKey", IntegerType(), True),
        StructField("PostingDate", DateType(), True),
        StructField("DateKey", IntegerType(), True),
        StructField("PostingYear", IntegerType(), True),  # Partition Key
        StructField("FiscalYear", IntegerType(), True),
        StructField("FiscalQuarter", StringType(), True),
        StructField("Quantity", DecimalType(18, 6), True),
        StructField("QuantitySign", IntegerType(), True),
        StructField("Cost", DecimalType(18, 6), True),
        StructField("CostSign", IntegerType(), True),
        StructField("LocationCode", StringType(), True),
        StructField("LocationKey", IntegerType(), True),
        StructField("DimensionBridgeKey", IntegerType(), True),
        StructField("TeamCode", StringType(), True),
        StructField("TeamName", StringType(), True),
        StructField("ProductCode", StringType(), True),
        StructField("ProductName", StringType(), True),
        StructField("HasTeamDimension", BooleanType(), True),
        StructField("HasProductDimension", BooleanType(), True),
    ]
)


@fact_table_handler(error_msg="Failed to create item fact table")
def create_item_fact(
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
    Create item fact table from item ledger entry table.

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
        DataFrame containing the item fact table
    """
    with logging.span("create_item_fact"):
        # Use source_table if provided, otherwise use default
        table_name = source_table or "ItemLedgerEntry"

        # Read source data
        source_df, source_table_name_used = read_table_from_config(
            spark=spark, table_base_name=table_name, db_path=silver_path, required=True
        )

        # Return empty DataFrame with schema if source is None
        if source_df is None:
            logging.error(f"Failed to read source table {table_name}")
            return enforce_schema(
                df=spark.createDataFrame([], FACT_ITEM_SCHEMA),
                target_schema=FACT_ITEM_SCHEMA,
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
            fact_join_keys={"ItemNo": "ItemNo", "SourceTable": "Item"},
            gold_path=gold_path,
        )

        # Add dimension flags
        fact_df = create_dimension_flags(
            fact_df, dimension_prefix_pairs=[("Team", "Team"), ("Product", "Product")]
        )

        # Add sign columns for Quantity and Cost
        fact_df = add_sign_columns(fact_df, value_columns=["Quantity", "Cost"])

        # Ensure Cost column exists
        if "Cost" not in fact_df.columns:
            fact_df = fact_df.withColumn("Cost", F.lit(None).cast("decimal(18,6)"))
            fact_df = fact_df.withColumn("CostSign", F.lit(None).cast("integer"))

        # Enforce schema to ensure all required columns exist with correct types
        fact_df = enforce_schema(
            df=fact_df,
            target_schema=FACT_ITEM_SCHEMA,
            cast_columns=True,
            add_missing=True,
        )

        # Add audit columns
        fact_df = add_audit_columns(fact_df, layer="gold")

        # Log dimension coverage statistics
        team_dim_pct = (
            fact_df.filter(F.col("HasTeamDimension")).count() / fact_df.count() * 100
            if fact_df.count() > 0
            else 0
        )
        product_dim_pct = (
            fact_df.filter(F.col("HasProductDimension")).count() / fact_df.count() * 100
            if fact_df.count() > 0
            else 0
        )

        logging.info(
            "Dimension coverage statistics",
            team_dimension_pct=round(team_dim_pct, 2),
            product_dimension_pct=round(product_dim_pct, 2),
        )

        logging.info(f"Created item fact with {fact_df.count()} rows")
        return fact_df
