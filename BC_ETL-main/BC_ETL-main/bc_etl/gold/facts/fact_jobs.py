"""
Jobs fact table creation module.
Handles the transformation of JobLedgerEntry data into the fact_Jobs gold table.
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
from bc_etl.utils.schema_utils import enforce_schema
from bc_etl.utils.table_utils import read_table_from_config

# Define Target Schema for jobs fact table
FACT_JOBS_SCHEMA = StructType(
    [
        StructField("$Company", StringType(), True),
        StructField("EntryNo", IntegerType(), True),
        StructField("JobNo", StringType(), True),
        StructField("JobKey", IntegerType(), True),  # From dim join
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
        StructField("Quantity", DecimalType(18, 6), True),  # Job quantity
        StructField("TotalCost", DecimalType(18, 6), True),  # Total cost amount
        StructField("TotalPrice", DecimalType(18, 6), True),  # Total price amount
        StructField("LineAmount", DecimalType(18, 6), True),  # Line amount
        StructField("LineDiscountAmount", DecimalType(18, 6), True),  # Line discount amount
        StructField("Type", IntegerType(), True),  # Resource, Item, etc.
        StructField("TypeText", StringType(), True),  # Human-readable type
        StructField("No", StringType(), True),  # Resource/Item number
        StructField("Description", StringType(), True),  # Description
        StructField("WorkTypeCode", StringType(), True),  # Work type code
        StructField("CustomerNo", StringType(), True),  # Customer number
        StructField("CustomerKey", IntegerType(), True),  # From dim join
    ]
)


@fact_table_handler(error_msg="Failed to create jobs fact table")
def create_jobs_fact(
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
    Create jobs fact table from Job Ledger Entry table.

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
        DataFrame containing the jobs fact table

    Raises:
        FactTableError: If fact table creation fails
    """
    with logging.span("create_jobs_fact"):
        # Use source_table if provided, otherwise use default
        table_name = source_table or "JobLedgerEntry"

        # Read source data
        source_df, source_table_name_used = read_table_from_config(
            spark=spark, table_base_name=table_name, db_path=silver_path, required=True
        )

        # Return empty DataFrame with schema if source is None
        if source_df is None:
            logging.error(f"Failed to read source table {table_name}")
            return enforce_schema(
                df=spark.createDataFrame([], FACT_JOBS_SCHEMA),
                target_schema=FACT_JOBS_SCHEMA,
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

        # Join with Job dimension
        fact_df = join_dimension(
            spark=spark,
            fact_df=fact_df,
            dimension_name="Job",
            fact_join_keys={"JobNo": "No"},
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

        # Join with Customer dimension
        fact_df = join_dimension(
            spark=spark,
            fact_df=fact_df,
            dimension_name="Customer",
            fact_join_keys={"CustomerNo": "No"},
            gold_path=gold_path,
        )

        # Join with DimensionBridge
        fact_df = join_dimension(
            spark=spark,
            fact_df=fact_df,
            dimension_name="DimensionBridge",
            fact_join_keys={"DimensionSetID": "DimensionSetID"},
            gold_path=gold_path,
        )

        # Add type text for better readability
        fact_df = fact_df.withColumn(
            "TypeText",
            F.when(F.col("Type") == 0, "Resource")
            .when(F.col("Type") == 1, "Item")
            .when(F.col("Type") == 2, "G/L Account")
            .when(F.col("Type") == 3, "Resource Group")
            .otherwise("Unknown"),
        )

        # Calculate derived metrics
        fact_df = fact_df.withColumn("Profit", F.col("TotalPrice").minus(F.col("TotalCost")))

        # Ensure schema compliance
        fact_df = enforce_schema(
            df=fact_df,
            target_schema=FACT_JOBS_SCHEMA,
            cast_columns=True,
            add_missing=True,
        )

        # Add audit columns
        fact_df = add_audit_columns(fact_df, layer="gold")

        # Select columns in the exact order defined by the schema
        required_columns = [field.name for field in FACT_JOBS_SCHEMA.fields]
        result_df = fact_df.select(*required_columns)

        logging.info(f"Created jobs fact with {result_df.count()} rows")
        return result_df
