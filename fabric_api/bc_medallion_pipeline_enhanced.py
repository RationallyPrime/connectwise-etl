"""
Enhanced BC Medallion Pipeline with Schema-Aware Structure.

This version of the BC pipeline is designed to work with the unified lakehouse
structure, using proper schema separation (bronze.bc, silver.bc, gold.bc).
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

from .bc_models import models
from .core.pipeline_utils import log_dataframe_info, log_error, log_info

logger = logging.getLogger(__name__)


class BCMedallionPipelineEnhanced:
    """Enhanced Business Central medallion pipeline with schema separation."""

    def __init__(self, spark: SparkSession):
        """Initialize the pipeline with a Spark session."""
        self.spark = spark
        self.schemas = {"bronze": "bronze.bc", "silver": "silver.bc", "gold": "gold.bc"}

        # Create schemas if they don't exist
        self._create_schemas()

        # Map BC table names to Pydantic models (without numeric suffixes)
        self.model_mapping = {
            "AccountingPeriod": models.AccountingPeriod,
            "CompanyInformation": models.CompanyInformation,
            "Currency": models.Currency,
            "CustLedgerEntry": models.CustLedgerEntry,
            "Customer": models.Customer,
            "DetailedCustLedgEntry": models.DetailedCustLedgEntry,
            "DetailedVendorLedgEntry": models.DetailedVendorLedgEntry,
            "Dimension": models.Dimension,
            "DimensionSetEntry": models.DimensionSetEntry,
            "DimensionValue": models.DimensionValue,
            "GLAccount": models.GLAccount,
            "GLEntry": models.GLEntry,
            "GeneralLedgerSetup": models.GeneralLedgerSetup,
            "Item": models.Item,
            "Job": models.Job,
            "JobLedgerEntry": models.JobLedgerEntry,
            "Resource": models.Resource,
            "SalesInvoiceHeader": models.SalesInvoiceHeader,
            "SalesInvoiceLine": models.SalesInvoiceLine,
            "Vendor": models.Vendor,
            "VendorLedgerEntry": models.VendorLedgerEntry,
        }

        # Define natural keys for each table
        self.natural_keys = {
            "AccountingPeriod": ["StartingDate"],
            "CompanyInformation": ["Name"],
            "Currency": ["Code"],
            "CustLedgerEntry": ["EntryNo"],
            "Customer": ["No"],
            "DetailedCustLedgEntry": ["EntryNo"],
            "DetailedVendorLedgEntry": ["EntryNo"],
            "Dimension": ["Code"],
            "DimensionSetEntry": ["DimensionSetID", "DimensionCode"],
            "DimensionValue": ["DimensionCode", "Code"],
            "GLAccount": ["No"],
            "GLEntry": ["EntryNo"],
            "GeneralLedgerSetup": ["PrimaryKey"],
            "Item": ["No"],
            "Job": ["No"],
            "JobLedgerEntry": ["EntryNo"],
            "Resource": ["No"],
            "SalesInvoiceHeader": ["No"],
            "SalesInvoiceLine": ["DocumentNo", "LineNo"],
            "Vendor": ["No"],
            "VendorLedgerEntry": ["EntryNo"],
        }

        # Define dimension tables
        self.dimension_tables = [
            "AccountingPeriod",
            "GLAccount",
            "Customer",
            "Vendor",
            "Item",
            "Dimension",
            "DimensionValue",
            "Currency",
            "CompanyInformation",
            "Job",
            "Resource",
            "GeneralLedgerSetup",
        ]

        # Define fact tables
        self.fact_tables = [
            "GLEntry",
            "CustLedgerEntry",
            "VendorLedgerEntry",
            "JobLedgerEntry",
            "SalesInvoiceHeader",
            "SalesInvoiceLine",
            "DetailedCustLedgEntry",
            "DetailedVendorLedgEntry",
        ]

    def _create_schemas(self):
        """Create necessary schemas if they don't exist."""
        for schema_name in self.schemas.values():
            try:
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                logger.info(f"Ensured schema exists: {schema_name}")
            except Exception as e:
                logger.error(f"Error creating schema {schema_name}: {e}")

    def bronze_to_silver(
        self, table_name: str, incremental: bool = False, watermark_column: str = "SystemModifiedAt"
    ) -> DataFrame | None:
        """Transform bronze table to silver with validation and standardization."""
        log_info(f"Processing {table_name} from Bronze to Silver")

        # Strip numeric suffix from table name if present
        import re

        clean_table_name = re.sub(r"\d+$", "", table_name)

        # Read bronze table with schema qualification
        bronze_table_path = f"{self.schemas['bronze']}.{table_name}"
        try:
            bronze_df = self.spark.table(bronze_table_path)
        except Exception as e:
            error_msg = str(e)
            if "doesn't exist" in error_msg or "not found" in error_msg:
                log_error(f"Table {bronze_table_path} not found or broken - skipping")
            else:
                log_error(f"Failed to read bronze table {bronze_table_path}: {error_msg}")
            return None

        # Apply incremental filter if enabled
        if incremental and watermark_column in bronze_df.columns:
            last_watermark = datetime.now() - timedelta(days=7)
            bronze_df = bronze_df.filter(F.col(watermark_column) >= last_watermark)

        # Validate against Pydantic model if available
        model_class = self.model_mapping.get(clean_table_name)
        if model_class:
            spark_schema = model_class.model_spark_schema()
            common_columns = list(set(bronze_df.columns) & set(spark_schema.fieldNames()))
            bronze_df = bronze_df.select(*common_columns)

            # Cast columns to match model schema types
            for field in spark_schema.fields:
                if field.name in bronze_df.columns:
                    bronze_df = bronze_df.withColumn(
                        field.name, F.col(field.name).cast(field.dataType)
                    )

        # Add processing metadata
        silver_df = bronze_df.withColumn("SilverProcessedAt", F.current_timestamp())

        # Apply data quality checks
        if clean_table_name in self.natural_keys:
            natural_keys = self.natural_keys[clean_table_name]
            if "$Company" in silver_df.columns:
                natural_keys = ["$Company", *natural_keys]
            silver_df = silver_df.dropDuplicates(natural_keys)

        log_dataframe_info(silver_df, f"Silver {clean_table_name}")

        # Return with clean table name attached
        silver_df._table_name = clean_table_name  # type: ignore[attr-defined]
        return silver_df

    def silver_to_gold_dimension(self, table_name: str) -> DataFrame | None:
        """Transform silver dimension table to gold with surrogate keys."""
        log_info(f"Processing dimension {table_name} from Silver to Gold")

        # Read silver table with schema qualification
        silver_table_path = f"{self.schemas['silver']}.{table_name}"
        try:
            silver_df = self.spark.table(silver_table_path)
        except Exception as e:
            log_error(f"Failed to read silver table {silver_table_path}: {e!s}")
            return None

        # Generate surrogate key
        gold_df = self.generate_surrogate_key(silver_df, table_name)

        # Add dimension-specific transformations
        if table_name == "GLAccount":
            gold_df = gold_df.withColumn(
                "AccountCategory",
                F.when(F.col("AccountCategory").isNull(), "Unknown").otherwise(
                    F.col("AccountCategory")
                ),
            )
        elif table_name == "Customer":
            gold_df = gold_df.withColumn(
                "CustomerSegment",
                F.when(F.col("CustomerPostingGroup").isNull(), "Unknown").otherwise(
                    F.col("CustomerPostingGroup")
                ),
            )

        # Add gold processing metadata
        gold_df = gold_df.withColumn("GoldProcessedAt", F.current_timestamp())

        log_dataframe_info(gold_df, f"Gold Dimension {table_name}")
        return gold_df

    def silver_to_gold_fact(self, table_name: str, min_year: int | None = None) -> DataFrame | None:
        """Transform silver fact table to gold with dimension lookups."""
        log_info(f"Processing fact {table_name} from Silver to Gold")

        # Read silver table
        silver_table_path = f"{self.schemas['silver']}.{table_name}"
        try:
            fact_df = self.spark.table(silver_table_path)
        except Exception as e:
            log_error(f"Failed to read silver table {silver_table_path}: {e!s}")
            return None

        # Filter by minimum year if specified
        if min_year and "PostingDate" in fact_df.columns:
            fact_df = fact_df.filter(F.year("PostingDate") >= min_year)

        # Join with date dimension
        if "PostingDate" in fact_df.columns:
            date_dim_path = f"{self.schemas['gold']}.dim_Date"
            try:
                date_dim = self.spark.table(date_dim_path)
                fact_df = fact_df.join(
                    date_dim, fact_df.PostingDate == date_dim.Date, "left"
                ).select(
                    fact_df["*"],
                    date_dim["DateKey"],
                    date_dim["FiscalYear"],
                    date_dim["FiscalQuarter"],
                    date_dim["Year"].alias("PostingYear"),
                )
            except Exception as e:
                log_error(f"Failed to join with date dimension: {e!s}")

        # Join with dimension bridge if DimensionSetID exists
        if "DimensionSetID" in fact_df.columns:
            dimension_bridge_path = f"{self.schemas['gold']}.dim_DimensionBridge"
            try:
                dimension_bridge = self.spark.table(dimension_bridge_path)
                fact_df = fact_df.join(
                    dimension_bridge,
                    (fact_df["DimensionSetID"] == dimension_bridge["DimensionSetID"])
                    & (fact_df["$Company"] == dimension_bridge["$Company"]),
                    "left",
                ).select(
                    fact_df["*"],
                    dimension_bridge["DimensionBridgeKey"],
                    *[
                        col
                        for col in dimension_bridge.columns
                        if col.endswith("Code") or col.endswith("Name") or col.startswith("Has")
                    ],
                )
            except Exception as e:
                log_error(f"Failed to join with dimension bridge: {e!s}")

        # Add calculated measures
        if "Amount" in fact_df.columns:
            fact_df = fact_df.withColumn(
                "AmountSign",
                F.when(F.col("Amount") > 0, 1).when(F.col("Amount") < 0, -1).otherwise(0),
            )

        # Add gold processing metadata
        fact_df = fact_df.withColumn("GoldProcessedAt", F.current_timestamp())

        log_dataframe_info(fact_df, f"Gold Fact {table_name}")
        return fact_df

    def generate_surrogate_key(
        self, df: DataFrame, table_name: str, key_name: str | None = None
    ) -> DataFrame:
        """Generate surrogate keys for dimension tables."""
        if key_name is None:
            key_name = f"SK_{table_name}"

        natural_keys = self.natural_keys.get(table_name, [])
        if not natural_keys:
            log_error(f"No natural keys defined for table {table_name}")
            return df

        # Check if all natural keys exist
        missing_keys = [k for k in natural_keys if k not in df.columns]
        if missing_keys:
            log_error(f"Missing natural keys for {table_name}: {missing_keys}")
            return df

        # Create window specification
        company_col = "$Company" if "$Company" in df.columns else None
        existing_keys = [k for k in natural_keys if k in df.columns]

        if not existing_keys:
            log_error(f"None of the natural keys {natural_keys} exist in {table_name}")
            return df

        order_by_cols = [F.col(k) for k in existing_keys]

        if company_col:
            window = Window.partitionBy(F.col(company_col)).orderBy(*order_by_cols)
        else:
            window = Window.orderBy(*order_by_cols)

        # Generate surrogate key
        result_df = df.withColumn(key_name, F.row_number().over(window))

        log_info(f"Generated surrogate key {key_name} for {table_name}")
        return result_df

    def create_date_dimension(
        self, start_date: date, end_date: date, fiscal_year_start_month: int = 1
    ) -> DataFrame:
        """Create a date dimension table."""
        log_info(f"Creating date dimension from {start_date} to {end_date}")

        # Generate date range
        dates = []
        current_date = start_date
        while current_date <= end_date:
            dates.append((current_date,))
            current_date += timedelta(days=1)

        # Create DataFrame
        date_df = self.spark.createDataFrame(dates, ["Date"])

        # Add date attributes
        date_df = date_df.withColumn(
            "DateKey", F.year("Date") * 10000 + F.month("Date") * 100 + F.dayofmonth("Date")
        )
        date_df = date_df.withColumn("Year", F.year("Date"))
        date_df = date_df.withColumn("Month", F.month("Date"))
        date_df = date_df.withColumn("Day", F.dayofmonth("Date"))
        date_df = date_df.withColumn("Quarter", F.quarter("Date"))
        date_df = date_df.withColumn("WeekOfYear", F.weekofyear("Date"))
        date_df = date_df.withColumn("DayOfWeek", F.dayofweek("Date"))
        date_df = date_df.withColumn("MonthName", F.date_format("Date", "MMMM"))
        date_df = date_df.withColumn(
            "IsWeekend", F.when(F.col("DayOfWeek").isin([1, 7]), True).otherwise(False)
        )

        # Add fiscal year calculations
        date_df = date_df.withColumn(
            "FiscalYear",
            F.when(F.col("Month") >= fiscal_year_start_month, F.col("Year")).otherwise(
                F.col("Year") - 1
            ),
        )

        date_df = date_df.withColumn(
            "FiscalQuarter",
            F.concat(
                F.lit("FY"),
                F.col("FiscalYear"),
                F.lit("-Q"),
                F.when(
                    F.col("Month") < fiscal_year_start_month,
                    F.ceil((F.col("Month") + 12 - fiscal_year_start_month + 1) / 3),
                ).otherwise(F.ceil((F.col("Month") - fiscal_year_start_month + 1) / 3)),
            ),
        )

        return date_df

    def create_dimension_bridge(self, dimension_types: dict[str, str] | None = None) -> DataFrame:
        """Create a dimension bridge table mapping dimension sets to dimension values."""
        log_info("Creating dimension bridge table")

        # Default dimension types
        if dimension_types is None:
            dimension_types = {
                "TEAM": "TEAM",
                "PRODUCT": "PRODUCT",
                "DEPARTMENT": "DEPARTMENT",
                "PROJECT": "PROJECT",
            }

        # Read dimension framework tables
        dim_set_entries = self.spark.table(f"{self.schemas['silver']}.DimensionSetEntry")
        dim_values = self.spark.table(f"{self.schemas['silver']}.DimensionValue")

        # Create base dataframe
        base_df = dim_set_entries.select(F.col("$Company"), F.col("DimensionSetID")).distinct()

        result_df = base_df

        # Process each dimension type
        for dim_type, dim_code in dimension_types.items():
            log_info(f"Processing dimension type: {dim_type}")

            type_entries = dim_set_entries.filter(F.col("DimensionCode") == dim_code)

            if type_entries.count() == 0:
                log_info(f"No entries found for dimension type {dim_type}")
                continue

            # Join dimension values
            type_df = type_entries.join(
                dim_values.filter(F.col("DimensionCode") == dim_code),
                [
                    type_entries.DimensionValueCode == dim_values.Code,
                    type_entries["$Company"] == dim_values["$Company"],
                ],
                "left",
            ).select(
                type_entries["$Company"],
                type_entries["DimensionSetID"],
                dim_values["Code"].alias(f"{dim_type}Code"),
                dim_values["Name"].alias(f"{dim_type}Name"),
            )

            # Join to result
            result_df = result_df.join(type_df, ["$Company", "DimensionSetID"], "left")

            # Add presence flags
            result_df = result_df.withColumn(
                f"Has{dim_type}Dimension",
                F.when(F.col(f"{dim_type}Code").isNotNull(), True).otherwise(False),
            )

        # Generate surrogate key
        window = Window.partitionBy("$Company").orderBy("DimensionSetID")
        result_df = result_df.withColumn("DimensionBridgeKey", F.row_number().over(window))

        log_info(f"Dimension bridge created with {result_df.count()} rows")
        return result_df

    def run_pipeline(
        self,
        tables: list[str] | None = None,
        incremental: bool = False,
        min_year: int | None = None,
        dimension_types: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Run the complete BC medallion pipeline."""
        log_info("Starting BC Medallion Pipeline")
        results = {"status": "started", "phases": {}}

        # Phase 1: Create Date Dimension
        log_info("Phase 1: Creating Date Dimension")
        try:
            start_date = date(min_year if min_year else 2020, 1, 1)
            end_date = date(datetime.now().year + 5, 12, 31)
            date_dim = self.create_date_dimension(start_date, end_date)

            date_table = f"{self.schemas['gold']}.dim_Date"
            date_dim.write.mode("overwrite").saveAsTable(date_table)
            results["phases"]["date_dimension"] = {"status": "success", "table": date_table}
        except Exception as e:
            results["phases"]["date_dimension"] = {"status": "error", "error": str(e)}
            log_error(f"Error creating date dimension: {e}")

        # Phase 2: Process Bronze to Silver
        log_info("Phase 2: Processing Bronze to Silver")
        silver_tables_created = []

        if not tables:
            bronze_tables = self.spark.sql(f"SHOW TABLES IN {self.schemas['bronze']}").collect()
            tables = [t.tableName for t in bronze_tables]

        results["phases"]["bronze_to_silver"] = {}
        for table_name in tables:
            # Skip broken tables
            if table_name in ["VendorLedgerEntry25"]:
                log_info(f"Skipping broken table: {table_name}")
                continue

            try:
                silver_df = self.bronze_to_silver(table_name, incremental=incremental)
                if silver_df:
                    clean_table_name = getattr(silver_df, "_table_name", table_name)
                    silver_table = f"{self.schemas['silver']}.{clean_table_name}"

                    mode = "append" if incremental else "overwrite"
                    silver_df.write.mode(mode).option("mergeSchema", "true").saveAsTable(
                        silver_table
                    )

                    silver_tables_created.append(clean_table_name)
                    results["phases"]["bronze_to_silver"][table_name] = {
                        "status": "success",
                        "silver_table": silver_table,
                    }
            except Exception as e:
                log_error(f"Failed to process {table_name}: {e!s}")
                results["phases"]["bronze_to_silver"][table_name] = {
                    "status": "error",
                    "error": str(e),
                }

        # Phase 3: Process Silver to Gold - Dimensions
        log_info("Phase 3: Processing Silver to Gold - Dimensions")
        results["phases"]["dimensions"] = {}

        for table_name in self.dimension_tables:
            if table_name in silver_tables_created:
                try:
                    gold_dim_df = self.silver_to_gold_dimension(table_name)
                    if gold_dim_df:
                        gold_table = f"{self.schemas['gold']}.dim_{table_name}"
                        gold_dim_df.write.mode("overwrite").saveAsTable(gold_table)
                        results["phases"]["dimensions"][table_name] = {
                            "status": "success",
                            "table": gold_table,
                        }
                except Exception as e:
                    log_error(f"Failed to process dimension {table_name}: {e!s}")
                    results["phases"]["dimensions"][table_name] = {
                        "status": "error",
                        "error": str(e),
                    }

        # Phase 4: Create Dimension Bridge
        log_info("Phase 4: Creating Dimension Bridge")
        if all(dim in silver_tables_created for dim in ["DimensionSetEntry", "DimensionValue"]):
            try:
                dimension_bridge = self.create_dimension_bridge(dimension_types)
                if dimension_bridge:
                    bridge_table = f"{self.schemas['gold']}.dim_DimensionBridge"
                    dimension_bridge.write.mode("overwrite").partitionBy("$Company").saveAsTable(
                        bridge_table
                    )
                    results["phases"]["dimension_bridge"] = {
                        "status": "success",
                        "table": bridge_table,
                    }
            except Exception as e:
                log_error(f"Failed to create dimension bridge: {e!s}")
                results["phases"]["dimension_bridge"] = {"status": "error", "error": str(e)}

        # Phase 5: Process Silver to Gold - Facts
        log_info("Phase 5: Processing Silver to Gold - Facts")
        results["phases"]["facts"] = {}

        for table_name in self.fact_tables:
            if table_name in silver_tables_created:
                try:
                    gold_fact_df = self.silver_to_gold_fact(table_name, min_year=min_year)
                    if gold_fact_df:
                        gold_table = f"{self.schemas['gold']}.fact_{table_name}"
                        partition_cols = []
                        if "PostingYear" in gold_fact_df.columns:
                            partition_cols.append("PostingYear")
                        if "$Company" in gold_fact_df.columns:
                            partition_cols.append("$Company")

                        write_op = gold_fact_df.write.mode("overwrite")
                        if partition_cols:
                            write_op = write_op.partitionBy(*partition_cols)
                        write_op.saveAsTable(gold_table)

                        results["phases"]["facts"][table_name] = {
                            "status": "success",
                            "table": gold_table,
                        }
                except Exception as e:
                    log_error(f"Failed to process fact {table_name}: {e!s}")
                    results["phases"]["facts"][table_name] = {"status": "error", "error": str(e)}

        results["status"] = "completed"
        log_info("BC Medallion Pipeline completed")
        return results


# Example usage:
"""
from fabric_api.bc_medallion_pipeline_enhanced import BCMedallionPipelineEnhanced

# Initialize pipeline
pipeline = BCMedallionPipelineEnhanced(spark)

# Run the pipeline
results = pipeline.run_pipeline(
    incremental=False,
    min_year=2020
)
"""
