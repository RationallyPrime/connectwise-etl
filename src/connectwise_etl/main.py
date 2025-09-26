"""ConnectWise ETL pipeline."""

from datetime import datetime, timedelta
from typing import Literal

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

# ETLConfig eliminated with config monster
from .incremental import IncrementalProcessor, build_incremental_conditions
from .utils import get_logger
from .utils.base import ErrorCode
from .utils.decorators import with_etl_error_handling
from .utils.exceptions import ETLConfigError

logger = get_logger(__name__)


def get_table_name(layer: str, entity: str) -> str:
    """Generate table name following ConnectWise convention."""
    return f"{layer}.{layer}_cw_{entity}"


def _flatten_structs(df: DataFrame, max_depth: int = 3) -> DataFrame:
    """Flatten nested struct columns with conflict resolution."""
    if max_depth <= 0 or df.isEmpty():
        return df

    struct_cols = [
        field.name for field in df.schema.fields if isinstance(field.dataType, StructType)
    ]

    if not struct_cols:
        return df

    # Track existing column names to avoid conflicts
    existing_names = {field.name for field in df.schema.fields}
    generated_names = set()

    select_cols = []
    for field in df.schema.fields:
        if field.name not in struct_cols:
            select_cols.append(F.col(field.name))
            generated_names.add(field.name)
        else:
            struct_type = field.dataType
            for struct_field in struct_type.fields:
                child_name = struct_field.name

                # Generate camelCase name
                if child_name.startswith("_"):
                    base_name = f"{field.name}{child_name}"
                else:
                    child_camel = child_name[0].upper() + child_name[1:] if child_name else ""
                    base_name = f"{field.name}{child_camel}"

                # Resolve naming conflicts
                final_name = base_name
                suffix = 1
                while final_name in existing_names or final_name in generated_names:
                    final_name = f"{base_name}_{suffix}"
                    suffix += 1

                generated_names.add(final_name)
                select_cols.append(F.col(f"{field.name}.{struct_field.name}").alias(final_name))

    flattened_df = df.select(select_cols)
    return _flatten_structs(flattened_df, max_depth - 1)


@with_etl_error_handling(operation="extract_bronze_data")
def extract_bronze_data(
    spark: SparkSession,
    mode: Literal["full", "incremental"],
    lookback_days: int,
) -> None:
    """Extract ConnectWise data to bronze tables."""
    from . import client

    extractor = client.ConnectWiseExtractor()

    incremental_processor = None
    if mode == "incremental":
        incremental_processor = IncrementalProcessor(spark)

    endpoints = {
        "Agreement": "/finance/agreements",
        "TimeEntry": "/time/entries",
        "ExpenseEntry": "/expense/entries",
        "ProductItem": "/procurement/products",
        "Invoice": "/finance/invoices",
        "Member": "/system/members",
        "Company": "/company/companies",
    }

    for entity_name, endpoint in endpoints.items():
        conditions = None
        if mode == "incremental":
            since_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
            conditions = build_incremental_conditions(entity_name, since_date)

        bronze_df = extractor.extract(endpoint=endpoint, conditions=conditions, page_size=1000)
        bronze_df = bronze_df.withColumn("etlTimestamp", F.current_timestamp())
        bronze_df = bronze_df.withColumn("etlEntity", F.lit(entity_name))

        record_count = bronze_df.count()
        if record_count == 0:
            logger.info(f"No new records for {entity_name}")
            continue

        table_name = get_table_name("bronze", entity_name.lower())

        if (
            mode == "incremental"
            and spark.catalog.tableExists(table_name)
            and incremental_processor
        ):
            merged, total = incremental_processor.merge_bronze_incremental(bronze_df, table_name)
            logger.info(f"Merged {merged} records into {table_name} (total: {total})")
        else:
            bronze_df.write.mode("overwrite").saveAsTable(table_name)
            logger.info(f"Stored {record_count} records in {table_name}")


@with_etl_error_handling(operation="transform_silver_data")
def transform_silver_data(
    spark: SparkSession,
    mode: Literal["full", "incremental"],
) -> None:
    """Transform bronze data to silver tables."""
    from .models.registry import models
    # EntityConfig eliminated - models themselves define the structure!

    incremental_processor = None
    if mode == "incremental":
        incremental_processor = IncrementalProcessor(spark)

    for entity_name, _ in models.items():
        bronze_table = get_table_name("bronze", entity_name)
        silver_table = get_table_name("silver", entity_name)

        if mode == "incremental" and incremental_processor:
            bronze_df = incremental_processor.get_changed_records(
                bronze_table, target_table=silver_table
            )
        else:
            bronze_df = spark.table(bronze_table)

        total_rows = bronze_df.count()
        if total_rows == 0:
            logger.info(f"No new records to process for {entity_name}")
            continue

        # Transform to silver
        silver_df = bronze_df
        silver_df = (
            silver_df.withColumn("_etl_processed_at", F.current_timestamp())
            .withColumn("_etl_source", F.lit("connectwise"))
            .withColumn("_etl_batch_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
        )

        # Flatten nested structures
        silver_df = _flatten_structs(silver_df, 3)

        if (
            mode == "incremental"
            and incremental_processor
            and spark.catalog.tableExists(silver_table)
        ):
            business_keys = ["id"] if "id" in silver_df.columns else []
            processed_count = incremental_processor.merge_silver_scd1(
                silver_df, silver_table, business_keys
            )
            logger.info(f"Merged {processed_count} records into {silver_table}")
        else:
            # Full mode: overwrite schema to handle any changes
            silver_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
                silver_table
            )
            logger.info(f"Processed {total_rows} records to {silver_table}")


@with_etl_error_handling(operation="create_gold_tables_yaml")
def create_gold_tables_yaml(spark: SparkSession) -> None:
    """Create fact tables and dimensions using YAML schemas."""
    from . import transforms
    from .yaml_dimensions import create_all_dimensions_yaml

    # Create dimensions first using YAML
    logger.info("Creating ConnectWise dimensions from YAML...")
    create_all_dimensions_yaml(spark)
    logger.info("✅ ConnectWise dimensions created from YAML")

    # Create facts using specialized transforms
    logger.info("Creating ConnectWise fact tables...")

    # Define the facts to create (simple dict config instead of complex classes)
    facts_to_create = [
        {
            "name": "timeentry",
            "silver_table": get_table_name("silver", "timeentry"),
            "gold_table": get_table_name("gold", "fact_timeentry"),
            "transform_function": "create_time_entry_fact",
        },
        {
            "name": "expenseentry",
            "silver_table": get_table_name("silver", "expenseentry"),
            "gold_table": get_table_name("gold", "fact_expenseentry"),
            "transform_function": "create_expense_entry_fact",
        },
        {
            "name": "productitem",
            "silver_table": get_table_name("silver", "productitem"),
            "gold_table": get_table_name("gold", "fact_productitem"),
            "transform_function": "create_product_fact",
        },
    ]

    for fact_info in facts_to_create:
        logger.info(f"Creating fact table: {fact_info['name']}")

        try:
            silver_df = spark.table(fact_info["silver_table"])

            # Use specialized transform function if available
            transform_func = getattr(transforms, fact_info["transform_function"], None)
            if transform_func:
                # Load agreement data for enrichment
                try:
                    agreement_df = spark.table(get_table_name("silver", "agreement"))
                except Exception:
                    agreement_df = None

                # Call appropriate function with correct parameters
                if fact_info["name"] == "timeentry":
                    gold_df = transform_func(
                        spark=spark,
                        time_entry_df=silver_df,
                        agreement_df=agreement_df,
                        config={"source": "connectwise", "business_key": "id"},
                    )
                elif fact_info["name"] == "expenseentry":
                    gold_df = transform_func(
                        spark=spark,
                        expense_df=silver_df,
                        agreement_df=agreement_df,
                        config={"source": "connectwise", "business_key": "id"},
                    )
                else:  # productitem and others
                    gold_df = transform_func(
                        spark=spark,
                        product_df=silver_df,
                        agreement_df=agreement_df,
                        config={"source": "connectwise", "business_key": "id"},
                    )
            else:
                # Fallback to generic fact creation
                from . import facts

                gold_df = facts.create_generic_fact_table(
                    fact_config={"source": "connectwise", "business_key": "id"},
                    silver_df=silver_df,
                    spark=spark,
                )

            gold_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
                fact_info["gold_table"]
            )
            logger.info(f"✅ Created fact table {fact_info['gold_table']}")

        except Exception as e:
            logger.error(f"Failed to create fact {fact_info['name']}: {e}")
            continue


@with_etl_error_handling(operation="run_etl_pipeline")
def run_etl_pipeline(
    spark: SparkSession,
    layers: list[Literal["bronze", "silver", "gold"]],
    mode: Literal["full", "incremental"],
    lookback_days: int,
) -> None:
    """Run ConnectWise ETL pipeline."""
    if not spark:
        raise ETLConfigError("SparkSession is required", code=ErrorCode.CONFIG_MISSING)
    if not layers:
        raise ETLConfigError("At least one layer must be specified", code=ErrorCode.CONFIG_MISSING)
    if mode not in ["full", "incremental"]:
        raise ETLConfigError(
            f"Invalid mode '{mode}'. Must be 'full' or 'incremental'", code=ErrorCode.CONFIG_INVALID
        )
    if lookback_days <= 0:
        raise ETLConfigError("lookback_days must be positive", code=ErrorCode.CONFIG_INVALID)

    logger.info("Running ConnectWise ETL pipeline")
    logger.info(f"Processing layers: {layers}")
    logger.info(f"Mode: {mode}, Lookback days: {lookback_days}")

    if "bronze" in layers:
        logger.info("Running bronze layer")
        extract_bronze_data(spark, mode, lookback_days)

    if "silver" in layers:
        logger.info("Running silver layer")
        transform_silver_data(spark, mode)

    if "gold" in layers:
        logger.info("Running gold layer")
        create_gold_tables_yaml(spark)


if __name__ == "__main__":
    raise ETLConfigError(
        "All parameters are required. See function signature.", code=ErrorCode.CONFIG_MISSING
    )
