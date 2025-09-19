"""Cross-integration ETL orchestration using dynamic integration detection."""

import logging
from typing import Any, Literal

from pyspark.sql import SparkSession

from .config.models import ETLConfig
from .incremental import (
    IncrementalProcessor,
    build_incremental_conditions,
    get_incremental_lookback_days,
)
from .integrations import detect_available_integrations
from .utils.base import ErrorCode
from .utils.decorators import with_etl_error_handling
from .utils.exceptions import (
    ETLConfigError,
    ETLInfrastructureError,
    ETLProcessingError,
)


@with_etl_error_handling(operation="run_etl_pipeline")
def run_etl_pipeline(
    config: ETLConfig,
    spark: SparkSession,
    integrations: list[str],
    layers: list[Literal["bronze", "silver", "gold"]],
    mode: Literal["full", "incremental"],
    lookback_days: int,
) -> None:
    """
    Run ETL pipeline across all available integrations.

    Args:
        config: REQUIRED ETL configuration
        spark: REQUIRED SparkSession
        integrations: REQUIRED list of integration names to process
        layers: REQUIRED list of layers to process
        mode: REQUIRED processing mode ("full" or "incremental")
        lookback_days: REQUIRED lookback days for incremental mode
    """
    # Validate inputs - FAIL FAST
    if not config:
        raise ETLConfigError("ETL configuration is required", code=ErrorCode.CONFIG_MISSING)
    if not spark:
        raise ETLConfigError("SparkSession is required", code=ErrorCode.CONFIG_MISSING)
    if not integrations:
        raise ETLConfigError("At least one integration must be specified", code=ErrorCode.CONFIG_MISSING)
    if not layers:
        raise ETLConfigError("At least one layer must be specified", code=ErrorCode.CONFIG_MISSING)
    if mode not in ["full", "incremental"]:
        raise ETLConfigError(
            f"Invalid mode '{mode}'. Must be 'full' or 'incremental'",
            code=ErrorCode.CONFIG_INVALID
        )
    if lookback_days <= 0:
        raise ETLConfigError(
            "lookback_days must be positive",
            code=ErrorCode.CONFIG_INVALID
        )

    # Detect available integrations
    available_integrations = detect_available_integrations()

    # Validate requested integrations are available
    for integration_name in integrations:
        if integration_name not in config.integrations:
            raise ETLConfigError(
                f"Integration '{integration_name}' not configured",
                code=ErrorCode.CONFIG_MISSING,
                details={"requested": integration_name, "available": list(config.integrations.keys())}
            )
        if not available_integrations.get(integration_name, {}).get("available"):
            raise ETLInfrastructureError(
                f"Integration '{integration_name}' is configured but not available (package not installed?)",
                code=ErrorCode.STORAGE_ACCESS_FAILED,
                details={"integration": integration_name}
            )

    logging.info(f"Running ETL pipeline for integrations: {integrations}")
    logging.info(f"Processing layers: {layers}")
    logging.info(f"Mode: {mode}, Lookback days: {lookback_days}")

    for integration_name in integrations:
        # Process integration through specified layers
        process_integration(
            config=config,
            spark=spark,
            integration_name=integration_name,
            integration_info=available_integrations[integration_name],
            layers=layers,
            mode=mode,
            lookback_days=lookback_days,
        )



@with_etl_error_handling(operation="process_integration")
def process_integration(
    config: ETLConfig,
    spark: SparkSession,
    integration_name: str,
    integration_info: dict[str, Any],
    layers: list[Literal["bronze", "silver", "gold"]],
    mode: Literal["full", "incremental"],
    lookback_days: int,
) -> None:
    """Process a single integration through specified layers."""
    logging.info(f"Processing integration: {integration_name} in {mode} mode")

    # Get integration-specific components
    extractor = integration_info.get("extractor")
    models = integration_info.get("models")

    if "bronze" in layers:
        logging.info(f"Running bronze layer for {integration_name}")
        if extractor:
            from datetime import datetime, timedelta

            import pyspark.sql.functions as F

            # Import incremental utilities if in incremental mode
            incremental_processor: IncrementalProcessor | None = None
            if mode == "incremental":
                incremental_processor = IncrementalProcessor(spark)

            # Process each entity based on mode
            if hasattr(extractor, "extract") and integration_name == "connectwise":
                # ConnectWise-specific extraction with conditions support
                endpoints = {
                    "Agreement": "/finance/agreements",
                    "TimeEntry": "/time/entries",
                    "ExpenseEntry": "/expense/entries",
                    "ProductItem": "/procurement/products",
                    "PostedInvoice": "/finance/invoices/posted",
                    "UnpostedInvoice": "/finance/invoices",
                }

                for entity_name, endpoint in endpoints.items():
                    try:
                        # Build extraction conditions for incremental mode
                        conditions = None
                        if mode == "incremental":
                            entity_lookback = get_incremental_lookback_days(
                                entity_name, lookback_days
                            )
                            since_date = (
                                datetime.now() - timedelta(days=entity_lookback)
                            ).strftime("%Y-%m-%d")
                            conditions = build_incremental_conditions(entity_name, since_date)

                            if conditions:
                                logging.info(
                                    f"Incremental extraction for {entity_name} with conditions: {conditions}"
                                )

                        # Extract data with conditions
                        bronze_df = extractor.extract(
                            endpoint=endpoint, conditions=conditions, page_size=1000
                        )

                        # Add ETL metadata - use existing column names for compatibility
                        bronze_df = bronze_df.withColumn("etlTimestamp", F.current_timestamp())
                        bronze_df = bronze_df.withColumn("etlEntity", F.lit(entity_name))

                        record_count = bronze_df.count()
                        if record_count == 0:
                            logging.info(f"No new records for {entity_name}")
                            continue

                        # Get table name from config
                        table_name = config.get_table_name(
                            "bronze",
                            integration_name,
                            entity_name.lower()
                        )

                        # Write based on mode
                        if mode == "incremental" and spark.catalog.tableExists(table_name) and incremental_processor is not None:
                            # Use MERGE for incremental
                            merged, total = incremental_processor.merge_bronze_incremental(
                                bronze_df, table_name
                            )
                            logging.info(
                                f"Merged {merged} records into {table_name} (total: {total})"
                            )
                        else:
                            # Full overwrite
                            bronze_df.write.mode("overwrite").saveAsTable(table_name)
                            logging.info(f"Stored {record_count} records in {table_name}")

                    except Exception as e:
                        raise ETLProcessingError(
                            f"Failed to process {entity_name}",
                            code=ErrorCode.BRONZE_EXTRACTION_ERROR,
                            details={"entity": entity_name, "error": str(e)}
                        ) from e

            else:
                # Fallback to extract_all for other integrations
                bronze_data = extractor.extract_all()

                # Store each entity in separate bronze table
                for entity_name, raw_data in bronze_data.items():
                    if raw_data:
                        bronze_df = spark.createDataFrame(raw_data)

                        # Get table name from config
                        table_name = config.get_table_name(
                            "bronze",
                            integration_name,
                            entity_name.lower()
                        )

                        bronze_df.write.mode("overwrite").saveAsTable(table_name)
                        logging.info(f"Stored {len(raw_data)} records in {table_name}")

    if "silver" in layers:
        logging.info(f"Running silver layer for {integration_name}")
        if models:
            try:
                # Import incremental utilities if in incremental mode
                incremental_processor = None
                if mode == "incremental":
                    incremental_processor = IncrementalProcessor(spark)

                # Silver: Validate and transform each entity
                for entity_name, model_class in models.items():
                    # Get table names from config
                    bronze_table = config.get_table_name(
                        "bronze",
                        integration_name,
                        entity_name.lower()
                    )
                    silver_table = config.get_table_name(
                        "silver",
                        integration_name,
                        entity_name.lower()
                    )

                    try:
                        # Get data based on mode
                        if mode == "incremental" and incremental_processor:
                            # Get only changed records from Bronze
                            bronze_df = incremental_processor.get_changed_records(
                                source_table=bronze_table, target_table=silver_table
                            )
                        else:
                            # Full refresh - get all records
                            bronze_df = spark.table(bronze_table)

                        total_rows = bronze_df.count()
                        if total_rows == 0:
                            logging.info(f"No new records to process for {entity_name}")
                            continue

                        logging.info(f"Processing {total_rows} rows from {bronze_table}")

                        # Silver layer: Use proven flattening logic
                        from . import silver
                        from .connectwise_config import EntityConfig

                        # Get entity config from integration config
                        integration_config = config.integrations.get(integration_name)
                        if not integration_config:
                            raise ETLConfigError(
                                f"Integration '{integration_name}' not found in config",
                                code=ErrorCode.CONFIG_MISSING
                            )

                        # Check if we have entity-specific config from integration
                        integration_entity_configs = integration_info.get("entity_configs", {})
                        if entity_name.lower() in integration_entity_configs:
                            entity_config = integration_entity_configs[entity_name.lower()]
                        else:
                            # Check if we have entity-specific config in integration config (legacy)
                            entity_configs = getattr(integration_config, 'entity_configs', {})
                            if entity_name.lower() in entity_configs:
                                entity_config = entity_configs[entity_name.lower()]
                            else:
                                # Create default entity config
                                entity_config = EntityConfig(
                                    name=entity_name.lower(),
                                    source=integration_name,
                                    model_class_name=model_class.__name__,
                                    flatten_nested=True,
                                    flatten_max_depth=3,
                                    preserve_columns=[],
                                    json_columns=[],
                                    column_mappings={},
                                    scd=None,
                                    business_keys=["id"] if "id" in bronze_df.columns else [],
                                    add_audit_columns=True,
                                    strip_null_columns=True
                                )

                        try:
                            # This will flatten structs with camelCase naming
                            silver_df = silver.apply_silver_transformations(
                                bronze_df, entity_config, model_class
                            )
                            logging.info(
                                f"Silver transformation successful with {silver_df.columns.__len__()} columns"
                            )
                        except Exception as e:
                            logging.error(f"Silver transformation error: {e}")
                            # Fallback: just flatten without other transformations
                            try:
                                silver_df = silver.flatten_nested_columns(bronze_df, 3)
                                logging.info("Used direct flattening as fallback")
                            except Exception as e2:
                                logging.error(f"Flattening also failed: {e2}")
                                # Last resort: take bronze as-is
                                silver_df = bronze_df

                        # Write based on mode
                        if (
                            mode == "incremental"
                            and incremental_processor
                            and spark.catalog.tableExists(silver_table)
                        ):
                            # Get business keys from entity config
                            business_keys = entity_config.business_keys or ["id"]

                            # Use MERGE for incremental
                            processed_count = incremental_processor.merge_silver_scd1(
                                silver_df, silver_table, business_keys
                            )
                            logging.info(f"Merged {processed_count} records into {silver_table}")
                        else:
                            # Full overwrite
                            silver_df.write.mode("overwrite").option(
                                "mergeSchema", "true"
                            ).saveAsTable(silver_table)
                            logging.info(f"Processed {total_rows} records to {silver_table}")

                    except Exception as e:
                        logging.error(f"Silver processing failed for {entity_name}: {e}")
                        continue

            except Exception as e:
                raise ETLProcessingError(
                    f"Silver layer failed for {integration_name}",
                    code=ErrorCode.SILVER_TRANSFORMATION_ERROR,
                    details={"integration": integration_name, "error": str(e)}
                ) from e

    if "gold" in layers:
        logging.info(f"Running gold layer for {integration_name}")
        try:
            from . import facts

            # Create dimensions FIRST before facts (facts need dimensions to exist)
            if integration_name == "connectwise":
                try:
                    from .dimension_config import (
                        refresh_connectwise_dimensions,
                    )

                    logging.info("Creating ConnectWise dimensions from silver tables...")
                    refresh_connectwise_dimensions(spark)
                    logging.info("✅ ConnectWise dimensions created successfully")
                except Exception as e:
                    raise ETLProcessingError(
                        "Failed to create ConnectWise dimensions",
                        code=ErrorCode.DIMENSION_CREATION_ERROR,
                        details={"integration": "connectwise", "error": str(e)}
                    ) from e

            # Check for integration-specific transforms
            integration_transforms = None
            if integration_name == "connectwise":
                try:
                    from . import transforms as cw_transforms

                    integration_transforms = cw_transforms
                    logging.info("Using ConnectWise-specific transforms")

                except ImportError as e:
                    logging.warning(f"Could not import ConnectWise transforms: {e}")

            # Gold: Create fact tables using configuration
            integration_config = config.integrations.get(integration_name)
            if not integration_config:
                logging.warning(f"No integration config found for {integration_name}")
                return

            # Check if integration has fact configs
            fact_configs = getattr(integration_config, 'fact_configs', {})
            logging.info(f"Gold layer fact_configs: {list(fact_configs.keys())}")

            for fact_name, fact_config in fact_configs.items():
                logging.info(f"Processing gold fact: {fact_name} for integration: {integration_name}")
                try:
                    # Get table names from config
                    silver_table = config.get_table_name(
                        "silver",
                        integration_name,
                        fact_config.source_entity
                    )
                    gold_table = config.get_table_name(
                        "gold",
                        integration_name,
                        fact_name,
                        table_type="fact"
                    )

                    # Read silver data
                    silver_df = spark.table(silver_table)

                    # Check if integration has specific transform for this fact
                    gold_dfs = {}
                    transform_used = False

                    if integration_transforms:
                        # ConnectWise specific transforms - check if custom transform exists
                        if integration_name == "connectwise":
                            if fact_name == "agreement" and hasattr(
                                integration_transforms, "create_agreement_dimensions"
                            ):
                                logging.info("Using ConnectWise agreement-specific transforms")
                                gold_dfs = integration_transforms.create_agreement_dimensions(
                                    spark=spark, agreement_df=silver_df, config=fact_config
                                )
                                transform_used = True
                            elif fact_name == "invoice" and hasattr(
                                integration_transforms, "create_invoice_facts"
                            ):
                                # For ConnectWise, need to load time entries and products to create invoice lines
                                try:
                                    # Load time entries
                                    timeentry_table = config.get_table_name(
                                        "silver", integration_name, "timeentry"
                                    )
                                    timeentry_df = spark.table(timeentry_table)

                                    # Load products
                                    productitem_table = config.get_table_name(
                                        "silver", integration_name, "productitem"
                                    )
                                    productitem_df = spark.table(productitem_table)

                                    # Load agreements for hierarchy resolution
                                    agreement_table = config.get_table_name(
                                        "silver", integration_name, "agreement"
                                    )
                                    agreement_df = spark.table(agreement_table)

                                    logging.info(
                                        "Using ConnectWise invoice-specific transforms"
                                    )
                                    gold_dfs = integration_transforms.create_invoice_facts(
                                        spark=spark,
                                        invoice_df=silver_df,
                                        config=fact_config,
                                        timeEntryDf=timeentry_df,
                                        productItemDf=productitem_df,
                                        agreementDf=agreement_df,
                                    )
                                    transform_used = True
                                except Exception as e:
                                    logging.warning(
                                        f"Could not load related tables for specialized transform: {e}"
                                    )
                            elif fact_name == "timeentry" and hasattr(
                                integration_transforms, "create_time_entry_fact"
                            ):
                                logging.info("Using ConnectWise time entry-specific transforms")
                                # Load member data for cost enrichment if available
                                member_df = None
                                try:
                                    member_table = config.get_table_name(
                                        "silver", integration_name, "member"
                                    )
                                    if spark.catalog.tableExists(member_table):
                                        member_df = spark.table(member_table)
                                except Exception:
                                    logging.debug("Member table not available for enrichment")

                                # Load agreement data for hierarchy resolution
                                agreement_df = None
                                try:
                                    agreement_table = config.get_table_name(
                                        "silver", integration_name, "agreement"
                                    )
                                    if spark.catalog.tableExists(agreement_table):
                                        agreement_df = spark.table(agreement_table)
                                except Exception:
                                    logging.debug(
                                        "Agreement table not available for enrichment"
                                    )

                                gold_df = integration_transforms.create_time_entry_fact(
                                    spark=spark,
                                    time_entry_df=silver_df,
                                    member_df=member_df,
                                    agreement_df=agreement_df,
                                    config=fact_config,
                                )
                                gold_dfs = {"fact_timeentry": gold_df}
                                transform_used = True
                            elif fact_name == "expenseentry" and hasattr(
                                integration_transforms, "create_expense_entry_fact"
                            ):
                                logging.info(
                                    "Using ConnectWise expense entry-specific transforms"
                                )
                                # Load agreement data for hierarchy resolution
                                agreement_df = None
                                try:
                                    agreement_table = config.get_table_name(
                                        "silver", integration_name, "agreement"
                                    )
                                    if spark.catalog.tableExists(agreement_table):
                                        agreement_df = spark.table(agreement_table)
                                except Exception:
                                    logging.debug(
                                        "Agreement table not available for enrichment"
                                    )

                                gold_df = integration_transforms.create_expense_entry_fact(
                                    spark=spark,
                                    expense_df=silver_df,
                                    agreement_df=agreement_df,
                                    config=fact_config,
                                )
                                gold_dfs = {"fact_expenseentry": gold_df}
                                transform_used = True

                    # Fallback to generic transform if no specific transform was used
                    if not transform_used:
                        logging.info(f"Using generic fact table creation for {fact_name}")
                        gold_df = facts.create_generic_fact_table(
                            config=config,
                            fact_config=fact_config,
                            silver_df=silver_df,
                            spark=spark,
                        )
                        gold_dfs = {fact_name: gold_df}

                    # Write all generated fact tables
                    for table_name, gold_df in gold_dfs.items():
                        # Use config for table name if custom name not provided
                        if table_name == fact_name:
                            final_table_name = gold_table
                        else:
                            final_table_name = config.get_table_name(
                                "gold", integration_name, table_name, table_type="fact"
                            )

                        gold_df.write.mode("overwrite").option(
                            "mergeSchema", "true"
                        ).saveAsTable(final_table_name)
                        logging.info(f"Created fact table {final_table_name}")

                except Exception as e:
                    logging.error(f"Gold processing failed for {fact_name}: {e}")
                    continue

            # Create dimensions from gold calculated columns after facts are created
            if integration_name == "connectwise":
                try:
                    from .dimensions import create_dimension_from_column

                    logging.info("Creating dimensions from gold calculated columns...")

                    # LineType dimension from invoice lines
                    invoice_line_table = config.get_table_name(
                        "gold", integration_name, "invoice_line", table_type="fact"
                    )
                    if spark.catalog.tableExists(invoice_line_table):
                        from .connectwise_config import DimensionConfig, DimensionType

                        line_type_dim_config = DimensionConfig(
                            name="line_type",
                            type=DimensionType.STANDARD,
                            source_table=invoice_line_table,
                            source_column="LineType",
                            natural_key_column="LineType",
                            surrogate_key_column="LineTypeKey",
                            description_column="LineTypeDescription",
                            additional_columns=[],
                            add_unknown_member=True,
                            unknown_member_key=-1,
                            unknown_member_description="Unknown Line Type",
                            add_audit_columns=True
                        )

                        line_type_dim = create_dimension_from_column(
                            config=config,
                            dimension_config=line_type_dim_config,
                            spark=spark,
                        )

                        dim_table_name = config.get_table_name(
                            "gold", "shared", "line_type", table_type="dim"
                        )
                        line_type_dim.write.mode("overwrite").format("delta").saveAsTable(dim_table_name)
                        logging.info(f"Created {dim_table_name}: {line_type_dim.count()} rows")
                    else:
                        logging.warning(f"{invoice_line_table} table not found, skipping LineType dimension")
                except Exception as e:
                    logging.warning(f"Could not create calculated dimensions: {e}")

        except Exception as e:
            raise ETLProcessingError(
                f"Gold layer failed for {integration_name}",
                code=ErrorCode.GOLD_FACT_FAILED,
                details={"integration": integration_name, "error": str(e)}
            ) from e


if __name__ == "__main__":
    # Example usage - all parameters are REQUIRED
    # from .config.models import ETLConfig
    # from pyspark.sql import SparkSession
    #
    # config = ETLConfig(
    #     lakehouse_name="MyLakehouse",
    #     database_name="MyDatabase",
    #     layers={"bronze": LayerConfig(schema="bronze"), "silver": LayerConfig(schema="silver"), "gold": LayerConfig(schema="gold")},
    #     integrations={}
    # )
    # spark = SparkSession.builder.appName("ETL-Pipeline").getOrCreate()
    #
    # run_etl_pipeline(
    #     config=config,
    #     spark=spark,
    #     integrations=["connectwise"],
    #     layers=["silver", "gold"],
    #     mode="full",
    #     lookback_days=30
    # )

    raise ETLConfigError(
        "All parameters are required. See example usage above.",
        code=ErrorCode.CONFIG_MISSING
    )
