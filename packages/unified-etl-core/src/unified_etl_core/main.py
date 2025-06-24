"""Cross-integration ETL orchestration using dynamic integration detection."""

import logging
from typing import Any

from unified_etl_core.integrations import detect_available_integrations, list_available_integrations


def run_etl_pipeline(
    integrations: list[str] | None = None,
    layers: list[str] | None = None,
    config: dict[str, Any] | None = None,
    table_mappings: dict[str, dict[str, str]] | None = None,
    mode: str = "full",
    lookback_days: int = 30,
) -> None:
    """
    Run ETL pipeline across all available integrations.

    Args:
        integrations: List of integration names to process (default: all available)
        layers: List of layers to process (default: ["bronze", "silver", "gold"])
        config: Pipeline configuration
        table_mappings: Optional custom table name mappings by layer
            Example: {"bronze": {"agreement": "bronze_cw_agreement"}}
        mode: "full" for complete refresh or "incremental" for delta processing
        lookback_days: For incremental mode, how many days to look back (default: 30)
    """
    # Detect available integrations
    available_integrations = detect_available_integrations()
    integration_names = integrations or list_available_integrations()
    layers = layers or ["bronze", "silver", "gold"]

    if not integration_names:
        logging.warning("No integrations available! Install integration packages.")
        return

    logging.info(f"Running ETL pipeline for integrations: {integration_names}")
    logging.info(f"Processing layers: {layers}")
    logging.info(f"Mode: {mode}, Lookback days: {lookback_days}")

    for integration_name in integration_names:
        if not available_integrations.get(integration_name, {}).get("available"):
            logging.warning(f"Skipping {integration_name}: not available")
            continue

        try:
            process_integration(
                integration_name,
                available_integrations[integration_name],
                layers,
                config,
                table_mappings,
                mode,
                lookback_days,
            )
        except Exception as e:
            logging.error(f"Failed processing {integration_name}: {e}")
            continue


def process_integration(
    integration_name: str,
    integration_info: dict[str, Any],
    layers: list[str],
    config: dict[str, Any] | None = None,
    table_mappings: dict[str, dict[str, str]] | None = None,
    mode: str = "full",
    lookback_days: int = 30,
) -> None:
    """Process a single integration through specified layers."""
    logging.info(f"Processing integration: {integration_name} in {mode} mode")

    # Get integration-specific components
    extractor = integration_info.get("extractor")
    models = integration_info.get("models")

    if "bronze" in layers:
        logging.info(f"Running bronze layer for {integration_name}")
        if extractor:
            try:
                from datetime import datetime, timedelta
                from pyspark.sql import SparkSession
                import pyspark.sql.functions as F
                
                spark = SparkSession.getActiveSession()
                if not spark:
                    raise RuntimeError("No active Spark session found")
                
                # Import incremental utilities if in incremental mode
                incremental_processor = None
                if mode == "incremental":
                    from unified_etl_core.incremental import (
                        IncrementalProcessor, 
                        build_incremental_conditions,
                        get_incremental_lookback_days
                    )
                    incremental_processor = IncrementalProcessor(spark)
                
                # Process each entity based on mode
                if hasattr(extractor, 'extract') and integration_name == "connectwise":
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
                                entity_lookback = get_incremental_lookback_days(entity_name, lookback_days)
                                since_date = (datetime.now() - timedelta(days=entity_lookback)).strftime("%Y-%m-%d")
                                conditions = build_incremental_conditions(entity_name, since_date)
                                
                                if conditions:
                                    logging.info(f"Incremental extraction for {entity_name} with conditions: {conditions}")
                            
                            # Extract data with conditions
                            bronze_df = extractor.extract(
                                endpoint=endpoint,
                                conditions=conditions,
                                page_size=1000
                            )
                            
                            # Add ETL metadata
                            bronze_df = bronze_df.withColumn("_etl_timestamp", F.current_timestamp())
                            bronze_df = bronze_df.withColumn("_etl_source", F.lit(integration_name))
                            bronze_df = bronze_df.withColumn("_etl_batch_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
                            
                            record_count = bronze_df.count()
                            if record_count == 0:
                                logging.info(f"No new records for {entity_name}")
                                continue
                            
                            # Determine table name
                            if table_mappings and "bronze" in table_mappings:
                                table_name = table_mappings["bronze"].get(
                                    entity_name, f"bronze_cw_{entity_name.lower()}"
                                )
                            else:
                                table_name = f"bronze_cw_{entity_name.lower()}"
                            
                            # Add schema prefix if needed
                            if "." not in table_name:
                                table_name = f"Lakehouse.bronze.{table_name}"
                            
                            # Write based on mode
                            if mode == "incremental" and spark.catalog.tableExists(table_name):
                                # Use MERGE for incremental
                                merged, total = incremental_processor.merge_bronze_incremental(
                                    bronze_df, table_name
                                )
                                logging.info(f"Merged {merged} records into {table_name} (total: {total})")
                            else:
                                # Full overwrite
                                bronze_df.write.mode("overwrite").saveAsTable(table_name)
                                logging.info(f"Stored {record_count} records in {table_name}")
                                
                        except Exception as e:
                            logging.error(f"Failed to process {entity_name}: {e}")
                            continue
                            
                else:
                    # Fallback to extract_all for other integrations
                    bronze_data = extractor.extract_all()
                    
                    # Store each entity in separate bronze table
                    for entity_name, raw_data in bronze_data.items():
                        if raw_data:
                            bronze_df = spark.createDataFrame(raw_data)
                            # Use custom table mapping or default pattern
                            if table_mappings and "bronze" in table_mappings:
                                table_name = table_mappings["bronze"].get(
                                    entity_name, f"bronze_cw_{entity_name}"
                                )
                            else:
                                table_name = f"bronze_cw_{entity_name}"
                            # Write to proper location
                            if "." in table_name:
                                bronze_df.write.mode("overwrite").saveAsTable(table_name)
                            else:
                                bronze_df.write.mode("overwrite").saveAsTable(
                                    f"Lakehouse.bronze.{table_name}"
                                )
                            logging.info(f"Stored {len(raw_data)} records in {table_name}")
                            
            except Exception as e:
                logging.error(f"Bronze layer failed for {integration_name}: {e}")
                raise

    if "silver" in layers:
        logging.info(f"Running silver layer for {integration_name}")
        if models:
            try:
                from pyspark.sql import SparkSession

                spark = SparkSession.getActiveSession()
                if not spark:
                    raise RuntimeError("No active Spark session found")
                
                # Import incremental utilities if in incremental mode
                incremental_processor = None
                if mode == "incremental":
                    from unified_etl_core.incremental import IncrementalProcessor
                    incremental_processor = IncrementalProcessor(spark)

                # Silver: Validate and transform each entity
                for entity_name, model_class in models.items():
                    # Use custom table mapping or default pattern
                    if table_mappings and "bronze" in table_mappings:
                        bronze_table = table_mappings["bronze"].get(
                            entity_name, f"bronze_cw_{entity_name}"
                        )
                    else:
                        bronze_table = f"bronze_cw_{entity_name}"
                    
                    # Get Silver table name
                    if table_mappings and "silver" in table_mappings:
                        silver_table = table_mappings["silver"].get(
                            entity_name, f"silver_cw_{entity_name}"
                        )
                    else:
                        silver_table = f"silver_cw_{entity_name}"
                    
                    try:
                        # Add schema prefix if needed
                        if "." not in bronze_table:
                            bronze_table = f"Lakehouse.bronze.{bronze_table}"
                        if "." not in silver_table:
                            silver_table = f"Lakehouse.silver.{silver_table}"
                        
                        # Get data based on mode
                        if mode == "incremental" and incremental_processor:
                            # Get only changed records from Bronze
                            bronze_df = incremental_processor.get_changed_records(
                                source_table=bronze_table,
                                target_table=silver_table
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
                        from unified_etl_core import silver

                        # Apply the silver transformations with proper flattening
                        entity_config = {
                            "source": integration_name,
                            "flatten_nested": True,  # Enable flattening
                            "flatten_max_depth": 3,
                        }

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
                                silver_df = silver.flatten_nested_columns(bronze_df)
                                logging.info("Used direct flattening as fallback")
                            except Exception as e2:
                                logging.error(f"Flattening also failed: {e2}")
                                # Last resort: take bronze as-is
                                silver_df = bronze_df

                        # Write based on mode
                        if mode == "incremental" and incremental_processor and spark.catalog.tableExists(silver_table):
                            # Get business keys from config (default to 'id')
                            from unified_etl_connectwise.config import SILVER_CONFIG
                            business_keys = ["id"]  # default
                            if integration_name == "connectwise" and entity_name in SILVER_CONFIG.get("entities", {}):
                                business_keys = SILVER_CONFIG["entities"][entity_name].get("business_keys", ["id"])
                            
                            # Use MERGE for incremental
                            processed_count = incremental_processor.merge_silver_scd1(
                                silver_df, silver_table, business_keys
                            )
                            logging.info(f"Merged {processed_count} records into {silver_table}")
                        else:
                            # Full overwrite
                            silver_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(silver_table)
                            logging.info(f"Processed {total_rows} records to {silver_table}")

                    except Exception as e:
                        logging.error(f"Silver processing failed for {entity_name}: {e}")
                        continue

            except Exception as e:
                logging.error(f"Silver layer failed for {integration_name}: {e}")
                raise

    if "gold" in layers:
        logging.info(f"Running gold layer for {integration_name}")
        try:
            from pyspark.sql import SparkSession

            from unified_etl_core import facts

            spark = SparkSession.getActiveSession()
            if not spark:
                raise RuntimeError("No active Spark session found")

            # Check for integration-specific transforms
            integration_transforms = None
            if integration_name == "connectwise":
                try:
                    from unified_etl_connectwise import transforms as cw_transforms

                    integration_transforms = cw_transforms
                    logging.info("Using ConnectWise-specific transforms")
                except ImportError as e:
                    logging.warning(f"Could not import ConnectWise transforms: {e}")
            elif integration_name == "businesscentral":
                try:
                    from unified_etl_businesscentral import transforms as bc_transforms

                    integration_transforms = bc_transforms
                    logging.info("Using Business Central-specific transforms")
                except ImportError as e:
                    logging.warning(f"Could not import Business Central transforms: {e}")

            # Gold: Create fact tables using configuration
            entity_configs = config.get("entities", {}) if config else {}
            for entity_name, entity_config in entity_configs.items():
                if entity_config.get("source") == integration_name:
                    try:
                        # Use custom table mapping or default pattern
                        if table_mappings and "silver" in table_mappings:
                            silver_table = table_mappings["silver"].get(
                                entity_name, f"silver_cw_{entity_name}"
                            )
                        else:
                            silver_table = f"silver_cw_{entity_name}"
                        # Handle fully qualified table names
                        if "." in silver_table:
                            silver_df = spark.table(silver_table)
                        else:
                            # Try with Lakehouse.silver prefix if not fully qualified
                            try:
                                silver_df = spark.table(f"Lakehouse.silver.{silver_table}")
                            except:
                                # Fallback to just table name
                                silver_df = spark.table(silver_table)

                        # Check if integration has specific transform for this entity
                        gold_dfs = {}
                        transform_used = False

                        if integration_transforms:
                            # ConnectWise specific transforms
                            if integration_name == "connectwise":
                                if entity_name == "agreement" and hasattr(
                                    integration_transforms, "create_agreement_facts"
                                ):
                                    logging.info("Using ConnectWise agreement-specific transforms")
                                    gold_dfs = integration_transforms.create_agreement_facts(
                                        spark=spark, agreement_df=silver_df, config=entity_config
                                    )
                                    transform_used = True
                                elif entity_name == "invoice" and hasattr(
                                    integration_transforms, "create_invoice_facts"
                                ):
                                    # For ConnectWise, need to load time entries and products to create invoice lines
                                    try:
                                        # Load time entries
                                        timeentry_table = (
                                            table_mappings["silver"].get(
                                                "timeentry", "silver_cw_timeentry"
                                            )
                                            if table_mappings and "silver" in table_mappings
                                            else "silver_cw_timeentry"
                                        )
                                        if "." in timeentry_table:
                                            timeentry_df = spark.table(timeentry_table)
                                        else:
                                            try:
                                                timeentry_df = spark.table(
                                                    f"Lakehouse.silver.{timeentry_table}"
                                                )
                                            except:
                                                timeentry_df = spark.table(timeentry_table)

                                        # Load products
                                        productitem_table = (
                                            table_mappings["silver"].get(
                                                "productitem", "silver_cw_productitem"
                                            )
                                            if table_mappings and "silver" in table_mappings
                                            else "silver_cw_productitem"
                                        )
                                        if "." in productitem_table:
                                            productitem_df = spark.table(productitem_table)
                                        else:
                                            try:
                                                productitem_df = spark.table(
                                                    f"Lakehouse.silver.{productitem_table}"
                                                )
                                            except:
                                                productitem_df = spark.table(productitem_table)

                                        # Load agreements for hierarchy resolution
                                        agreement_table = (
                                            table_mappings["silver"].get(
                                                "agreement", "silver_cw_agreement"
                                            )
                                            if table_mappings and "silver" in table_mappings
                                            else "silver_cw_agreement"
                                        )
                                        if "." in agreement_table:
                                            agreement_df = spark.table(agreement_table)
                                        else:
                                            try:
                                                agreement_df = spark.table(
                                                    f"Lakehouse.silver.{agreement_table}"
                                                )
                                            except:
                                                agreement_df = spark.table(agreement_table)

                                        logging.info(
                                            "Using ConnectWise invoice-specific transforms"
                                        )
                                        gold_dfs = integration_transforms.create_invoice_facts(
                                            spark=spark,
                                            invoice_df=silver_df,
                                            config=entity_config,
                                            timeEntryDf=timeentry_df,
                                            productItemDf=productitem_df,
                                            agreementDf=agreement_df,
                                        )
                                        transform_used = True
                                    except Exception as e:
                                        logging.warning(
                                            f"Could not load related tables for specialized transform: {e}"
                                        )

                        # Fallback to generic transform if no specific transform was used
                        if not transform_used:
                            logging.info(f"Using generic fact table creation for {entity_name}")
                            gold_df = facts.create_generic_fact_table(
                                silver_df=silver_df,
                                entity_name=entity_name,
                                surrogate_keys=entity_config["surrogate_keys"],
                                business_keys=entity_config["business_keys"],
                                calculated_columns=entity_config["calculated_columns"],
                                source=integration_name,
                            )
                            gold_dfs = {f"fact_{entity_name}": gold_df}

                        # Write all generated fact tables
                        for fact_name, gold_df in gold_dfs.items():
                            # Use custom table mapping or default pattern
                            if table_mappings and "gold" in table_mappings:
                                gold_table = table_mappings["gold"].get(
                                    fact_name, f"gold_cw_{fact_name}"
                                )
                            else:
                                gold_table = f"gold_cw_{fact_name}"
                            # Write to proper location
                            if "." in gold_table:
                                gold_df.write.mode("overwrite").saveAsTable(gold_table)
                            else:
                                gold_df.write.mode("overwrite").saveAsTable(
                                    f"Lakehouse.gold.{gold_table}"
                                )
                            logging.info(f"Created fact table {gold_table}")

                    except Exception as e:
                        logging.error(f"Gold processing failed for {entity_name}: {e}")
                        continue

        except Exception as e:
            logging.error(f"Gold layer failed for {integration_name}: {e}")
            raise


if __name__ == "__main__":
    # Example: Run pipeline for all available integrations
    run_etl_pipeline()

    # Example: Run specific integrations and layers
    # run_etl_pipeline(integrations=["connectwise", "businesscentral"], layers=["silver", "gold"])
