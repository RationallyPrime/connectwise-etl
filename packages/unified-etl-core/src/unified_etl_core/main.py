"""Cross-integration ETL orchestration using dynamic integration detection."""

import logging
from typing import Any

from unified_etl_core.integrations import detect_available_integrations, list_available_integrations
from unified_etl_core import facts, gold, silver


def run_etl_pipeline(
    integrations: list[str] | None = None,
    layers: list[str] | None = None,
    config: dict[str, Any] | None = None,
) -> None:
    """
    Run ETL pipeline across all available integrations.
    
    Args:
        integrations: List of integration names to process (default: all available)
        layers: List of layers to process (default: ["bronze", "silver", "gold"])
        config: Pipeline configuration
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
    
    for integration_name in integration_names:
        if not available_integrations.get(integration_name, {}).get("available"):
            logging.warning(f"Skipping {integration_name}: not available")
            continue
            
        try:
            process_integration(integration_name, available_integrations[integration_name], layers, config)
        except Exception as e:
            logging.error(f"Failed processing {integration_name}: {e}")
            continue


def process_integration(
    integration_name: str, 
    integration_info: dict[str, Any], 
    layers: list[str],
    config: dict[str, Any] | None = None
) -> None:
    """Process a single integration through specified layers."""
    logging.info(f"Processing integration: {integration_name}")
    
    # Get integration-specific components
    extractor = integration_info.get("extractor")
    models = integration_info.get("models")
    
    if "bronze" in layers:
        logging.info(f"Running bronze layer for {integration_name}")
        if extractor:
            # Bronze: Extract raw data
            bronze_data = extractor.extract_all()
            # Store in bronze tables (using Fabric's global spark session)
            # spark.createDataFrame(bronze_data).write.saveAsTable(f"bronze_{integration_name}")
    
    if "silver" in layers:
        logging.info(f"Running silver layer for {integration_name}")
        if models:
            # Silver: Universal validation and transformation
            for entity_name, model_class in models.items():
                # bronze_df = spark.table(f"bronze_{integration_name}_{entity_name}")
                # valid_models, errors = silver.validate_batch(bronze_data, model_class)
                # silver_df = silver.convert_models_to_dataframe(valid_models, model_class)
                # silver_df.write.saveAsTable(f"silver_{integration_name}_{entity_name}")
                pass
    
    if "gold" in layers:
        logging.info(f"Running gold layer for {integration_name}")
        # Gold: Universal fact table creation
        entity_configs = config.get("entities", {}) if config else {}
        for entity_name, entity_config in entity_configs.items():
            if entity_config.get("source") == integration_name:
                # silver_df = spark.table(f"silver_{integration_name}_{entity_name}")
                # gold_df = facts.create_generic_fact_table(
                #     silver_df=silver_df,
                #     entity_name=entity_name,
                #     surrogate_keys=entity_config["surrogate_keys"],
                #     business_keys=entity_config["business_keys"],
                #     calculated_columns=entity_config["calculated_columns"],
                #     source=integration_name
                # )
                # gold_df.write.saveAsTable(f"gold_{integration_name}_{entity_name}")
                pass


if __name__ == "__main__":
    # Example: Run pipeline for all available integrations
    run_etl_pipeline()
    
    # Example: Run specific integrations and layers
    # run_etl_pipeline(integrations=["connectwise", "businesscentral"], layers=["silver", "gold"])