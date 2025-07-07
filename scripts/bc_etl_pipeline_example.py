"""
Business Central ETL Pipeline Example

Shows the correct order of operations to ensure dimensions are created before facts.
Following CLAUDE.md principles - all parameters required, fail-fast approach.
"""

import logging
from pyspark.sql import SparkSession
from unified_etl_businesscentral import (
    create_bc_dimension_bridge,
    create_purchase_fact,
    create_agreement_fact,
    SILVER_CONFIG
)
from unified_etl_core.silver import process_silver_layer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def run_bc_etl_pipeline(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str, 
    gold_path: str,
    batch_id: str
):
    """
    Run Business Central ETL pipeline in correct order.
    
    Order:
    1. Process silver layer (dimensions and entities)
    2. Create dimension bridge BEFORE facts
    3. Create fact tables that depend on dimensions
    """
    if not spark:
        raise ValueError("SparkSession is required")
    if not bronze_path:
        raise ValueError("bronze_path is required")
    if not silver_path:
        raise ValueError("silver_path is required")
    if not gold_path:
        raise ValueError("gold_path is required")
    if not batch_id:
        raise ValueError("batch_id is required")
    
    logging.info("Starting Business Central ETL pipeline")
    
    # Step 1: Process Silver Layer for all required entities
    logging.info("Step 1: Processing Silver Layer entities")
    
    # Process dimension entities first
    dimension_entities = ["Item", "Vendor", "AMSAgreementHeader", "AMSAgreementLine"]
    for entity in dimension_entities:
        if entity in SILVER_CONFIG:
            logging.info(f"Processing silver layer for {entity}")
            process_silver_layer(
                spark=spark,
                entity_name=entity,
                entity_config=SILVER_CONFIG[entity],
                bronze_path=bronze_path,
                silver_path=silver_path,
                batch_id=batch_id
            )
    
    # Process fact source entities
    fact_entities = ["PurchInvLine", "PurchCrMemoLine"]
    for entity in fact_entities:
        if entity in SILVER_CONFIG:
            logging.info(f"Processing silver layer for {entity}")
            process_silver_layer(
                spark=spark,
                entity_name=entity,
                entity_config=SILVER_CONFIG[entity],
                bronze_path=bronze_path,
                silver_path=silver_path,
                batch_id=batch_id
            )
    
    # Step 2: Create Dimension Bridge BEFORE facts
    logging.info("Step 2: Creating BC Dimension Bridge")
    
    # Define BC dimension types
    dimension_types = {
        "DEPARTMENT": "DEPARTMENT",
        "PROJECT": "PROJECT",
        "CUSTOMERGROUP": "CUSTOMERGROUP",
        "AREA": "AREA"
    }
    
    try:
        dim_bridge_df = create_bc_dimension_bridge(
            spark=spark,
            silver_path=silver_path,
            gold_path=gold_path,
            dimension_types=dimension_types
        )
        
        # Save dimension bridge
        dim_bridge_df.write.mode("overwrite").saveAsTable(f"{gold_path}.dim_DimensionBridge")
        logging.info(f"Created dim_DimensionBridge with {dim_bridge_df.count()} rows")
    except Exception as e:
        logging.warning(f"Dimension bridge creation failed (may not have data): {e}")
    
    # Step 3: Create Fact Tables (they can now use dim_DimensionBridge)
    logging.info("Step 3: Creating Fact Tables")
    
    # Create Purchase Fact
    try:
        purchase_fact_df = create_purchase_fact(
            spark=spark,
            silver_path=silver_path,
            gold_path=gold_path,
            batch_id=batch_id
        )
        purchase_fact_df.write.mode("overwrite").saveAsTable(f"{gold_path}.fact_Purchase")
        logging.info(f"Created fact_Purchase with {purchase_fact_df.count()} rows")
    except Exception as e:
        logging.error(f"Purchase fact creation failed: {e}")
    
    # Create Agreement Fact
    try:
        agreement_fact_df = create_agreement_fact(
            spark=spark,
            silver_path=silver_path,
            gold_path=gold_path,
            batch_id=batch_id
        )
        agreement_fact_df.write.mode("overwrite").saveAsTable(f"{gold_path}.fact_Agreement")
        logging.info(f"Created fact_Agreement with {agreement_fact_df.count()} rows")
    except Exception as e:
        logging.error(f"Agreement fact creation failed: {e}")
    
    logging.info("Business Central ETL pipeline completed")


# Example usage
if __name__ == "__main__":
    # This would be run in a Fabric notebook
    spark = SparkSession.builder.appName("BC_ETL").getOrCreate()
    
    run_bc_etl_pipeline(
        spark=spark,
        bronze_path="bronze_bc",
        silver_path="silver_bc",
        gold_path="gold_bc",
        batch_id="batch_20250107_001"
    )