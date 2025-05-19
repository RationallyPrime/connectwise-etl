"""
Enhanced PSA Pipeline with Schema-Aware Structure.

This version of the PSA pipeline is designed to work with the unified lakehouse
structure, using proper schema separation (bronze.psa, silver.psa, gold.psa).
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, cast

from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

from .client import ConnectWiseClient
from .core.config import ENTITY_CONFIG
from .core.spark_utils import get_spark_session
from .extract.generic import extract_entity
from .storage.fabric_delta import dataframe_from_models, write_to_delta
from .transform.dataframe_utils import flatten_all_nested_structures


logger = logging.getLogger(__name__)


class PSAPipeline:
    """Enhanced PSA pipeline with schema-aware medallion architecture."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        """Initialize the PSA pipeline."""
        self.spark = spark or get_spark_session()
        self.schemas = {
            "bronze": "bronze.psa",
            "silver": "silver.psa", 
            "gold": "gold.psa"
        }
        
        # Ensure schemas exist
        self._create_schemas()
    
    def _create_schemas(self):
        """Create necessary schemas if they don't exist."""
        for schema_name in self.schemas.values():
            try:
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                logger.info(f"Ensured schema exists: {schema_name}")
            except Exception as e:
                logger.error(f"Error creating schema {schema_name}: {e}")
    
    def process_entity_to_bronze(
        self,
        entity_name: str,
        client: Optional[ConnectWiseClient] = None,
        conditions: Optional[str] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None,
        mode: str = "append"
    ) -> Dict[str, Any]:
        """Process a single entity to bronze layer."""
        client = client or ConnectWiseClient()
        
        logger.info(f"Extracting {entity_name} data for bronze layer...")
        data_result = extract_entity(
            client=client,
            entity_name=entity_name,
            page_size=page_size,
            max_pages=max_pages,
            conditions=conditions,
            return_validated=True
        )
        
        valid_data, validation_errors = cast(Tuple[List[BaseModel], List[Dict[str, Any]]], data_result)
        
        total_records = len(valid_data) + len(validation_errors)
        if total_records == 0:
            logger.warning(f"No {entity_name} data extracted")
            return {"entity": entity_name, "extracted": 0, "bronze_rows": 0}
        
        # Create DataFrame from validated models
        if valid_data:
            raw_df = dataframe_from_models(valid_data, entity_name)
        else:
            error_df = self.spark.createDataFrame(validation_errors)
            raw_df = error_df
        
        # Add ETL metadata
        raw_df = raw_df.withColumn("etlTimestamp", lit(datetime.utcnow().isoformat()))
        raw_df = raw_df.withColumn("etlEntity", lit(entity_name))
        
        # Write to bronze.psa schema
        bronze_table = f"{self.schemas['bronze']}.{entity_name}"
        raw_df.write.mode(mode).saveAsTable(bronze_table)
        
        row_count = raw_df.count()
        logger.info(f"Wrote {row_count} rows to {bronze_table}")
        
        return {
            "entity": entity_name,
            "extracted": total_records,
            "bronze_rows": row_count,
            "bronze_table": bronze_table
        }
    
    def process_bronze_to_silver(
        self,
        entity_name: str,
        mode: str = "overwrite"
    ) -> Dict[str, Any]:
        """Process data from bronze to silver layer."""
        bronze_table = f"{self.schemas['bronze']}.{entity_name}"
        
        try:
            bronze_df = self.spark.table(bronze_table)
        except Exception as e:
            logger.error(f"Error reading bronze table {bronze_table}: {e}")
            return {"entity": entity_name, "status": "error", "error": str(e)}
        
        initial_count = bronze_df.count()
        logger.info(f"Found {initial_count} rows in {bronze_table}")
        
        # Apply transformations
        # 1. Flatten nested structures
        flattened_df = flatten_all_nested_structures(bronze_df)
        
        # 2. Add any entity-specific transformations
        silver_df = self._apply_entity_transformations(flattened_df, entity_name)
        
        # 3. Write to silver.psa schema
        silver_table = f"{self.schemas['silver']}.{entity_name}"
        silver_df.write.mode(mode).option("mergeSchema", "true").saveAsTable(silver_table)
        
        final_count = silver_df.count()
        logger.info(f"Wrote {final_count} rows to {silver_table}")
        
        return {
            "entity": entity_name,
            "status": "success",
            "bronze_rows": initial_count,
            "silver_rows": final_count,
            "silver_table": silver_table
        }
    
    def _apply_entity_transformations(self, df: DataFrame, entity_name: str) -> DataFrame:
        """Apply entity-specific transformations."""
        # Agreement specific transformations
        if entity_name == "Agreement":
            # Extract agreement number from custom fields if available
            if "customFields" in df.columns:
                try:
                    df = df.withColumn(
                        "agreementNumber",
                        col("customFields").getItem("agreementNumber")
                    )
                except:
                    pass
        
        # TimeEntry specific transformations
        elif entity_name == "TimeEntry":
            # Ensure invoice ID column naming consistency
            if "invoice_id" in df.columns and "invoiceId" not in df.columns:
                df = df.withColumn("invoiceId", col("invoice_id"))
        
        # PostedInvoice specific transformations
        elif entity_name == "PostedInvoice":
            # Add any invoice-specific logic here
            pass
        
        return df
    
    def process_silver_to_gold(self) -> Dict[str, Any]:
        """Process silver layer to create gold layer data marts."""
        from .gold.conformed_dimensions import run_conformed_dimensions
        from .gold.psa_financial_model import run_psa_financial_gold
        
        results = {}
        
        try:
            # Create conformed dimensions
            logger.info("Creating conformed dimensions...")
            dim_results = run_conformed_dimensions(
                silver_path=self.schemas['silver'],
                gold_path=self.schemas['gold'],
                spark=self.spark,
                include_bc_data=False  # PSA standalone mode
            )
            results["dimensions"] = dim_results
            
            # Create PSA financial model
            logger.info("Creating PSA financial model...")
            financial_results = run_psa_financial_gold(
                silver_path=self.schemas['silver'],
                gold_path=self.schemas['gold'],
                spark=self.spark
            )
            results["facts"] = financial_results
            
            results["status"] = "success"
            
        except Exception as e:
            logger.error(f"Error processing silver to gold: {e}")
            results["status"] = "error"
            results["error"] = str(e)
        
        return results
    
    def run_full_pipeline(
        self,
        entity_names: Optional[List[str]] = None,
        client: Optional[ConnectWiseClient] = None,
        conditions: Optional[str] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None,
        bronze_mode: str = "append",
        silver_mode: str = "overwrite",
        process_gold: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """Run the full PSA medallion pipeline."""
        client = client or ConnectWiseClient()
        
        if entity_names is None:
            entity_names = ["Agreement", "TimeEntry", "ExpenseEntry", "ProductItem", "PostedInvoice"]
        
        results = {}
        
        # Phase 1: Extract to bronze
        logger.info("Phase 1: Extracting to bronze layer")
        for entity_name in entity_names:
            logger.info(f"Processing {entity_name} to bronze...")
            bronze_result = self.process_entity_to_bronze(
                entity_name=entity_name,
                client=client,
                conditions=conditions,
                page_size=page_size,
                max_pages=max_pages,
                mode=bronze_mode
            )
            results[entity_name] = {"bronze": bronze_result}
        
        # Phase 2: Transform bronze to silver
        logger.info("Phase 2: Transforming bronze to silver")
        for entity_name in entity_names:
            if results[entity_name]["bronze"]["bronze_rows"] > 0:
                logger.info(f"Processing {entity_name} from bronze to silver...")
                silver_result = self.process_bronze_to_silver(
                    entity_name=entity_name,
                    mode=silver_mode
                )
                results[entity_name]["silver"] = silver_result
            else:
                results[entity_name]["silver"] = {"status": "skipped", "reason": "No data in bronze"}
        
        # Phase 3: Process gold layer
        if process_gold:
            logger.info("Phase 3: Processing gold layer")
            gold_results = self.process_silver_to_gold()
            results["gold"] = gold_results
        
        return results
    
    def run_daily_pipeline(
        self,
        entity_names: Optional[List[str]] = None,
        days_back: int = 1,
        client: Optional[ConnectWiseClient] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None,
        process_gold: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """Run daily incremental pipeline."""
        yesterday = datetime.now() - timedelta(days=days_back)
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        today = datetime.now()
        today_str = today.strftime("%Y-%m-%d")
        
        logger.info(f"Running daily pipeline for {yesterday_str}")
        
        # Build date-based condition
        condition = f"lastUpdated>=[{yesterday_str}] AND lastUpdated<[{today_str}]"
        
        return self.run_full_pipeline(
            entity_names=entity_names,
            client=client,
            conditions=condition,
            page_size=page_size,
            max_pages=max_pages,
            bronze_mode="append",
            silver_mode="overwrite",
            process_gold=process_gold
        )


# Example usage:
"""
from fabric_api.pipeline_psa_enhanced import PSAPipeline

# Initialize pipeline
pipeline = PSAPipeline(spark)

# Run full pipeline
results = pipeline.run_full_pipeline(
    entity_names=["Agreement", "TimeEntry"],
    process_gold=True
)

# Or run daily incremental
daily_results = pipeline.run_daily_pipeline(
    days_back=1,
    process_gold=True
)
"""