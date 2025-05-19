"""
Unified Lakehouse Orchestrator for PSA and BC Integration.

This orchestrator manages the complete medallion architecture for both PSA and BC data,
ensuring proper schema separation while enabling seamless integration.

Lakehouse Structure:
- bronze.psa.*   - Raw PSA data from ConnectWise API
- bronze.bc.*    - Raw BC data from Business Central API
- silver.psa.*   - Cleaned and validated PSA data
- silver.bc.*    - Cleaned and validated BC data
- gold.conformed.* - Shared dimensional data
- gold.psa.*     - PSA-specific facts and aggregations
- gold.bc.*      - BC-specific facts and aggregations
- gold.integrated.* - Cross-system integration views
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

from pyspark.sql import SparkSession

from .bc_medallion_pipeline import BCMedallionPipeline
from .pipeline import run_full_pipeline as run_psa_pipeline
from .pipeline.psa_gold_pipeline import run_psa_gold_pipeline
from .gold.conformed_dimensions import run_conformed_dimensions


logger = logging.getLogger(__name__)


class UnifiedLakehouseOrchestrator:
    """Orchestrates both PSA and BC data pipelines in a unified lakehouse."""
    
    def __init__(self, spark: SparkSession):
        """Initialize the orchestrator with a Spark session."""
        self.spark = spark
        self.bc_pipeline = BCMedallionPipeline(spark)
        
        # Schema prefixes for organized data structure
        self.schemas = {
            "bronze_psa": "bronze.psa",
            "bronze_bc": "bronze.bc",
            "silver_psa": "silver.psa",
            "silver_bc": "silver.bc",
            "gold_conformed": "gold.conformed",
            "gold_psa": "gold.psa",
            "gold_bc": "gold.bc",
            "gold_integrated": "gold.integrated"
        }
        
        # Create schemas if they don't exist
        self._create_schemas()
    
    def _create_schemas(self):
        """Create necessary database schemas if they don't exist."""
        for schema_name in set(self.schemas.values()):
            try:
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                logger.info(f"Ensured schema exists: {schema_name}")
            except Exception as e:
                logger.error(f"Error creating schema {schema_name}: {e}")
    
    def run_psa_bronze_to_silver(
        self,
        entities: Optional[List[str]] = None,
        incremental: bool = False,
        conditions: Optional[str] = None
    ) -> Dict[str, Any]:
        """Run PSA bronze to silver processing."""
        logger.info("Starting PSA Bronze to Silver processing")
        results = {}
        
        # If no entities specified, use all PSA entities
        if entities is None:
            entities = ["Agreement", "TimeEntry", "ExpenseEntry", "ProductItem", "PostedInvoice"]
        
        for entity in entities:
            try:
                # Read from bronze.psa schema
                bronze_table = f"{self.schemas['bronze_psa']}.{entity}"
                bronze_df = self.spark.table(bronze_table)
                
                # Apply transformations (flatten, validate, etc.)
                from .transform.dataframe_utils import flatten_all_nested_structures
                silver_df = flatten_all_nested_structures(bronze_df)
                
                # Write to silver.psa schema
                silver_table = f"{self.schemas['silver_psa']}.{entity}"
                mode = "append" if incremental else "overwrite"
                silver_df.write.mode(mode).saveAsTable(silver_table)
                
                results[entity] = {
                    "status": "success",
                    "bronze_rows": bronze_df.count(),
                    "silver_rows": silver_df.count()
                }
                logger.info(f"Processed {entity} from bronze to silver")
                
            except Exception as e:
                logger.error(f"Error processing {entity}: {e}")
                results[entity] = {"status": "error", "error": str(e)}
        
        return results
    
    def run_bc_bronze_to_silver(
        self,
        tables: Optional[List[str]] = None,
        incremental: bool = False
    ) -> Dict[str, Any]:
        """Run BC bronze to silver processing."""
        logger.info("Starting BC Bronze to Silver processing")
        
        # Get all BC tables from bronze if not specified
        if tables is None:
            bronze_tables = self.spark.sql(f"SHOW TABLES IN {self.schemas['bronze_bc']}").collect()
            tables = [t.tableName for t in bronze_tables]
        
        results = {}
        for table in tables:
            try:
                silver_df = self.bc_pipeline.bronze_to_silver(
                    table_name=table,
                    bronze_path=self.schemas['bronze_bc'],
                    incremental=incremental
                )
                
                if silver_df:
                    # Get clean table name (without numeric suffix)
                    clean_table_name = getattr(silver_df, '_table_name', table)
                    silver_table = f"{self.schemas['silver_bc']}.{clean_table_name}"
                    
                    mode = "append" if incremental else "overwrite"
                    silver_df.write.mode(mode).option("mergeSchema", "true").saveAsTable(silver_table)
                    
                    results[table] = {
                        "status": "success",
                        "silver_rows": silver_df.count()
                    }
                else:
                    results[table] = {"status": "skipped", "reason": "No data"}
                    
            except Exception as e:
                logger.error(f"Error processing BC table {table}: {e}")
                results[table] = {"status": "error", "error": str(e)}
        
        return results
    
    def run_conformed_dimensions(
        self,
        include_bc_data: bool = True
    ) -> Dict[str, int]:
        """Create conformed dimensions in gold layer."""
        logger.info("Creating conformed dimensions")
        
        # Run conformed dimension creation
        return run_conformed_dimensions(
            silver_path=self.schemas['silver_psa'],
            gold_path=self.schemas['gold_conformed'],
            spark=self.spark,
            include_bc_data=include_bc_data
        )
    
    def run_psa_gold_processing(self) -> Dict[str, Any]:
        """Run PSA-specific gold layer processing."""
        logger.info("Running PSA gold processing")
        
        # Run PSA gold pipeline with proper schema paths
        return run_psa_gold_pipeline(
            silver_path=self.schemas['silver_psa'],
            gold_path=self.schemas['gold_psa'],
            include_bc_integration=True,
            spark=self.spark
        )
    
    def run_bc_gold_processing(
        self,
        dimension_types: Optional[Dict[str, str]] = None,
        min_year: Optional[int] = None
    ) -> Dict[str, Any]:
        """Run BC-specific gold layer processing."""
        logger.info("Running BC gold processing")
        
        results = {}
        
        # Get silver tables
        silver_tables = self.spark.sql(f"SHOW TABLES IN {self.schemas['silver_bc']}").collect()
        dimension_tables = []
        fact_tables = []
        
        for table in silver_tables:
            table_name = table.tableName
            if table_name in self.bc_pipeline.dimension_tables:
                dimension_tables.append(table_name)
            elif table_name in self.bc_pipeline.fact_tables:
                fact_tables.append(table_name)
        
        # Process dimensions
        for dim_table in dimension_tables:
            try:
                gold_dim_df = self.bc_pipeline.silver_to_gold_dimension(
                    table_name=dim_table,
                    silver_path=self.schemas['silver_bc']
                )
                if gold_dim_df:
                    gold_table = f"{self.schemas['gold_bc']}.dim_{dim_table}"
                    gold_dim_df.write.mode("overwrite").saveAsTable(gold_table)
                    results[f"dim_{dim_table}"] = gold_dim_df.count()
            except Exception as e:
                logger.error(f"Error processing dimension {dim_table}: {e}")
                results[f"dim_{dim_table}"] = {"error": str(e)}
        
        # Create dimension bridge
        try:
            dimension_bridge = self.bc_pipeline.create_dimension_bridge(
                silver_path=self.schemas['silver_bc'],
                gold_path=self.schemas['gold_bc'],
                dimension_types=dimension_types
            )
            if dimension_bridge:
                bridge_table = f"{self.schemas['gold_bc']}.dim_DimensionBridge"
                dimension_bridge.write.mode("overwrite").partitionBy("$Company").saveAsTable(bridge_table)
                results["dim_DimensionBridge"] = dimension_bridge.count()
        except Exception as e:
            logger.error(f"Error creating dimension bridge: {e}")
            results["dim_DimensionBridge"] = {"error": str(e)}
        
        # Process facts
        for fact_table in fact_tables:
            try:
                gold_fact_df = self.bc_pipeline.silver_to_gold_fact(
                    table_name=fact_table,
                    silver_path=self.schemas['silver_bc'],
                    gold_path=self.schemas['gold_bc'],
                    min_year=min_year
                )
                if gold_fact_df:
                    gold_table = f"{self.schemas['gold_bc']}.fact_{fact_table}"
                    partition_cols = []
                    if "PostingYear" in gold_fact_df.columns:
                        partition_cols.append("PostingYear")
                    if "$Company" in gold_fact_df.columns:
                        partition_cols.append("$Company")
                    
                    write_op = gold_fact_df.write.mode("overwrite")
                    if partition_cols:
                        write_op = write_op.partitionBy(*partition_cols)
                    write_op.saveAsTable(gold_table)
                    results[f"fact_{fact_table}"] = gold_fact_df.count()
            except Exception as e:
                logger.error(f"Error processing fact {fact_table}: {e}")
                results[f"fact_{fact_table}"] = {"error": str(e)}
        
        return results
    
    def create_integrated_views(self) -> Dict[str, Any]:
        """Create integrated views that combine PSA and BC data."""
        logger.info("Creating integrated views")
        results = {}
        
        try:
            # Create unified financial view
            view_sql = f"""
            CREATE OR REPLACE VIEW {self.schemas['gold_integrated']}.vw_unified_financial AS
            SELECT 
                -- Common dimensions
                c.customer_key,
                c.customer_name,
                c.bc_customer_no,
                e.employee_key,
                e.employee_name,
                e.bc_resource_no,
                t.date_key,
                t.fiscal_year,
                t.fiscal_quarter,
                
                -- PSA metrics
                psa.line_type,
                psa.revenue as psa_revenue,
                psa.cost as psa_cost,
                psa.quantity as psa_quantity,
                
                -- BC metrics (when available)
                bc.Amount as bc_amount,
                bc.AccountCategory as bc_account_category,
                
                -- Source indicators
                'PSA' as source_system,
                psa.line_item_key as source_key
                
            FROM {self.schemas['gold_psa']}.fact_psa_line_items psa
            JOIN {self.schemas['gold_conformed']}.dim_customer c ON psa.customer_key = c.customer_key
            JOIN {self.schemas['gold_conformed']}.dim_employee e ON psa.employee_key = e.employee_key
            JOIN {self.schemas['gold_conformed']}.dim_time t ON psa.posting_date = t.date
            LEFT JOIN {self.schemas['gold_bc']}.fact_GLEntry bc ON bc.DocumentNo = psa.invoiceNumber
            """
            
            self.spark.sql(view_sql)
            results["vw_unified_financial"] = "created"
            
            # Create BC job ledger view from PSA data
            job_ledger_sql = f"""
            CREATE OR REPLACE VIEW {self.schemas['gold_integrated']}.vw_bc_job_ledger AS
            SELECT
                psa.line_item_key as EntryNo,
                aj.bc_job_no as JobNo,
                psa.posting_date as PostingDate,
                psa.invoiceNumber as DocumentNo,
                psa.bc_line_type as Type,
                COALESCE(e.bc_resource_no, e.psa_member_id) as No,
                psa.description as Description,
                psa.quantity as Quantity,
                psa.cost as DirectUnitCostLCY,
                psa.revenue as TotalPriceLCY,
                w.bc_work_type_code as WorkTypeCode,
                aj.agreement_number as ExternalDocumentNo,
                c.psa_company_id as Company
            FROM {self.schemas['gold_psa']}.fact_psa_line_items psa
            JOIN {self.schemas['gold_conformed']}.dim_agreement_job aj ON psa.agreement_id = aj.agreement_id
            JOIN {self.schemas['gold_conformed']}.dim_employee e ON psa.employee_key = e.employee_key
            JOIN {self.schemas['gold_conformed']}.dim_customer c ON psa.customer_key = c.customer_key
            LEFT JOIN {self.schemas['gold_conformed']}.dim_work_type w ON psa.work_type_key = w.work_type_key
            """
            
            self.spark.sql(job_ledger_sql)
            results["vw_bc_job_ledger"] = "created"
            
            logger.info("Successfully created integrated views")
            
        except Exception as e:
            logger.error(f"Error creating integrated views: {e}")
            results["error"] = str(e)
        
        return results
    
    def run_full_pipeline(
        self,
        run_psa: bool = True,
        run_bc: bool = True,
        create_integrated: bool = True,
        incremental: bool = False,
        psa_entities: Optional[List[str]] = None,
        bc_tables: Optional[List[str]] = None,
        min_year: Optional[int] = None
    ) -> Dict[str, Any]:
        """Run the complete unified pipeline."""
        results = {
            "start_time": datetime.now(),
            "psa": {},
            "bc": {},
            "conformed": {},
            "integrated": {}
        }
        
        try:
            # Phase 1: Bronze to Silver
            if run_psa:
                logger.info("Phase 1a: PSA Bronze to Silver")
                results["psa"]["silver"] = self.run_psa_bronze_to_silver(
                    entities=psa_entities,
                    incremental=incremental
                )
            
            if run_bc:
                logger.info("Phase 1b: BC Bronze to Silver")
                results["bc"]["silver"] = self.run_bc_bronze_to_silver(
                    tables=bc_tables,
                    incremental=incremental
                )
            
            # Phase 2: Create Conformed Dimensions
            logger.info("Phase 2: Creating Conformed Dimensions")
            results["conformed"] = self.run_conformed_dimensions(
                include_bc_data=run_bc
            )
            
            # Phase 3: Silver to Gold
            if run_psa:
                logger.info("Phase 3a: PSA Silver to Gold")
                results["psa"]["gold"] = self.run_psa_gold_processing()
            
            if run_bc:
                logger.info("Phase 3b: BC Silver to Gold")
                results["bc"]["gold"] = self.run_bc_gold_processing(min_year=min_year)
            
            # Phase 4: Create Integrated Views
            if create_integrated and run_psa and run_bc:
                logger.info("Phase 4: Creating Integrated Views")
                results["integrated"] = self.create_integrated_views()
            
            results["status"] = "SUCCESS"
            results["end_time"] = datetime.now()
            results["duration"] = (results["end_time"] - results["start_time"]).total_seconds()
            
            logger.info(f"Pipeline completed successfully in {results['duration']} seconds")
            
        except Exception as e:
            results["status"] = "FAILED"
            results["error"] = str(e)
            logger.error(f"Pipeline failed: {e}")
            raise
        
        return results


# Example usage in Microsoft Fabric notebook:
"""
from pyspark.sql import SparkSession
from fabric_api.unified_lakehouse_orchestrator import UnifiedLakehouseOrchestrator

# Initialize Spark session (already available in Fabric)
spark = spark

# Create orchestrator
orchestrator = UnifiedLakehouseOrchestrator(spark)

# Run full pipeline
results = orchestrator.run_full_pipeline(
    run_psa=True,
    run_bc=True,
    create_integrated=True,
    incremental=False,
    min_year=2023
)

# Or run individual components
psa_results = orchestrator.run_psa_bronze_to_silver()
bc_results = orchestrator.run_bc_bronze_to_silver()
conformed_results = orchestrator.run_conformed_dimensions()
"""