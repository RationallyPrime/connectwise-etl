"""Minimal pipeline orchestrator - configuration-driven, no magic."""
from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import SparkSession

from unified_etl_core.config.loader import load_config
from unified_etl_core.config.models import PipelineConfig
from unified_etl_core.storage.fabric_delta import read_delta_table, write_delta_table

logger = logging.getLogger(__name__)


class Pipeline:
    """Configuration-driven ETL pipeline orchestrator."""
    
    def __init__(
        self,
        config: PipelineConfig | None = None,
        config_path: str | Path | None = None,
        spark: SparkSession | None = None,
    ):
        """Initialize pipeline.
        
        Args:
            config: Pipeline configuration object
            config_path: Path to YAML configuration file
            spark: SparkSession instance
        """
        if config is None and config_path is None:
            raise ValueError("Either config or config_path must be provided")
            
        if config is not None:
            self.config = config
        else:
            # At this point, config_path cannot be None due to the check above
            assert config_path is not None
            self.config = load_config(config_path)
        self.spark = spark or self._get_spark()
        
    def _get_spark(self) -> SparkSession:
        """Get or create SparkSession."""
        return SparkSession.builder.appName("UnifiedETL").getOrCreate()
        
    def run_bronze(self, source: str, entity: str) -> None:
        """Run bronze layer extraction for a specific entity.
        
        Args:
            source: Source system name (e.g., "connectwise")
            entity: Entity name (e.g., "agreement")
        """
        entity_config = self.config.entities.get(entity)
        if not entity_config:
            raise ValueError(f"Entity {entity} not found in configuration")
            
        if entity_config.source != source:
            raise ValueError(f"Entity {entity} is not from source {source}")
            
        # Get the source-specific extractor
        if source == "connectwise":
            # Dynamic import to avoid hard dependency
            import importlib
            module = importlib.import_module("unified_etl_connectwise.extract")
            extractor_class = getattr(module, "ConnectWiseExtractor")
            extractor = extractor_class(self.config.sources[source])
        elif source == "business_central":
            # BC data comes from lakehouse, not API
            logger.info(f"Business Central data is already in lakehouse for {entity}")
            return
        else:
            raise ValueError(f"Unknown source: {source}")
            
        # Extract data
        logger.info(f"Extracting {entity} from {source}")
        data = extractor.extract(entity_config.endpoint)
        
        # Write to bronze
        bronze_path = f"{self.config.bronze_path}/{entity_config.bronze_table}"
        write_delta_table(
            df=data,
            path=bronze_path,
            mode="overwrite",
        )
        logger.info(f"Wrote {entity} to bronze: {bronze_path}")
        
    def run_silver(self, entity: str) -> None:
        """Run silver layer transformations for a specific entity.
        
        Args:
            entity: Entity name
        """
        entity_config = self.config.entities.get(entity)
        if not entity_config:
            raise ValueError(f"Entity {entity} not found in configuration")
            
        # Read from bronze
        bronze_path = f"{self.config.bronze_path}/{entity_config.bronze_table}"
        df = read_delta_table(self.spark, bronze_path)
        
        # Import the source-specific model for validation
        if entity_config.source == "connectwise":
            from unified_etl_connectwise.models import Agreement, Invoice, TimeEntry
            model_map = {
                "agreement": Agreement,
                "invoice": Invoice, 
                "time_entry": TimeEntry,
            }
            model_class = model_map.get(entity)
        else:
            # Business Central models would go here
            model_class = None
            
        # Apply silver transformations
        # 1. Parse JSON if needed
        # 2. Validate with Pydantic model
        # 3. Type conversion
        # 4. Flatten nested structures
        # 5. Strip suffixes/prefixes
        
        # For now, just pass through
        # TODO: Implement actual transformations
        
        # Write to silver
        silver_path = f"{self.config.silver_path}/{entity_config.silver_table}"
        write_delta_table(
            df=df,
            path=silver_path,
            mode="overwrite",
        )
        logger.info(f"Wrote {entity} to silver: {silver_path}")
        
    def run_gold(self, entity: str) -> None:
        """Run gold layer transformations for a specific entity.
        
        Args:
            entity: Entity name
        """
        entity_config = self.config.entities.get(entity)
        if not entity_config:
            raise ValueError(f"Entity {entity} not found in configuration")
            
        # Read from silver
        silver_path = f"{self.config.silver_path}/{entity_config.silver_table}"
        df = read_delta_table(self.spark, silver_path)
        
        # Apply gold transformations
        # This is where business logic lives
        # - Surrogate keys
        # - Dimensional modeling
        # - Business calculations
        
        # For now, just pass through
        # TODO: Implement actual transformations
        
        # Write to gold tables
        for table_name in entity_config.gold_tables:
            gold_path = f"{self.config.gold_path}/{table_name}"
            write_delta_table(
                df=df,
                path=gold_path,
                mode="overwrite",
            )
            logger.info(f"Wrote {entity} to gold: {gold_path}")
            
    def run(
        self,
        sources: list[str] | None = None,
        entities: list[str] | None = None,
        layers: list[str] | None = None,
    ) -> None:
        """Run the pipeline.
        
        Args:
            sources: Sources to process (default: all)
            entities: Entities to process (default: all)
            layers: Layers to run (bronze/silver/gold, default: all)
        """
        sources = sources or list(self.config.sources.keys())
        entities = entities or list(self.config.entities.keys())
        layers = layers or ["bronze", "silver", "gold"]
        
        for entity in entities:
            entity_config = self.config.entities[entity]
            
            if "bronze" in layers and entity_config.source in sources:
                self.run_bronze(entity_config.source, entity)
                
            if "silver" in layers:
                self.run_silver(entity)
                
            if "gold" in layers:
                self.run_gold(entity)