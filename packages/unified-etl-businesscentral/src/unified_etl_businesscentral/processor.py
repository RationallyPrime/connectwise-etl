"""
Business Central Bronze Layer Processor.

Processes BC2ADLS landed data with Pydantic validation and SparkDantic integration.
Integrates with the unified ETL framework's medallion architecture.
"""

from typing import Any

import structlog
from pydantic import ValidationError
from pyspark.sql import DataFrame, SparkSession
from sparkdantic import SparkModel
from unified_etl_core.utils.base import ErrorCode
from unified_etl_core.utils.decorators import with_etl_error_handling
from unified_etl_core.utils.exceptions import ETLConfigError, ETLProcessingError

logger = structlog.get_logger(__name__)


class BusinessCentralBronzeProcessor:
    """
    Process BC2ADLS landed data with Pydantic validation.

    Integrates BC2ADLS data with the unified ETL framework by:
    1. Reading BC2ADLS parquet/JSON files
    2. Applying Pydantic validation (Bronze layer responsibility)
    3. Creating SparkDantic DataFrames for Silver processing
    """

    def __init__(self, spark: SparkSession, catalog: str = "lakehouse"):
        """
        Initialize BC Bronze processor.

        Args:
            spark: REQUIRED SparkSession
            catalog: Catalog name where bronze tables exist (default: "lakehouse")
        """
        if not spark:
            raise ETLConfigError("SparkSession is required", code=ErrorCode.CONFIG_MISSING)
        if not catalog:
            raise ETLConfigError("catalog is required", code=ErrorCode.CONFIG_MISSING)

        self.spark = spark
        self.catalog = catalog

        logger.info("Initialized BC Bronze processor", catalog=self.catalog)

    @with_etl_error_handling(operation="process_entity")
    def process_entity(self, entity_name: str, model_class: type[SparkModel]) -> DataFrame:
        """
        Process a single BC entity with Pydantic validation.

        Args:
            entity_name: REQUIRED entity name (e.g., "Customer", "GLAccount")
            model_class: REQUIRED SparkModel class for validation

        Returns:
            Validated DataFrame with SparkDantic schema
        """
        if not entity_name:
            raise ETLConfigError("entity_name is required", code=ErrorCode.CONFIG_MISSING)
        if not model_class:
            raise ETLConfigError("model_class is required", code=ErrorCode.CONFIG_MISSING)
        if not hasattr(model_class, "model_spark_schema"):
            raise ETLConfigError(
                f"Model {model_class.__name__} must inherit from SparkModel. "
                f"All models must be auto-generated with SparkDantic support.",
                code=ErrorCode.SCHEMA_MISMATCH
            )

        logger.info("Processing BC entity", entity_name=entity_name)

        # Read from existing bronze table
        try:
            raw_df = self._read_bronze_table(entity_name)
        except Exception as e:
            raise ETLProcessingError(
                f"Failed to read bronze table for {entity_name}: {e!s}",
                code=ErrorCode.STORAGE_ACCESS_FAILED,
                details={"entity": entity_name, "catalog": self.catalog}
            )

        # Convert to Python objects for validation (BC datasets are typically small)
        try:
            raw_data = [row.asDict() for row in raw_df.collect()]
            logger.info("Read BC2ADLS data", entity_name=entity_name, record_count=len(raw_data))
        except Exception as e:
            raise ETLProcessingError(
                f"Failed to collect BC2ADLS data for {entity_name}: {e!s}",
                code=ErrorCode.BRONZE_EXTRACT_FAILED,
                details={"entity": entity_name, "error": str(e)}
            )

        # Pydantic validation (same pattern as ConnectWise Bronze layer)
        validated_data = []
        validation_errors = []

        for i, item in enumerate(raw_data):
            record_id = item.get("No", item.get("Id", f"Unknown-{i}"))
            try:
                model = model_class.model_validate(item)
                validated_data.append(model.model_dump())
            except ValidationError as e:
                validation_errors.append({
                    "record_id": record_id,
                    "entity": entity_name,
                    "errors": e.errors(),
                    "raw_data": item
                })
                logger.warning("Validation error for BC record",
                              entity_name=entity_name, record_id=record_id, error=str(e))

        # Log validation results
        total_records = len(raw_data)
        valid_records = len(validated_data)
        error_count = len(validation_errors)

        logger.info("Validation complete",
                   entity_name=entity_name,
                   valid_records=valid_records,
                   total_records=total_records,
                   error_count=error_count)

        if error_count > 0:
            # Log detailed errors but continue processing valid records
            logger.warning("Found validation errors",
                          entity_name=entity_name,
                          error_count=error_count)
            for error in validation_errors[:5]:  # Log first 5 errors
                logger.warning("Validation error details", **error)

        # Create DataFrame with SparkDantic schema
        try:
            schema = model_class.model_spark_schema()
            df = self.spark.createDataFrame(validated_data, schema=schema)

            logger.info("Created SparkDantic DataFrame",
                       entity_name=entity_name,
                       row_count=df.count(),
                       column_count=len(df.columns))
            return df

        except Exception as e:
            raise ETLProcessingError(
                f"Failed to create SparkDantic DataFrame for {entity_name}: {e!s}",
                code=ErrorCode.BRONZE_VALIDATION_FAILED,
                details={
                    "entity": entity_name,
                    "model_class": model_class.__name__,
                    "valid_records": len(validated_data),
                    "error": str(e)
                }
            )

    def _read_bronze_table(self, entity_name: str) -> DataFrame:
        """
        Read existing bronze table for a specific entity.

        BC bronze tables are already created by BC2ADLS like bronze.Customer18, bronze.Item27, etc.

        Args:
            entity_name: Entity name to read (e.g., "customer", "item")

        Returns:
            Raw DataFrame from bronze table
        """
        # Get all bronze tables to find the right one
        try:
            bronze_tables = [t.name for t in self.spark.catalog.listTables("bronze")]
            logger.info("Available bronze tables", count=len(bronze_tables), entity_name=entity_name)

            # Find the table that matches our entity (case-insensitive prefix match)
            matching_table = None
            entity_lower = entity_name.lower()

            for table in bronze_tables:
                if table.lower().startswith(entity_lower):
                    matching_table = table
                    break

            if not matching_table:
                # Extract available entity prefixes for debugging
                available_entities = set()
                for table in bronze_tables:
                    import re
                    entity_part = re.sub(r'\d+$', '', table).lower()
                    if entity_part:
                        available_entities.add(entity_part)

                raise ETLProcessingError(
                    f"No bronze table found for entity {entity_name}",
                    code=ErrorCode.STORAGE_ACCESS_FAILED,
                    details={
                        "entity": entity_name,
                        "bronze_tables": bronze_tables[:10],  # First 10 for debugging
                        "available_entities": sorted(available_entities)
                    }
                )

            full_table_name = f"{self.catalog}.bronze.{matching_table}"
            df = self.spark.table(full_table_name)
            record_count = df.count()

            logger.info("Read bronze table",
                       entity_name=entity_name,
                       table_name=full_table_name,
                       record_count=record_count)
            return df

        except Exception as e:
            raise ETLProcessingError(
                f"Failed to read bronze table for {entity_name}: {e!s}",
                code=ErrorCode.STORAGE_ACCESS_FAILED,
                details={
                    "entity": entity_name,
                    "catalog": self.catalog,
                    "error": str(e)
                }
            )

    @with_etl_error_handling(operation="process_all_entities")
    def process_all_entities(self, entities: dict[str, type[SparkModel]]) -> dict[str, DataFrame]:
        """
        Process multiple BC entities in batch.

        Args:
            entities: REQUIRED dict mapping entity names to SparkModel classes

        Returns:
            Dict mapping entity names to validated DataFrames
        """
        if not entities:
            raise ETLConfigError("entities dict is required", code=ErrorCode.CONFIG_MISSING)

        results = {}

        for entity_name, model_class in entities.items():
            try:
                df = self.process_entity(entity_name, model_class)
                results[entity_name] = df
            except Exception as e:
                logger.error("Failed to process entity", entity_name=entity_name, error=str(e))
                # Continue processing other entities
                continue

        logger.info("Processed entities",
                   processed_count=len(results),
                   total_count=len(entities))
        return results

    @with_etl_error_handling(operation="extract_all")
    def extract_all(self) -> dict[str, list[dict[str, Any]]]:
        """
        Extract all BC entities - main.py interface compatibility.

        This method provides compatibility with main.py's extract_all() pattern.
        Returns validated data as dict of entity names to list of records.

        Returns:
            Dict mapping entity names to lists of validated records
        """
        from . import SILVER_CONFIG, models

        results = {}

        # Process all entities that have both SILVER_CONFIG and model classes
        for entity_name, config in SILVER_CONFIG.items():
            model_class = models.get(config["model"].lower())
            if not model_class:
                logger.warning("No model class found for entity",
                              entity_name=entity_name,
                              model_name=config['model'])
                continue

            try:
                # Get validated DataFrame
                df = self.process_entity(entity_name, model_class)

                # Convert to list of dicts for main.py compatibility
                records = [row.asDict() for row in df.collect()]
                results[entity_name] = records

                logger.info("Extracted entity records",
                           entity_name=entity_name,
                           record_count=len(records))

            except Exception as e:
                logger.error("Failed to extract entity",
                           entity_name=entity_name,
                           error=str(e))
                results[entity_name] = []

        logger.info("BC extract_all complete",
                   entity_count=len(results))
        return results

    def list_available_entities(self) -> list[str]:
        """
        List all available entities in BC2ADLS path.

        Returns:
            List of entity directory names
        """
        try:
            # Use Spark to list directories in BC2ADLS path
            # This is a simple implementation - could be enhanced with more sophisticated discovery
            if not self.spark._jvm or not self.spark._jsc:
                logger.warning("Spark JVM not available for directory listing")
                return []

            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(  # type: ignore[reportOptionalCall]
                self.spark._jsc.hadoopConfiguration()  # type: ignore[reportOptionalCall]
            )
            path = self.spark._jvm.org.apache.hadoop.fs.Path(self.bc2adls_path)  # type: ignore[reportOptionalCall]

            if not fs.exists(path):
                logger.warning("BC2ADLS path does not exist", path=self.bc2adls_path)
                return []

            file_statuses = fs.listStatus(path)
            entities = []

            for status in file_statuses:
                if status.isDirectory():
                    dir_name = status.getPath().getName()
                    entities.append(dir_name)

            logger.info("Found entities in BC2ADLS",
                       entity_count=len(entities),
                       entities=entities)
            return sorted(entities)

        except Exception as e:
            logger.error("Failed to list BC2ADLS entities", error=str(e))
            return []
