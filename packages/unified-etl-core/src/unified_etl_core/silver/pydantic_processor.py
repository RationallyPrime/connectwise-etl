"""Pydantic-based Silver layer processing that leverages auto-generated models."""
import logging
import sys
from typing import Dict, Any, List, Type, Optional
from datetime import datetime

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType
from pydantic import BaseModel, ValidationError

from unified_etl_core.storage.fabric_delta import write_to_delta
from unified_etl_core.utils.exceptions import ValidationError as ETLValidationError


def validate_with_pydantic(
    df: DataFrame, 
    model_class: Type[BaseModel],
    sample_size: int = 1000
) -> tuple[int, int, List[Dict[str, Any]]]:
    """
    Validate DataFrame against Pydantic model.
    
    Args:
        df: Input DataFrame
        model_class: Pydantic model class for validation
        sample_size: Number of records to validate (for performance)
        
    Returns:
        Tuple of (valid_count, invalid_count, validation_errors)
    """
    # Get sample data for validation
    sample_data = df.limit(sample_size).toPandas().to_dict('records')
    
    valid_count = 0
    invalid_count = 0
    validation_errors = []
    
    for i, record in enumerate(sample_data):
        try:
            # Validate record against Pydantic model
            model_class(**record)
            valid_count += 1
        except ValidationError as e:
            invalid_count += 1
            validation_errors.append({
                'record_index': i,
                'errors': [str(error) for error in e.errors()],
                'record_sample': {k: v for k, v in list(record.items())[:5]}  # First 5 fields
            })
    
    logging.info(f"Validation results: {valid_count} valid, {invalid_count} invalid out of {len(sample_data)} sampled")
    
    return valid_count, invalid_count, validation_errors


def generate_spark_schema_from_pydantic(model_class: Type[BaseModel]) -> StructType:
    """
    Generate Spark schema from Pydantic model using SparkDantic.
    
    Args:
        model_class: Pydantic model class (should inherit from SparkModel)
        
    Returns:
        Spark StructType schema
    """
    try:
        # Use SparkDantic's auto-generation if available
        if hasattr(model_class, 'model_spark_schema'):
            return model_class.model_spark_schema()
        else:
            raise ETLValidationError(f"Model {model_class.__name__} does not support Spark schema generation")
    except Exception as e:
        logging.error(f"Failed to generate Spark schema for {model_class.__name__}: {e}")
        raise


def apply_silver_transformations(
    df: DataFrame,
    entity_config: Dict[str, Any],
    model_class: Optional[Type[BaseModel]] = None
) -> DataFrame:
    """
    Apply Silver layer transformations following medallion architecture.
    
    Args:
        df: Bronze DataFrame
        entity_config: Configuration for this entity
        model_class: Optional Pydantic model for validation
        
    Returns:
        Transformed Silver DataFrame
    """
    silver_df = df
    
    # Add ETL metadata columns
    silver_df = (silver_df
                .withColumn("_etl_processed_at", F.current_timestamp())
                .withColumn("_etl_source", F.lit("connectwise"))
                .withColumn("_etl_batch_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S"))))
    
    # Apply data type conversions from config
    if 'column_mappings' in entity_config:
        for column, mapping in entity_config['column_mappings'].items():
            if column in silver_df.columns:
                target_type = mapping.get('target_type')
                if target_type == 'timestamp':
                    silver_df = silver_df.withColumn(
                        column, 
                        F.to_timestamp(F.col(column))
                    )
                elif target_type == 'double':
                    silver_df = silver_df.withColumn(
                        column,
                        F.col(column).cast('double')
                    )
                elif target_type == 'integer':
                    silver_df = silver_df.withColumn(
                        column,
                        F.col(column).cast('integer')
                    )
    
    # Flatten nested structures if configured
    if entity_config.get('flatten_nested', False):
        # This would expand nested JSON columns into flat structure
        # Implementation depends on specific nested structures
        pass
    
    # Preserve camelCase naming as per CLAUDE.md
    # No column name transformation needed since we preserve source conventions
    
    return silver_df


def process_bronze_to_silver_with_pydantic(
    entity_name: str,
    bronze_table_name: str,
    lakehouse_root: str,
    entity_config: Dict[str, Any],
    model_class: Optional[Type[BaseModel]] = None
) -> None:
    """
    Process Bronze to Silver using Pydantic validation and configuration.
    
    Args:
        entity_name: Name of entity
        bronze_table_name: Bronze table name
        lakehouse_root: Root path for tables
        entity_config: Entity-specific configuration
        model_class: Pydantic model for validation
    """
    # Get Fabric global spark session
    spark = sys.modules['__main__'].spark
    
    # Read Bronze data
    bronze_path = f"{lakehouse_root}bronze/{bronze_table_name}"
    logging.info(f"Reading Bronze data from: {bronze_path}")
    
    try:
        bronze_df = spark.read.format("delta").load(bronze_path)
        record_count = bronze_df.count()
        logging.info(f"Read {record_count} records from Bronze")
    except Exception as e:
        logging.error(f"Failed to read Bronze table {bronze_path}: {e}")
        raise
    
    # Validate with Pydantic model if provided
    if model_class:
        valid_count, invalid_count, errors = validate_with_pydantic(bronze_df, model_class)
        if invalid_count > 0:
            logging.warning(f"Found {invalid_count} validation errors in sample data")
            # Log first few errors for debugging
            for error in errors[:3]:
                logging.warning(f"Validation error: {error}")
    
    # Apply Silver transformations
    silver_df = apply_silver_transformations(bronze_df, entity_config, model_class)
    
    # Write to Silver using fabric_delta utility
    silver_path = f"{lakehouse_root}silver/silver_cw_{entity_name}"
    logging.info(f"Writing Silver data to: {silver_path}")
    
    try:
        write_to_delta(
            df=silver_df,
            table_path=silver_path,
            mode="overwrite",
            merge_schema=True
        )
        logging.info(f"Successfully wrote {record_count} records to Silver")
    except Exception as e:
        logging.error(f"Failed to write Silver table {silver_path}: {e}")
        raise