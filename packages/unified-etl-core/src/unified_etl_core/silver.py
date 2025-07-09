"""
Unified Silver layer processing - configuration-driven Pydantic transformation.

Consolidates all Silver transformation capabilities:
- Pydantic validation (REQUIRED)
- Data type conversion
- Structure flattening
- Column standardization
- SCD processing

Following CLAUDE.md: Generic, configuration-driven, Pydantic+SparkDantic required.
No optional behaviors. No silent failures. Fail fast.
"""

import logging
from datetime import datetime
from typing import Any

import pyspark.sql.functions as F  # noqa: N812
from pydantic import ValidationError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window
from sparkdantic import SparkModel

from .config import ETLConfig, EntityConfig
from .utils.base import ErrorCode
from .utils.decorators import with_etl_error_handling
from .utils.exceptions import ETLConfigError, ETLProcessingError


@with_etl_error_handling(operation="validate_batch")
def validate_batch(
    data: list[dict[str, Any]], model_class: type[SparkModel]
) -> tuple[list[SparkModel], list[dict[str, Any]]]:
    """Validate batch of raw data against SparkModel."""
    valid_models: list[SparkModel] = []
    validation_errors: list[dict[str, Any]] = []

    for i, item in enumerate(data):
        record_id = item.get("id", f"Unknown-{i}")
        try:
            model = model_class.model_validate(item)
            valid_models.append(model)
        except ValidationError as e:
            validation_errors.append(
                {
                    "entity": model_class.__name__,
                    "raw_data_id": record_id,
                    "errors": e.errors(),
                    "raw_data": item,
                    "timestamp": datetime.now().isoformat(),
                }
            )

    return valid_models, validation_errors


@with_etl_error_handling(operation="convert_models_to_dataframe")
def convert_models_to_dataframe(
    valid_models: list[SparkModel], model_class: type[SparkModel]
) -> DataFrame:
    """Convert validated SparkModel instances to DataFrame using SparkDantic."""
    # Get SparkDantic schema
    schema = model_class.model_spark_schema()

    # Convert models to dictionaries
    model_dicts: list[dict[str, Any]] = [model.model_dump() for model in valid_models]

    # Create DataFrame with proper schema (using Fabric's global spark session)
    import sys

    spark = sys.modules["__main__"].spark
    return spark.createDataFrame(model_dicts, schema)


# Complete type mapping for data type conversion
TYPE_MAPPING = {
    "string": StringType(),
    "varchar": StringType(),
    "nvarchar": StringType(),
    "char": StringType(),
    "nchar": StringType(),
    "text": StringType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "bigint": IntegerType(),
    "float": DoubleType(),
    "double": DoubleType(),
    "real": DoubleType(),
    "decimal": DecimalType(18, 2),
    "numeric": DecimalType(18, 2),
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "bit": BooleanType(),
    "date": DateType(),
    "datetime": TimestampType(),
    "timestamp": TimestampType(),
}


@with_etl_error_handling(operation="validate_with_pydantic")
def validate_with_pydantic(
    df: DataFrame, model_class: type[SparkModel], sample_size: int = 1000
) -> tuple[int, int, list[dict[str, Any]]]:
    """Validate DataFrame against Pydantic model. Model is REQUIRED."""
    
    sample_data = df.limit(sample_size).toPandas().to_dict("records")
    valid_count = 0
    invalid_count = 0
    validation_errors = []

    for i, record in enumerate(sample_data):
        try:
            model_class(**record)
            valid_count += 1
        except ValidationError as e:
            invalid_count += 1
            validation_errors.append(
                {
                    "record_index": i,
                    "errors": [str(error) for error in e.errors()],
                    "record_sample": dict(list(record.items())[:5]),
                }
            )

    logging.info(f"Validation: {valid_count} valid, {invalid_count} invalid of {len(sample_data)}")
    return valid_count, invalid_count, validation_errors


@with_etl_error_handling(operation="generate_spark_schema")
def generate_spark_schema_from_pydantic(model_class: type[SparkModel]) -> StructType:
    """Generate Spark schema from Pydantic model. SparkDantic is REQUIRED."""
    if not hasattr(model_class, "model_spark_schema"):
        raise ETLConfigError(
            f"Model {model_class.__name__} must inherit from SparkModel. "
            f"All models must be auto-generated with SparkDantic support.",
            code=ErrorCode.SCHEMA_MISMATCH,
            details={"model": model_class.__name__, "source": "generate_spark_schema"}
        )

    try:
        return model_class.model_spark_schema()
    except Exception as e:
        raise ETLProcessingError(
            f"Failed to generate Spark schema for {model_class.__name__}: {e}",
            code=ErrorCode.SILVER_TRANSFORM_FAILED,
            details={"model": model_class.__name__, "error": str(e)}
        ) from e


@with_etl_error_handling(operation="apply_data_types")
def apply_data_types(df: DataFrame, entity_config: EntityConfig) -> DataFrame:
    """Apply data type conversions based on configuration."""
    # Validate config first
    entity_config.validate_config()
    
    result_df = df
    column_mappings = entity_config.column_mappings

    # Column mappings are REQUIRED - no defensive coding
    if not column_mappings:
        raise ETLConfigError(
            "Column mappings are required for data type conversion",
            code=ErrorCode.CONFIG_MISSING,
            details={"entity": entity_config.name}
        )

    for column_name, mapping in column_mappings.items():
        if mapping.source_column not in result_df.columns:
            raise ETLProcessingError(
                f"Source column '{mapping.source_column}' not found in DataFrame",
                code=ErrorCode.SILVER_TRANSFORM_FAILED,
                details={
                    "column": mapping.source_column,
                    "available_columns": result_df.columns
                }
            )

        spark_type = TYPE_MAPPING.get(mapping.target_type.value.lower())
        if not spark_type:
            raise ETLConfigError(
                f"Unknown target type '{mapping.target_type}' for column '{mapping.source_column}'",
                code=ErrorCode.CONFIG_INVALID,
                details={"column": mapping.source_column, "target_type": mapping.target_type}
            )

        # Get the current column type
        fields_dict = {field.name: field for field in result_df.schema.fields}
        current_type = fields_dict[mapping.source_column].dataType
        logging.info(f"Converting {mapping.source_column} from {current_type} to {spark_type}")

        if mapping.target_type.value.lower() in ["timestamp", "datetime", "datetime2"]:
            result_df = result_df.withColumn(
                mapping.target_column, 
                F.to_timestamp(F.col(mapping.source_column))
            )
        else:
            result_df = result_df.withColumn(
                mapping.target_column,
                F.col(mapping.source_column).cast(spark_type)
            )
        
        # Remove old column if renamed
        if mapping.source_column != mapping.target_column:
            result_df = result_df.drop(mapping.source_column)

    return result_df


@with_etl_error_handling(operation="flatten_nested_columns")
def flatten_nested_columns(df: DataFrame, max_depth: int) -> DataFrame:
    """Flatten nested columns (structs, maps, arrays) into separate columns."""
    # Return early for empty DataFrames
    if df.isEmpty():
        return df

    # Helper function to check if a column needs flattening
    def needs_flattening(field_dtype):
        return (
            isinstance(field_dtype, StructType)
            or (
                isinstance(field_dtype, ArrayType)
                and isinstance(field_dtype.elementType, StructType)
            )
            or isinstance(field_dtype, MapType)
        )

    # Check if there are nested structures that need flattening
    fields = df.schema.fields
    nested_cols = [field.name for field in fields if needs_flattening(field.dataType)]

    # If no nested columns or max depth reached, return the DataFrame as is
    if len(nested_cols) == 0 or max_depth <= 0:
        return df

    logging.info(f"Flattening {len(nested_cols)} nested columns: {nested_cols}")

    # Process struct columns
    struct_cols = [field.name for field in fields if isinstance(field.dataType, StructType)]
    expanded_cols = []
    generated_names = set()  # Track column names we've already generated

    # First, track all top-level column names that aren't being flattened
    top_level_names = {field.name for field in fields if field.name not in struct_cols}

    # Handle all columns
    for field in fields:
        if field.name not in struct_cols:
            # Non-struct columns - just keep them
            expanded_cols.append(F.col(field.name))
            generated_names.add(field.name)
        else:
            # For struct columns, flatten each field with camelCase naming
            if isinstance(field.dataType, StructType):
                for struct_field in field.dataType.fields:
                    child_name = struct_field.name

                    # Generate camelCase name for the flattened field
                    if child_name.startswith("_"):
                        base_name = f"{field.name}{child_name}"
                    else:
                        # CamelCase: parentField + ChildField (capitalize first letter)
                        child_camel = child_name[0].upper() + child_name[1:] if child_name else ""
                        base_name = f"{field.name}{child_camel}"

                    # Handle naming conflicts
                    final_name = base_name
                    suffix = 1
                    while final_name in generated_names or final_name in top_level_names:
                        final_name = f"{base_name}_{suffix}"
                        suffix += 1

                    generated_names.add(final_name)
                    expanded_cols.append(
                        F.col(f"{field.name}.{struct_field.name}").alias(final_name)
                    )

    # Create DataFrame with expanded columns
    expanded_df = df.select(expanded_cols)

    # Handle arrays and maps
    result_df = expanded_df
    for field in result_df.schema.fields:
        if isinstance(field.dataType, ArrayType):
            # Convert arrays to JSON strings
            result_df = result_df.withColumn(field.name, F.to_json(F.col(field.name)))
        elif isinstance(field.dataType, MapType):
            # Convert maps to JSON strings
            result_df = result_df.withColumn(field.name, F.to_json(F.col(field.name)))

    # Recursively apply flattening
    return flatten_nested_columns(result_df, max_depth - 1)


@with_etl_error_handling(operation="parse_json_columns")
def parse_json_columns(df: DataFrame, json_columns: list[str]) -> DataFrame:
    """Parse specified JSON string columns into structured data."""
    result_df = df

    for col_name in json_columns:
        if col_name not in result_df.columns:
            logging.warning(f"JSON column '{col_name}' not found in DataFrame")
            continue

        try:
            result_df = result_df.withColumn(
                f"{col_name}_parsed", F.from_json(F.col(col_name), "string")
            )
        except Exception as e:
            raise ETLProcessingError(
                f"JSON parsing failed for {col_name}: {e}",
                code=ErrorCode.SILVER_TRANSFORM_FAILED,
                details={"column": col_name, "error": str(e)}
            ) from e

    return result_df


@with_etl_error_handling(operation="standardize_column_names")
def standardize_column_names(df: DataFrame, entity_config: EntityConfig) -> DataFrame:
    """Standardize column names. Preserves camelCase per CLAUDE.md."""
    result_df = df
    # Use column mappings for renaming
    column_standards = {m.source_column: m.target_column for m in entity_config.column_mappings.values()}

    for old_name, new_name in column_standards.items():
        if old_name in result_df.columns:
            result_df = result_df.withColumnRenamed(old_name, new_name)

    return result_df


@with_etl_error_handling(operation="apply_scd_type_1")
def apply_scd_type_1(
    df: DataFrame, business_keys: list[str], timestamp_col: str
) -> DataFrame:
    """Apply SCD Type 1 (overwrite) logic. Business keys are REQUIRED."""
    if not business_keys:
        raise ETLConfigError(
            "SCD Type 1 requires business_keys to be specified",
            code=ErrorCode.CONFIG_MISSING,
            details={"source": "apply_scd_type_1", "operation": "validate_config"}
        )

    missing_keys = [key for key in business_keys if key not in df.columns]
    if missing_keys:
        raise ETLProcessingError(
            f"Business keys not found in DataFrame: {missing_keys}",
            code=ErrorCode.SILVER_SCD_FAILED,
            details={"missing_keys": missing_keys, "available_columns": df.columns}
        )

    if timestamp_col not in df.columns:
        raise ETLProcessingError(
            f"Timestamp column '{timestamp_col}' not found in DataFrame",
            code=ErrorCode.SILVER_SCD_FAILED,
            details={"timestamp_col": timestamp_col, "available_columns": df.columns}
        )

    try:
        window = Window.partitionBy(*business_keys).orderBy(F.desc(timestamp_col))
        result_df = (
            df.withColumn("_row_num", F.row_number().over(window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

        logging.info(f"SCD Type 1: {df.count()} -> {result_df.count()} records")
        return result_df

    except Exception as e:
        raise ETLProcessingError(
            f"SCD Type 1 processing failed: {e}",
            code=ErrorCode.SILVER_SCD_FAILED,
            details={"business_keys": business_keys, "timestamp_col": timestamp_col, "error": str(e)}
        ) from e


@with_etl_error_handling(operation="apply_scd_type_2")
def apply_scd_type_2(
    df: DataFrame,
    business_keys: list[str],
    timestamp_col: str,
    valid_from_col: str = "ValidFrom",
    valid_to_col: str = "ValidTo",
    is_current_col: str = "IsCurrent",
) -> DataFrame:
    """Apply SCD Type 2 (historize) logic. Business keys are REQUIRED."""
    if not business_keys:
        raise ETLConfigError(
            "SCD Type 2 requires business_keys to be specified",
            code=ErrorCode.CONFIG_MISSING,
            details={"source": "apply_scd_type_2", "operation": "validate_config"}
        )

    missing_keys = [key for key in business_keys if key not in df.columns]
    if missing_keys:
        raise ETLProcessingError(
            f"Business keys not found in DataFrame: {missing_keys}",
            code=ErrorCode.SILVER_SCD_FAILED,
            details={"missing_keys": missing_keys, "available_columns": df.columns}
        )

    if timestamp_col not in df.columns:
        raise ETLProcessingError(
            f"Timestamp column '{timestamp_col}' not found in DataFrame",
            code=ErrorCode.SILVER_SCD_FAILED,
            details={"timestamp_col": timestamp_col, "available_columns": df.columns}
        )

    try:
        window = Window.partitionBy(*business_keys).orderBy(timestamp_col)
        result_df = (
            df.withColumn(valid_from_col, F.col(timestamp_col))
            .withColumn(valid_to_col, F.lead(F.col(timestamp_col)).over(window))
            .withColumn(
                is_current_col,
                F.when(F.col(valid_to_col).isNull(), F.lit(True)).otherwise(F.lit(False)),
            )
        )

        logging.info(f"SCD Type 2 applied to {result_df.count()} records")
        return result_df

    except Exception as e:
        raise ETLProcessingError(
            f"SCD Type 2 processing failed: {e}",
            code=ErrorCode.SILVER_SCD_FAILED,
            details={"business_keys": business_keys, "timestamp_col": timestamp_col, "error": str(e)}
        ) from e


@with_etl_error_handling(operation="add_etl_metadata")
def add_etl_metadata(df: DataFrame, source: str) -> DataFrame:
    """Add ETL metadata columns. Source is REQUIRED."""
    if not source:
        raise ETLConfigError(
            "Source system name is required for ETL metadata",
            code=ErrorCode.CONFIG_MISSING
        )

    return (
        df.withColumn("_etl_processed_at", F.current_timestamp())
        .withColumn("_etl_source", F.lit(source))
        .withColumn("_etl_batch_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    )


@with_etl_error_handling(operation="apply_silver_transformations")
def apply_silver_transformations(
    df: DataFrame, entity_config: EntityConfig, model_class: type[SparkModel]
) -> DataFrame:
    """
    Apply comprehensive Silver transformations.

    Args:
        df: Bronze DataFrame
        entity_config: REQUIRED entity configuration
        model_class: REQUIRED Pydantic model for validation
    """
    # Validate config first - FAIL FAST
    entity_config.validate_config()

    # Validate the model has SparkDantic support
    _ = generate_spark_schema_from_pydantic(model_class)

    silver_df = df

    # 1. Add ETL metadata (source is required)
    silver_df = add_etl_metadata(silver_df, entity_config.source)

    # 2. Skip validation for Silver - data was validated in Bronze
    # Per CLAUDE.md: Bronze validates during API extraction, Silver transforms
    logging.info("Skipping validation - data was validated in Bronze layer")

    # 3. Parse JSON columns if specified
    if entity_config.json_columns:
        silver_df = parse_json_columns(silver_df, entity_config.json_columns)

    # 4. Apply data type conversions
    silver_df = apply_data_types(silver_df, entity_config)

    # 5. Flatten nested structures if configured
    if entity_config.flatten_nested:
        silver_df = flatten_nested_columns(silver_df, entity_config.flatten_max_depth)

    # 6. Standardize column names
    silver_df = standardize_column_names(silver_df, entity_config)

    # 7. Apply SCD logic if configured
    if entity_config.scd:
        entity_config.scd.validate_business_keys()
        
        if entity_config.scd.type == 1:
            silver_df = apply_scd_type_1(
                silver_df, 
                entity_config.scd.business_keys,
                entity_config.scd.timestamp_column
            )
        elif entity_config.scd.type == 2:
            silver_df = apply_scd_type_2(
                silver_df,
                entity_config.scd.business_keys,
                entity_config.scd.timestamp_column
            )
        else:
            raise ETLConfigError(
                f"Unsupported SCD type: {entity_config.scd.type}",
                code=ErrorCode.CONFIG_INVALID
            )

    return silver_df


@with_etl_error_handling(operation="process_bronze_to_silver")
def process_bronze_to_silver(
    config: ETLConfig,
    entity_config: EntityConfig,
    model_class: type[SparkModel],
    spark: SparkSession,
) -> None:
    """
    Process Bronze to Silver with unified transformations.

    ALL parameters are REQUIRED. No optional behaviors.
    """
    # Validate configs - FAIL FAST
    entity_config.validate_config()

    # Get table names from config
    bronze_table = config.get_table_name("bronze", entity_config.source, entity_config.name)
    silver_table = config.get_table_name("silver", entity_config.source, entity_config.name)
    
    logging.info(f"Processing {entity_config.name}: {bronze_table} -> {silver_table}")

    # Read Bronze data
    bronze_df = spark.table(bronze_table)
    record_count = bronze_df.count()
    logging.info(f"Bronze records: {record_count}")

    # Apply Silver transformations
    silver_df = apply_silver_transformations(bronze_df, entity_config, model_class)

    # Write to Silver
    silver_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(silver_table)
    logging.info(f"Silver written: {record_count} records to {silver_table}")
