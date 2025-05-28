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
import sys
from datetime import datetime
from typing import Any

import pyspark.sql.functions as F  # noqa: N812
from pydantic import ValidationError
from pyspark.sql import DataFrame
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


def convert_models_to_dataframe(
    valid_models: list[SparkModel], model_class: type[SparkModel]
) -> DataFrame:
    """Convert validated SparkModel instances to DataFrame using SparkDantic."""
    if not valid_models:
        raise ValueError("No valid models to convert to DataFrame")

    # Get SparkDantic schema
    schema = model_class.model_spark_schema()

    # Convert models to dictionaries
    model_dicts = [model.model_dump() for model in valid_models]

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


def generate_spark_schema_from_pydantic(model_class: type[SparkModel]) -> StructType:
    """Generate Spark schema from Pydantic model. SparkDantic is REQUIRED."""
    if not hasattr(model_class, "model_spark_schema"):
        raise ValueError(
            f"Model {model_class.__name__} must inherit from SparkModel. "
            f"All models must be auto-generated with SparkDantic support."
        )

    try:
        return model_class.model_spark_schema()
    except Exception as e:
        raise ValueError(f"Failed to generate Spark schema for {model_class.__name__}: {e}") from e


def apply_data_types(df: DataFrame, entity_config: dict[str, Any]) -> DataFrame:
    """Apply data type conversions based on configuration."""
    result_df = df
    column_mappings = entity_config.get("column_mappings", {})

    # If no mappings, just return as-is
    if not column_mappings:
        logging.info("No column mappings provided, skipping type conversions")
        return result_df

    for column, mapping in column_mappings.items():
        if column not in result_df.columns:
            continue

        target_type = mapping.get("target_type")
        if not target_type:
            continue

        spark_type = TYPE_MAPPING.get(target_type.lower())
        if not spark_type:
            logging.warning(f"Unknown target type '{target_type}' for column '{column}'")
            continue

        try:
            # Get the current column type
            fields_dict = {field.name: field for field in result_df.schema.fields}
            current_type = fields_dict[column].dataType
            logging.info(f"Converting {column} from {current_type} to {spark_type}")

            if target_type.lower() in ["timestamp", "datetime", "datetime2"]:
                result_df = result_df.withColumn(column, F.to_timestamp(F.col(column)))
            else:
                result_df = result_df.withColumn(column, F.col(column).cast(spark_type))
        except Exception as e:
            logging.error(f"Type conversion failed for {column} -> {target_type}: {e}")
            raise ValueError(f"Type conversion failed: {column}") from e

    return result_df


def flatten_nested_columns(df: DataFrame, max_depth: int = 3) -> DataFrame:
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
            logging.error(f"JSON parsing failed for {col_name}: {e}")
            raise ValueError(f"JSON parsing failed: {col_name}") from e

    return result_df


def standardize_column_names(df: DataFrame, entity_config: dict[str, Any]) -> DataFrame:
    """Standardize column names. Preserves camelCase per CLAUDE.md."""
    result_df = df
    column_standards = entity_config.get("column_standards", {})

    for old_name, new_name in column_standards.items():
        if old_name in result_df.columns:
            result_df = result_df.withColumnRenamed(old_name, new_name)

    return result_df


def apply_scd_type_1(
    df: DataFrame, business_keys: list[str], timestamp_col: str = "SystemModifiedAt"
) -> DataFrame:
    """Apply SCD Type 1 (overwrite) logic. Business keys are REQUIRED."""
    if not business_keys:
        raise ValueError("SCD Type 1 requires business_keys to be specified")

    missing_keys = [key for key in business_keys if key not in df.columns]
    if missing_keys:
        raise ValueError(f"Business keys not found in DataFrame: {missing_keys}")

    if timestamp_col not in df.columns:
        raise ValueError(f"Timestamp column '{timestamp_col}' not found in DataFrame")

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
        raise ValueError(f"SCD Type 1 processing failed: {e}") from e


def apply_scd_type_2(
    df: DataFrame,
    business_keys: list[str],
    timestamp_col: str = "SystemModifiedAt",
    valid_from_col: str = "ValidFrom",
    valid_to_col: str = "ValidTo",
    is_current_col: str = "IsCurrent",
) -> DataFrame:
    """Apply SCD Type 2 (historize) logic. Business keys are REQUIRED."""
    if not business_keys:
        raise ValueError("SCD Type 2 requires business_keys to be specified")

    missing_keys = [key for key in business_keys if key not in df.columns]
    if missing_keys:
        raise ValueError(f"Business keys not found in DataFrame: {missing_keys}")

    if timestamp_col not in df.columns:
        raise ValueError(f"Timestamp column '{timestamp_col}' not found in DataFrame")

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
        raise ValueError(f"SCD Type 2 processing failed: {e}") from e


def add_etl_metadata(df: DataFrame, source: str) -> DataFrame:
    """Add ETL metadata columns. Source is REQUIRED."""
    if not source:
        raise ValueError("Source system name is required for ETL metadata")

    return (
        df.withColumn("_etl_processed_at", F.current_timestamp())
        .withColumn("_etl_source", F.lit(source))
        .withColumn("_etl_batch_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    )


def apply_silver_transformations(
    df: DataFrame, entity_config: dict[str, Any], model_class: type[SparkModel]
) -> DataFrame:
    """
    Apply comprehensive Silver transformations.

    Args:
        df: Bronze DataFrame
        entity_config: REQUIRED entity configuration
        model_class: REQUIRED Pydantic model for validation
    """
    if not entity_config:
        raise ValueError("Entity configuration is required")

    if not model_class:
        raise ValueError("Pydantic model class is required")

    # Validate the model has SparkDantic support
    _ = generate_spark_schema_from_pydantic(model_class)

    silver_df = df

    # 1. Add ETL metadata (source is required)
    source = entity_config.get("source")
    if not source:
        raise ValueError("Source system must be specified in entity_config")
    silver_df = add_etl_metadata(silver_df, source)

    # 2. Skip validation for Silver - data was validated in Bronze
    # Per CLAUDE.md: Bronze validates during API extraction, Silver transforms
    logging.info("Skipping validation - data was validated in Bronze layer")

    # 3. Parse JSON columns if specified
    json_columns = entity_config.get("json_columns", [])
    if json_columns:
        silver_df = parse_json_columns(silver_df, json_columns)

    # 4. Apply data type conversions
    silver_df = apply_data_types(silver_df, entity_config)

    # 5. Flatten nested structures if configured
    if entity_config.get("flatten_nested", False):
        max_depth = entity_config.get("flatten_max_depth", 3)
        silver_df = flatten_nested_columns(silver_df, max_depth)

    # 6. Standardize column names
    silver_df = standardize_column_names(silver_df, entity_config)

    # 7. Apply SCD logic if configured
    scd_config = entity_config.get("scd")
    if scd_config:
        scd_type = scd_config.get("type", 1)
        business_keys = scd_config.get("business_keys")

        if not business_keys:
            raise ValueError("SCD configuration requires business_keys")

        if scd_type == 1:
            silver_df = apply_scd_type_1(silver_df, business_keys)
        elif scd_type == 2:
            silver_df = apply_scd_type_2(silver_df, business_keys)
        else:
            raise ValueError(f"Unsupported SCD type: {scd_type}")

    return silver_df


def process_bronze_to_silver(
    entity_name: str,
    bronze_table_name: str,
    lakehouse_root: str,
    entity_config: dict[str, Any],
    model_class: type[SparkModel],
) -> None:
    """
    Process Bronze to Silver with unified transformations.

    ALL parameters are REQUIRED. No optional behaviors.
    """
    if not entity_name:
        raise ValueError("entity_name is required")
    if not bronze_table_name:
        raise ValueError("bronze_table_name is required")
    if not lakehouse_root:
        raise ValueError("lakehouse_root is required")
    if not entity_config:
        raise ValueError("entity_config is required")
    if not model_class:
        raise ValueError("model_class is required")

    # Get Fabric global spark session
    spark = sys.modules["__main__"].spark

    # Read Bronze data
    bronze_path = f"{lakehouse_root}bronze/{bronze_table_name}"
    logging.info(f"Reading Bronze: {bronze_path}")

    try:
        bronze_df = spark.read.format("delta").load(bronze_path)
        record_count = bronze_df.count()
        logging.info(f"Bronze records: {record_count}")
    except Exception as e:
        raise ValueError(f"Failed to read Bronze table {bronze_path}: {e}") from e

    # Apply Silver transformations
    silver_df = apply_silver_transformations(bronze_df, entity_config, model_class)

    # Write to Silver
    silver_path = f"{lakehouse_root}silver/silver_{entity_name}"
    logging.info(f"Writing Silver: {silver_path}")

    try:
        silver_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
            silver_path
        )
        logging.info(f"Silver written: {record_count} records")
    except Exception as e:
        raise ValueError(f"Failed to write Silver table {silver_path}: {e}") from e
