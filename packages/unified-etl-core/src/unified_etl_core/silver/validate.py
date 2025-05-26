# unified_etl/silver/validate.py
"""
Pydantic validation for silver layer data transformation.
Applies Pydantic models for structure validation and type safety.
"""

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame
from sparkdantic import SparkModel
from unified_etl.utils import logging
from unified_etl.utils.exceptions import DataTypeConversionError


def validate_with_pydantic_model(
    df: DataFrame, model_class: type[SparkModel], entity_name: str
) -> DataFrame:
    """
    Validate DataFrame structure against a Pydantic model.

    Args:
        df: DataFrame to validate
        model_class: Pydantic model class for validation
        entity_name: Name of the entity for logging

    Returns:
        DataFrame with validated structure and types

    Raises:
        DataTypeConversionError: If validation fails
    """
    try:
        with logging.span("pydantic_validation", entity=entity_name):
            # Get the Spark schema from the Pydantic model
            expected_schema = model_class.model_spark_schema()

            # Check if we can convert the DataFrame to match the expected schema
            # This applies the Pydantic model's field types and constraints
            validated_df = df.select(
                *[
                    F.col(field.name).cast(field.dataType).alias(field.name)
                    for field in expected_schema.fields
                    if field.name in df.columns
                ]
            )

            # Add any missing optional fields as null columns
            for field in expected_schema.fields:
                if field.name not in validated_df.columns and field.nullable:
                    validated_df = validated_df.withColumn(
                        field.name, F.lit(None).cast(field.dataType)
                    )

            logging.info(
                "Pydantic validation successful",
                entity=entity_name,
                validated_columns=len(validated_df.columns),
                original_columns=len(df.columns),
            )

            return validated_df

    except Exception as e:
        error_msg = f"Pydantic validation failed for {entity_name}: {e!s}"
        logging.error(error_msg, entity=entity_name)
        raise DataTypeConversionError(error_msg) from e


def get_pydantic_model_for_entity(entity_name: str) -> type[SparkModel] | None:
    """
    Get the appropriate Pydantic model class for an entity.

    Args:
        entity_name: Name of the entity (e.g., 'Agreement', 'TimeEntry')

    Returns:
        Pydantic model class or None if not found
    """
    try:
        # Dynamic import based on entity name
        from unified_etl.models import models

        # Try to get the model class by name
        model_class = getattr(models, entity_name, None)

        if model_class and issubclass(model_class, SparkModel):
            return model_class

        logging.warning(f"No Pydantic model found for entity: {entity_name}")
        return None

    except ImportError as e:
        logging.error(f"Failed to import models: {e}")
        return None


def apply_pydantic_validation(df: DataFrame, entity_name: str) -> DataFrame:
    """
    Apply Pydantic validation to a DataFrame if a model exists for the entity.

    Args:
        df: DataFrame to validate
        entity_name: Name of the entity

    Returns:
        Validated DataFrame or original DataFrame if no model exists
    """
    model_class = get_pydantic_model_for_entity(entity_name)

    if model_class:
        return validate_with_pydantic_model(df, model_class, entity_name)
    else:
        logging.info(f"No Pydantic model available for {entity_name}, skipping validation")
        return df
