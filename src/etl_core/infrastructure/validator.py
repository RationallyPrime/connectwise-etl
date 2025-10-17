"""Generic data validator using Pydantic and SparkDantic."""

from typing import Any, Iterator

from pydantic import ValidationError as PydanticValidationError
from pyspark.sql import SparkSession

from etl_core.domain.protocols import TSparkModelClass, ValidationResult
from etl_core.utils.errors import ValidationError
from etl_core.utils.logging import get_logger

logger = get_logger(__name__)


class PydanticDataValidator:
    """
    Generic data validator implementing DataValidatorProtocol.

    Uses Pydantic for validation and SparkDantic for DataFrame creation.
    Reusable across all integrations - no integration-specific logic.
    """

    def validate_and_create_dataframe(
        self,
        raw_data: Iterator[dict[str, Any]],
        model_class: TSparkModelClass,
        spark: SparkSession,
        entity_name: str,
    ) -> ValidationResult:
        """
        Validate raw data using Pydantic and create Spark DataFrame.

        Args:
            raw_data: Iterator of raw dictionaries from the data source.
            model_class: SparkDantic model class for validation and schema.
            spark: Active Spark session.
            entity_name: Entity name for logging context.

        Returns:
            ValidationResult with DataFrame, counts, and optional invalid sample.

        Raises:
            ValidationError: If critical validation failure occurs.
        """
        validated_records = []
        invalid_records = []
        valid_count = 0
        invalid_count = 0

        logger.info(f"Validating {entity_name} records", entity=entity_name)

        for record in raw_data:
            try:
                # Validate using Pydantic model
                validated = model_class(**record)
                validated_records.append(validated.model_dump())
                valid_count += 1
            except PydanticValidationError as e:
                invalid_count += 1
                logger.warning(
                    f"Validation failed for {entity_name} record",
                    entity=entity_name,
                    errors=e.errors(),
                    record_sample=str(record)[:200],  # Truncate for logging
                )
                # Keep sample of invalid records (limit to 100)
                if len(invalid_records) < 100:
                    invalid_records.append(
                        {
                            "record": record,
                            "error": str(e),
                            "error_count": len(e.errors()),
                        }
                    )

        logger.info(
            f"Validation complete for {entity_name}",
            entity=entity_name,
            valid=valid_count,
            invalid=invalid_count,
        )

        # If no valid records, this might be a critical error
        if valid_count == 0 and invalid_count > 0:
            raise ValidationError(
                f"All {invalid_count} {entity_name} records failed validation",
                details={
                    "source": "PydanticDataValidator",
                    "operation": "validate_and_create_dataframe",
                    "entity_name": entity_name,
                    "invalid_count": invalid_count,
                },
            )

        # Create DataFrame using SparkDantic schema
        if not validated_records:
            # Return empty DataFrame with correct schema
            schema = model_class.model_spark_schema()
            dataframe = spark.createDataFrame([], schema)
        else:
            schema = model_class.model_spark_schema()
            dataframe = spark.createDataFrame(validated_records, schema)

        # Create invalid sample DataFrame if we have invalid records
        invalid_sample = None
        if invalid_records:
            try:
                # Create simple DataFrame with invalid record info
                invalid_sample = spark.createDataFrame(invalid_records)
            except Exception as e:
                logger.warning(
                    f"Could not create invalid sample DataFrame: {e}",
                    entity=entity_name,
                )

        return ValidationResult(
            dataframe=dataframe,
            valid_count=valid_count,
            invalid_count=invalid_count,
            invalid_sample=invalid_sample,
        )
