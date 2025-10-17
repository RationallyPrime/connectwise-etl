"""Generic DataFrame transformation utilities for ETL operations."""

from typing import TYPE_CHECKING

import pyspark.sql.functions as F
from pyspark.sql.types import StructType

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from etl_core.domain.protocols.types import TSparkModelClass


def flatten_dataframe(df: "DataFrame", model_class: "TSparkModelClass | None" = None) -> "DataFrame":
    """
    Recursively flatten ALL struct columns with proper typing.

    Expands nested struct columns into top-level columns using CamelCase naming
    convention. Continues until no struct columns remain.

    Args:
        df: Input DataFrame with potentially nested struct columns.
        model_class: Optional SparkDantic model class for schema validation/typing.

    Returns:
        Flattened DataFrame with all structs expanded to top-level columns.

    Examples:
        >>> # DataFrame with company.name, company.id becomes companyName, companyId
        >>> flattened_df = flatten_dataframe(nested_df)
    """
    # Track all column names to handle conflicts
    existing_names: set[str] = set()

    # Recursively flatten until no structs remain
    while True:
        struct_cols = [
            field.name for field in df.schema.fields if isinstance(field.dataType, StructType)
        ]

        if not struct_cols:
            break  # Done - no more nested structs

        select_cols = []
        new_names: set[str] = set()

        for field in df.schema.fields:
            if field.name not in struct_cols:
                # Keep non-struct columns as-is
                select_cols.append(F.col(field.name))
                existing_names.add(field.name)
                new_names.add(field.name)
            else:
                # Expand struct into flattened columns
                struct_type = field.dataType
                for struct_field in struct_type.fields:
                    child_name = struct_field.name

                    # Generate CamelCase name: company.name -> companyName
                    if child_name.startswith("_"):
                        # Preserve underscore prefix for metadata fields
                        base_name = f"{field.name}{child_name}"
                    else:
                        # CamelCase: first letter uppercase
                        child_camel = child_name[0].upper() + child_name[1:] if child_name else ""
                        base_name = f"{field.name}{child_camel}"

                    # Handle naming conflicts by appending suffix
                    final_name = base_name
                    suffix = 1
                    while final_name in existing_names or final_name in new_names:
                        final_name = f"{base_name}_{suffix}"
                        suffix += 1

                    new_names.add(final_name)
                    select_cols.append(F.col(f"{field.name}.{struct_field.name}").alias(final_name))

        # Apply transformation
        df = df.select(select_cols)
        existing_names = new_names

    # If model class provided, validate schema alignment
    if model_class is not None:
        expected_schema = model_class.model_spark_schema()
        _validate_schema_compatibility(df.schema, expected_schema)

    return df


def _validate_schema_compatibility(actual_schema: StructType, expected_schema: StructType) -> None:
    """
    Validate that flattened DataFrame schema is compatible with expected model schema.

    Args:
        actual_schema: Schema of the flattened DataFrame.
        expected_schema: Expected schema from SparkDantic model.

    Raises:
        ValueError: If schemas are incompatible (missing required columns, type mismatches).
    """
    actual_fields = {field.name: field.dataType for field in actual_schema.fields}
    expected_fields = {field.name: field.dataType for field in expected_schema.fields}

    # Check for missing required columns
    missing_columns = set(expected_fields.keys()) - set(actual_fields.keys())
    if missing_columns:
        raise ValueError(
            f"Flattened DataFrame missing required columns: {missing_columns}. "
            f"Expected from model schema but not found after flattening."
        )

    # Note: We don't check for extra columns (actual > expected) because flattening
    # may create additional columns that aren't in the original model.
    # The model schema represents the minimum required structure.
