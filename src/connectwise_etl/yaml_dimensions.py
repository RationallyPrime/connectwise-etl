"""Clean YAML-based dimension creation - eliminates config monster."""

import logging
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

from .config.models import ETLConfig
from .facts import _add_etl_metadata
from .schema_loader import SchemaLoader
from .utils.base import ErrorCode
from .utils.decorators import with_etl_error_handling
from .utils.exceptions import ETLConfigError, ETLProcessingError

logger = logging.getLogger(__name__)


@with_etl_error_handling(operation="create_dimension_from_yaml")
def create_dimension_from_yaml(
    dimension_name: str,
    spark: SparkSession,
    schema_dir: Path | str | None = None,
) -> DataFrame:
    """Create a dimension table from YAML schema - simple and clean."""
    if not schema_dir:
        schema_dir = Path(__file__).parent.parent.parent / "configs"

    # Load YAML schemas
    loader = SchemaLoader(schema_dir)
    dimensions = loader.load_dimensions()

    if dimension_name not in dimensions:
        raise ETLConfigError(
            f"Dimension '{dimension_name}' not found in YAML config",
            code=ErrorCode.CONFIG_MISSING,
            details={"available": list(dimensions.keys())}
        )

    dim_schema = dimensions[dimension_name]

    # Read source data
    source_df = spark.table(dim_schema.source_table)
    natural_key = dim_schema.natural_key

    if natural_key not in source_df.columns:
        raise ETLProcessingError(
            f"Natural key '{natural_key}' not found in {dim_schema.source_table}",
            code=ErrorCode.GOLD_DIMENSION_ERROR,
            details={"available_columns": source_df.columns}
        )

    # Create dimension with distinct values and usage counts
    dim_df = (
        source_df
        .where(F.col(natural_key).isNotNull())
        .groupBy(natural_key)
        .agg(F.count("*").alias("usage_count"))
        .orderBy(F.desc("usage_count"))
    )

    # Add surrogate key (from YAML primary_key)
    window = Window.orderBy(F.desc("usage_count"))
    dim_df = dim_df.withColumn(
        dim_schema.primary_key,
        F.row_number().over(window)
    )

    # Add any other columns from the source that are defined in YAML schema
    available_columns = set(source_df.columns)
    for col_schema in dim_schema.columns:
        col_name = col_schema.name
        if col_name != dim_schema.primary_key and col_name != natural_key:
            if col_name in available_columns:
                # Add the column (take first non-null value)
                enriched = (
                    source_df
                    .where(F.col(col_name).isNotNull())
                    .groupBy(natural_key)
                    .agg(F.first(col_name).alias(col_name))
                )
                dim_df = dim_df.join(enriched, on=natural_key, how="left")

    # Add standard dimension attributes
    dim_df = (
        dim_df
        .withColumn("IsActive", F.lit(True))
        .withColumn("EffectiveDate", F.current_timestamp())
        .withColumn("EndDate", F.lit(None).cast("timestamp"))
    )

    # Add ETL metadata
    dim_df = _add_etl_metadata(dim_df, layer="gold", source="yaml_dimensions")

    return dim_df


@with_etl_error_handling(operation="create_all_dimensions_yaml")
def create_all_dimensions_yaml(
    spark: SparkSession,
    schema_dir: Path | str | None = None,
) -> dict[str, DataFrame]:
    """Create all dimensions from YAML - clean and simple."""
    if not schema_dir:
        schema_dir = Path(__file__).parent.parent.parent / "configs"

    # Load all dimension schemas
    loader = SchemaLoader(schema_dir)
    dimension_schemas = loader.load_dimensions()

    dimensions = {}

    for dim_name, dim_schema in dimension_schemas.items():
        logger.info(f"Creating dimension: {dim_name}")

        # Create dimension DataFrame
        dim_df = create_dimension_from_yaml(dim_name, spark, schema_dir)

        # Store dimension
        dimensions[dim_name] = dim_df

        # Write to table using name from YAML
        table_name = dim_schema.table_name
        dim_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

        logger.info(f"✅ Created {table_name} with {dim_df.count()} values")

    return dimensions


def get_dimension_mappings_from_yaml(
    fact_table_columns: list[str],
    schema_dir: Path | str | None = None,
) -> list[tuple[str, str, str]]:
    """Generate dimension mappings from YAML schemas and fact table columns.

    Returns list of (fact_column, dimension_table, surrogate_key_column) tuples.
    """
    if not schema_dir:
        schema_dir = Path(__file__).parent.parent.parent / "configs"

    loader = SchemaLoader(schema_dir)
    dimension_schemas = loader.load_dimensions()

    mappings = []

    for dim_name, dim_schema in dimension_schemas.items():
        natural_key = dim_schema.natural_key

        # Check if fact table has this dimension's natural key
        if natural_key in fact_table_columns:
            mappings.append((
                natural_key,                # fact column (e.g., "memberId")
                dim_schema.table_name,      # dimension table (e.g., "gold.dimmember")
                dim_schema.primary_key      # surrogate key (e.g., "MemberKey")
            ))

    return mappings