from __future__ import annotations

import logging
import os
from typing import Any, Callable, TypeAlias

import pandas as pd

"""fabric_api.transform

Transform layer – turns validated Pydantic domain objects from *fabric_api.extract* into Spark
DataFrames and persists them as Delta tables inside the Microsoft Fabric lakehouse.

Public surface mirrors a DataSource-centric design: callers pass data as dictionary lists.
Internally we:

1. Convert data to pandas DataFrames
2. Apply business rules (skip/flag Tímapottur, etc.)
3. Upgrade to Spark DataFrames
4. Persist each entity as a managed Delta table under ``/lakehouse/default/Tables/connectwise``

All helpers are side‑effect‑free except ``_write_delta`` which performs I/O.
"""


try:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession
except ImportError as exc:  # noqa: D401 – raise helpful error
    raise RuntimeError(
        "pyspark is required for fabric_api.transform – install with "
        "`pip install pyspark` or include the 'fabric' extra."
    ) from exc



__all__: list[str] = [
    "transform_and_load",
    "TransformResult",
]

# ----------------------------------------------------------------------------
# Type aliases
# ----------------------------------------------------------------------------

TransformResult: TypeAlias = dict[str, str]  # entity → delta path

# ----------------------------------------------------------------------------
# Internal helpers
# ----------------------------------------------------------------------------


def _models_to_pandas(models: list[Any]) -> pd.DataFrame:
    """Serialise a list of Pydantic models into a *pandas* DataFrame."""
    if not models:
        return pd.DataFrame()

    return pd.DataFrame(data=[m.model_dump() if hasattr(m, 'model_dump') else m for m in models])


def _apply_common_rules(df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401
    """Rules shared by every entity (currently none)."""
    return df


def _apply_invoice_rules(df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401
    if df.empty:
        return df

    # Ensure VAT is stored as percentage (legacy AL rule)
    if "vat_percentage" in df.columns:
        df.loc[df["vat_percentage"] < 1, "vat_percentage"] *= 100

    return df


def _apply_agreement_rules(df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401
    """Apply data transformation rules to agreements dataframe."""
    if df.empty:
        return df

    # Make a copy to avoid modifying the original
    result = df.copy()

    # Filter out Tímapottur agreements if needed
    if "type" in result.columns:
        # Use DataFrame indexing to ensure we return a DataFrame, not a Series
        mask = ~result["type"].str.contains("Tímapottur", na=False)
        result = result.loc[mask, :]

    # Ensure all required columns exist
    required_columns = ["id", "name", "type", "parent_agreement_id"]
    for col in required_columns:
        if col not in result.columns:
            result[col] = None

    # Ensure id column is an integer
    if "id" in result.columns:
        result["id"] = pd.to_numeric(result["id"], errors="coerce")

    # Other agreement-specific transformations can be added here

    return result


def _sparkify(spark: SparkSession, pdf: pd.DataFrame) -> SparkDataFrame:  # noqa: D401
    """Convert a *pandas* DataFrame into Spark DataFrame preserving schema."""
    if pdf.empty:
        # Create a simple empty DataFrame with minimal schema
        return spark.createDataFrame([], "string")

    logger = logging.getLogger(__name__)

    # Handle specific known string columns that might be all-null
    # These are columns that we know should be strings based on the model definitions
    string_columns = [
        "invoice_number",
        "type",
        "name",
        "agreement_number",
        "identifier",
        "notes",
        "description",
    ]

    for col in string_columns:
        if col in pdf.columns:
            # Explicitly check if all values are null
            is_all_null = pdf[col].isnull().all()
            if is_all_null:
                logger.info(f"Explicitly converting all-null column {col} to empty strings")
                pdf[col] = pdf[col].fillna("")

    try:
        # Create DataFrame directly
        return spark.createDataFrame(pdf)
    except Exception as e:
        logger.warning(f"Error creating DataFrame directly: {str(e)}")
        
        # Fallback: Try with schema inference via JSON
        try:
            # Convert to JSON and back to infer schema properly
            pdf_json = pdf.to_json(orient="records")
            schema_df = spark.read.json(spark.sparkContext.parallelize([pdf_json]))

            # Create an RDD of Row objects
            pandas_df_collected = pdf.to_dict("records")
            rdd = spark.sparkContext.parallelize(pandas_df_collected)

            # Create DataFrame with JSON-inferred schema
            return spark.createDataFrame(rdd, schema_df.schema)
        except Exception as json_err:
            logger.error(f"Failed schema inference via JSON: {str(json_err)}")
            # Last resort - return empty DataFrame
            return spark.createDataFrame([], "string")


def _write_to_parquet(sdf: SparkDataFrame, path: str, *, mode: str = "overwrite") -> str:
    """
    Write a DataFrame to Parquet format.

    Args:
        sdf: Spark DataFrame to write
        path: Path to write to
        mode: Write mode (default: overwrite)

    Returns:
        The path where the Parquet data was written
    """
    logger = logging.getLogger(__name__)

    if sdf.isEmpty():
        logger.warning(f"Skipping write to {path} - DataFrame is empty")
        return path  # nothing to write

    # Cache to materialize
    cached_df = sdf.cache()
    cached_df.count()  # Force materialization

    logger.info(f"Writing {cached_df.count()} rows to Parquet at {path}")

    # Write the DataFrame to Parquet
    (cached_df.write.format(source="parquet").mode(saveMode=mode).save(path))

    # Clean up
    cached_df.unpersist()

    return path


def _write_to_delta(
    sdf: SparkDataFrame, path: str, *, mode: str = "append", table_name: str = ""
) -> None:
    """
    Write a DataFrame to Delta format and register as a table.

    Args:
        sdf: Spark DataFrame to write
        path: Path to write to
        mode: Write mode (default: append)
        table_name: Table name to register (optional)
    """
    logger = logging.getLogger(__name__)

    if sdf.isEmpty():
        logger.warning(f"Skipping write to {path} - DataFrame is empty")
        return  # nothing to write

    spark = sdf.sparkSession

    # Cache to materialize
    cached_df = sdf.cache()
    cached_df.count()  # Force materialization

    logger.info(f"Writing {cached_df.count()} rows to Delta at {path}")

    # Write the DataFrame to Delta
    (
        cached_df.write.format(source="delta")
        .mode(saveMode=mode)
        .option("overwriteSchema", "true")
        .option("mergeSchema", "true")
        .save(path)
    )

    # Register the table if a table_name was provided
    if table_name:
        spark.sql(f"REFRESH TABLE {table_name}")
        logger.info(f"Refreshed table {table_name}")

    # Clean up
    cached_df.unpersist()


def _write_delta(
    sdf: SparkDataFrame,
    path: str,
    *,
    mode: str = "append",
) -> None:
    """Write *sdf* to Delta at *path* and register the table if needed."""
    logger = logging.getLogger(__name__)

    if sdf.isEmpty():
        logger.warning(f"Skipping write to {path} - DataFrame is empty")
        return  # nothing to write

    # Clean up the path and extract table name
    clean_path = path.rstrip("/")
    table_name = clean_path.split("/")[-1]  # Get just the table name

    # For OneLake, we need to use the fully qualified path format
    # Format: abfss://lakehouse@tenant.dfs.fabric.microsoft.com/lakehouse/default/Tables/connectwise/table_name
    # But allow using the shorter format as input
    if clean_path.startswith("/lakehouse"):
        # This is a shortened path - convert to fully qualified abfss path if possible
        storage_account = os.getenv("FABRIC_STORAGE_ACCOUNT")
        tenant_id = os.getenv("FABRIC_TENANT_ID")

        if storage_account and tenant_id:
            # Convert to fully qualified path
            abfss_path = f"abfss://lakehouse@{storage_account}.dfs.fabric.microsoft.com{clean_path}"
            logger.info(f"Converting path to fully qualified ABFSS path: {abfss_path}")
            clean_path = abfss_path

    # For OneLake, we need to create a proper table name
    # Format: connectwise.table_name
    full_table_name = f"connectwise.{table_name}"

    spark = sdf.sparkSession

    try:
        # Step 1: Write to Parquet first to avoid Delta schema issues
        # This is particularly helpful for complex nested types with nulls
        parquet_path = f"{clean_path}_parquet"

        # Write to Parquet (which can handle complex types with nulls)
        _write_to_parquet(sdf, parquet_path, mode="overwrite")

        # Step 2: Read back from Parquet and write to Delta
        try:
            # Read back from Parquet - this step normalizes the schema
            logger.info("Reading back from Parquet to normalize schema")
            normalized_df = spark.read.format("parquet").load(parquet_path)

            # Write to Delta with the normalized schema
            _write_to_delta(normalized_df, clean_path, mode=mode, table_name=full_table_name)

        except Exception as delta_err:
            logger.error(f"Error writing from Parquet to Delta: {str(delta_err)}")
            # If Delta write fails, at least we have the Parquet data as backup
            logger.warning(f"Data is preserved in Parquet format at: {parquet_path}")
            raise

    except Exception as e:
        # Provide detailed error information
        error_msg = f"Error in write process for {clean_path}: {str(e)}"
        logger.error(f"ERROR: {error_msg}")

        # Re-raise the exception for handling by the caller
        raise


def _process_entity(
    spark: SparkSession,
    data: list[Any],
    *,
    apply_rules: Callable[[pd.DataFrame], pd.DataFrame],
    entity_slug: str,
    lakehouse_root: str,
    mode: str,
) -> str:  # noqa: D401
    pdf: pd.DataFrame = _models_to_pandas(data)
    pdf = apply_rules(pdf)
    sdf: SparkDataFrame = _sparkify(spark, pdf)
    delta_path: str = f"{lakehouse_root}/{entity_slug}"
    _write_delta(sdf, path=delta_path, mode=mode)
    return delta_path


# ----------------------------------------------------------------------------
# Public orchestration helper
# ----------------------------------------------------------------------------


def transform_and_load(
    *,
    spark: SparkSession | None = None,
    posted_invoices: list[dict],
    unposted_invoices: list[dict],
    time_entries: list[dict],
    expenses: list[dict],
    products: list[dict],
    agreements: list[dict],
    errors: list[dict] = None,
    lakehouse_root: str = "/lakehouse/default/Tables/connectwise",
    mode: str = "append",
) -> TransformResult:  # noqa: D401
    """Convert data collections → Delta tables and return their paths."""

    spark = spark or SparkSession.builder.getOrCreate()  # type: ignore
    errors = errors or []

    results: TransformResult = {}

    results["postedinvoice"] = _process_entity(
        spark,
        posted_invoices,
        apply_rules=_apply_invoice_rules,
        entity_slug="postedinvoice",
        lakehouse_root=lakehouse_root,
        mode=mode,
    )
    
    results["unpostedinvoice"] = _process_entity(
        spark,
        unposted_invoices,
        apply_rules=_apply_invoice_rules,
        entity_slug="unpostedinvoice",
        lakehouse_root=lakehouse_root,
        mode=mode,
    )

    results["timeentry"] = _process_entity(
        spark,
        time_entries,
        apply_rules=_apply_common_rules,
        entity_slug="timeentry",
        lakehouse_root=lakehouse_root,
        mode=mode,
    )

    results["expenseentry"] = _process_entity(
        spark,
        expenses,
        apply_rules=_apply_common_rules,
        entity_slug="expenseentry",
        lakehouse_root=lakehouse_root,
        mode=mode,
    )

    results["productitem"] = _process_entity(
        spark,
        products,
        apply_rules=_apply_common_rules,
        entity_slug="productitem",
        lakehouse_root=lakehouse_root,
        mode=mode,
    )
    
    results["agreement"] = _process_entity(
        spark,
        agreements,
        apply_rules=_apply_agreement_rules,
        entity_slug="agreement",
        lakehouse_root=lakehouse_root,
        mode=mode,
    )

    # Error table – *overwrite* to always keep snapshot from latest run
    results["validation_errors"] = _process_entity(
        spark,
        errors,
        apply_rules=_apply_common_rules,
        entity_slug="validation_errors",
        lakehouse_root=lakehouse_root,
        mode="overwrite",
    )

    return results


# ----------------------------------------------------------------------------
# CLI – developer convenience only
# ----------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover – manual testing
    import argparse

    parser = argparse.ArgumentParser(description="Test transform layer with dummy data")
    parser.add_argument("--lakehouse-root", default="/lakehouse/default/Tables/connectwise")
    args = parser.parse_args()

    spark_session = SparkSession.builder.appName("cw-transform-test").getOrCreate()  # type: ignore

    # Sanity‑check with empty lists – ensures code paths don't blow up
    out = transform_and_load(
        spark=spark_session,
        posted_invoices=[],
        unposted_invoices=[],
        time_entries=[],
        expenses=[],
        products=[],
        agreements=[],
        errors=[],
        lakehouse_root=args.lakehouse_root,
    )

    print("Written Delta tables:")
    for entity, path in out.items():
        print(f"  - {entity}: {path}")