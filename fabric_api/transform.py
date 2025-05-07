from __future__ import annotations

"""fabric_api.transform

Transform layer – turns validated Pydantic domain objects from *fabric_api.extract* into Spark DataFrames and persists them as Delta tables inside the Microsoft Fabric lakehouse.

Public surface mirrors the legacy AL design: callers pass plain Python lists of model instances. Internally we:

1. Serialise the models into *pandas* DataFrames.
2. Apply business rules (skip/flag “Tímapottur”, convert VAT decimals to %).
3. Upgrade to Spark DataFrames.
4. Persist each entity as a managed Delta table under ``/lakehouse/default/Tables/connectwise``.

All helpers are side‑effect‑free except ``_write_delta`` which performs I/O.
"""

from typing import Any, Callable, TypeAlias
from datetime import datetime
import logging
import os

import pandas as pd

try:
    from pyspark.sql import SparkSession, DataFrame as SparkDataFrame

    # Add type annotation for builder to help IDE understand the pattern
    from pyspark.sql.session import SparkSession
except ImportError as exc:  # noqa: D401 – raise helpful error
    raise RuntimeError(
        "pyspark is required for fabric_api.transform – install with "
        "`pip install pyspark` or include the 'fabric' extra."
    ) from exc

from .models import (
    ManageInvoiceHeader,
    ManageInvoiceLine,
    ManageTimeEntry,
    ManageInvoiceExpense,
    ManageProduct,
    ManageInvoiceError,
)
from .utils import (
    is_timapottur_agreement,
)
from .spark_utils import (
    table_exists,
    create_empty_table_if_not_exists,
)

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

    return pd.DataFrame(data=[m.model_dump(mode="json") for m in models])


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
    if "agreementType" in result.columns:
        # Use DataFrame indexing to ensure we return a DataFrame, not a Series
        mask = ~result["agreementType"].str.contains("Tímapottur", na=False)
        result = result.loc[mask, :]

    # Ensure all required columns exist
    required_columns = ["id", "name", "type", "agreementType", "parentAgreementId"]
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

    # Define explicit schema for known array columns to avoid ARRAY<VOID> errors
    array_column_schemas = {
        "invoice_lines": "array<struct<invoice_number:string, line_no:int, quantity:double, amount:double>>",
        "time_entries": "array<struct<time_entry_id:int, invoice_number:string, employee:string, agreement_id:int>>",
        "expenses": "array<struct<invoice_id:int, line_no:int, type:string, quantity:double, amount:double>>",
        "products": "array<struct<product_id:int, invoice_number:string, description:string>>",
        "errors": "array<struct<invoice_number:string, error_message:string, table_name:string>>",
    }

    # Check and fix array columns with empty values
    for col, schema in array_column_schemas.items():
        if col in pdf.columns:
            # If the column is all-null or contains only empty lists
            all_empty = True
            for val in pdf[col]:
                if val and (isinstance(val, list) and len(val) > 0):
                    all_empty = False
                    break

            if all_empty:
                logger.info(f"Column {col} contains only empty arrays - fixing type")
                pdf[col] = pdf[col].apply(lambda x: [] if pd.isna(x) else x)

    # Fix complex nested structures with potential null types
    # This is necessary to prevent Delta's DELTA_COMPLEX_TYPE_COLUMN_CONTAINS_NULL_TYPE error
    for col in pdf.columns:
        # Handle list/array columns that might contain nulls or nested data
        if pdf[col].dtype == "object":
            # Convert None values in object columns to empty lists or strings to avoid null type issues
            null_indices = pdf[pdf[col].isnull()].index.tolist()

            if null_indices:
                # Check if this is likely an array column by examining non-null values
                non_null_indices = pdf[~pdf[col].isnull()].index.tolist()
                if non_null_indices:
                    sample_value = pdf.loc[non_null_indices[0], col]

                    # Replace nulls with appropriate empty values based on sample type
                    for idx in null_indices:
                        if isinstance(sample_value, list):
                            pdf.at[idx, col] = []
                        elif isinstance(sample_value, dict):
                            pdf.at[idx, col] = {}
                        else:
                            # For other object types, replace nulls with empty string
                            pdf.at[idx, col] = ""
                else:
                    # If ALL values are null, explicitly set column type to string
                    logger.info(f"Column {col} has all NULL values - setting as empty strings")
                    pdf[col] = pdf[col].fillna("")

    # Handle specific known string columns that might be all-null
    # These are columns that we know should be strings based on the model definitions
    string_columns = [
        "customer_name",
        "social_security_no",
        "site",
        "project",
        "gl_entry_ids",
        "sales_invoice_no",
        "agreement_number",
        "invoice_number",
        "invoice_type",
        "type",
        "agreement_type",
        "employee",
    ]

    for col in string_columns:
        if col in pdf.columns:
            # Explicitly check if all values are null without using Series boolean operations
            is_all_null = pdf[col].isnull().all()  # Returns a scalar boolean
            if is_all_null:  # Now checking a scalar boolean, not a Series
                logger.info(f"Explicitly converting all-null column {col} to empty strings")
                pdf[col] = pdf[col].fillna("")

    # Option 1: For dataframes WITHOUT complex array columns
    simple_columns = [col for col in pdf.columns if col not in array_column_schemas]
    if len(pdf.columns) == len(simple_columns):
        # If we don't have any array columns, use the simple approach
        return spark.createDataFrame(pdf)

    # Option 2: For dataframes WITH complex array columns, use schema inference
    try:
        # Convert to JSON and back to infer schema properly
        pdf_json = pdf.to_json(orient="records")
        schema_df = spark.read.json(spark.sparkContext.parallelize([pdf_json]))

        # Create an RDD of Row objects
        pandas_df_collected = pdf.to_dict("records")
        rdd = spark.sparkContext.parallelize(pandas_df_collected)

        # Create DataFrame with JSON-inferred schema
        return spark.createDataFrame(rdd, schema_df.schema)
    except Exception as e:
        logger.warning(f"Error creating DataFrame with schema inference: {str(e)}")
        logger.warning("Falling back to direct DataFrame creation")

        # Fallback: Try direct creation with explicit schema handling
        return spark.createDataFrame(pdf)


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
            logger.info(f"Reading back from Parquet to normalize schema")
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

        # Add diagnostic information for common OneLake errors
        if "400" in str(e) or "401" in str(e) or "403" in str(e):
            logger.error("\nThis appears to be an OneLake permissions or configuration issue.")
            logger.error("Check the following:")
            logger.error("1. Your lakehouse path is correctly configured")
            logger.error("2. You have write permissions to this location")
            logger.error("3. The table name follows OneLake naming conventions")
            logger.error("4. Try using a fully qualified path like 'abfss://...' if needed")
        elif "from cannot be less than 0" in str(e):
            logger.error(
                "\nThis appears to be a Livy pagination issue with empty DataFrames or lineage."
            )
            logger.error("Try the following:")
            logger.error("1. Make sure you're not using negative indices")
            logger.error("2. Verify that table schemas match between read and write operations")
        elif "DELTA_COMPLEX_TYPE_COLUMN_CONTAINS_NULL_TYPE" in str(e):
            logger.error("\nThis is a Delta schema validation error.")
            logger.error("Try the following:")
            logger.error("1. Data has been preserved in Parquet format for inspection")
            logger.error(
                "2. You can query the Parquet data directly using: spark.read.parquet('{parquet_path}')"
            )

        # Re-raise the exception for handling by the caller
        raise


def _process_entity(
    spark: SparkSession,
    models: list[Any],
    *,
    apply_rules: Callable[[pd.DataFrame], pd.DataFrame],
    entity_slug: str,
    lakehouse_root: str,
    mode: str,
) -> str:  # noqa: D401
    pdf: pd.DataFrame = _models_to_pandas(models)
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
    invoice_headers: list[ManageInvoiceHeader],
    invoice_lines: list[ManageInvoiceLine],
    time_entries: list[ManageTimeEntry],
    expenses: list[ManageInvoiceExpense],
    products: list[ManageProduct],
    agreements: list[dict] | None = None,  # Fix type hint to allow None
    errors: list[ManageInvoiceError],
    lakehouse_root: str = "/lakehouse/default/Tables/connectwise",
    mode: str = "append",
) -> TransformResult:  # noqa: D401
    """Convert model collections → Delta tables and return their paths."""

    spark = spark or SparkSession.builder.getOrCreate()  # type: ignore

    results: TransformResult = {}

    results["invoice_headers"] = _process_entity(
        spark,
        invoice_headers,
        apply_rules=_apply_invoice_rules,
        entity_slug="invoice_headers",
        lakehouse_root=lakehouse_root,
        mode=mode,
    )

    results["invoice_lines"] = _process_entity(
        spark,
        models=invoice_lines,
        apply_rules=_apply_common_rules,
        entity_slug="invoice_lines",
        lakehouse_root=lakehouse_root,
        mode=mode,
    )

    results["time_entries"] = _process_entity(
        spark,
        models=time_entries,
        apply_rules=_apply_agreement_rules,
        entity_slug="time_entries",
        lakehouse_root=lakehouse_root,
        mode=mode,
    )

    results["expenses"] = _process_entity(
        spark,
        models=expenses,
        apply_rules=_apply_agreement_rules,
        entity_slug="expenses",
        lakehouse_root=lakehouse_root,
        mode=mode,
    )

    results["products"] = _process_entity(
        spark,
        models=products,
        apply_rules=_apply_agreement_rules,
        entity_slug="products",
        lakehouse_root=lakehouse_root,
        mode=mode,
    )

    if agreements is not None:
        agreements_df = pd.DataFrame(agreements)
        agreements_df = _apply_agreement_rules(agreements_df)
        agreements_sdf = _sparkify(spark, agreements_df)
        agreements_delta_path = f"{lakehouse_root}/agreements"
        _write_delta(agreements_sdf, path=agreements_delta_path, mode=mode)
        results["agreements"] = agreements_delta_path

    # Error table – *overwrite* to always keep snapshot from latest run
    results["invoice_errors"] = _process_entity(
        spark,
        models=errors,
        apply_rules=_apply_common_rules,
        entity_slug="invoice_errors",
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

    # Sanity‑check with empty lists – ensures code paths don’t blow up
    out = transform_and_load(
        spark=spark_session,
        invoice_headers=[],
        invoice_lines=[],
        time_entries=[],
        expenses=[],
        products=[],
        errors=[],
        lakehouse_root=args.lakehouse_root,
    )

    print("Written Delta tables:")
    for entity, path in out.items():
        print(f"  - {entity}: {path}")
