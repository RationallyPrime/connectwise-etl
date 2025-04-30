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
    if df.empty or "agreement_type" not in df.columns:
        return df

    df["is_timapottur"] = df["agreement_type"].apply(is_timapottur_agreement)
    return df


def _sparkify(spark: SparkSession, pdf: pd.DataFrame) -> SparkDataFrame:  # noqa: D401
    """Convert a *pandas* DataFrame into Spark DataFrame preserving schema."""
    if pdf.empty:
        # Create empty DF with no columns but valid schema
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=pd.DataFrame().schema)
    return spark.createDataFrame(pdf)


def _write_delta(
    sdf: SparkDataFrame,
    path: str,
    *,
    mode: str = "append",
) -> None:  # noqa: D401
    """Write *sdf* to Delta at *path* and register the table if needed."""
    if sdf.isEmpty():
        return  # nothing to write

    (  # noqa: WPS323 – valid chain
        sdf.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .save(path)
    )

    table_name: str = path.rstrip("/").split("/")[-1]
    sdf.sparkSession.sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{path}'"
    )


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
