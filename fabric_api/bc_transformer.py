"""Business Central transformer for Silver layer - preserves all fields."""

from pyspark.sql import DataFrame, SparkSession, functions as F

from fabric_api.storage.fabric_delta import write_to_delta
from fabric_api.transform.dataframe_utils import flatten_all_nested_structures

# BC table number mapping - just for reference
BC_TABLE_NUMBERS = {
    "GLEntry": "17",
    "Customer": "18",
    "Vendor": "23",
    "VendorLedgerEntry": "25",
    "CustLedgerEntry": "21",
    "Job": "167",
    "JobLedgerEntry": "169",
    "SalesInvoiceHeader": "112",
    "SalesInvoiceLine": "113",
    "DimensionSetEntry": "480",
    "Item": "27",
    "Resource": "156",
    "DimensionValue": "349",
    "Dimension": "348",
    "GLAccount": "15",
    "Currency": "4",
    "CompanyInformation": "79",
    "AccountingPeriod": "50",
    "GeneralLedgerSetup": "98",
    "DetailedCustLedgEntry": "379",
    "DetailedVendorLedgEntry": "380",
}


def clean_column_name(col_name: str) -> str:
    """Remove numeric suffix from BC column names but preserve the original case."""
    # Remove numeric suffix after hyphen
    if "-" in col_name:
        return col_name.split("-")[0]
    return col_name


def transform_bc_entity(
    spark: SparkSession,
    entity_name: str,
    bronze_path: str = None,
    silver_path: str = None,
    mode: str = "overwrite",
) -> DataFrame:
    """Transform BC entity from Bronze to Silver layer - preserves all fields."""

    # Get table suffix from mapping
    table_suffix = BC_TABLE_NUMBERS.get(entity_name)
    if not table_suffix:
        raise ValueError(f"Unknown BC entity: {entity_name}")

    # Default paths if not provided
    if not bronze_path:
        bronze_path = f"bronze.{entity_name}{table_suffix}"
    if not silver_path:
        silver_path = f"silver.{entity_name}"

    # Read bronze data
    print(f"Reading bronze data from: {bronze_path}")
    bronze_df = spark.read.table(bronze_path)

    # Apply flattening if needed
    flat_df = flatten_all_nested_structures(bronze_df)

    # Rename columns to remove numeric suffix but preserve case
    for col in flat_df.columns:
        clean_name = clean_column_name(col)
        if clean_name != col:
            flat_df = flat_df.withColumnRenamed(col, clean_name)

    # Add metadata columns
    flat_df = flat_df.withColumn("_extract_timestamp", F.current_timestamp()).withColumn(
        "_entity_name", F.lit(entity_name)
    )

    # Write to silver
    print(f"Writing to silver: {silver_path}")
    write_to_delta(flat_df, silver_path, mode=mode)

    return flat_df


def transform_all_bc_entities(
    spark: SparkSession, entities: list[str] = None, mode: str = "overwrite"
) -> dict[str, int]:
    """Transform all BC entities from Bronze to Silver layer."""
    if not entities:
        entities = list(BC_TABLE_NUMBERS.keys())

    results = {}

    for entity in entities:
        try:
            df = transform_bc_entity(spark, entity, mode=mode)
            results[entity] = df.count()
            print(f"✓ {entity}: {results[entity]} rows transformed")
        except Exception as e:
            print(f"✗ {entity}: Error - {e!s}")
            results[entity] = -1

    return results


def get_bc_table_mapping() -> dict[str, str]:
    """Get the BC entity to table number mapping."""
    return BC_TABLE_NUMBERS
