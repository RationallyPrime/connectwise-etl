#!/usr/bin/env python
"""
Streamlined orchestration pipeline for ConnectWise data ETL.
Optimized for Microsoft Fabric execution environment.

This pipeline implements a full medallion architecture:
1. Bronze: Extract raw data as-is from API
2. Silver: Flatten, validate, and prune columns
3. Gold: Apply business logic for BI-ready data marts
"""

import logging
from datetime import datetime, timedelta
from typing import Any, cast

from pydantic import BaseModel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from .client import ConnectWiseClient
from .core.config import ENTITY_CONFIG
from .core.spark_utils import get_spark_session
from .extract.generic import extract_entity
from .gold.invoice_processing import run_gold_invoice_processing
from .storage.fabric_delta import dataframe_from_models, write_to_delta
from .transform.dataframe_utils import flatten_all_nested_structures

logger = logging.getLogger(__name__)

# Define column pruning configurations for each entity
COLUMN_PRUNE_CONFIG = {
    "Agreement": {
        "keep": [
            "id",
            "name",
            "type",
            "company",
            "customer",
            "startDate",
            "endDate",
            "agreementStatus",
            "billToCompany",
            "billAmount",
            "location",
            "contact",
            "customFields",  # Needed for agreement number extraction
            "parentAgreement",  # Needed for agreement hierarchy
        ],
        "rename": {"billToCompany_id": "billToCompanyId", "company_id": "companyId"},
    },
    "TimeEntry": {
        "keep": [
            "id",
            "company",
            "member",
            "timeStart",
            "timeEnd",
            "actualHours",
            "billableOption",
            "notes",
            "agreement",
            "invoice",
            "workType",
            "workRole",
            "chargeToType",
            "chargeToId",
        ],
        "rename": {
            "company_id": "companyId",
            "member_id": "memberId",
            "agreement_id": "agreementId",
        },
    },
    "ExpenseEntry": {
        "keep": [
            "id",
            "company",
            "member",
            "date",
            "amount",
            "billableOption",
            "type",
            "notes",
            "agreement",
            "invoice",
            "chargeToType",
            "chargeToId",
            "mobileGuid",
        ],
        "rename": {
            "company_id": "companyId",
            "member_id": "memberId",
            "agreement_id": "agreementId",
        },
    },
    "ProductItem": {
        "keep": [
            "id",
            "catalogItem",
            "description",
            "quantity",
            "price",
            "cost",
            "billableOption",
            "agreement",
            "invoice",
            "location",
            "businessUnit",
            "vendor",
        ],
        "rename": {"catalogItem_id": "catalogItemId", "location_id": "locationId"},
    },
    "PostedInvoice": {
        "keep": [
            "id",
            "invoiceNumber",
            "type",
            "status",
            "company",
            "billToCompany",
            "date",
            "dueDate",
            "subtotal",
            "total",
            "salesTax",
            "agreement",
            "project",
            "ticket",
            "companyId",  # Keep the ID columns too
            "billToCompanyId",
        ],
        "rename": {
            "company_id": "companyId", 
            "billToCompany_id": "billToCompanyId",
            "status_name": "statusName",
            "company_name": "companyName",
            "billToCompany_name": "billToCompanyName",
            "agreement_id": "agreementId",
            "project_id": "projectId",
            "ticket_id": "ticketId"
        },
        "split_lines": True,  # Flag to split into header/line tables
    },
    "UnpostedInvoice": {
        "keep": [
            "id",
            "invoiceNumber",
            "invoiceType",
            "company",
            "billToCompany",
            "invoiceDate",
            "dueDate",
            "subTotal",
            "total",
            "salesTaxAmount",
            "description",
        ],
        # No rename needed - columns are already in camelCase after flattening
        "rename": {},
    },
}


def prune_columns(df: DataFrame, entity_name: str) -> DataFrame:
    """
    Prune unnecessary columns based on the configuration.

    Args:
        df: Input DataFrame
        entity_name: Name of the entity

    Returns:
        DataFrame with only necessary columns
    """
    if entity_name not in COLUMN_PRUNE_CONFIG:
        logger.warning(f"No pruning config for {entity_name}, keeping all columns")
        return df

    config = COLUMN_PRUNE_CONFIG[entity_name]

    # Get current columns
    current_columns = df.columns

    # Determine columns to keep
    columns_to_keep = []
    column_mapping = config.get("rename", {})

    # Always keep metadata columns
    metadata_columns = ["etlTimestamp", "etlEntity"]

    for col_name in current_columns:
        # Remove database prefix if present (e.g., "bronze.id" -> "id")
        clean_col_name = col_name.split(".")[-1] if "." in col_name else col_name
        
        # Keep metadata columns
        if clean_col_name in metadata_columns:
            columns_to_keep.append(col_name)
            continue

        # Check if it's a column we want to keep
        base_col = clean_col_name.split("_")[0]  # Handle flattened columns like company_id
        if base_col in config["keep"] or clean_col_name in config["keep"]:
            # Check if we need to rename it
            if clean_col_name in column_mapping:
                columns_to_keep.append(col(col_name).alias(column_mapping[clean_col_name]))
            else:
                columns_to_keep.append(col_name)

    # Select only the columns we want
    pruned_df = df.select(*columns_to_keep)

    return pruned_df


def split_invoice_to_header_line(
    df: DataFrame, entity_name: str
) -> tuple[DataFrame, DataFrame | None]:
    """
    Split invoice data into header and line tables for better BI structure.

    Args:
        df: Invoice DataFrame
        entity_name: Name of the entity (PostedInvoice)

    Returns:
        Tuple of (header_df, lines_df) or (df, None) if not an invoice
    """
    if entity_name != "PostedInvoice" or "split_lines" not in COLUMN_PRUNE_CONFIG.get(
        entity_name, {}
    ):
        return df, None

    # Define header columns (from the flattened structure)
    # Use camelCase consistently
    header_columns = [
        "id",
        "invoiceNumber",
        "type",
        "statusName",
        "companyId",
        "companyName",
        "billToCompanyId",
        "billToCompanyName",
        "date",
        "dueDate",
        "subtotal",
        "total",
        "salesTax",
        "agreementId",
        "projectId",
        "ticketId",
        "etlTimestamp",
        "etlEntity",
    ]

    # Extract header data - one row per invoice
    header_df = df.select(*[col for col in header_columns if col in df.columns]).dropDuplicates(
        ["id"]
    )

    # For line items, we need to parse/extract from the invoice details
    # This is a simplified example - actual implementation would depend on data structure
    lines_df = None

    # If we have line item data in arrays or nested structures, we'd process it here
    # For now, returning None for lines_df

    return header_df, lines_df


def process_bronze_to_silver(
    entity_name: str, bronze_path: str | None = None, silver_path: str | None = None, spark=None
) -> dict[str, Any]:
    """
    Process data from bronze to silver layer with transformations.

    Args:
        entity_name: Name of the entity
        bronze_path: Path to bronze table
        silver_path: Path to silver table
        spark: Spark session

    Returns:
        Processing results
    """
    spark = spark or get_spark_session()

    # Read from bronze
    bronze_table = f"bronze.{entity_name}"
    logger.info(f"Reading from bronze table: {bronze_table}")

    try:
        bronze_df = spark.table(bronze_table)
    except Exception as e:
        logger.error(f"Error reading bronze table {bronze_table}: {e}")
        return {"entity": entity_name, "status": "error", "error": str(e), "rows_processed": 0}

    initial_count = bronze_df.count()
    logger.info(f"Found {initial_count} rows in bronze table")

    # Apply transformations
    # 1. Flatten nested structures
    logger.info("Flattening nested structures...")
    flattened_df = flatten_all_nested_structures(bronze_df)

    # 2. Prune unnecessary columns
    logger.info("Pruning columns...")
    pruned_df = prune_columns(flattened_df, entity_name)

    # 3. Handle special cases like invoice splitting
    header_df, lines_df = split_invoice_to_header_line(pruned_df, entity_name)

    # 4. Write to silver layer
    silver_table = f"silver.{entity_name}"
    silver_table_path = silver_path or f"/lakehouse/default/Tables/silver/{entity_name}"

    # Write main table
    write_to_delta(
        df=header_df,
        entity_name=entity_name,
        base_path=silver_table_path,
        mode="overwrite",
        add_timestamp=False,  # Already has timestamp from bronze
    )

    # Write lines table if exists
    lines_count = 0
    if lines_df is not None:
        lines_table_path = (
            silver_path or f"/lakehouse/default/Tables/silver/{entity_name}_lines"
        )

        write_to_delta(
            df=lines_df,
            entity_name=f"{entity_name}_lines",
            base_path=lines_table_path,
            mode="overwrite",
            add_timestamp=False,
        )
        lines_count = lines_df.count()

    final_count = header_df.count()

    return {
        "entity": entity_name,
        "status": "success",
        "bronze_rows": initial_count,
        "silver_rows": final_count,
        "lines_rows": lines_count,
        "silver_table": silver_table,
    }


def process_entity_to_bronze(
    entity_name: str,
    client: ConnectWiseClient | None = None,
    conditions: str | None = None,
    page_size: int = 100,
    max_pages: int | None = None,
    bronze_path: str | None = None,
    mode: str = "append",
) -> dict[str, Any]:
    """
    Process a single entity to bronze layer (raw data).

    Args:
        entity_name: Name of the entity to process
        client: ConnectWiseClient instance (created if None)
        conditions: API query conditions
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch
        bronze_path: Base path for bronze tables
        mode: Write mode (append, overwrite)

    Returns:
        Dictionary with processing results
    """
    # Create client if needed
    client = client or ConnectWiseClient()

    # Extract raw data for bronze layer - we want everything
    logger.info(f"Extracting {entity_name} data for bronze layer...")
    data_result = extract_entity(
        client=client,
        entity_name=entity_name,
        page_size=page_size,
        max_pages=max_pages,
        conditions=conditions,
        return_validated=True,  # Get both valid data and errors
    )

    # Since return_validated=True, we get a tuple
    valid_data, validation_errors = cast(tuple[list[BaseModel], list[dict[str, Any]]], data_result)

    total_records = len(valid_data) + len(validation_errors)
    if total_records == 0:
        logger.warning(f"No {entity_name} data extracted")
        return {"entity": entity_name, "extracted": 0, "bronze_rows": 0, "bronze_path": ""}

    # Create DataFrame from valid data using proper schema
    spark = get_spark_session()
    logger.info(f"Creating DataFrame from {len(valid_data)} valid {entity_name} records")

    # Create DataFrame from validated models with schema
    if valid_data:
        raw_df = dataframe_from_models(valid_data, entity_name)
    else:
        # For errors, create a simple DataFrame from raw JSON
        # Type: ignore needed for PySpark's strict type checking
        error_df = spark.createDataFrame(validation_errors)  # type: ignore
        raw_df = error_df
    raw_df = raw_df.withColumn("etlTimestamp", lit(datetime.utcnow().isoformat()))
    raw_df = raw_df.withColumn("etlEntity", lit(entity_name))

    # Write to bronze layer
    entity_config = ENTITY_CONFIG.get(entity_name, {})
    bronze_table_name = entity_config.get("output_table", entity_name)

    logger.info(f"Writing {entity_name} data to bronze layer")
    path, row_count = write_to_delta(
        df=raw_df,
        entity_name=bronze_table_name,
        base_path=bronze_path or "/lakehouse/default/Tables/bronze",
        mode=mode,
        add_timestamp=False,  # We already added it
    )

    return {
        "entity": entity_name,
        "extracted": total_records,
        "bronze_rows": row_count,
        "bronze_path": path,
    }


def run_full_pipeline(
    entity_names: list[str] | None = None,
    client: ConnectWiseClient | None = None,
    conditions: str | None = None,
    page_size: int = 100,
    max_pages: int | None = None,
    bronze_path: str | None = None,
    silver_path: str | None = None,
    gold_path: str | None = None,
    mode: str = "append",
    process_gold: bool = True,
) -> dict[str, dict[str, Any]]:
    """
    Run the full medallion ETL pipeline: bronze, silver, and gold layers.

    Args:
        entity_names: List of entity names to process (all if None)
        client: ConnectWiseClient instance (created if None)
        conditions: API query conditions
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch
        bronze_path: Base path for bronze tables
        silver_path: Base path for silver tables
        gold_path: Base path for gold tables
        mode: Write mode for bronze (append, overwrite)
        process_gold: Whether to run gold layer processing

    Returns:
        Dictionary mapping entity names to result dictionaries
    """
    # Create client if needed
    client = client or ConnectWiseClient()
    spark = get_spark_session()

    # Use all entities if none specified
    if entity_names is None:
        entity_names = list(ENTITY_CONFIG.keys())

    results = {}

    # Phase 1: Extract to bronze
    for entity_name in entity_names:
        logger.info(f"Processing {entity_name} to bronze...")
        bronze_result = process_entity_to_bronze(
            entity_name=entity_name,
            client=client,
            conditions=conditions,
            page_size=page_size,
            max_pages=max_pages,
            bronze_path=bronze_path,
            mode=mode,
        )
        results[entity_name] = {"bronze": bronze_result}

    # Phase 2: Transform bronze to silver
    for entity_name in entity_names:
        if results[entity_name]["bronze"]["bronze_rows"] > 0:
            logger.info(f"Processing {entity_name} from bronze to silver...")
            silver_result = process_bronze_to_silver(
                entity_name=entity_name,
                bronze_path=bronze_path,
                silver_path=silver_path,
                spark=spark,
            )
            results[entity_name]["silver"] = silver_result
        else:
            results[entity_name]["silver"] = {"status": "skipped", "reason": "No data in bronze"}

    # Phase 3: Process gold layer for BI
    if process_gold:
        logger.info("Processing gold layer for BI data marts...")
        gold_results = run_gold_invoice_processing(
            silver_path=silver_path or "/lakehouse/default/Tables/silver",
            gold_path=gold_path or "/lakehouse/default/Tables/gold",
            spark=spark,
        )
        results["gold"] = gold_results

    return results


def run_daily_pipeline(
    entity_names: list[str] | None = None,
    days_back: int = 1,
    client: ConnectWiseClient | None = None,
    page_size: int = 100,
    max_pages: int | None = None,
    bronze_path: str | None = None,
    silver_path: str | None = None,
    gold_path: str | None = None,
    process_gold: bool = True,
) -> dict[str, dict[str, Any]]:
    """
    Run daily medallion ETL pipeline for recent data.

    Args:
        entity_names: List of entity names to process (all if None)
        days_back: Number of days to look back
        client: ConnectWiseClient instance (created if None)
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch
        bronze_path: Base path for bronze tables
        silver_path: Base path for silver tables
        gold_path: Base path for gold tables
        process_gold: Whether to run gold layer processing

    Returns:
        Dictionary mapping entity names to result dictionaries
    """
    # Calculate yesterday's date
    yesterday = datetime.now() - timedelta(days=days_back)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    # Today's date
    today = datetime.now()
    today_str = today.strftime("%Y-%m-%d")

    logger.info(f"Running daily pipeline for {yesterday_str}")

    # Build date-based condition
    condition = f"lastUpdated>=[{yesterday_str}] AND lastUpdated<[{today_str}]"

    # Use the full pipeline with date conditions
    return run_full_pipeline(
        entity_names=entity_names,
        client=client,
        conditions=condition,
        page_size=page_size,
        max_pages=max_pages,
        bronze_path=bronze_path,
        silver_path=silver_path,
        gold_path=gold_path,
        mode="append",  # Daily runs should append
        process_gold=process_gold,
    )
