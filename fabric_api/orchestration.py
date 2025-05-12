#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Orchestration module for ConnectWise ETL pipeline.
This module provides a clear, modular approach to orchestrating the entire ETL process.
"""

import os
import logging
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime, date, timedelta
from pyspark.sql import SparkSession, DataFrame

# Import utility modules
from fabric_api.client import ConnectWiseClient
from fabric_api import validation
from fabric_api import delta_utils
from fabric_api import onelake_utils

# Import extract modules for raw data fetching
from fabric_api.extract import (
    agreements,
    invoices,
    time,
    expenses,
    products,
)

# Initialize logger with reduced verbosity for Fabric notebooks
logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)  # Only show warnings and errors by default

# Default values for ETL parameters
DEFAULT_LAKEHOUSE_ROOT = "/lakehouse/default/Tables/connectwise"
DEFAULT_BRONZE_PATH = f"{DEFAULT_LAKEHOUSE_ROOT}/bronze"
DEFAULT_WRITE_MODE = "append"
DEFAULT_MAX_PAGES = 50
DEFAULT_PAGE_SIZE = 100


class ETLOrchestrator:
    """
    Orchestrates the ETL process for ConnectWise data.

    This class provides a clear interface for running the ETL process with
    modular, configurable components for extract, transform, and load.
    """

    def __init__(
        self,
        client: Optional[ConnectWiseClient] = None,
        spark: Optional[SparkSession] = None,
        bronze_path: str = DEFAULT_BRONZE_PATH,
        write_mode: str = DEFAULT_WRITE_MODE,
        use_onelake: bool = True,
    ):
        """
        Initialize the ETL orchestrator.

        Args:
            client: ConnectWiseClient instance for API calls (created if None)
            spark: SparkSession instance for Spark operations (created if None)
            bronze_path: Base path for bronze layer Delta tables
            write_mode: Write mode for Delta tables (append or overwrite)
            use_onelake: Whether to use OneLake path conventions and features
        """
        self.client = client or ConnectWiseClient()
        self.spark = spark or self._create_spark_session()
        self.bronze_path = bronze_path
        self.write_mode = write_mode
        self.use_onelake = use_onelake

        # Initialize OneLake database if needed
        if self.use_onelake:
            onelake_utils.create_database_if_not_exists(self.spark)

        # Initialize components
        self._init_components()

    def _create_spark_session(self) -> SparkSession:
        """Create a SparkSession for Spark operations."""
        try:
            # Create a Spark session with Fabric-optimized configuration
            builder = SparkSession.builder.appName("ConnectWiseETL")

            # Add Fabric-specific configurations
            builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

            # For Fabric compatibility, add these optimizations
            builder = builder.config("spark.sql.adaptive.enabled", "true") \
                            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                            .config("spark.sql.adaptive.skewJoin.enabled", "true")

            return builder.getOrCreate()
        except Exception as e:
            logger.error(f"Failed to create SparkSession: {str(e)}")
            raise

    def _init_components(self) -> None:
        """Initialize ETL components and mappings."""
        # Define entity configurations for orchestration with OneLake partitioning
        self.entity_configs = {
            "Agreement": {
                "extract_func": agreements.fetch_agreements_raw,
                "validate_func": validation.validate_agreements,
                "partition_cols": onelake_utils.get_partition_columns("agreement") if self.use_onelake else [],
            },
            "PostedInvoice": {
                "extract_func": invoices.fetch_posted_invoices_raw,
                "validate_func": validation.validate_posted_invoices,
                "partition_cols": onelake_utils.get_partition_columns("posted_invoice") if self.use_onelake else [],
            },
            "UnpostedInvoice": {
                "extract_func": invoices.fetch_unposted_invoices_raw,
                "validate_func": validation.validate_unposted_invoices,
                "partition_cols": onelake_utils.get_partition_columns("unposted_invoice") if self.use_onelake else [],
            },
            "TimeEntry": {
                "extract_func": time.fetch_time_entries_raw,
                "validate_func": validation.validate_time_entries,
                "partition_cols": onelake_utils.get_partition_columns("time_entry") if self.use_onelake else [],
            },
            "ExpenseEntry": {
                "extract_func": expenses.fetch_expense_entries_raw,
                "validate_func": validation.validate_expense_entries,
                "partition_cols": onelake_utils.get_partition_columns("expense_entry") if self.use_onelake else [],
            },
            "ProductItem": {
                "extract_func": products.fetch_product_items_raw,
                "validate_func": validation.validate_product_items,
                "partition_cols": onelake_utils.get_partition_columns("product_item") if self.use_onelake else [],
            },
        }

    def process_entity(
        self,
        entity_name: str,
        conditions: Optional[str] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        max_pages: Optional[int] = DEFAULT_MAX_PAGES,
    ) -> Tuple[DataFrame, int, int]:
        """
        Process a single entity type through the full ETL pipeline.

        Args:
            entity_name: Name of the entity to process
            conditions: API query conditions for filtering
            page_size: Number of records per page for API calls
            max_pages: Maximum number of pages to retrieve

        Returns:
            Tuple of (DataFrame written to Delta, number of rows, number of errors)
        """
        if entity_name not in self.entity_configs:
            raise ValueError(f"Unknown entity: {entity_name}. Must be one of {list(self.entity_configs.keys())}")

        config = self.entity_configs[entity_name]
        extract_func = config["extract_func"]
        validate_func = config["validate_func"]
        partition_cols = config["partition_cols"]

        # Extract: Fetch raw data
        logger.info(f"Extracting {entity_name} data from API...")
        raw_data = extract_func(
            client=self.client,
            page_size=page_size,
            max_pages=max_pages,
            conditions=conditions,
        )
        logger.info(f"Extracted {len(raw_data)} {entity_name} records")

        # Transform: Validate data with Pydantic models
        logger.info(f"Validating {entity_name} data...")
        
        # Temporarily reduce validation logger verbosity in production environment
        from fabric_api.validation import logger as validation_logger
        original_level = validation_logger.level
        # Set to WARNING to reduce console output in Fabric notebooks
        validation_logger.setLevel(logging.WARNING)
        
        # Perform validation
        validation_result = validate_func(raw_data)
        
        # Restore original logger level
        validation_logger.setLevel(original_level)

        # Determine path - use OneLake utilities if enabled
        if self.use_onelake:
            delta_path = onelake_utils.get_table_path(entity_name)
        else:
            delta_path = delta_utils.build_delta_path(self.bronze_path, entity_name, use_onelake=False)

        # Create DataFrame from validated models, with OneLake preparation if needed
        logger.info(f"Converting {len(validation_result.valid_objects)} valid {entity_name} records to DataFrame...")
        df = delta_utils.spark_from_model_list(
            spark=self.spark,
            models=validation_result.valid_objects,
            entity_name=entity_name if self.use_onelake else None
        )

        # Write to Delta using OneLake utilities if enabled
        logger.info(f"Writing {entity_name} data to Delta at {delta_path}...")
        if self.use_onelake:
            table_path, _, row_count = onelake_utils.write_to_onelake(
                df=df,
                entity_name=entity_name,
                spark=self.spark,
                mode=self.write_mode,
                create_table=True
            )
        else:
            table_path, row_count = delta_utils.write_to_delta(
                df=df,
                path=delta_path,
                mode=self.write_mode,
                partition_cols=partition_cols,
                add_timestamp=True,
                entity_name=None  # Don't use OneLake utilities
            )

        # Write validation errors if any
        if validation_result.errors:
            logger.info(f"Writing {len(validation_result.errors)} validation errors...")
            if self.use_onelake:
                # Use OneLake utilities for validation errors
                errors_df = self.spark.createDataFrame(validation_result.errors)
                onelake_utils.write_to_onelake(
                    df=errors_df,
                    entity_name="validation_errors",
                    spark=self.spark,
                    mode="append"
                )
            else:
                # Use standard delta_utils for errors
                delta_utils.write_validation_errors(
                    spark=self.spark,
                    errors=validation_result.errors,
                    base_path=self.bronze_path,
                )

        # Return DataFrame and stats
        return df, row_count, len(validation_result.errors)

    def process_entities(
        self,
        entity_names: Optional[List[str]] = None,
        conditions: Optional[Dict[str, str]] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        max_pages: Optional[int] = DEFAULT_MAX_PAGES,
    ) -> Dict[str, Tuple[DataFrame, int, int]]:
        """
        Process multiple entity types through the ETL pipeline.

        Args:
            entity_names: List of entity names to process (all if None)
            conditions: Dictionary mapping entity names to API query conditions
            page_size: Number of records per page for API calls
            max_pages: Maximum number of pages to retrieve

        Returns:
            Dictionary mapping entity names to (DataFrame, row_count, error_count) tuples
        """
        # Use all entities if none specified
        if entity_names is None:
            entity_names = list(self.entity_configs.keys())

        results = {}
        for entity_name in entity_names:
            entity_condition = None if not conditions else conditions.get(entity_name)
            df, row_count, error_count = self.process_entity(
                entity_name=entity_name,
                conditions=entity_condition,
                page_size=page_size,
                max_pages=max_pages,
            )
            results[entity_name] = (df, row_count, error_count)

        return results

    def process_incremental(
        self,
        entity_names: Optional[List[str]] = None,
        last_sync_date: Optional[date] = None,
        lookback_days: int = 30,
        date_field: str = "lastUpdated",
        page_size: int = DEFAULT_PAGE_SIZE,
        max_pages: Optional[int] = DEFAULT_MAX_PAGES,
    ) -> Dict[str, Tuple[DataFrame, int, int]]:
        """
        Process entities with incremental loading based on a date field.

        Args:
            entity_names: List of entity names to process (all if None)
            last_sync_date: Date of last successful sync (defaults to lookback_days ago)
            lookback_days: Number of days to look back if no last_sync_date
            date_field: Field to use for date filtering
            page_size: Number of records per page for API calls
            max_pages: Maximum number of pages to retrieve

        Returns:
            Dictionary mapping entity names to (DataFrame, row_count, error_count) tuples
        """
        # Determine start date (with one day offset to avoid missing records)
        if last_sync_date is None:
            start_date = date.today() - timedelta(days=lookback_days)
        else:
            start_date = last_sync_date - timedelta(days=1)  # Offset by one day

        start_date_str = start_date.strftime("%Y-%m-%dT00:00:00Z")
        logger.info(f"Running incremental load from {start_date_str}")

        # Build date-based conditions for each entity
        conditions = {}
        for entity_name in entity_names or self.entity_configs.keys():
            conditions[entity_name] = f"{date_field}>{start_date_str}"

        # Use standard process_entities with date conditions
        return self.process_entities(
            entity_names=entity_names,
            conditions=conditions,
            page_size=page_size,
            max_pages=max_pages,
        )


# Convenience functions for direct use
def run_full_etl(
    client: Optional[ConnectWiseClient] = None,
    spark: Optional[SparkSession] = None,
    bronze_path: str = DEFAULT_BRONZE_PATH,
    write_mode: str = DEFAULT_WRITE_MODE,
    entity_names: Optional[List[str]] = None,
    max_pages: Optional[int] = DEFAULT_MAX_PAGES,
    use_onelake: bool = True,
) -> Dict[str, Tuple[DataFrame, int, int]]:
    """
    Run a full ETL process for ConnectWise data.

    Args:
        client: ConnectWiseClient instance (created if None)
        spark: SparkSession instance (created if None)
        bronze_path: Base path for bronze layer Delta tables
        write_mode: Write mode for Delta tables
        entity_names: List of entity names to process (all if None)
        max_pages: Maximum number of pages to retrieve
        use_onelake: Whether to use OneLake optimizations

    Returns:
        Dictionary mapping entity names to (DataFrame, row_count, error_count) tuples
    """
    orchestrator = ETLOrchestrator(
        client=client,
        spark=spark,
        bronze_path=bronze_path,
        write_mode=write_mode,
        use_onelake=use_onelake,
    )

    return orchestrator.process_entities(
        entity_names=entity_names,
        max_pages=max_pages,
    )


def run_incremental_etl(
    client: Optional[ConnectWiseClient] = None,
    spark: Optional[SparkSession] = None,
    bronze_path: str = DEFAULT_BRONZE_PATH,
    lookback_days: int = 30,
    entity_names: Optional[List[str]] = None,
    max_pages: Optional[int] = DEFAULT_MAX_PAGES,
    use_onelake: bool = True,
) -> Dict[str, Tuple[DataFrame, int, int]]:
    """
    Run an incremental ETL process for ConnectWise data.

    Args:
        client: ConnectWiseClient instance (created if None)
        spark: SparkSession instance (created if None)
        bronze_path: Base path for bronze layer Delta tables
        lookback_days: Number of days to look back for changed records
        entity_names: List of entity names to process (all if None)
        max_pages: Maximum number of pages to retrieve
        use_onelake: Whether to use OneLake optimizations

    Returns:
        Dictionary mapping entity names to (DataFrame, row_count, error_count) tuples
    """
    orchestrator = ETLOrchestrator(
        client=client,
        spark=spark,
        bronze_path=bronze_path,
        write_mode="append",  # Always use append for incremental
        use_onelake=use_onelake,
    )

    return orchestrator.process_incremental(
        entity_names=entity_names,
        lookback_days=lookback_days,
        max_pages=max_pages,
    )


def run_onelake_etl(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lakehouse_root: str = onelake_utils.DEFAULT_TABLES_PATH,
    mode: str = "append",
    max_pages: int = DEFAULT_MAX_PAGES,
) -> Dict[str, str]:
    """
    Run an ETL process directly to OneLake tables with all OneLake optimizations.
    This is a simplified interface for Microsoft Fabric notebooks.

    Args:
        start_date: Start date for incremental load (YYYY-MM-DD)
        end_date: End date for incremental load (YYYY-MM-DD)
        lakehouse_root: Root path for lakehouse tables
        mode: Write mode (append or overwrite)
        max_pages: Maximum pages to retrieve per entity

    Returns:
        Dictionary mapping entity names to their table paths
    """
    # Create a Spark session optimized for Fabric
    spark = SparkSession.builder.appName("ConnectWise-OneLake") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    # Initialize the OneLake database
    onelake_utils.create_database_if_not_exists(spark)

    # Determine if this is incremental or full load
    is_incremental = start_date is not None or end_date is not None

    # Run the appropriate ETL process
    if is_incremental:
        # Convert string dates to datetime objects
        from datetime import datetime, timedelta

        # Handle date conversion
        if start_date:
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
        else:
            # Default to 30 days ago
            start_date_obj = datetime.now().date() - timedelta(days=30)

        lookback_days = (datetime.now().date() - start_date_obj).days

        # Run incremental ETL with date filtering
        results = run_incremental_etl(
            spark=spark,
            bronze_path=lakehouse_root,
            lookback_days=lookback_days,
            max_pages=max_pages,
            use_onelake=True
        )
    else:
        # Run full ETL
        results = run_full_etl(
            spark=spark,
            bronze_path=lakehouse_root,
            write_mode=mode,
            max_pages=max_pages,
            use_onelake=True
        )

    # Translate results to table paths
    table_paths = {}
    for entity_name, (_, row_count, error_count) in results.items():
        table_name = onelake_utils.get_table_name(entity_name)
        table_path = onelake_utils.get_table_path(entity_name)
        table_paths[entity_name] = table_path
        logger.info(f"Entity {entity_name} loaded to {table_name}: {row_count} rows, {error_count} errors")

    return table_paths


if __name__ == "__main__":
    # Quick test of the orchestration module
    import argparse

    parser = argparse.ArgumentParser(description="Run ConnectWise ETL process")
    parser.add_argument("--mode", choices=["full", "incremental", "onelake"], default="incremental",
                        help="ETL mode: full, incremental, or onelake")
    parser.add_argument("--entity", default=None, help="Single entity type to process")
    parser.add_argument("--lookback", type=int, default=30, help="Lookback days for incremental")
    parser.add_argument("--bronze-path", default=DEFAULT_BRONZE_PATH, help="Bronze layer path")
    parser.add_argument("--max-pages", type=int, default=1, help="Max pages to fetch (for testing)")
    parser.add_argument("--no-onelake", action="store_true", help="Disable OneLake optimizations")

    args = parser.parse_args()

    # Set up entity names list
    entity_names = [args.entity] if args.entity else None

    # Determine whether to use OneLake
    use_onelake = not args.no_onelake

    if args.mode == "onelake":
        # Run with all OneLake optimizations
        table_paths = run_onelake_etl(
            max_pages=args.max_pages
        )

        # Print results
        print("\nOneLake ETL Results:")
        for entity_name, path in table_paths.items():
            print(f"  {entity_name}: {path}")

    elif args.mode == "full":
        # Run full ETL
        results = run_full_etl(
            bronze_path=args.bronze_path,
            entity_names=entity_names,
            max_pages=args.max_pages,
            write_mode="overwrite",
            use_onelake=use_onelake,
        )

        # Print results
        print("\nETL Results:")
        for entity_name, (_, row_count, error_count) in results.items():
            print(f"  {entity_name}: {row_count} rows written, {error_count} validation errors")
    else:
        # Run incremental ETL
        results = run_incremental_etl(
            bronze_path=args.bronze_path,
            lookback_days=args.lookback,
            entity_names=entity_names,
            max_pages=args.max_pages,
            use_onelake=use_onelake,
        )

        # Print results
        print("\nETL Results:")
        for entity_name, (_, row_count, error_count) in results.items():
            print(f"  {entity_name}: {row_count} rows written, {error_count} validation errors")