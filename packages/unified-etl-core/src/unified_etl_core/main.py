"""
Main orchestration module for the unified ETL framework.

This module provides the primary entry point for running ETL processes
across different data sources (ConnectWise PSA, Business Central, etc.)
using the medallion architecture.
"""

import logging

from unified_etl_core.pipeline.bronze_silver import BronzeSilverPipeline
from unified_etl_core.pipeline.silver_gold import SilverGoldPipeline
from unified_etl_core.utils.config import ConfigManager
from unified_etl_core.utils.logging import setup_logging
from unified_etl_core.utils.spark import get_spark_session

logger = logging.getLogger(__name__)


def run_etl(
    config_path: str | None = None,
    tables: list[str] | None = None,
    lakehouse_root: str = "/lakehouse/default/Tables",
    mode: str = "append",
    log_level: str = "INFO",
) -> dict[str, bool]:
    """
    Run the complete ETL pipeline for specified tables.

    Args:
        config_path: Path to YAML configuration file
        tables: List of table names to process (if None, processes all configured tables)
        lakehouse_root: Root path for OneLake tables
        mode: Write mode for Delta tables ("append", "overwrite")
        log_level: Logging level

    Returns:
        Dictionary mapping table names to success status
    """
    setup_logging(level=log_level)
    logger.info("Starting unified ETL pipeline")

    # Initialize Spark session
    spark = get_spark_session(app_name="UnifiedETL", log_level=log_level)

    # Load configuration
    config_manager = ConfigManager(config_path)

    # Determine which tables to process
    if tables is None:
        tables_to_process = list(config_manager.get_all_table_configs().keys())
    else:
        tables_to_process = tables

    logger.info(f"Processing tables: {', '.join(tables_to_process)}")

    results = {}

    try:
        # Bronze to Silver pipeline
        bronze_silver = BronzeSilverPipeline(
            spark=spark, config_manager=config_manager, lakehouse_root=lakehouse_root
        )

        # Silver to Gold pipeline
        silver_gold = SilverGoldPipeline(
            spark=spark, config_manager=config_manager, lakehouse_root=lakehouse_root
        )

        for table in tables_to_process:
            try:
                logger.info(f"Processing table: {table}")

                # Run Bronze → Silver
                bronze_silver.process_table(table, mode=mode)
                logger.info(f"Completed Bronze → Silver for {table}")

                # Run Silver → Gold
                silver_gold.process_table(table, mode=mode)
                logger.info(f"Completed Silver → Gold for {table}")

                results[table] = True

            except Exception as e:
                logger.error(f"Failed to process table {table}: {e}")
                results[table] = False

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

    finally:
        spark.stop()

    logger.info("ETL pipeline completed")
    return results


def run_bronze_only(
    source: str,
    tables: list[str] | None = None,
    config_path: str | None = None,
    lakehouse_root: str = "/lakehouse/default/Tables",
    mode: str = "append",
    log_level: str = "INFO",
) -> dict[str, bool]:
    """
    Run only the bronze layer data ingestion for a specific source.

    Args:
        source: Data source name (e.g., "connectwise", "business_central")
        tables: List of table names to process
        config_path: Path to YAML configuration file
        lakehouse_root: Root path for OneLake tables
        mode: Write mode for Delta tables
        log_level: Logging level

    Returns:
        Dictionary mapping table names to success status
    """
    setup_logging(level=log_level)
    logger.info(f"Starting bronze ingestion for source: {source}")

    # Implementation depends on source type
    # This will be extended to support different sources
    if source == "connectwise":
        # ConnectWise-specific bronze ingestion logic
        # This will be implemented in the next phase
        pass
    elif source == "business_central":
        # Business Central-specific bronze ingestion logic
        pass
    else:
        raise ValueError(f"Unsupported source: {source}")

    return {}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Unified ETL Pipeline")
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument("--tables", nargs="+", help="Specific tables to process")
    parser.add_argument(
        "--lakehouse-root", default="/lakehouse/default/Tables", help="Root path for OneLake tables"
    )
    parser.add_argument(
        "--mode",
        choices=["append", "overwrite"],
        default="append",
        help="Write mode for Delta tables",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )
    parser.add_argument("--bronze-only", help="Run bronze ingestion only for specified source")

    args = parser.parse_args()

    if args.bronze_only:
        results = run_bronze_only(
            source=args.bronze_only,
            tables=args.tables,
            config_path=args.config,
            lakehouse_root=args.lakehouse_root,
            mode=args.mode,
            log_level=args.log_level,
        )
    else:
        results = run_etl(
            config_path=args.config,
            tables=args.tables,
            lakehouse_root=args.lakehouse_root,
            mode=args.mode,
            log_level=args.log_level,
        )

    # Print results
    failed_tables = [table for table, success in results.items() if not success]
    if failed_tables:
        print(f"Failed tables: {', '.join(failed_tables)}")
        exit(1)
    else:
        print("All tables processed successfully")
