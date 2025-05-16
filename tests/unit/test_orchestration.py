#!/usr/bin/env python
"""
Test script for the new modular ETL orchestration.
"""

import logging
import os
import tempfile

from pyspark.sql import SparkSession

from fabric_api.orchestration import ETLOrchestrator, run_full_etl, run_incremental_etl

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_etl_orchestrator():
    """
    Test the ETLOrchestrator with a temporary output directory.
    """
    logger.info("Testing ETL orchestration")

    # Create a temporary directory for output
    with tempfile.TemporaryDirectory() as temp_dir:
        bronze_path = os.path.join(temp_dir, "bronze")
        os.makedirs(bronze_path, exist_ok=True)

        # Create a SparkSession
        spark = SparkSession.builder.appName("ETLOrchestratorTest").getOrCreate()

        try:
            # Create an orchestrator
            orchestrator = ETLOrchestrator(
                spark=spark,
                bronze_path=bronze_path,
                write_mode="overwrite",
            )

            # Process a single entity with limited data
            logger.info("Testing process_entity with Agreement")
            df, row_count, error_count = orchestrator.process_entity(
                entity_name="Agreement",
                max_pages=1,  # Limit to one page for testing
            )

            logger.info(f"Processed Agreement: {row_count} rows, {error_count} errors")

            # Show schema
            logger.info("Agreement DataFrame schema:")
            df.printSchema()

            # Show sample data (if any)
            if not df.isEmpty():
                logger.info("Sample data:")
                df.show(5, truncate=False)

            # Test with multiple entities
            logger.info("\nTesting process_entities with TimeEntry and ProductItem")
            results = orchestrator.process_entities(
                entity_names=["TimeEntry", "ProductItem"],
                max_pages=1,  # Limit to one page for testing
            )

            for entity_name, (entity_df, entity_row_count, entity_error_count) in results.items():
                logger.info(
                    f"Processed {entity_name}: {entity_row_count} rows, {entity_error_count} errors"
                )

                # Show schema
                logger.info(f"{entity_name} DataFrame schema:")
                entity_df.printSchema()

            # Test incremental loading
            logger.info("\nTesting process_incremental with all entities")
            results = orchestrator.process_incremental(
                lookback_days=7,  # Just last week
                max_pages=1,  # Limit to one page for testing
            )

            for entity_name, (_, entity_row_count, entity_error_count) in results.items():
                logger.info(
                    f"Processed {entity_name} incrementally: {entity_row_count} rows, {entity_error_count} errors"
                )

            logger.info("\nETL Orchestrator test completed successfully")

        finally:
            # Stop the SparkSession
            spark.stop()


def test_convenience_functions():
    """
    Test the convenience functions for direct ETL runs.
    """
    logger.info("Testing convenience functions")

    # Create a temporary directory for output
    with tempfile.TemporaryDirectory() as temp_dir:
        bronze_path = os.path.join(temp_dir, "bronze")
        os.makedirs(bronze_path, exist_ok=True)

        # Test full ETL with a single entity
        logger.info("Testing run_full_etl with Agreement")
        results = run_full_etl(
            bronze_path=bronze_path,
            entity_names=["Agreement"],
            max_pages=1,  # Limit to one page for testing
        )

        for entity_name, (_, row_count, error_count) in results.items():
            logger.info(f"Full ETL for {entity_name}: {row_count} rows, {error_count} errors")

        # Test incremental ETL with a single entity
        logger.info("\nTesting run_incremental_etl with TimeEntry")
        results = run_incremental_etl(
            bronze_path=bronze_path,
            entity_names=["TimeEntry"],
            lookback_days=7,
            max_pages=1,  # Limit to one page for testing
        )

        for entity_name, (_, row_count, error_count) in results.items():
            logger.info(
                f"Incremental ETL for {entity_name}: {row_count} rows, {error_count} errors"
            )

        logger.info("\nConvenience functions test completed successfully")


if __name__ == "__main__":
    test_etl_orchestrator()
    test_convenience_functions()
