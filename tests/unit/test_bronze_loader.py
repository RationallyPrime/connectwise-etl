#!/usr/bin/env python

"""
Test script for bronze_loader functionality.
"""

import logging
import os

from pyspark.sql import SparkSession

from fabric_api.bronze_loader import process_entities

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load environment variables from .env file if present
try:
    from dotenv import load_dotenv
    load_dotenv()
    logger.info("Loaded environment variables from .env file")
except ImportError:
    logger.warning("python-dotenv not installed, skipping .env loading")

def main():
    """
    Main function to test the bronze_loader.
    """
    # Create a local SparkSession for testing with a direct approach
    # to avoid builder attribute access issues
    import pyspark
    spark = SparkSession(pyspark.SparkContext("local[*]", "BronzeLoaderTest"))

    logger.info("Created SparkSession for testing")

    # Set output directory for Delta tables
    output_dir = os.path.join(os.getcwd(), "delta_test")
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Output directory: {output_dir}")

    # Entities to test
    entities_to_test = ["TimeEntry", "ProductItem"]

    logger.info(f"Testing bronze_loader for: {', '.join(entities_to_test)}")

    # Process entities
    results = process_entities(
        spark=spark,
        entity_names=entities_to_test,
        bronze_path=output_dir,
        page_size=10,
        max_pages=1,  # Limit for testing
    )

    # Display results
    for entity_name, (df, errors) in results.items():
        logger.info(f"{entity_name}: {df.count()} records, {len(errors)} validation errors")

        # Show sample of the data
        logger.info(f"Sample of {entity_name} data:")
        df.show(5, truncate=False)

        # Show schema
        logger.info(f"Schema for {entity_name}:")
        df.printSchema()

    logger.info(f"\nBronze loading complete. Data saved to: {output_dir}")

    # Stop SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
