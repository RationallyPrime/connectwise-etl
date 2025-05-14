#!/usr/bin/env python
"""
Test script for verifying auto-generated model functionality.
Also serves as a helper script to regenerate models.
"""

import json
import logging
import os
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def regenerate_models():
    """Regenerate all models using the model_generator."""
    from fabric_api.core.model_generator import generate_models

    # Path to the OpenAPI schema
    schema_file = "PSA_OpenAPI_schema.json"

    # Temporary output directory for clean generation
    temp_output_dir = "fabric_api/connectwise_models_temp"

    # Final output directory
    final_output_dir = "fabric_api/connectwise_models"

    # Entities to generate
    entities = ["Agreement", "TimeEntry", "ExpenseEntry", "Invoice", "UnpostedInvoice", "ProductItem"]

    try:
        # Clean up temp directory if it exists
        if os.path.exists(temp_output_dir):
            shutil.rmtree(temp_output_dir)

        # Create temp directory
        os.makedirs(temp_output_dir, exist_ok=True)

        # Generate models to temp directory
        success = generate_models(schema_file, temp_output_dir, entities)

        if success:
            logger.info("Model generation successful. Copying to final location...")

            # Create final output directory if it doesn't exist
            os.makedirs(final_output_dir, exist_ok=True)

            # Copy generated files to final location
            for item in os.listdir(temp_output_dir):
                src_path = os.path.join(temp_output_dir, item)
                dst_path = os.path.join(final_output_dir, item)
                if os.path.isfile(src_path):
                    shutil.copy2(src_path, dst_path)
                    logger.info(f"Copied {item} to {final_output_dir}")

            # Clean up temp directory
            shutil.rmtree(temp_output_dir)

            logger.info("Models successfully regenerated!")
            return True

        else:
            logger.error("Model generation failed. Check the logs for details.")
            return False

    except Exception as e:
        logger.exception(f"Error during model generation: {e}")
        return False

def test_agreement_model():
    """Test that the generated Agreement model works correctly."""
    from fabric_api.connectwise_models import Agreement

    # Test creating an Agreement with just required fields
    agreement = Agreement(
        name="Test Agreement",
        type={"id": 1, "name": "Regular"},
        company={"id": 123, "name": "Test Company"},
        contact={"id": 456, "name": "Test Contact"},
    )

    logger.info(f"Created Agreement with required fields: {agreement}")

    # Test serialization
    json_data = agreement.model_dump_json()
    logger.info(f"JSON data: {json_data}")

    # Test that we can get the Spark schema
    if hasattr(Agreement, "model_spark_schema"):
        schema = Agreement.model_spark_schema()
        logger.info(f"Spark schema available: {schema}")

def test_reference_fields():
    """Test that reference fields are properly handled."""
    from fabric_api.connectwise_models import AgreementReference

    # Test that we can create a reference model
    ref = AgreementReference(
        root={
            "id": 123,
            "name": "Test Reference"
        }
    )

    logger.info(f"Created reference: {ref}")

    # Test json serialization
    json_data = ref.model_dump_json()
    logger.info(f"Reference as JSON: {json_data}")

def test_sample_file():
    """Test parsing a sample file."""
    from fabric_api.connectwise_models import Agreement

    # Load a sample file if it exists
    sample_path = Path("entity_samples/Agreement.json")
    if not sample_path.exists():
        logger.warning(f"Sample file {sample_path} not found")
        return

    with open(sample_path) as f:
        sample_data = json.load(f)

    # Try to parse it
    try:
        agreement = Agreement.model_validate(sample_data)
        logger.info(f"Successfully validated sample Agreement: {agreement.name}")
    except Exception as e:
        logger.error(f"Failed to validate sample: {e}")

def test_dataframe_creation():
    """Test creating a DataFrame from models."""
    from fabric_api.connectwise_models import Agreement

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("ModelTest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .getOrCreate()

    # Create some sample agreements
    agreements = [
        Agreement(
            name=f"Test Agreement {i}",
            type={"id": 1, "name": "Regular"},
            company={"id": 123, "name": "Test Company"},
            contact={"id": 456, "name": "Test Contact"},
        )
        for i in range(5)
    ]

    # Create a DataFrame
    try:
        # Convert to dict first
        dict_data = [agreement.model_dump() for agreement in agreements]

        # Try schema-based creation
        schema = Agreement.model_spark_schema()
        df = spark.createDataFrame(dict_data, schema)

        logger.info(f"Created DataFrame with {df.count()} rows")
        df.printSchema()
        df.show(2)
    except Exception as e:
        logger.error(f"Failed to create DataFrame: {e}")

def main():
    """Run models regeneration and/or tests based on arguments."""
    import argparse

    parser = argparse.ArgumentParser(description='Model generation and testing')
    parser.add_argument('--regenerate', action='store_true', help='Regenerate models')
    parser.add_argument('--test', action='store_true', help='Run tests')

    args = parser.parse_args()

    # Default to running tests if no args provided
    if not (args.regenerate or args.test):
        args.test = True

    if args.regenerate:
        logger.info("Regenerating models...")
        success = regenerate_models()
        if not success:
            sys.exit(1)

    if args.test:
        logger.info("Testing Agreement model...")
        test_agreement_model()

        logger.info("\nTesting reference fields...")
        test_reference_fields()

        logger.info("\nTesting sample file...")
        test_sample_file()

        logger.info("\nTesting DataFrame creation...")
        test_dataframe_creation()

if __name__ == "__main__":
    main()
