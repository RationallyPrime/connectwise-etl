#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test script to validate SparkModel schema conversion for ConnectWise models.
"""

import logging
from pyspark.sql import SparkSession
from fabric_api.connectwise_models import (
    Agreement,
    TimeEntry,
    PostedInvoice,
    UnpostedInvoice,
    ExpenseEntry,
    ProductItem
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def test_schema_conversion():
    """
    Test the model_spark_schema() method for each model type.
    """
    logger.info("Testing Spark schema conversion for ConnectWise models")
    
    # List of models to test
    models = [
        Agreement,
        TimeEntry,
        PostedInvoice,
        UnpostedInvoice,
        ExpenseEntry,
        ProductItem
    ]
    
    # Test schema conversion for each model
    for model_class in models:
        model_name = model_class.__name__
        logger.info(f"Testing schema conversion for {model_name}")
        
        try:
            # Get Spark schema from the model
            spark_schema = model_class.model_spark_schema()
            logger.info(f"✅ Successfully converted {model_name} to Spark schema")
            logger.info(f"Schema has {len(spark_schema.fields)} fields")
            
            # Print a sample of the schema fields
            sample_fields = spark_schema.fields[:3]  # Show first 3 fields
            for field in sample_fields:
                logger.info(f"  - {field.name}: {field.dataType}")
                
        except Exception as e:
            logger.error(f"❌ Error converting {model_name} to Spark schema: {str(e)}")
    
    logger.info("Schema conversion test complete")

def test_dataframe_creation():
    """
    Test creating empty DataFrames with the schemas.
    """
    logger.info("Testing DataFrame creation with Spark schemas")
    
    # Create a SparkSession
    spark = SparkSession.builder.appName("SchemaTest").getOrCreate()
    
    # List of models to test
    models = [
        Agreement,
        TimeEntry,
        PostedInvoice,
        UnpostedInvoice,
        ExpenseEntry,
        ProductItem
    ]
    
    # Test DataFrame creation for each model
    for model_class in models:
        model_name = model_class.__name__
        logger.info(f"Testing DataFrame creation for {model_name}")
        
        try:
            # Get Spark schema
            spark_schema = model_class.model_spark_schema()
            
            # Create empty DataFrame with the schema
            df = spark.createDataFrame([], spark_schema)
            logger.info(f"✅ Successfully created DataFrame for {model_name}")
            
            # Show the DataFrame schema
            logger.info("DataFrame schema:")
            for field in df.schema.fields:
                logger.info(f"  - {field.name}: {field.dataType}")
                
        except Exception as e:
            logger.error(f"❌ Error creating DataFrame for {model_name}: {str(e)}")
    
    # Stop the SparkSession
    spark.stop()
    
    logger.info("DataFrame creation test complete")

if __name__ == "__main__":
    test_schema_conversion()
    test_dataframe_creation()