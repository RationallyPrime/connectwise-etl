#!/usr/bin/env python
"""
Example usage of generated CDM models.

This script demonstrates how to use the CDM models for data validation
and Spark schema generation.
"""

from pyspark.sql import SparkSession
from cdm_models import Currency, GLAccount, Customer

# Example 1: Create model instances
def example_model_usage():
    """Show how to create and use CDM model instances."""
    
    # Create a Currency instance
    currency = Currency(
        code="USD",
        iso_code="USD",
        description="US Dollar",
        company="CRONUS USA"
    )
    
    # Access fields by their Python names
    print(f"Currency code: {currency.code}")
    print(f"Company: {currency.company}")
    
    # Convert to dictionary with CDM field names
    cdm_dict = currency.model_dump(by_alias=True)
    print(f"CDM dictionary: {cdm_dict}")
    
    # Validate data
    try:
        invalid_currency = Currency(
            code="This is way too long for the code field maximum length",
            iso_code="INVALID"
        )
    except Exception as e:
        print(f"Validation error: {e}")


# Example 2: Generate Spark schemas
def example_spark_schema():
    """Show how to generate Spark schemas from CDM models."""
    
    # Get Spark schema for Currency
    currency_schema = Currency.model_spark_schema()
    print("Currency Spark schema:")
    print(currency_schema)
    
    # Get Spark schema for GLAccount  
    glaccount_schema = GLAccount.model_spark_schema()
    print("\nGLAccount Spark schema:")
    print(glaccount_schema)


# Example 3: Read CDM data into Spark DataFrame
def example_spark_dataframe():
    """Show how to read CDM data into Spark DataFrame with validation."""
    
    spark = SparkSession.builder.appName("CDM Example").getOrCreate()
    
    # Sample data matching CDM format
    currency_data = [
        {
            "Code-1": "USD",
            "ISOCode-4": "USD", 
            "Description-15": "US Dollar",
            "$Company": "CRONUS USA"
        },
        {
            "Code-1": "EUR",
            "ISOCode-4": "EUR",
            "Description-15": "Euro",
            "$Company": "CRONUS International"
        }
    ]
    
    # Create DataFrame with CDM schema
    currency_df = spark.createDataFrame(currency_data, Currency.model_spark_schema())
    currency_df.show()
    
    # Transform to Python field names
    for row in currency_df.collect():
        currency = Currency(**row.asDict())
        print(f"Currency: {currency.code} - {currency.description}")
    
    spark.stop()


if __name__ == "__main__":
    print("=== CDM Model Usage Example ===")
    example_model_usage()
    
    print("\n=== Spark Schema Generation ===")
    example_spark_schema()
    
    # Uncomment to run Spark example
    # print("\n=== Spark DataFrame Example ===")
    # example_spark_dataframe()