#!/usr/bin/env python3
"""Test script to show Gold layer schema produced by the unified ETL framework."""

import sys
from datetime import datetime
from typing import Any, Dict, List

# Mock spark session for testing without Fabric
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType, LongType
)

# Import the gold layer transforms
from unified_etl_connectwise.transforms import (
    create_time_entry_fact,
    create_invoice_line_fact,
    create_expense_entry_fact
)
from unified_etl_core.gold import generate_surrogate_key, add_etl_metadata
from unified_etl_core.date_utils import generate_date_dimension


def create_mock_spark_session():
    """Create a local Spark session for testing."""
    return SparkSession.builder \
        .appName("Gold Layer Schema Test") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def create_sample_silver_data(spark: SparkSession):
    """Create sample silver layer data for testing."""
    
    # Sample time entry data
    time_entries = [
        {
            "id": 1,
            "companyReconcileId": "CMP001",
            "memberReconcileId": "MEM001", 
            "dateEntered": "2024-01-15T10:00:00Z",
            "chargeToType": "ServiceTicket",
            "billableOption": "Billable",
            "actualHours": 2.5,
            "hourlyRate": 150.0,
            "agreementId": 100,
            "agreementType": "yÞjónusta",
            "workTypeId": 10,
            "workType": "Development",
            "notes": "Fixed bug in payment module"
        },
        {
            "id": 2,
            "companyReconcileId": "CMP002",
            "memberReconcileId": "MEM002",
            "dateEntered": "2024-01-16T14:00:00Z", 
            "chargeToType": "Project",
            "billableOption": "DoNotBill",
            "actualHours": 1.0,
            "hourlyRate": 0.0,
            "agreementId": 200,
            "agreementType": "Innri verkefni",
            "workTypeId": 20,
            "workType": "Internal",
            "notes": "Team meeting"
        }
    ]
    
    # Sample invoice line data
    invoice_lines = [
        {
            "id": 1,
            "invoiceId": 1000,
            "lineNumber": 1,
            "productReconcileId": "PRD001",
            "description": "Development Services",
            "quantity": 10.0,
            "price": 150.0,
            "cost": 0.0,
            "agreementId": 100,
            "ticketId": 500,
            "projectId": 0,  # Changed from None to 0
            "taxable": True
        }
    ]
    
    # Sample expense entry data  
    expense_entries = [
        {
            "id": 1,
            "companyReconcileId": "CMP001",
            "memberReconcileId": "MEM001",
            "expenseReportId": 50,
            "chargeToType": "ServiceTicket",
            "chargeToId": 500,
            "date": "2024-01-15",
            "amount": 250.0,
            "billableOption": "Billable",
            "agreementId": 100,
            "agreementType": "yÞjónusta"
        }
    ]
    
    # Convert to DataFrames
    df_time_entries = spark.createDataFrame(time_entries)
    df_invoice_lines = spark.createDataFrame(invoice_lines)
    df_expense_entries = spark.createDataFrame(expense_entries)
    
    return df_time_entries, df_invoice_lines, df_expense_entries


def print_schema_info(df_name: str, df):
    """Pretty print schema information for a DataFrame."""
    print(f"\n{'='*60}")
    print(f"Gold Layer Schema: {df_name}")
    print(f"{'='*60}")
    
    # Get schema
    schema = df.schema
    
    # Print columns with types
    print("\nColumns:")
    for field in schema.fields:
        nullable = "nullable" if field.nullable else "required"
        print(f"  - {field.name}: {field.dataType.simpleString()} ({nullable})")
    
    # Show sample data
    print(f"\nSample Data ({df.count()} rows):")
    df.show(truncate=False)
    
    # Print DDL
    print("\nDDL Schema:")
    print(df.schema.simpleString())


def main():
    """Main function to demonstrate Gold layer schema."""
    print("Gold Layer Schema Test")
    print("=" * 60)
    
    # Create Spark session
    spark = create_mock_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Create sample silver data
        df_time, df_invoice, df_expense = create_sample_silver_data(spark)
        
        # Generate date dimension
        print("\nGenerating date dimension...")
        df_date = generate_date_dimension(
            spark=spark,
            start_date="2024-01-01", 
            end_date="2024-12-31"
        )
        print_schema_info("dim_date", df_date)
        
        # Create time entry fact
        print("\nCreating time entry fact...")
        df_time_fact = create_time_entry_fact(
            spark=spark,
            time_entry_df=df_time,
            agreement_df=None,  # Would come from silver layer
            member_df=None,  # Would come from silver layer
            config={}
        )
        print_schema_info("fact_time_entry", df_time_fact)
        
        # Create invoice line fact
        print("\nCreating invoice line fact...")
        df_invoice_fact = create_invoice_line_fact(
            df=df_invoice,
            invoice_dim_df=None,  # Would come from silver layer
            product_dim_df=None,  # Would come from silver layer
            agreement_dim_df=None  # Would come from silver layer
        )
        print_schema_info("fact_invoice_line", df_invoice_fact)
        
        # Create expense entry fact
        print("\nCreating expense entry fact...")
        df_expense_fact = create_expense_entry_fact(
            df=df_expense,
            date_dim_df=df_date,
            member_dim_df=None,  # Would come from silver layer  
            company_dim_df=None,  # Would come from silver layer
            expense_report_dim_df=None,  # Would come from silver layer
            agreement_dim_df=None  # Would come from silver layer
        )
        print_schema_info("fact_expense_entry", df_expense_fact)
        
        # Show how surrogate keys are added
        print("\nDemonstrating surrogate key generation...")
        df_with_sk = generate_surrogate_key(
            df=df_time,
            business_keys=["companyReconcileId", "memberReconcileId", "id"],
            surrogate_key_name="time_entry_key"
        )
        print("\nSurrogate keys added:")
        df_with_sk.select("time_entry_key", "companyReconcileId", "memberReconcileId", "id").show()
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()