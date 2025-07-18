#!/usr/bin/env python3
"""Test script to verify the flatten_max_depth fix."""

import sys
import os

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from unified_etl_core.config.entity import EntityConfig
from unified_etl_core.silver import flatten_nested_columns
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def test_flatten_max_depth_validation():
    """Test that flatten_max_depth is properly validated."""
    
    # Test 1: Valid integer value
    try:
        config = EntityConfig(
            name="test",
            source="test",
            model_class_name="TestModel",
            flatten_nested=True,
            flatten_max_depth=3,  # integer
            preserve_columns=[],
            column_mappings={},
            json_columns=[],
            business_keys=["id"],
            scd=None,
            add_audit_columns=True,
            strip_null_columns=True
        )
        print("✅ Integer flatten_max_depth: PASSED")
    except Exception as e:
        print(f"❌ Integer flatten_max_depth: FAILED - {e}")
    
    # Test 2: String value that can be converted to int
    try:
        config = EntityConfig(
            name="test",
            source="test",
            model_class_name="TestModel",
            flatten_nested=True,
            flatten_max_depth="3",  # string
            preserve_columns=[],
            column_mappings={},
            json_columns=[],
            business_keys=["id"],
            scd=None,
            add_audit_columns=True,
            strip_null_columns=True
        )
        print("✅ String flatten_max_depth (convertible): PASSED")
        print(f"   Converted value: {config.flatten_max_depth} (type: {type(config.flatten_max_depth)})")
    except Exception as e:
        print(f"❌ String flatten_max_depth (convertible): FAILED - {e}")
    
    # Test 3: Invalid string value
    try:
        config = EntityConfig(
            name="test",
            source="test",
            model_class_name="TestModel",
            flatten_nested=True,
            flatten_max_depth="invalid",  # invalid string
            preserve_columns=[],
            column_mappings={},
            json_columns=[],
            business_keys=["id"],
            scd=None,
            add_audit_columns=True,
            strip_null_columns=True
        )
        print("❌ Invalid string flatten_max_depth: FAILED - Should have raised error")
    except ValueError as e:
        print("✅ Invalid string flatten_max_depth: PASSED - Correctly raised ValueError")
        print(f"   Error message: {e}")
    except Exception as e:
        print(f"❌ Invalid string flatten_max_depth: FAILED - Wrong exception type: {e}")

def test_flatten_function_validation():
    """Test that the flatten_nested_columns function handles type conversion."""
    
    # Create a simple Spark session
    spark = SparkSession.builder.appName("FlattenTest").master("local").getOrCreate()
    
    # Create a simple DataFrame with nested structure
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("nested", StructType([
            StructField("field1", StringType(), True),
            StructField("field2", StringType(), True)
        ]), True)
    ])
    
    df = spark.createDataFrame([
        (1, {"field1": "value1", "field2": "value2"})
    ], schema)
    
    # Test 1: Integer max_depth
    try:
        result = flatten_nested_columns(df, 3)
        print("✅ flatten_nested_columns with integer max_depth: PASSED")
    except Exception as e:
        print(f"❌ flatten_nested_columns with integer max_depth: FAILED - {e}")
    
    # Test 2: String max_depth (should be converted)
    try:
        result = flatten_nested_columns(df, "3")
        print("✅ flatten_nested_columns with string max_depth: PASSED")
    except Exception as e:
        print(f"❌ flatten_nested_columns with string max_depth: FAILED - {e}")
    
    # Test 3: Invalid max_depth
    try:
        result = flatten_nested_columns(df, "invalid")
        print("❌ flatten_nested_columns with invalid max_depth: FAILED - Should have raised error")
    except Exception as e:
        print("✅ flatten_nested_columns with invalid max_depth: PASSED - Correctly raised error")
        print(f"   Error: {e}")
    
    spark.stop()

if __name__ == "__main__":
    print("Testing flatten_max_depth validation fixes...")
    print("=" * 60)
    
    print("\n1. Testing EntityConfig validation:")
    test_flatten_max_depth_validation()
    
    print("\n2. Testing flatten_nested_columns function:")
    test_flatten_function_validation()
    
    print("\n" + "=" * 60)
    print("Test completed!")