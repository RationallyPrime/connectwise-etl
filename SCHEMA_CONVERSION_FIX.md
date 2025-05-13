# Sparkdantic Schema Conversion Fix

## Issue

When running the ConnectWise PSA to Microsoft Fabric ETL pipeline, the following error occurred when trying to create Spark DataFrames from the reference field models:

```
TypeConversionError: Error converting field `company` to PySpark type
```

This error happens in the sparkdantic library when it tries to convert nested `ReferenceModel` fields (like the `company` field in the Agreement model) to PySpark types. The error occurs at line 208 in `sparkdantic/model.py` in the `create_json_spark_schema` function.

## Analysis

The issue is related to how sparkdantic handles nested SparkModel instances during schema creation. When a model contains fields that are other SparkModel instances (like our reference types), sparkdantic tries to recursively convert them to Spark schema types, but it's failing with the error.

In our case, we're using a lot of reference fields (like `CompanyReference`, `ContactReference`, etc.) that are all subclasses of our `ReferenceModel` base class. The error occurs when sparkdantic tries to convert these nested models.

## Fix

In version 0.2.2, we've implemented the following fix:

1. Enhanced the `ReferenceModel` class with a custom `model_spark_schema` method that:
   - Creates a simple, predefined StructType schema for all reference models
   - Includes caching to avoid potential recursion issues
   - Ensures consistent schema definition across all reference type subclasses

2. The custom schema for reference models includes three fields:
   - `id` (IntegerType, not nullable)
   - `name` (StringType, nullable)
   - `identifier` (StringType, nullable)

3. This approach ensures that all reference field types have a well-defined, compatible schema that sparkdantic can use without encountering conversion errors.

## Code Changes

We added the following to the `ReferenceModel` class:

```python
# Class variable to cache the Spark schema
# This helps sparkdantic avoid recursion issues
_spark_schema_cache: ClassVar[Dict[str, StructType]] = {}

@classmethod
def model_spark_schema(
    cls,
    safe_casting: bool = False,
    by_alias: bool = True,
    mode: str = 'validation',
    exclude_fields: bool = False,
) -> StructType:
    """
    Custom implementation of model_spark_schema to handle reference models correctly.
    
    This method creates a simple StructType schema for reference models
    to avoid the TypeConversionError in sparkdantic.
    """
    # Check if we have a cached schema for this class
    class_name = cls.__name__
    if class_name in cls._spark_schema_cache:
        return cls._spark_schema_cache[class_name]
        
    # Create a simple schema for reference models
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("identifier", StringType(), True)
    ])
    
    # Cache the schema for future use
    cls._spark_schema_cache[class_name] = schema
    
    return schema
```

## Impact

- The ETL pipeline can now successfully create Spark DataFrames from models with nested reference fields
- Schema conversion is more robust and predictable for all reference field types
- The fix avoids complex dependencies on sparkdantic's internal conversion logic
- Schema caching improves performance for models with many reference fields

## Updated Package

The updated package is available as `fabric_api-0.2.2-py3-none-any.whl` in the `dist/` directory.

## Testing

The fix has been tested with the `Agreement` model that contains multiple reference fields, including `company`, `contact`, and other reference types. The schema generation now works correctly for all these nested fields.