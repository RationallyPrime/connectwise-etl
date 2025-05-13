# ConnectWise Model Improvements

This document outlines the improvements made to the ConnectWise entity models to address validation errors and improve compatibility with Microsoft Fabric.

## Summary of Improvements

1. **Pydantic v2 Compatibility**
   - Created a `ReferenceModel` class to replace `RootModel` for better Spark compatibility
   - Changed `allow_population_by_field_name` to `validate_by_name`
   - Updated serialization methods to use `model_dump()` instead of `dict()`

2. **Optional Field Handling**
   - Made all non-required fields properly optional with `| None` syntax and `default=None`
   - Added default values for all optional fields to prevent validation errors
   - Ensured nested reference fields are properly marked as optional when appropriate

3. **Type Enhancements**
   - Added proper enum classes for enumeration fields (BillTime, ApplicationCycle, etc.)
   - Improved datetime handling for date fields
   - Fixed boolean field types (previously some were incorrectly handled as integers)

4. **Reference Types**
   - Created a specialized `ReferenceModel` class that extends `SparkModel` and handles various reference formats
   - Added validator logic to handle different API response structures for references
   - Ensured reference types have a stable schema for Spark compatibility

5. **Datamodel Generation**
   - Improved model generation script with better handling of complex nested types
   - Added post-processing to fix Pydantic v2 compatibility issues
   - Maintained camelCase field names to match the API directly

## Key Fixes for Schema Issues

### 1. ReferenceModel Implementation

We created a dedicated `ReferenceModel` class to replace the `RootModel` approach:

```python
class ReferenceModel(SparkModel):
    """
    Base class for ConnectWise reference fields.
    """
    class Config:
        validate_by_name = True
    
    id: int
    name: Optional[str] = None
    identifier: Optional[str] = None
    
    @model_validator(mode='before')
    @classmethod
    def parse_reference(cls, data: Any) -> Dict[str, Any]:
        """
        Pre-validate the reference data to handle various formats.
        """
        if not isinstance(data, dict):
            if data is None:
                return {"id": 0}
            return {"id": data}
            
        if "id" not in data:
            data["id"] = 0
            
        return data
```

This provides several advantages:
- Defines a concrete schema that Spark can understand (unlike Any)
- Handles different reference formats from the API
- Properly validates and transforms data
- Maintains compatibility with Spark's schema generation

### 2. Specialized Reference Types

All reference fields (like CompanyReference, SiteReference) now use this pattern:

```python
class CompanyReference(ReferenceModel):
    """Reference to a Company"""
    pass
```

This maintains type safety while ensuring schema compatibility.

## Benefits of the Improvements

1. **Reduced Validation Errors**: By properly marking fields as optional with defaults, validation errors for missing fields are greatly reduced.

2. **Better Spark Compatibility**: The improved models work better with Spark's schema inference by using proper types and concrete schemas.

3. **Simpler API Integration**: The camelCase field names match the API directly, reducing transformation errors.

4. **More Robust Reference Handling**: The new ReferenceModel approach handles variations in reference object structure while maintaining schema compatibility.

## Before and After Comparison

### Before:
```python
class CompanyReference(RootModel, SparkModel):
    class Config:
        validate_by_name = True

    root: Any  # Causes schema generation issues in Spark
```

### After:
```python
class CompanyReference(ReferenceModel):
    """Reference to a Company"""
    pass

# With ReferenceModel defined as:
class ReferenceModel(SparkModel):
    class Config:
        validate_by_name = True
    
    id: int
    name: Optional[str] = None
    identifier: Optional[str] = None
    
    # Plus validator logic to handle different formats
```

## Testing

The models have been tested with sample data from the API and successfully validate both Agreement and PostedInvoice entities, which were previously failing validation. The schema generation also works correctly with Spark, enabling DataFrame creation and write operations.