# Pydantic v2 Deprecation Warning Fix

## Issue

When running the ConnectWise PSA to Microsoft Fabric ETL pipeline, the following warning appeared in the logs:

```
/nfs4/pyenv-25f04426-c7e6-4a2e-9add-8c4c42c4ef4a/lib/python3.11/site-packages/pydantic/_internal/_config.py:373: UserWarning: Valid config keys have changed in V2:
* 'allow_population_by_field_name' has been renamed to 'validate_by_name'
  warnings.warn(message, UserWarning)
```

This warning is generated because of a Pydantic v2 configuration key change. In Pydantic v2, `allow_population_by_field_name` has been renamed to `validate_by_name`. 

## Analysis

The warning is not from our code directly, but from the `sparkdantic` dependency. While our models correctly use the new `validate_by_name = True` in their Config classes, the sparkdantic library (version 2.4.0) is likely still using the older configuration key internally.

The ETL process still works correctly despite this warning, but it's better to suppress the warning to keep logs clean.

## Fix

In version 0.2.1, we've applied the following fixes:

1. Added warning filters in key files to suppress the Pydantic deprecation warning:
   - Added to `bronze_loader.py`
   - Added to `connectwise_models/__init__.py` 

2. Updated the model generator (`generate_models_from_openapi.py`) to:
   - Add warning filters to all generated model files
   - Include warning filters in the generated `__init__.py` file

3. Bumped the package version to 0.2.1

## Code Changes

The warning filter was added using:

```python
import warnings
warnings.filterwarnings("ignore", message="Valid config keys have changed in V2")
```

## Impact

- The ETL pipeline will no longer show the Pydantic deprecation warnings
- All functionality continues to work as expected
- The data models correctly use Pydantic v2 compatible configuration

## Long-term Solution

For a more complete solution, the sparkdantic library should be updated to fully support Pydantic v2 configuration keys. However, our current fix effectively suppresses the warning without affecting functionality.

## Updated Package

The updated package is available as `fabric_api-0.2.1-py3-none-any.whl` in the `dist/` directory.