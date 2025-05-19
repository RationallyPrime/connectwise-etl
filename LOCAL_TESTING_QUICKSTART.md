# Local Testing Quick Start Guide

This guide helps you quickly test the PSA/BC unified lakehouse locally before deploying to Microsoft Fabric.

## 1. Setup Local Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements-dev.txt

# Install project in development mode
pip install -e .
```

## 2. Generate Models

### Generate PSA Models from OpenAPI
```bash
python regenerate_models.py \
    --schema PSA_OpenAPI_schema.json \
    --output-dir fabric_api/connectwise_models
```

### Generate BC Models from CDM
```bash
python fabric_api/generate_bc_models_camelcase.py \
    --input-dir cdm_manifests \
    --output fabric_api/bc_models/models.py
```

## 3. Run Quick Tests

### Test Model Validation
```bash
# Test individual model validation
python -m pytest tests/test_model_validation.py -v

# Test specific model
python -m pytest tests/test_model_validation.py::TestPSAModels::test_time_entry_validation -v
```

### Test Pipeline Components
```bash
# Test pipeline with local Spark
python -m pytest tests/test_pipeline_local.py -v

# Test specific component
python -m pytest tests/test_pipeline_local.py::TestLocalPipeline::test_bronze_layer_creation -v
```

## 4. Run Complete Test Suite

```bash
# Run all tests with detailed report
python tests/run_local_tests.py
```

## 5. Test Individual Components

### Test Model Schema Generation
```python
# test_model_schemas.py
from fabric_api.connectwise_models import TimeEntry
from fabric_api.bc_models import Customer

# Check PSA schema
time_schema = TimeEntry.model_spark_schema()
print(f"TimeEntry fields: {[f.name for f in time_schema.fields]}")

# Check BC schema
customer_schema = Customer.model_spark_schema()
print(f"Customer fields: {[f.name for f in customer_schema.fields]}")
```

### Test Data Validation
```python
# test_data_validation.py
from fabric_api.connectwise_models import TimeEntry

# Valid data
valid_data = {
    "id": 123,
    "memberId": 456,
    "actualHours": 8.5,
    "hourlyRate": 150.0
}

try:
    time_entry = TimeEntry(**valid_data)
    print(f"✅ Valid: Revenue = ${time_entry.actualHours * time_entry.hourlyRate}")
except Exception as e:
    print(f"❌ Error: {e}")

# Invalid data
invalid_data = {
    "id": "not_a_number",
    "memberId": 456
}

try:
    time_entry = TimeEntry(**invalid_data)
    print("✅ Should not get here")
except Exception as e:
    print(f"✅ Expected error: {e}")
```

### Test Local Pipeline
```python
# test_local_pipeline.py
from pyspark.sql import SparkSession
import tempfile

# Create local Spark
spark = SparkSession.builder \
    .appName("LocalTest") \
    .master("local[*]") \
    .getOrCreate()

# Test data
test_data = [
    {"id": 1, "memberId": 100, "actualHours": 8.0},
    {"id": 2, "memberId": 101, "actualHours": 7.5}
]

# Create DataFrame
df = spark.createDataFrame(test_data)

# Test transformation
transformed_df = df.filter(df.actualHours > 0) \
    .withColumn("revenue", df.actualHours * 150.0)

# Verify results
result = transformed_df.collect()
print(f"Records: {len(result)}")
print(f"Total revenue: {sum(r.revenue for r in result)}")

spark.stop()
```

## 6. Common Test Scenarios

### Schema Evolution Testing
```bash
# Test backward compatibility
python -m pytest tests/test_model_validation.py::TestSchemaEvolution -v
```

### Performance Testing
```bash
# Test with larger datasets
python -m pytest tests/test_pipeline_local.py::TestPerformance -v
```

### Integration Testing
```bash
# Test component integration
python -m pytest tests/test_pipeline_local.py::TestLocalPipeline::test_integrated_view_query -v
```

## 7. Pre-Deployment Checklist

Run this before deploying to Fabric:

```bash
# 1. Verify models exist
ls -la fabric_api/connectwise_models/models.py
ls -la fabric_api/bc_models/models.py

# 2. Run validation tests
python -m pytest tests/test_model_validation.py

# 3. Test pipeline components
python -m pytest tests/test_pipeline_local.py

# 4. Run full test suite
python tests/run_local_tests.py

# 5. Check for any TODO or FIXME
grep -r "TODO\|FIXME" fabric_api/

# 6. Verify no hardcoded paths
grep -r "/home/\|C:\\\\" fabric_api/
```

## 8. Troubleshooting

### Spark Issues
```bash
# Set Spark environment
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_HOME=/path/to/spark

# Check Spark version
python -c "import pyspark; print(pyspark.__version__)"
```

### Model Generation Issues
```bash
# Debug model generation
python regenerate_models.py --entities TimeEntry --debug

# Check generated schema
python -c "from fabric_api.connectwise_models import TimeEntry; print(TimeEntry.schema())"
```

### Test Failures
```bash
# Run with detailed output
pytest tests/test_model_validation.py -vv -s

# Run with debugging
pytest tests/test_model_validation.py --pdb
```

## Summary

1. **Setup**: Install dependencies and generate models
2. **Unit Tests**: Test models and components individually  
3. **Integration Tests**: Test pipeline components together
4. **Full Suite**: Run complete test suite with `run_local_tests.py`
5. **Deploy**: Once all tests pass, deploy to Fabric

The local testing ensures your code is ready for Microsoft Fabric without surprises!