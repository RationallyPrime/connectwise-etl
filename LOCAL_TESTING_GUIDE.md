# Local Testing Guide - PSA/BC Unified Lakehouse

This guide shows how to test the unified lakehouse architecture locally before deploying to Microsoft Fabric.

## 1. Local Development Environment Setup

### Prerequisites
```bash
# Python 3.11+
python --version

# Install dependencies
pip install uv
uv pip install -e ".[dev]"

# Key packages needed locally
pip install pyspark pandas pyarrow pydantic sparkdantic pytest pytest-mock
```

### Local Spark Setup
```python
# local_spark_setup.py
from pyspark.sql import SparkSession
import os

def get_local_spark():
    """Create local Spark session for testing."""
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
    
    spark = SparkSession.builder \
        .appName("LocalLakehouseTest") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.warehouse.dir", "./spark-warehouse") \
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    return spark
```

## 2. Model Testing

### Test Model Generation
```python
# tests/test_model_generation.py
import pytest
from pathlib import Path
import json
import tempfile

def test_psa_model_generation():
    """Test PSA model generation from OpenAPI."""
    # Create minimal OpenAPI schema for testing
    test_schema = {
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0"},
        "components": {
            "schemas": {
                "TimeEntry": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "memberId": {"type": "integer"},
                        "actualHours": {"type": "number"}
                    }
                }
            }
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json') as f:
        json.dump(test_schema, f)
        f.flush()
        
        # Test generation
        from regenerate_models import generate_models
        output_dir = Path("./test_models")
        generate_models({"TimeEntry": {}}, f.name, str(output_dir))
        
        # Verify generated model
        assert (output_dir / "models.py").exists()
        
def test_bc_model_generation():
    """Test BC model generation from CDM."""
    # Create minimal CDM manifest for testing
    test_cdm = {
        "definitions": [{
            "entityName": "Customer-18",
            "displayName": "Customer",
            "hasAttributes": [{
                "name": "No-18",
                "dataFormat": "String",
                "maximumLength": 20
            }]
        }]
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.cdm.json') as f:
        json.dump(test_cdm, f)
        f.flush()
        
        # Test generation
        from fabric_api.generate_bc_models_camelcase import process_cdm_directory
        output_path = Path("./test_bc_models.py")
        process_cdm_directory(Path(f.name).parent, output_path)
        
        assert output_path.exists()
```

### Test Model Validation
```python
# tests/test_model_validation.py
import pytest
from pydantic import ValidationError
from fabric_api.connectwise_models import TimeEntry, Agreement
from fabric_api.bc_models import Customer, GLEntry

def test_psa_model_validation():
    """Test PSA model validation."""
    # Valid data
    valid_time = TimeEntry(
        id=123,
        memberId=456,
        actualHours=8.5
    )
    assert valid_time.id == 123
    
    # Invalid data (wrong type)
    with pytest.raises(ValidationError):
        TimeEntry(
            id="not_a_number",
            memberId=456
        )

def test_bc_model_validation():
    """Test BC model validation."""
    # Valid data
    valid_customer = Customer(
        No="CUST001",
        Name="Test Customer",
        Company="CRONUS"
    )
    assert valid_customer.No == "CUST001"
    
    # Test alias mapping
    assert Customer.model_fields["No"].alias == "No-18"

def test_spark_schema_generation():
    """Test Spark schema generation from models."""
    from pyspark.sql.types import StructType, StringType, IntegerType
    
    # PSA schema
    time_schema = TimeEntry.model_spark_schema()
    assert isinstance(time_schema, StructType)
    assert "id" in [f.name for f in time_schema.fields]
    
    # BC schema
    customer_schema = Customer.model_spark_schema()
    assert isinstance(customer_schema, StructType)
    assert "No" in [f.name for f in customer_schema.fields]
```

## 3. Pipeline Testing

### Mock API Testing
```python
# tests/test_pipeline_mocked.py
import pytest
from unittest.mock import Mock, patch
from fabric_api.pipeline_psa_enhanced_with_models import PSAPipeline
from fabric_api.connectwise_models import TimeEntry

@pytest.fixture
def mock_spark():
    """Create mock Spark session."""
    spark = Mock()
    spark.sql = Mock()
    spark.table = Mock()
    spark.createDataFrame = Mock()
    return spark

@pytest.fixture
def mock_client():
    """Create mock ConnectWise client."""
    client = Mock()
    # Mock API response
    client.get_data = Mock(return_value=[
        {"id": 1, "memberId": 100, "actualHours": 8.0},
        {"id": 2, "memberId": 101, "actualHours": 7.5}
    ])
    return client

def test_psa_pipeline_extraction(mock_spark, mock_client):
    """Test PSA pipeline extraction with mocked API."""
    pipeline = PSAPipeline(spark=mock_spark)
    
    with patch('fabric_api.extract.generic.extract_entity') as mock_extract:
        # Mock extract to return validated models
        mock_extract.return_value = ([
            TimeEntry(id=1, memberId=100, actualHours=8.0),
            TimeEntry(id=2, memberId=101, actualHours=7.5)
        ], [])
        
        result = pipeline.process_entity_to_bronze(
            entity_name="TimeEntry",
            client=mock_client,
            page_size=10,
            max_pages=1
        )
        
        assert result["entity"] == "TimeEntry"
        assert result["extracted"] == 2
        assert mock_extract.called

def test_bronze_to_silver_transformation(mock_spark):
    """Test bronze to silver transformation."""
    pipeline = PSAPipeline(spark=mock_spark)
    
    # Mock bronze table
    mock_df = Mock()
    mock_df.count.return_value = 100
    mock_df.columns = ["id", "memberId", "actualHours"]
    mock_spark.table.return_value = mock_df
    
    # Mock transformations
    mock_df.select.return_value = mock_df
    mock_df.withColumn.return_value = mock_df
    mock_df.write.mode.return_value.option.return_value.saveAsTable = Mock()
    
    result = pipeline.process_bronze_to_silver(
        entity_name="TimeEntry",
        mode="overwrite"
    )
    
    assert result["status"] == "success"
    assert result["bronze_rows"] == 100
    assert mock_spark.table.called_with("bronze.psa.TimeEntry")
```

### Local File System Testing
```python
# tests/test_local_filesystem.py
import pytest
import tempfile
from pathlib import Path
from pyspark.sql import SparkSession
from fabric_api.pipeline_psa_enhanced_with_models import PSAPipeline

@pytest.fixture
def local_spark():
    """Create local Spark session."""
    spark = SparkSession.builder \
        .appName("LocalTest") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
        .getOrCreate()
    yield spark
    spark.stop()

def test_local_bronze_to_silver(local_spark):
    """Test pipeline with local file system."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        test_data = [
            {"id": 1, "memberId": 100, "actualHours": 8.0},
            {"id": 2, "memberId": 101, "actualHours": 7.5}
        ]
        
        # Write to local parquet
        df = local_spark.createDataFrame(test_data)
        bronze_path = f"{tmpdir}/bronze/TimeEntry"
        df.write.mode("overwrite").parquet(bronze_path)
        
        # Create pipeline with local paths
        pipeline = PSAPipeline(spark=local_spark)
        
        # Override schema creation to use local paths
        pipeline.schemas = {
            "bronze": f"parquet.`{tmpdir}/bronze`",
            "silver": f"parquet.`{tmpdir}/silver`",
            "gold": f"parquet.`{tmpdir}/gold`"
        }
        
        # Test transformation
        # Would need to modify pipeline to work with local paths
        # This is a simplified example
```

## 4. Integration Testing

### Test Unified Orchestrator
```python
# tests/test_unified_orchestrator.py
import pytest
from unittest.mock import Mock, patch
from fabric_api.unified_lakehouse_orchestrator import UnifiedLakehouseOrchestrator

def test_orchestrator_initialization():
    """Test orchestrator setup."""
    mock_spark = Mock()
    mock_spark.sql = Mock()
    
    orchestrator = UnifiedLakehouseOrchestrator(spark=mock_spark)
    
    # Verify schemas created
    expected_schemas = [
        "bronze.psa", "bronze.bc",
        "silver.psa", "silver.bc",
        "gold.conformed", "gold.psa", "gold.bc", "gold.integrated"
    ]
    
    for schema in expected_schemas:
        mock_spark.sql.assert_any_call(f"CREATE SCHEMA IF NOT EXISTS {schema}")

def test_conformed_dimension_creation():
    """Test conformed dimension logic."""
    mock_spark = Mock()
    
    # Mock PSA and BC data
    psa_customers = Mock()
    psa_customers.select.return_value.distinct.return_value = Mock()
    
    bc_customers = Mock()
    
    with patch('fabric_api.gold.conformed_dimensions_enhanced.ConformedDimensionBuilder'):
        orchestrator = UnifiedLakehouseOrchestrator(spark=mock_spark)
        results = orchestrator.run_conformed_dimensions()
        
        assert isinstance(results, dict)
```

## 5. Schema Testing

### Test Schema Compatibility
```python
# tests/test_schema_compatibility.py
import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from fabric_api.connectwise_models import TimeEntry
from fabric_api.bc_models import Customer

def test_schema_evolution():
    """Test handling of schema changes."""
    # Original schema
    v1_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("memberId", IntegerType(), True)
    ])
    
    # New schema with additional field
    v2_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("memberId", IntegerType(), True),
        StructField("newField", StringType(), True)
    ])
    
    # Test compatibility
    v1_fields = set(f.name for f in v1_schema.fields)
    v2_fields = set(f.name for f in v2_schema.fields)
    
    assert v1_fields.issubset(v2_fields)  # v2 is backward compatible

def test_cross_system_mapping():
    """Test field mapping between PSA and BC."""
    # PSA field
    psa_member_field = "memberId"  # From TimeEntry
    
    # BC field
    bc_resource_field = "No"  # From Resource
    
    # Conformed dimension mapping
    mapping = {
        "psa_member_id": psa_member_field,
        "bc_resource_no": bc_resource_field,
        "employee_key": "generated_surrogate_key"
    }
    
    assert mapping["psa_member_id"] in TimeEntry.model_fields
```

## 6. Performance Testing

### Test with Sample Data
```python
# tests/test_performance.py
import time
import pytest
from fabric_api.pipeline_psa_enhanced_with_models import PSAPipeline

def test_pipeline_performance(local_spark):
    """Test pipeline performance with sample data."""
    # Generate test data
    num_records = 10000
    test_data = [
        {
            "id": i,
            "memberId": i % 100,
            "actualHours": 8.0,
            "hourlyRate": 150.0
        }
        for i in range(num_records)
    ]
    
    # Create DataFrame
    df = local_spark.createDataFrame(test_data)
    
    # Measure transformation time
    start_time = time.time()
    
    # Apply transformations
    transformed_df = df.filter(df.actualHours > 0) \
                      .groupBy("memberId") \
                      .sum("actualHours")
    
    # Force computation
    result_count = transformed_df.count()
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"Processed {num_records} records in {duration:.2f} seconds")
    assert duration < 10  # Should complete in under 10 seconds
```

## 7. Local Test Runner

### Create Test Suite
```python
# run_local_tests.py
#!/usr/bin/env python
"""Run all local tests before Fabric deployment."""

import subprocess
import sys
from pathlib import Path

def run_tests():
    """Run test suite."""
    test_categories = [
        ("Model Tests", "tests/test_model_*.py"),
        ("Pipeline Tests", "tests/test_pipeline_*.py"),
        ("Integration Tests", "tests/test_*_integration.py"),
        ("Schema Tests", "tests/test_schema_*.py"),
    ]
    
    all_passed = True
    
    for category, pattern in test_categories:
        print(f"\n{'='*50}")
        print(f"Running {category}")
        print('='*50)
        
        result = subprocess.run([
            sys.executable, "-m", "pytest", "-v", pattern
        ])
        
        if result.returncode != 0:
            all_passed = False
            print(f"❌ {category} failed")
        else:
            print(f"✅ {category} passed")
    
    return all_passed

if __name__ == "__main__":
    if run_tests():
        print("\n✅ All tests passed! Ready for Fabric deployment.")
    else:
        print("\n❌ Some tests failed. Fix issues before deployment.")
        sys.exit(1)
```

## 8. Mock Fabric Environment

### Create Fabric-like Test Environment
```python
# tests/mock_fabric_env.py
"""Mock Microsoft Fabric environment for testing."""

class MockFabricEnvironment:
    """Simulate Fabric lakehouse environment."""
    
    def __init__(self, base_path="/tmp/mock_lakehouse"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(exist_ok=True)
        
        # Create Fabric-like structure
        self.tables_path = self.base_path / "Tables"
        self.files_path = self.base_path / "Files"
        
        self.tables_path.mkdir(exist_ok=True)
        self.files_path.mkdir(exist_ok=True)
    
    def get_table_path(self, table_name):
        """Get Fabric-style table path."""
        return f"abfss://workspace@onelake.dfs.fabric.microsoft.com/{self.tables_path}/{table_name}"
    
    def create_bronze_table(self, entity_name, data):
        """Create mock bronze table."""
        table_path = self.tables_path / "bronze" / entity_name
        table_path.mkdir(parents=True, exist_ok=True)
        
        # Write parquet file
        df = spark.createDataFrame(data)
        df.write.mode("overwrite").parquet(str(table_path))
        
        return str(table_path)

# Usage in tests
def test_with_mock_fabric():
    """Test with mock Fabric environment."""
    fabric = MockFabricEnvironment()
    
    # Create test data
    test_data = [{"id": 1, "name": "Test"}]
    bronze_path = fabric.create_bronze_table("TestEntity", test_data)
    
    # Run pipeline with mock paths
    pipeline = PSAPipeline(spark=local_spark)
    # Override paths to use local mock
    pipeline.schemas = {
        "bronze": f"parquet.`{fabric.tables_path}/bronze`",
        "silver": f"parquet.`{fabric.tables_path}/silver`",
        "gold": f"parquet.`{fabric.tables_path}/gold`"
    }
```

## 9. Pre-Deployment Checklist

### Automated Checks
```python
# tests/pre_deployment_checks.py
"""Pre-deployment validation checks."""

def check_models_generated():
    """Verify all models are generated."""
    required_models = [
        "fabric_api/connectwise_models/models.py",
        "fabric_api/bc_models/models.py"
    ]
    
    for model_file in required_models:
        if not Path(model_file).exists():
            raise FileNotFoundError(f"Missing model file: {model_file}")
    
    print("✅ All models generated")

def check_dependencies():
    """Verify all dependencies are installed."""
    required_packages = [
        "pyspark",
        "pydantic",
        "sparkdantic",
        "pandas",
        "pyarrow"
    ]
    
    import pkg_resources
    
    for package in required_packages:
        try:
            pkg_resources.get_distribution(package)
            print(f"✅ {package} installed")
        except:
            print(f"❌ {package} missing")
            raise

def check_schema_compatibility():
    """Verify schema compatibility."""
    from fabric_api.connectwise_models import TimeEntry
    from fabric_api.bc_models import Customer
    
    # Test schema generation
    time_schema = TimeEntry.model_spark_schema()
    customer_schema = Customer.model_spark_schema()
    
    assert len(time_schema.fields) > 0
    assert len(customer_schema.fields) > 0
    
    print("✅ Schemas compatible")

def run_pre_deployment_checks():
    """Run all pre-deployment checks."""
    checks = [
        check_models_generated,
        check_dependencies,
        check_schema_compatibility
    ]
    
    for check in checks:
        check()
    
    print("\n🚀 Ready for Fabric deployment!")

if __name__ == "__main__":
    run_pre_deployment_checks()
```

## Summary

You can test extensively locally:

1. **Model Testing**: Validate generation, schema creation, and field mapping
2. **Unit Testing**: Test individual pipeline components with mocks
3. **Integration Testing**: Test component interactions
4. **Performance Testing**: Validate with sample data
5. **Schema Testing**: Ensure compatibility and evolution
6. **Mock Fabric Environment**: Simulate lakehouse structure locally

### Recommended Testing Flow:

```bash
# 1. Install dependencies
pip install -r requirements-dev.txt

# 2. Generate models
python regenerate_models.py
python fabric_api/generate_bc_models_camelcase.py

# 3. Run unit tests
pytest tests/test_model_*.py -v

# 4. Run integration tests
pytest tests/test_*_integration.py -v

# 5. Run performance tests
pytest tests/test_performance.py -v

# 6. Run pre-deployment checks
python tests/pre_deployment_checks.py

# 7. Full test suite
python run_local_tests.py
```

This approach ensures your code is thoroughly tested before deploying to Fabric, reducing the risk of issues in the production environment.