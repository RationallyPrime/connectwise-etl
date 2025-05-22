#!/usr/bin/env python
"""Test pipeline functionality locally before Fabric deployment."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

# Mock Fabric environment
import os

os.environ["SPARK_HOME"] = "/usr/local/spark"  # Adjust for your environment

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


@pytest.fixture(scope="session")
def spark():
    """Create local Spark session for testing."""
    spark = (
        SparkSession.builder.appName("LocalPipelineTest")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    yield spark
    spark.stop()


@pytest.fixture
def test_data():
    """Generate test data for pipeline."""
    return {
        "time_entries": [
            {"id": 1, "memberId": 100, "actualHours": 8.0, "hourlyRate": 150.0},
            {"id": 2, "memberId": 101, "actualHours": 7.5, "hourlyRate": 175.0},
            {"id": 3, "memberId": 100, "actualHours": 6.0, "hourlyRate": 150.0},
        ],
        "agreements": [
            {"id": 1, "agreementNumber": "AGR-001", "name": "Support Agreement"},
            {"id": 2, "agreementNumber": "AGR-002", "name": "Development Agreement"},
        ],
        "customers": [
            {"No": "CUST001", "Name": "Test Customer 1", "CustomerPostingGroup": "DOMESTIC"},
            {"No": "CUST002", "Name": "Test Customer 2", "CustomerPostingGroup": "FOREIGN"},
        ],
        "gl_entries": [
            {"EntryNo": 1, "Amount": 1000.0, "GLAccountNo": "1100", "PostingDate": "2024-01-01"},
            {"EntryNo": 2, "Amount": -500.0, "GLAccountNo": "2100", "PostingDate": "2024-01-02"},
        ],
    }


class TestLocalPipeline:
    """Test pipeline components locally."""

    def test_bronze_layer_creation(self, spark, test_data):
        """Test creating bronze layer with test data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create bronze tables
            bronze_path = Path(tmpdir) / "bronze"

            # PSA tables
            psa_path = bronze_path / "psa"
            time_df = spark.createDataFrame(test_data["time_entries"])
            time_df.write.mode("overwrite").parquet(str(psa_path / "TimeEntry"))

            agreement_df = spark.createDataFrame(test_data["agreements"])
            agreement_df.write.mode("overwrite").parquet(str(psa_path / "Agreement"))

            # BC tables
            bc_path = bronze_path / "bc"
            customer_df = spark.createDataFrame(test_data["customers"])
            customer_df.write.mode("overwrite").parquet(str(bc_path / "Customer18"))

            gl_df = spark.createDataFrame(test_data["gl_entries"])
            gl_df.write.mode("overwrite").parquet(str(bc_path / "GLEntry17"))

            # Verify tables created
            assert (psa_path / "TimeEntry").exists()
            assert (bc_path / "Customer18").exists()

            # Read and verify data
            read_time_df = spark.read.parquet(str(psa_path / "TimeEntry"))
            assert read_time_df.count() == 3

    def test_bronze_to_silver_transformation(self, spark, test_data):
        """Test transforming bronze to silver."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create bronze data
            bronze_df = spark.createDataFrame(test_data["time_entries"])

            # Simulate transformation
            # 1. Add metadata
            silver_df = bronze_df.withColumn("etlTimestamp", F.current_timestamp())
            silver_df = silver_df.withColumn("etlEntity", F.lit("TimeEntry"))

            # 2. Type casting (simulating model validation)
            silver_df = silver_df.withColumn("id", F.col("id").cast(IntegerType())).withColumn(
                "actualHours", F.col("actualHours").cast(DoubleType())
            )

            # 3. Data quality checks
            silver_df = silver_df.filter(F.col("actualHours") > 0)

            # Write to silver
            silver_path = Path(tmpdir) / "silver" / "psa" / "TimeEntry"
            silver_df.write.mode("overwrite").parquet(str(silver_path))

            # Verify transformation
            read_silver_df = spark.read.parquet(str(silver_path))
            assert read_silver_df.count() == 3
            assert "etlTimestamp" in read_silver_df.columns
            assert read_silver_df.schema["id"].dataType == IntegerType()

    def test_conformed_dimension_creation(self, spark, test_data):
        """Test creating conformed dimensions."""
        # Create employee dimension from PSA time entries
        time_df = spark.createDataFrame(test_data["time_entries"])

        # Extract unique employees
        employee_dim = time_df.select("memberId").distinct()
        employee_dim = employee_dim.withColumn(
            "employee_key", F.row_number().over(Window.orderBy("memberId"))
        )
        employee_dim = employee_dim.withColumn(
            "employee_name", F.concat(F.lit("Employee "), F.col("memberId"))
        )

        # Add BC mapping (simulated)
        employee_dim = employee_dim.withColumn(
            "bc_resource_no", F.concat(F.lit("RES"), F.col("memberId"))
        )

        assert employee_dim.count() == 2  # Two unique employees
        assert "employee_key" in employee_dim.columns
        assert "bc_resource_no" in employee_dim.columns

    def test_fact_table_creation(self, spark, test_data):
        """Test creating fact tables."""
        # Create unified line items from time entries
        time_df = spark.createDataFrame(test_data["time_entries"])

        # Calculate revenue
        fact_df = time_df.withColumn("revenue", F.col("actualHours") * F.col("hourlyRate"))
        fact_df = fact_df.withColumn("cost", F.lit(0))  # Time entries have no direct cost
        fact_df = fact_df.withColumn("margin", F.col("revenue") - F.col("cost"))

        # Add dimension keys (simulated)
        fact_df = fact_df.withColumn("employee_key", F.col("memberId"))  # Would be proper lookup
        fact_df = fact_df.withColumn("date_key", F.lit(20240101))  # Would be proper date

        # Verify calculations
        total_revenue = fact_df.agg(F.sum("revenue")).collect()[0][0]
        expected_revenue = (8.0 * 150.0) + (7.5 * 175.0) + (6.0 * 150.0)
        assert abs(total_revenue - expected_revenue) < 0.01

    def test_integrated_view_query(self, spark, test_data):
        """Test querying integrated views."""
        # Create sample integrated data
        psa_data = spark.createDataFrame(
            [
                {"employee_key": 1, "revenue": 1000.0, "hours": 8.0, "source": "PSA"},
                {"employee_key": 2, "revenue": 1500.0, "hours": 10.0, "source": "PSA"},
            ]
        )

        bc_data = spark.createDataFrame(
            [
                {"employee_key": 1, "amount": 2000.0, "source": "BC"},
                {"employee_key": 3, "amount": 1000.0, "source": "BC"},
            ]
        )

        # Create unified view
        unified = psa_data.join(bc_data, "employee_key", "full_outer")
        unified = unified.select(
            "employee_key",
            F.coalesce(F.col("revenue"), F.lit(0)).alias("psa_revenue"),
            F.coalesce(F.col("amount"), F.lit(0)).alias("bc_amount"),
            F.coalesce(F.col("revenue"), F.lit(0))
            + F.coalesce(F.col("amount"), F.lit(0)).alias("total_amount"),
        )

        # Query results
        results = unified.filter(F.col("total_amount") > 1500).collect()
        assert len(results) == 2


class TestMockAPI:
    """Test with mocked API responses."""

    @patch("fabric_api.client.ConnectWiseClient")
    def test_mock_api_extraction(self, mock_client):
        """Test API extraction with mocks."""
        # Mock API responses
        mock_instance = Mock()
        mock_client.return_value = mock_instance

        # Mock get_data method
        mock_instance.get_data.return_value = [
            {"id": 1, "memberId": 100, "actualHours": 8.0},
            {"id": 2, "memberId": 101, "actualHours": 7.5},
        ]

        # Test extraction

        # This would normally call the API
        # Here we're testing the mock works
        client = mock_client()
        data = client.get_data("time/entries")

        assert len(data) == 2
        assert data[0]["id"] == 1

    def test_error_handling(self):
        """Test error handling in pipeline."""
        # Test with invalid data
        invalid_data = [
            {"id": "not_a_number", "memberId": 100},  # Invalid ID type
            {"id": 2, "memberId": "not_a_number"},  # Invalid member ID
        ]

        valid_records = []
        error_records = []

        for record in invalid_data:
            try:
                # Simulate validation
                if not isinstance(record["id"], int):
                    raise ValueError(f"Invalid ID: {record['id']}")
                if not isinstance(record["memberId"], int):
                    raise ValueError(f"Invalid memberId: {record['memberId']}")
                valid_records.append(record)
            except Exception as e:
                error_records.append({"record": record, "error": str(e)})

        assert len(valid_records) == 0
        assert len(error_records) == 2


class TestSchemaManagement:
    """Test schema management and evolution."""

    def test_schema_merge(self, spark):
        """Test schema merging capability."""
        schema_v1 = StructType(
            [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
        )

        schema_v2 = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),  # New field
            ]
        )

        # Create DataFrames with different schemas
        df_v1 = spark.createDataFrame([(1, "John")], schema_v1)
        df_v2 = spark.createDataFrame([(2, "Jane", "jane@example.com")], schema_v2)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Write v1
            df_v1.write.mode("overwrite").parquet(f"{tmpdir}/data")

            # Write v2 with schema merge
            df_v2.write.mode("append").option("mergeSchema", "true").parquet(f"{tmpdir}/data")

            # Read merged schema
            merged_df = spark.read.parquet(f"{tmpdir}/data")

            # Verify schema evolution
            assert "email" in merged_df.columns
            assert merged_df.count() == 2


class TestPerformance:
    """Test performance with larger datasets."""

    def test_large_dataset_processing(self, spark):
        """Test processing larger datasets."""
        # Generate larger dataset
        num_records = 10000
        large_data = [
            {"id": i, "memberId": i % 100, "actualHours": 8.0, "hourlyRate": 150.0 + (i % 50)}
            for i in range(num_records)
        ]

        # Create DataFrame
        df = spark.createDataFrame(large_data)

        # Test aggregation performance
        import time

        start_time = time.time()

        # Perform aggregations
        result = (
            df.groupBy("memberId")
            .agg(F.sum("actualHours").alias("total_hours"), F.avg("hourlyRate").alias("avg_rate"))
            .collect()
        )

        end_time = time.time()
        duration = end_time - start_time

        assert len(result) == 100  # 100 unique members
        assert duration < 5  # Should complete quickly
        print(f"Processed {num_records} records in {duration:.2f} seconds")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
