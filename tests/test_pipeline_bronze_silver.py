"""
Integration tests for the bronze-to-silver pipeline.

This validates that the pipeline correctly:
1. Extracts raw data to bronze
2. Transforms bronze to silver with flattening and column pruning
3. Handles invoice splitting (when implemented)
"""

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dotenv import load_dotenv

# Load environment variables from .env file
root_dir = Path(__file__).parent.parent
env_path = root_dir / ".env"
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Import our modules
from fabric_api.client import ConnectWiseClient
from fabric_api.pipeline import (
    process_entity_to_bronze,
    process_bronze_to_silver,
    run_full_pipeline,
    COLUMN_PRUNE_CONFIG,
)


@pytest.fixture
def cw_client():
    """Create a ConnectWiseClient using environment variables."""
    return ConnectWiseClient()


@pytest.fixture
def mock_spark_session():
    """Create a mock Spark session with properly mocked operations."""
    with patch("fabric_api.pipeline.get_spark_session") as mock_get_spark:
        spark = MagicMock()

        # Mock DataFrame operations
        mock_df = MagicMock()
        mock_df.count.return_value = 10
        mock_df.columns = [
            "id",
            "name",
            "company_id",
            "company_name",
            "etl_timestamp",
            "etl_entity",
        ]
        mock_df.select.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.dropDuplicates.return_value = mock_df

        # Mock spark.table and spark.createDataFrame
        spark.table.return_value = mock_df
        spark.createDataFrame.return_value = mock_df

        mock_get_spark.return_value = spark
        yield spark


@pytest.fixture
def mock_pyspark_functions():
    """Mock PySpark SQL functions."""
    with patch("fabric_api.pipeline.col") as mock_col, patch("fabric_api.pipeline.lit") as mock_lit:
        # Mock col function
        mock_col.side_effect = lambda x: MagicMock(alias=lambda y: f"{x}_as_{y}")

        # Mock lit function
        mock_lit.side_effect = lambda x: f"lit_{x}"

        yield


def test_bronze_extraction(cw_client, mock_spark_session, mock_pyspark_functions):
    """Test extracting data to bronze layer."""
    # Mock the extract_entity function
    mock_data = [
        {
            "id": 1,
            "name": "Test Agreement",
            "type": {"id": 1, "name": "Standard"},
            "company": {"id": 100, "name": "Test Company"},
            "_info": {"lastUpdated": "2025-01-01T00:00:00Z"},
        }
    ]

    with (
        patch("fabric_api.pipeline.extract_entity", return_value=mock_data),
        patch("fabric_api.pipeline.write_to_delta", return_value=("/path/to/bronze", 1)),
    ):
        result = process_entity_to_bronze(
            entity_name="Agreement", client=cw_client, bronze_path="/test/bronze", mode="overwrite"
        )

        assert result["entity"] == "Agreement"
        assert result["extracted"] == 1
        assert result["bronze_rows"] == 1


def test_column_pruning_config():
    """Test that column pruning configurations are properly defined."""
    # Check that all entities have configurations
    required_entities = [
        "Agreement",
        "TimeEntry",
        "ExpenseEntry",
        "ProductItem",
        "PostedInvoice",
        "UnpostedInvoice",
    ]

    for entity in required_entities:
        assert entity in COLUMN_PRUNE_CONFIG, f"Missing pruning config for {entity}"
        config = COLUMN_PRUNE_CONFIG[entity]
        assert "keep" in config, f"Missing 'keep' list for {entity}"
        assert isinstance(config["keep"], list), f"'keep' must be a list for {entity}"


def test_bronze_to_silver_transformation(mock_spark_session, mock_pyspark_functions):
    """Test transformation from bronze to silver layer."""
    with (
        patch(
            "fabric_api.pipeline.flatten_all_nested_structures",
            return_value=mock_spark_session.table.return_value,
        ),
        patch("fabric_api.pipeline.write_to_delta", return_value=("/path/to/silver", 10)),
    ):
        result = process_bronze_to_silver(
            entity_name="Agreement", bronze_path="/test/bronze", silver_path="/test/silver"
        )

        assert result["entity"] == "Agreement"
        assert result["status"] == "success"
        assert result["bronze_rows"] == 10
        assert result["silver_rows"] == 10


def test_full_pipeline(cw_client, mock_spark_session, mock_pyspark_functions):
    """Test the full bronze-to-silver pipeline."""
    mock_data = [
        {
            "id": 1,
            "name": "Test Agreement",
            "type": {"id": 1, "name": "Standard"},
            "company": {"id": 100, "name": "Test Company"},
            "_info": {"lastUpdated": "2025-01-01T00:00:00Z"},
        }
    ]

    with (
        patch("fabric_api.pipeline.extract_entity", return_value=mock_data),
        patch("fabric_api.pipeline.write_to_delta", return_value=("/path/to/delta", 1)),
        patch(
            "fabric_api.pipeline.flatten_all_nested_structures",
            return_value=mock_spark_session.table.return_value,
        ),
    ):
        results = run_full_pipeline(
            entity_names=["Agreement"],
            client=cw_client,
            bronze_path="/test/bronze",
            silver_path="/test/silver",
            mode="overwrite",
        )

        assert "Agreement" in results
        assert "bronze" in results["Agreement"]
        assert "silver" in results["Agreement"]
        assert results["Agreement"]["bronze"]["bronze_rows"] == 1
        assert results["Agreement"]["silver"]["status"] == "success"


def test_real_extraction_smoke_test(cw_client):
    """Smoke test with real API - just verify we can extract something."""
    # This is a minimal smoke test - won't run full pipeline but verifies extraction works
    result = process_entity_to_bronze(
        entity_name="Agreement",
        client=cw_client,
        max_pages=1,
        page_size=1,  # Get just one record as a smoke test
        mode="overwrite",
    )

    assert result["entity"] == "Agreement"
    # We don't assert counts as data may vary, just that it runs without error


if __name__ == "__main__":
    pytest.main(["-v", __file__])
