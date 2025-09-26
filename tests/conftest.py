"""Pytest configuration and shared fixtures."""

import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    spark = (
        SparkSession.builder.appName("ConnectWise ETL Tests")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mock ConnectWise environment variables."""
    monkeypatch.setenv("CW_AUTH_USERNAME", "test_user")
    monkeypatch.setenv("CW_AUTH_PASSWORD", "test_pass")
    monkeypatch.setenv("CW_CLIENTID", "test_client")
    monkeypatch.setenv("CW_BASE_URL", "https://test.connectwise.com/v4_6_release/apis/3.0")


@pytest.fixture
def sample_time_entry_data():
    """Sample time entry data for testing."""
    return {
        "id": 12345,
        "chargeToType": "ServiceTicket",
        "chargeToId": 67890,
        "member": {"id": 1, "name": "Test User"},
        "dateEntered": "2024-01-15T10:30:00Z",
        "timeStart": "2024-01-15T09:00:00Z",
        "timeEnd": "2024-01-15T10:00:00Z",
        "actualHours": 1.0,
        "billableOption": "Billable",
        "notes": "Test work performed",
    }


@pytest.fixture
def sample_company_data():
    """Sample company data for testing."""
    return {
        "id": 100,
        "identifier": "TESTCO",
        "name": "Test Company",
        "dateAcquired": "2023-01-01T00:00:00Z",
        "status": {"id": 1, "name": "Active"},
        "types": [{"id": 1, "name": "Client"}],
    }
