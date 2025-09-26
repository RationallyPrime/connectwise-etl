"""Integration tests for ConnectWise API client."""

import os
import pytest
from unittest.mock import patch, MagicMock
import requests

from connectwise_etl.client import ConnectWiseClient


@pytest.mark.integration
class TestConnectWiseClient:
    """Test ConnectWise API client connectivity and functionality."""

    @pytest.fixture
    def client(self, mock_env_vars):
        """Create a ConnectWiseClient instance with mocked credentials."""
        return ConnectWiseClient()

    def test_client_initialization_with_credentials(self, mock_env_vars):
        """Test client initializes with proper credentials."""
        client = ConnectWiseClient()

        assert client.basic_username == "test_user"
        assert client.basic_password == "test_pass"
        assert client.client_id == "test_client"
        assert client.base_url == "https://test.connectwise.com/v4_6_release/apis/3.0"

    def test_client_initialization_missing_credentials(self, monkeypatch):
        """Test client raises error when credentials are missing."""
        monkeypatch.delenv("CW_AUTH_USERNAME", raising=False)

        with pytest.raises(ValueError, match="Basic auth credentials"):
            ConnectWiseClient()

    def test_client_initialization_missing_client_id(self, monkeypatch):
        """Test client raises error when client ID is missing."""
        monkeypatch.setenv("CW_AUTH_USERNAME", "user")
        monkeypatch.setenv("CW_AUTH_PASSWORD", "pass")
        monkeypatch.delenv("CW_CLIENTID", raising=False)

        with pytest.raises(ValueError, match="Client ID"):
            ConnectWiseClient()

    def test_headers_generation(self, client):
        """Test that headers are generated correctly."""
        headers = client._headers()

        assert headers["clientId"] == "test_client"
        assert "application/vnd.connectwise.com+json" in headers["Accept"]
        assert headers["Content-Type"] == "application/json"

    @patch("requests.Session.get")
    def test_get_request(self, mock_get, client):
        """Test GET request with parameters."""
        mock_response = MagicMock()
        mock_response.json.return_value = [{"id": 1, "name": "Test"}]
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        response = client.get(
            "/test/endpoint", fields="id,name", conditions="id>0", order_by="id asc"
        )

        assert response == mock_response
        mock_get.assert_called_once()

        # Check the call arguments
        call_args = mock_get.call_args
        assert "/test/endpoint" in call_args[0][0]
        assert call_args[1]["params"]["fields"] == "id,name"
        assert call_args[1]["params"]["conditions"] == "id>0"
        assert call_args[1]["params"]["orderBy"] == "id asc"

    @patch("requests.Session.get")
    def test_paginate(self, mock_get, client):
        """Test pagination functionality."""
        # Mock two pages of results
        page1 = [{"id": 1}, {"id": 2}]
        page2 = [{"id": 3}]

        mock_responses = []
        for page_data in [page1, page2, []]:  # Empty list signals end
            mock_response = MagicMock()
            mock_response.json.return_value = page_data
            mock_response.raise_for_status = MagicMock()
            mock_responses.append(mock_response)

        mock_get.side_effect = mock_responses

        results = client.paginate(endpoint="/test/endpoint", entity_name="tests", page_size=2)

        assert len(results) == 3
        assert results[0]["id"] == 1
        assert results[2]["id"] == 3

    @patch("requests.Session.post")
    def test_post_request(self, mock_post, client):
        """Test POST request with JSON data."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"id": 123, "status": "created"}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        test_data = {"name": "Test", "value": 42}
        response = client.post("/test/create", json_data=test_data)

        assert response == mock_response
        mock_post.assert_called_once()

        # Check JSON data was passed
        call_args = mock_post.call_args
        assert call_args[1]["json"] == test_data

    def test_create_batch_identifier(self):
        """Test batch identifier creation."""
        from datetime import datetime

        # Test with specific timestamp
        test_dt = datetime(2024, 1, 15, 10, 30, 45)
        result = ConnectWiseClient.create_batch_identifier(test_dt)
        assert result == "20240115-103045"

        # Test without timestamp (should use current time)
        result = ConnectWiseClient.create_batch_identifier()
        assert len(result) == 15  # YYYYMMDD-HHMMSS
        assert result[8] == "-"


@pytest.mark.integration
@pytest.mark.skipif(
    not all(os.getenv(var) for var in ["CW_AUTH_USERNAME", "CW_AUTH_PASSWORD", "CW_CLIENTID"]),
    reason="Real ConnectWise credentials not available",
)
class TestConnectWiseClientLive:
    """Live integration tests that require real ConnectWise credentials."""

    @pytest.fixture
    def live_client(self):
        """Create a real ConnectWiseClient instance."""
        return ConnectWiseClient()

    def test_live_connection(self, live_client):
        """Test actual connection to ConnectWise API."""
        # Try to get a small sample of data
        response = live_client.get("/system/info")

        # Should not raise an exception
        assert response is not None

    def test_live_pagination(self, live_client):
        """Test pagination with real API."""
        # Get just 5 companies as a test
        results = live_client.paginate(
            endpoint="/company/companies", entity_name="companies", page_size=5, max_pages=1
        )

        assert isinstance(results, list)
        assert len(results) <= 5

    @pytest.mark.slow
    def test_extract_time_entries(self, live_client):
        """Test extracting time entries (may be slow)."""
        # Get time entries from last 7 days
        from datetime import datetime, timedelta

        since_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        conditions = f"dateEntered>=[{since_date}]"

        df = live_client.extract(endpoint="/time/entries", conditions=conditions, page_size=100)

        # Should return a DataFrame (even if empty)
        assert df is not None
        assert hasattr(df, "count")  # It's a Spark DataFrame
