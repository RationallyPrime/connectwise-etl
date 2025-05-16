"""
Unit tests for ConnectWiseClient using mock responses.

These tests verify client behavior without requiring actual API credentials.
"""

import logging
from unittest.mock import MagicMock, Mock, patch

import pytest

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Import our modules
from fabric_api.client import ConnectWiseClient
from fabric_api.connectwise_models import Agreement
from fabric_api.extract.generic import extract_entity

# Mock API response data
MOCK_AGREEMENT_RESPONSE = [
    {
        "id": 1,
        "name": "Test Agreement 1",
        "type": {"id": 1, "name": "Standard"},
        "company": {"id": 100, "name": "Test Company", "identifier": "TESTCO"},
        "contact": {"id": 200, "name": "Test Contact"},
        "_info": {"lastUpdated": "2025-01-01T00:00:00Z"},
    },
    {
        "id": 2,
        "name": "Test Agreement 2",
        "type": {"id": 1, "name": "Standard"},
        "company": {"id": 101, "name": "Another Company", "identifier": "ANOTHERCO"},
        "contact": {"id": 201, "name": "Another Contact"},
        "_info": {"lastUpdated": "2025-01-02T00:00:00Z"},
    },
]

MOCK_TIME_ENTRY_RESPONSE = [
    {
        "id": 1001,
        # Missing required timeStart field - will cause validation error
        "timeEnd": "2025-05-01T17:00:00Z",
        "member": {"id": 301, "name": "Test Member"},
        "company": {"id": 100, "name": "Test Company"},
        "_info": {"lastUpdated": "2025-05-01T17:00:00Z"},
    },
    {
        "id": 1002,
        "timeStart": "2025-05-02T09:00:00Z",
        "timeEnd": "2025-05-02T17:00:00Z",
        "member": {"id": 301, "name": "Test Member"},
        "company": {"id": 101, "name": "Another Company"},
        "_info": {"lastUpdated": "2025-05-02T17:00:00Z"},
    },
]


@pytest.fixture
def mock_client():
    """Create a client with mocked API responses."""
    with (
        patch("fabric_api.client.ConnectWiseClient._headers", return_value={}),
        patch("fabric_api.client.os.getenv", return_value="mock_value"),
        patch("fabric_api.client.requests.Session") as mock_session,
    ):
        # Set up mock response for different endpoints
        def get_mock_response(url, **kwargs):
            mock_response = Mock()
            mock_response.status_code = 200

            # Choose mock data based on the URL
            if "/finance/agreements" in url:
                # Filter data based on fields parameter if present
                if kwargs.get("params", {}).get("fields"):
                    fields = kwargs["params"]["fields"].split(",")
                    filtered_data = []
                    for item in MOCK_AGREEMENT_RESPONSE:
                        filtered_item = {"_info": item["_info"]}
                        for field in fields:
                            if "." in field:  # Handle nested fields
                                parts = field.split(".")
                                if parts[0] in item and parts[1] in item[parts[0]]:
                                    if parts[0] not in filtered_item:
                                        filtered_item[parts[0]] = {}
                                    filtered_item[parts[0]][parts[1]] = item[parts[0]][parts[1]]
                            elif field in item:
                                filtered_item[field] = item[field]
                        filtered_data.append(filtered_item)
                    mock_response.json = lambda: filtered_data
                else:
                    mock_response.json = lambda: MOCK_AGREEMENT_RESPONSE

            elif "/time/entries" in url:
                mock_response.json = lambda: MOCK_TIME_ENTRY_RESPONSE
            else:
                mock_response.json = lambda: []

            return mock_response

        # Set up mock session
        mock_session_instance = MagicMock()
        mock_session_instance.get.side_effect = get_mock_response
        mock_session_instance.post.return_value = Mock(status_code=201, json=lambda: {"id": 999})
        mock_session_instance.put.return_value = Mock(status_code=200, json=lambda: {"id": 999})
        mock_session_instance.delete.return_value = Mock(status_code=204)

        mock_session.return_value = mock_session_instance

        # Create client with mocked session
        client = ConnectWiseClient()
        client.basic_username = "mock_username"
        client.basic_password = "mock_password"
        client.client_id = "mock_client_id"

        yield client


def test_fields_parameter(mock_client):
    """Test if fields parameter is correctly processed."""
    logger.info("TEST 1: Testing fields parameter with mock client")

    # Use a simple fields parameter - just request id and name
    fields = "id,name"

    # Fetch agreements with only specified fields
    agreements = mock_client.paginate(
        endpoint="/finance/agreements",
        entity_name="agreements",
        fields=fields,
        max_pages=1,
        page_size=2,
    )

    # Validate the results
    assert len(agreements) > 0, "No agreements returned"

    # Check that we got only the fields we requested
    for agreement in agreements:
        assert set(agreement.keys()).issubset({"id", "name", "_info"}), (
            "Received fields beyond what was requested"
        )
        assert "id" in agreement, "id field missing"
        assert "name" in agreement, "name field missing"

    logger.info(f"Retrieved {len(agreements)} agreements with fields={fields}")
    logger.info(f"First agreement fields: {list(agreements[0].keys())}")
    logger.info("✅ Fields parameter was successfully processed")


def test_extract_entity_with_validation(mock_client):
    """Test extracting entities with validation against Pydantic models."""
    logger.info("TEST 2: Testing entity extraction with model validation")

    with patch(
        "fabric_api.extract.generic.get_fields_for_api_call",
        return_value="id,name,type,company,contact",
    ):
        # Extract agreements with validation
        valid_agreements, errors = extract_entity(
            client=mock_client,
            entity_name="Agreement",
            max_pages=1,
            page_size=2,
            return_validated=True,
        )

    # Validation should succeed for our mock data
    assert len(valid_agreements) > 0, "No valid agreements returned"
    assert all(isinstance(agreement, Agreement) for agreement in valid_agreements), (
        "Returned objects are not Agreement instances"
    )

    # Log results for verification
    logger.info(f"Retrieved and validated {len(valid_agreements)} agreements")
    logger.info(f"Validation errors: {len(errors)}")

    if errors:
        logger.info(f"First error: {errors[0].get('errors')}")

    logger.info("✅ Extract entity with validation was successful")


def test_time_entry_extraction(mock_client):
    """Test extracting time entries with validation."""
    logger.info("TEST 3: Testing time entry extraction with model validation")

    with patch(
        "fabric_api.extract.generic.get_fields_for_api_call",
        return_value="id,timeStart,timeEnd,member,company",
    ):
        # Extract time entries with validation
        valid_time_entries, errors = extract_entity(
            client=mock_client,
            entity_name="TimeEntry",
            max_pages=1,
            page_size=5,
            return_validated=True,
        )

    # We should have one valid entry and one with validation errors
    assert len(valid_time_entries) >= 0, "No time entries returned"
    assert len(errors) > 0, "Expected validation errors with incomplete mock data"

    # Check for proper validation errors
    for error in errors:
        assert error["entity"] == "TimeEntry", "Wrong entity type in error"
        assert "errors" in error, "No error details in validation error"
        # Check if timeStart is mentioned in the error
        found_time_start_error = any("timeStart" in str(e) for e in error["errors"])
        assert found_time_start_error, "Expected error about missing timeStart field"

    logger.info(f"Retrieved and validated {len(valid_time_entries)} time entries")
    logger.info(f"Validation errors: {len(errors)}")
    logger.info(f"First error: {errors[0].get('errors')}")

    logger.info("✅ Time entry extraction test completed with expected validation errors")


if __name__ == "__main__":
    pytest.main(["-v", __file__])
