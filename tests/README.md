# Testing the ConnectWise PSA to Microsoft Fabric Integration

This directory contains tests for the ConnectWise PSA to Microsoft Fabric integration project.

## Test Organization

Tests are organized into two main categories:

1. **Real API Tests**: Run against the actual ConnectWise API (requires credentials)
2. **Mock Tests**: Run with mock data (no credentials needed)

## Running Tests

### Prerequisites

- Python 3.11 or newer
- Dependencies installed via `uv pip install -e .`
- pytest (`uv pip install pytest`)

### Environment Setup

To run tests against the real ConnectWise API, create a `.env` file in the project root with:

```
CW_AUTH_USERNAME=your_username
CW_AUTH_PASSWORD=your_password
CW_CLIENTID=your_client_id
```

A template is provided in `.env.example`.

### Running Individual Test Files

```bash
# Run a specific test file
python -m pytest tests/test_client_mock.py -v

# Run all tests
python -m pytest
```

## Test Files

- `test_client_fields.py`: Tests ConnectWiseClient API parameter handling (requires API credentials)
- `test_client_mock.py`: Tests client functionality using mock responses (no credentials needed)
- Additional validation tests for each entity type

## Adding New Tests

When adding new tests:

1. Create both a real API test and a mock test when possible
2. Follow the pattern of:
   - Arrange: Set up test data and parameters
   - Act: Call the function to test
   - Assert: Verify the outcome
   - Clean up (if needed)
3. Use descriptive test names that explain what is being tested

## Mocked Testing

The mock tests use Python's `unittest.mock` library to simulate API responses. This allows testing without real API credentials and ensures consistent test data.

Example of creating a mock:

```python
@pytest.fixture
def mock_client():
    with patch('fabric_api.client.requests.Session') as mock_session:
        # Set up mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"id": 1, "name": "Test"}]
        
        # Configure session to return the mock response
        mock_session_instance = MagicMock()
        mock_session_instance.get.return_value = mock_response
        mock_session.return_value = mock_session_instance
        
        # Create client with mocked session
        client = ConnectWiseClient()
        yield client
```