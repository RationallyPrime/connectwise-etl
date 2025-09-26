#!/bin/bash
# Run tests for ConnectWise ETL

set -e

echo "Running ConnectWise ETL Tests..."
echo "================================"

# Run unit tests first (fast)
echo -e "\n📦 Running Unit Tests..."
pytest tests/unit -v -m unit

# Run integration tests if requested
if [[ "$1" == "--integration" ]]; then
    echo -e "\n🔌 Running Integration Tests..."
    pytest tests/integration -v -m integration
fi

# Run all tests with coverage if requested
if [[ "$1" == "--coverage" ]]; then
    echo -e "\n📊 Running All Tests with Coverage..."
    pytest tests/ --cov=src/connectwise_etl --cov-report=html --cov-report=term
    echo "Coverage report generated in htmlcov/index.html"
fi

# Run only fast tests by default
if [[ -z "$1" ]]; then
    echo -e "\nTip: Use --integration to run integration tests"
    echo "     Use --coverage to generate coverage report"
fi

echo -e "\n✅ Tests Complete!"
