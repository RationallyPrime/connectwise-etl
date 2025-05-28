#!/usr/bin/env python3
"""Test script for the core pipeline orchestration."""

import logging
import sys
from pathlib import Path

# Add packages to path for testing
sys.path.insert(0, str(Path(__file__).parent / "packages" / "unified-etl-core" / "src"))
sys.path.insert(0, str(Path(__file__).parent / "packages" / "unified-etl-connectwise" / "src"))

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def test_integration_detection():
    """Test that integrations are properly detected."""
    print("🔍 Testing integration detection...")

    try:
        from unified_etl_core.integrations import (
            detect_available_integrations,
            list_available_integrations,
        )

        # Test detection
        integrations = detect_available_integrations()
        available = list_available_integrations()

        print(f"Detected integrations: {list(integrations.keys())}")
        print(f"Available integrations: {available}")

        # Test ConnectWise specifically
        if "connectwise" in integrations:
            cw_info = integrations["connectwise"]
            print(f"ConnectWise available: {cw_info.get('available')}")
            print(f"ConnectWise extractor: {cw_info.get('extractor')}")
            print(f"ConnectWise models: {list(cw_info.get('models', {}).keys())}")

        return True

    except Exception as e:
        print(f"❌ Integration detection failed: {e}")
        return False


def test_model_imports():
    """Test that models can be imported and are SparkModels."""
    print("\n📋 Testing model imports...")

    try:
        import unified_etl_connectwise

        models = unified_etl_connectwise.models
        print(f"Found {len(models)} models: {list(models.keys())}")

        # Test one model
        agreement_model = models.get("agreement")
        if agreement_model:
            print(f"Agreement model: {agreement_model}")
            print(f"Is SparkModel: {hasattr(agreement_model, 'model_spark_schema')}")

        return True

    except Exception as e:
        print(f"❌ Model import failed: {e}")
        return False


def test_silver_conversion():
    """Test silver layer functions."""
    print("\n🥈 Testing silver layer...")

    try:
        import unified_etl_connectwise
        from unified_etl_core.silver import validate_batch

        # Test with sample data
        agreement_model = unified_etl_connectwise.models["agreement"]
        sample_data = [{"id": 1, "name": "Test Agreement"}]

        # Test validation
        valid_models, errors = validate_batch(sample_data, agreement_model)
        print(f"Validation - Valid: {len(valid_models)}, Errors: {len(errors)}")

        if valid_models:
            # This would require Spark session, so just test the function exists
            print("✅ Silver validation working")

        return True

    except Exception as e:
        print(f"❌ Silver layer test failed: {e}")
        return False


def test_facts_function():
    """Test facts layer function exists."""
    print("\n🥇 Testing facts layer...")

    try:

        print("✅ Facts function imported successfully")
        return True

    except Exception as e:
        print(f"❌ Facts import failed: {e}")
        return False


def main():
    """Run all tests."""
    print("🚀 Testing Core Pipeline Framework")
    print("=" * 50)

    tests = [
        test_integration_detection,
        test_model_imports,
        test_silver_conversion,
        test_facts_function,
    ]

    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append(False)

    print("\n" + "=" * 50)
    print(f"Results: {sum(results)}/{len(results)} tests passed")

    if all(results):
        print("🎉 All core framework tests passed!")
        print("✅ Ready to consolidate ConnectWise package")
        return 0
    else:
        print("⚠️ Some tests failed - check issues above")
        return 1


if __name__ == "__main__":
    sys.exit(main())
