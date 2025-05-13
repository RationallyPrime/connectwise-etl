"""
Test script to validate Pydantic models with sample JSON data.
"""
import json
import traceback
from pathlib import Path
from typing import Dict, Any, Type

from fabric_api.connectwise_models.agreement import Agreement
from fabric_api.connectwise_models.posted_invoice import PostedInvoice


def load_sample(entity_name: str) -> Dict[str, Any]:
    """Load sample data from entity_samples directory."""
    sample_path = Path("entity_samples") / f"{entity_name}.json"
    with open(sample_path, "r") as f:
        return json.load(f)


def test_model_validation(entity_name: str, model_class: Type) -> bool:
    """Test model validation with sample data."""
    try:
        # Load sample data
        sample_data = load_sample(entity_name)
        
        # Remove _info field if present
        if "_info" in sample_data:
            del sample_data["_info"]
        
        # Remove customFields if present
        if "customFields" in sample_data:
            del sample_data["customFields"]
        
        # Validate with model
        model_instance = model_class(**sample_data)
        print(f"✓ Successfully validated {entity_name} model!")
        
        # Convert to dict and back to ensure serialization works
        dict_data = model_instance.model_dump()
        model_instance2 = model_class(**dict_data)
        print(f"✓ Successfully serialized and deserialized {entity_name} model!")
        
        return True
    except Exception as e:
        print(f"✗ Validation failed for {entity_name}: {str(e)}")
        traceback.print_exc()
        return False


def main():
    """Run validation tests for all models."""
    print("Testing updated models with sample data...")
    print("=" * 50)
    
    # Test Agreement model
    test_model_validation("Agreement", Agreement)
    print("-" * 50)
    
    # Test PostedInvoice model
    test_model_validation("PostedInvoice", PostedInvoice)
    print("=" * 50)
    
    print("Validation tests completed.")


if __name__ == "__main__":
    main()