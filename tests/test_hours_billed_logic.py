"""Simple test to verify the hoursBilled change in invoice line calculation."""

import pytest
from unified_etl_connectwise.config import ConnectWiseConfig


def test_invoice_line_uses_hours_billed():
    """Test that we're using hoursBilled in invoice line calculations."""
    # Read the transforms.py file to verify the change
    with open("packages/unified-etl-connectwise/src/unified_etl_connectwise/transforms.py", "r") as f:
        content = f.read()
    
    # Find the time_lines section
    time_lines_start = content.find("time_lines = time_enriched.select(")
    assert time_lines_start != -1, "Could not find time_lines definition"
    
    # Find the lineAmount calculation
    line_amount_start = content.find('alias("lineAmount")', time_lines_start)
    assert line_amount_start != -1, "Could not find lineAmount alias"
    
    # Get the line that calculates lineAmount
    # Go back to find the start of the expression
    calc_start = content.rfind("(", time_lines_start, line_amount_start)
    calc_line = content[calc_start:line_amount_start]
    
    # Verify it uses hoursBilled
    assert "hoursBilled" in calc_line, f"lineAmount calculation doesn't use hoursBilled: {calc_line}"
    assert "actualHours" not in calc_line, f"lineAmount calculation still uses actualHours: {calc_line}"
    
    print(f"✓ Verified: lineAmount calculation uses hoursBilled")
    print(f"  Calculation: {calc_line.strip()}")


def test_quantity_still_uses_actual_hours():
    """Test that quantity field still uses actualHours."""
    with open("packages/unified-etl-connectwise/src/unified_etl_connectwise/transforms.py", "r") as f:
        content = f.read()
    
    # Find the time_lines section
    time_lines_start = content.find("time_lines = time_enriched.select(")
    
    # Find the quantity field
    quantity_start = content.find('alias("quantity")', time_lines_start)
    assert quantity_start != -1, "Could not find quantity alias"
    
    # Get the line that sets quantity
    quantity_line_start = content.rfind("F.col(", time_lines_start, quantity_start)
    quantity_line = content[quantity_line_start:quantity_start]
    
    # Verify it uses actualHours
    assert "actualHours" in quantity_line, f"quantity doesn't use actualHours: {quantity_line}"
    
    print(f"✓ Verified: quantity still uses actualHours")
    print(f"  Quantity: {quantity_line.strip()}")


def test_config_validation():
    """Test that the new typed config system validates properly."""
    # Test missing required field
    with pytest.raises(TypeError):
        # Missing api_url
        config = ConnectWiseConfig(
            company_id="test",
            public_key="test",
            private_key="test",
            client_id="test"
        )
    
    # Test valid config
    config = ConnectWiseConfig(
        api_url="https://api.example.com",
        company_id="test",
        public_key="test",
        private_key="test",
        client_id="test"
    )
    assert config.api_url == "https://api.example.com"
    assert config.batch_size == 1000  # default value
    
    print("✓ Config validation working correctly")


if __name__ == "__main__":
    test_invoice_line_uses_hours_billed()
    test_quantity_still_uses_actual_hours()
    test_config_validation()
    print("\n✅ All tests passed!")