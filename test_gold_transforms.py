#!/usr/bin/env python3
"""
Quick test to verify transform detection issue
"""

import importlib

# Test what the framework sees
def test_transform_detection():
    print("🔍 Testing ConnectWise transform detection...\n")
    
    # Import the transforms module
    try:
        from unified_etl_connectwise import transforms as cw_transforms
        print("✅ Successfully imported ConnectWise transforms")
        print(f"   Module: {cw_transforms}")
    except ImportError as e:
        print(f"❌ Failed to import: {e}")
        return
    
    # List all available functions
    print("\n📋 Available functions in transforms module:")
    for attr in dir(cw_transforms):
        if not attr.startswith('_'):
            obj = getattr(cw_transforms, attr)
            if callable(obj):
                print(f"   - {attr}")
    
    # Test what main.py is looking for
    print("\n🎯 What main.py checks for:")
    entities_to_check = ["agreement", "invoice", "timeentry", "expenseentry"]
    
    for entity_name in entities_to_check:
        print(f"\nEntity: {entity_name}")
        
        # Check for the function names main.py expects
        if entity_name == "agreement":
            func_name = "create_agreement_facts"  # What main.py looks for
            exists = hasattr(cw_transforms, func_name)
            print(f"   - Looking for: {func_name} ... {'✅ Found' if exists else '❌ Not found'}")
            
            # Check what actually exists
            actual_funcs = [f for f in dir(cw_transforms) if 'agreement' in f and 'create' in f]
            if actual_funcs:
                print(f"   - Actually has: {', '.join(actual_funcs)}")
                
        elif entity_name == "invoice":
            func_name = "create_invoice_facts"  # What main.py looks for
            exists = hasattr(cw_transforms, func_name)
            print(f"   - Looking for: {func_name} ... {'✅ Found' if exists else '❌ Not found'}")
            
            # Check what actually exists
            actual_funcs = [f for f in dir(cw_transforms) if 'invoice' in f and 'create' in f]
            if actual_funcs:
                print(f"   - Actually has: {', '.join(actual_funcs)}")
                
        else:
            # main.py doesn't check for other entities
            print(f"   - main.py doesn't check for {entity_name} transforms")
            
            # But let's see if we have functions for them
            actual_funcs = [f for f in dir(cw_transforms) if entity_name in f and 'create' in f]
            if actual_funcs:
                print(f"   - But we have: {', '.join(actual_funcs)}")
    
    # Check if we have the wrapper functions
    print("\n🔄 Checking for wrapper functions:")
    wrapper_funcs = ["create_agreement_facts", "create_invoice_facts"]
    for func in wrapper_funcs:
        exists = hasattr(cw_transforms, func)
        print(f"   - {func}: {'✅ Exists' if exists else '❌ Missing'}")

if __name__ == "__main__":
    test_transform_detection()