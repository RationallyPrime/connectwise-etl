#!/usr/bin/env python
"""Test integration detection."""

import sys
from pprint import pprint

# Test integration detection
try:
    from unified_etl_core.integrations import detect_available_integrations
    
    print("🔍 Testing integration detection...")
    integrations = detect_available_integrations()
    
    print(f"\n✅ Detected {len(integrations)} integrations:")
    pprint(integrations)
    
    # Specifically check Business Central
    bc_info = integrations.get("businesscentral", {})
    print(f"\n📊 Business Central integration:")
    print(f"  Available: {bc_info.get('available', False)}")
    print(f"  Has extractor: {bc_info.get('extractor') is not None}")
    print(f"  Has models: {bc_info.get('models') is not None}")
    
    if bc_info.get('models'):
        print(f"  Model count: {len(bc_info['models'])}")
        print(f"  Models: {list(bc_info['models'].keys())}")
    
except Exception as e:
    print(f"❌ Integration detection failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n✅ Integration detection test passed!")