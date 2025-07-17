# Test Business Central Integration Detection
# Cell 1: Reinstall updated package
%pip install --force-reinstall unified_etl_businesscentral-1.0.0-py3-none-any.whl

# Cell 2: Test integration detection
from unified_etl_core.integrations import detect_available_integrations

print("🔍 Testing Integration Detection:")
integrations = detect_available_integrations()

for name, info in integrations.items():
    print(f"  {name}: {info}")

# Cell 3: Test BC specific exports
try:
    import unified_etl_businesscentral
    print(f"\n📦 BC Package Exports:")
    print(f"  extractor: {unified_etl_businesscentral.extractor}")
    print(f"  models: {list(unified_etl_businesscentral.models.keys())}")
except Exception as e:
    print(f"❌ BC Package Error: {e}")

# Cell 4: Test model imports
try:
    from unified_etl_businesscentral.models import Customer18
    print(f"\n✅ Model import successful: {Customer18}")
except Exception as e:
    print(f"❌ Model import error: {e}")