#!/usr/bin/env python3
"""Patch the OpenAPI schema to match actual API behavior."""

import json

def patch_schema():
    """Fix known issues in the OpenAPI schema."""

    # Load the schema
    with open('PSA_OpenAPI_schema.json', 'r') as f:
        schema = json.load(f)

    patches_applied = []

    # Patch CustomFieldValue.value to accept string or object
    if 'components' in schema and 'schemas' in schema['components']:
        if 'CustomFieldValue' in schema['components']['schemas']:
            old_value = schema['components']['schemas']['CustomFieldValue']['properties']['value']

            # Change from {"type": "object"} to oneOf string or object
            schema['components']['schemas']['CustomFieldValue']['properties']['value'] = {
                "oneOf": [
                    {"type": "string"},
                    {"type": "object"}
                ]
            }

            patches_applied.append("CustomFieldValue.value: object -> string|object")

            print(f"✅ Patched CustomFieldValue.value")
            print(f"   Old: {json.dumps(old_value)}")
            print(f"   New: {json.dumps(schema['components']['schemas']['CustomFieldValue']['properties']['value'])}")

    # Save the patched schema
    output_file = 'PSA_OpenAPI_schema_patched.json'
    with open(output_file, 'w') as f:
        json.dump(schema, f, indent=2)

    print(f"\n💾 Saved patched schema to {output_file}")
    print(f"Patches applied: {patches_applied}")

    return output_file

if __name__ == "__main__":
    patch_schema()