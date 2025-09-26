#!/usr/bin/env python3
"""Examine the actual structure of customFields in raw API responses."""

import os
import json
from pathlib import Path

# Load .env file if it exists (Python format)
env_file = Path(".env")
if env_file.exists():
    exec(env_file.read_text())

from connectwise_etl.client import ConnectWiseClient

def examine_custom_fields():
    """Fetch raw data and examine customFields structure."""

    client = ConnectWiseClient()

    # Focus on Agreement since that has the most validation errors
    print("🔍 Fetching raw Agreement data to examine customFields...")

    raw_agreements = client.paginate(
        endpoint="/finance/agreements",
        entity_name="Agreements",
        page_size=100,
        max_pages=2
    )

    print(f"✅ Fetched {len(raw_agreements)} agreements\n")

    # Examine customFields structure
    custom_fields_examples = {}
    records_with_custom_fields = 0

    for i, record in enumerate(raw_agreements):
        if 'customFields' in record and record['customFields']:
            records_with_custom_fields += 1

            # Collect examples of different value types
            for field in record['customFields']:
                if 'value' in field:
                    value = field['value']
                    value_type = type(value).__name__

                    # Store examples of each type
                    if value_type not in custom_fields_examples:
                        custom_fields_examples[value_type] = []

                    example = {
                        'record_index': i,
                        'field_caption': field.get('caption', 'N/A'),
                        'field_type': field.get('type', 'N/A'),
                        'value': value,
                        'full_field': field
                    }

                    # Limit examples per type
                    if len(custom_fields_examples[value_type]) < 5:
                        custom_fields_examples[value_type].append(example)

    # Report findings
    print(f"📊 Analysis Results:")
    print(f"  - Records with customFields: {records_with_custom_fields}/{len(raw_agreements)}")
    print(f"  - Value types found: {list(custom_fields_examples.keys())}\n")

    # Show examples of each type
    for value_type, examples in custom_fields_examples.items():
        print(f"\n🔸 Type: {value_type} ({len(examples)} examples)")
        for ex in examples[:3]:  # Show up to 3 examples
            print(f"  Record #{ex['record_index']}:")
            print(f"    Caption: {ex['field_caption']}")
            print(f"    Field Type: {ex['field_type']}")
            print(f"    Value: {ex['value']!r}")
            if value_type == 'dict':
                print(f"    Dict keys: {list(ex['value'].keys()) if isinstance(ex['value'], dict) else 'N/A'}")
            print()

    # Also check Company records since they had errors too
    print("\n" + "="*60)
    print("🔍 Checking Company records for comparison...")

    raw_companies = client.paginate(
        endpoint="/company/companies",
        entity_name="Companies",
        page_size=100,
        max_pages=2
    )

    company_custom_field_types = set()
    for record in raw_companies:
        if 'customFields' in record and record['customFields']:
            for field in record['customFields']:
                if 'value' in field:
                    company_custom_field_types.add(type(field['value']).__name__)

    print(f"Company customFields value types: {company_custom_field_types}")

    # Save a sample for detailed inspection
    sample_file = "custom_fields_sample.json"
    sample_data = {
        'agreements_with_custom_fields': [
            rec for rec in raw_agreements[:10]
            if 'customFields' in rec and rec['customFields']
        ][:3],
        'value_type_examples': {
            k: [{'value': ex['value'], 'field_type': ex['field_type']}
                for ex in v[:2]]
            for k, v in custom_fields_examples.items()
        }
    }

    with open(sample_file, 'w') as f:
        json.dump(sample_data, f, indent=2, default=str)

    print(f"\n💾 Saved sample data to {sample_file} for inspection")

if __name__ == "__main__":
    examine_custom_fields()