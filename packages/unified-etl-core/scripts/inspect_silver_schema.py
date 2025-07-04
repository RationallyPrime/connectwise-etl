#!/usr/bin/env python3
"""
Inspect the actual schema of silver tables to find correct column names.
Run this in a Fabric notebook to see what columns actually exist.
"""

import sys

# Get spark from the main module (Fabric notebook)
spark = sys.modules["__main__"].spark

# Tables to inspect
tables_to_inspect = [
    "Lakehouse.silver.silver_cw_timeentry",
    "Lakehouse.silver.silver_cw_agreement",
    "Lakehouse.silver.silver_cw_invoice",
    "Lakehouse.silver.silver_cw_expenseentry",
    "Lakehouse.silver.silver_cw_productitem"
]

print("=== Silver Table Schema Inspection ===\n")

for table_name in tables_to_inspect:
    print(f"\n{'='*60}")
    print(f"Table: {table_name}")
    print('='*60)

    try:
        # Get DataFrame
        df = spark.table(table_name)

        # Get row count
        count = df.count()
        print(f"Row count: {count:,}")

        # Get all columns
        columns = df.columns
        print(f"\nTotal columns: {len(columns)}")

        # Group columns by pattern
        print("\nColumns by pattern:")

        # Business unit related
        business_cols = [c for c in columns if 'business' in c.lower() or 'unit' in c.lower()]
        if business_cols:
            print("\n  Business Unit related:")
            for col in sorted(business_cols):
                print(f"    - {col}")

        # Department related
        dept_cols = [c for c in columns if 'department' in c.lower()]
        if dept_cols:
            print("\n  Department related:")
            for col in sorted(dept_cols):
                print(f"    - {col}")

        # Member related
        member_cols = [c for c in columns if 'member' in c.lower()]
        if member_cols:
            print("\n  Member related:")
            for col in sorted(member_cols):
                print(f"    - {col}")

        # Company related
        company_cols = [c for c in columns if 'company' in c.lower()]
        if company_cols:
            print("\n  Company related:")
            for col in sorted(company_cols):
                print(f"    - {col}")

        # Work type related
        work_cols = [c for c in columns if 'work' in c.lower() and 'type' in c.lower()]
        if work_cols:
            print("\n  Work Type related:")
            for col in sorted(work_cols):
                print(f"    - {col}")

        # Agreement type related
        if 'agreement' in table_name.lower():
            type_cols = [c for c in columns if 'type' in c.lower()]
            if type_cols:
                print("\n  Agreement Type related:")
                for col in sorted(type_cols):
                    print(f"    - {col}")

            billing_cols = [c for c in columns if 'billing' in c.lower() or 'bill' in c.lower()]
            if billing_cols:
                print("\n  Billing related:")
                for col in sorted(billing_cols):
                    print(f"    - {col}")

        # Status related
        status_cols = [c for c in columns if 'status' in c.lower()]
        if status_cols:
            print("\n  Status related:")
            for col in sorted(status_cols):
                print(f"    - {col}")

        # Classification related (for expense)
        if 'expense' in table_name.lower():
            class_cols = [c for c in columns if 'classif' in c.lower()]
            if class_cols:
                print("\n  Classification related:")
                for col in sorted(class_cols):
                    print(f"    - {col}")

        # Product class related
        if 'product' in table_name.lower():
            prod_cols = [c for c in columns if 'class' in c.lower() or 'product' in c.lower()]
            if prod_cols:
                print("\n  Product related:")
                for col in sorted(prod_cols):
                    print(f"    - {col}")

        # Show all columns sorted
        print("\nAll columns (sorted):")
        for col in sorted(columns):
            print(f"  - {col}")

    except Exception as e:
        print(f"ERROR accessing table: {e}")
        print(f"Error type: {type(e).__name__}")

print("\n" + "="*60)
print("Inspection complete!")
