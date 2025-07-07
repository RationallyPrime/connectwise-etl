# Script to compare old and new dimension bridges

print("🔍 Comparing Old vs New Dimension Bridge Structure")
print("="*60)

# Load the new dimension bridge
try:
    new_bridge_df = spark.table("LH.gold.dim_DimensionBridge")
    print(f"\n📊 New Dimension Bridge: {new_bridge_df.count()} rows")
    print("\nColumns in new bridge:")
    for col in sorted(new_bridge_df.columns):
        print(f"  - {col}")
    
    # Show sample of new bridge
    print("\nSample of new dimension bridge:")
    new_bridge_df.show(5, truncate=False)
    
    # Count by dimension combinations
    print("\n📈 Dimension combinations in new bridge:")
    
    # Check which dimension columns exist
    dim_cols = [col for col in new_bridge_df.columns if col.endswith("Code")]
    print(f"\nDimension columns found: {dim_cols}")
    
    # For each dimension, count how many rows have it
    print("\n📊 Dimension usage counts:")
    for dim_col in sorted(dim_cols):
        has_col = f"Has{dim_col.replace('Code', '')}Dimension"
        if has_col in new_bridge_df.columns:
            count = new_bridge_df.filter(f"{has_col} = true").count()
            print(f"  {dim_col}: {count} rows")
        else:
            # If no Has column, check for non-null values
            count = new_bridge_df.filter(f"{dim_col} IS NOT NULL").count()
            print(f"  {dim_col}: {count} rows (non-null)")
    
except Exception as e:
    print(f"❌ Error loading new dimension bridge: {e}")

print("\n" + "="*60)

# Load and analyze the old CSV
print("\n📁 Old Dimension Bridge (from CSV)")
try:
    old_bridge_df = spark.read.csv(
        "/home/rationallyprime/CascadeProjects/PSA/dim_bridge_old.csv",
        header=True,
        inferSchema=True
    )
    print(f"Total rows: {old_bridge_df.count()}")
    
    # Show dimensions tracked
    print("\nDimensions in old bridge:")
    print("  - TEAM (TEAMCode, TEAMName)")
    print("  - PRODUCT (PRODUCTCode, PRODUCTName)")
    
    # Count dimension usage
    team_count = old_bridge_df.filter("HasTEAMDimension = true").count()
    product_count = old_bridge_df.filter("HasPRODUCTDimension = true").count()
    both_count = old_bridge_df.filter("HasTEAMDimension = true AND HasPRODUCTDimension = true").count()
    
    print(f"\nDimension usage in old bridge:")
    print(f"  TEAM dimension: {team_count} rows")
    print(f"  PRODUCT dimension: {product_count} rows")
    print(f"  Both TEAM & PRODUCT: {both_count} rows")
    
    # Check unique companies
    companies = old_bridge_df.select("$Company").distinct().collect()
    print(f"\nCompanies in old bridge: {[row['$Company'] for row in companies]}")
    
except Exception as e:
    print(f"❌ Error loading old CSV: {e}")

print("\n" + "="*60)
print("\n📋 Summary:")
print("The new dimension bridge includes ALL BC dimensions:")
print("  - DEILD (Department)")
print("  - VERKEFNI (Project)")  
print("  - STM (Employees)")
print("  - TEAM")
print("  - PRODUCT")
print("  - FERÐIR (Travel)")
print("  - BÍLAR (Cars)")
print("  - PRD (Product Group)")
print("  - TYPE (Product Type)")
print("  - ER (Re-invoicing)")
print("  - SM (Sales & Marketing)")
print("  - VIDBURDIR (Events)")
print("\nThe old bridge only tracked TEAM and PRODUCT dimensions.")