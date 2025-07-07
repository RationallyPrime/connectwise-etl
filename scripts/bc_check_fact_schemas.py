# Script to check what's in the gold schema

print("🥇 Checking Gold Schema Tables")
print("="*60)

# First, list all tables in the gold schema
print("\n📋 Tables in LH.gold schema:")
try:
    gold_tables = spark.catalog.listTables("gold")
    for table in gold_tables:
        print(f"  - {table.name}")
except Exception as e:
    print(f"❌ Error listing tables: {e}")
    # Try alternative approach
    try:
        spark.sql("SHOW TABLES IN LH.gold").show(50, truncate=False)
    except:
        pass

print("\n" + "="*60)
print("\n📊 Checking each Gold table schema:")

# Get all tables and check their schemas
try:
    gold_tables = spark.catalog.listTables("gold")
    
    for table in gold_tables:
        print(f"\n🔍 {table.name}")
        print("-" * 50)
        
        try:
            df = spark.table(f"LH.gold.{table.name}")
            
            # Basic info
            print(f"Row count: {df.count()}")
            print(f"Column count: {len(df.columns)}")
            
            # Show schema
            print("\nSchema:")
            for col_name, col_type in df.dtypes:
                print(f"  {col_name}: {col_type}")
            
            # For dimension tables, show key columns
            if table.name.startswith("dim_"):
                key_cols = [col for col in df.columns if col.endswith("Key")]
                if key_cols:
                    print(f"\nKey columns: {key_cols}")
            
            # For fact tables, show measures and keys
            if table.name.startswith("fact_"):
                print("\nMeasure columns:")
                measure_cols = [col for col in df.columns if any(x in col.lower() for x in ["amount", "quantity", "cost", "price"])]
                for col in measure_cols[:10]:  # First 10
                    print(f"  - {col}")
                    
            # Show first few rows for small dimension tables
            if table.name.startswith("dim_") and df.count() < 100:
                print(f"\nSample data:")
                df.show(5, truncate=False)
                
        except Exception as e:
            print(f"❌ Error reading table: {e}")
            
except Exception as e:
    print(f"❌ Error accessing gold schema: {e}")