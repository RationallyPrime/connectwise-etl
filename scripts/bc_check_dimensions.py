# Script to check what dimension codes actually exist in BC data

# Check Dimension table
print("Checking BC Dimension codes...")
try:
    dim_df = spark.table("LH.silver.Dimension348")
    print(f"\nTotal dimensions: {dim_df.count()}")
    
    # Show all dimension codes
    print("\nAvailable dimension codes:")
    dim_df.select("Code", "Name").orderBy("Code").show(50, truncate=False)
    
except Exception as e:
    print(f"Error reading Dimension348 table: {e}")
    # Try to see what columns exist
    try:
        dim_df = spark.table("LH.silver.Dimension348")
        print("Available columns in Dimension348:")
        print(dim_df.columns)
        dim_df.show(5, truncate=False)
    except:
        pass

# Check DimensionValue table
print("\n" + "="*50)
print("Checking BC DimensionValue entries...")
try:
    dim_val_df = spark.table("LH.silver.DimensionValue349")
    print(f"\nTotal dimension values: {dim_val_df.count()}")
    
    # Show count by dimension code
    print("\nDimension value counts by code:")
    dim_val_df.groupBy("DimensionCode").count().orderBy("DimensionCode").show(50, truncate=False)
    
except Exception as e:
    print(f"Error reading DimensionValue349 table: {e}")
    # Try to see what columns exist
    try:
        dim_val_df = spark.table("LH.silver.DimensionValue349")
        print("Available columns in DimensionValue349:")
        print(dim_val_df.columns)
        dim_val_df.show(5, truncate=False)
    except:
        pass

# Check DimensionSetEntry table
print("\n" + "="*50)
print("Checking BC DimensionSetEntry...")
try:
    dse_df = spark.table("LH.silver.DimensionSetEntry480")
    print(f"\nTotal dimension set entries: {dse_df.count()}")
    
    # Show unique dimension codes used
    print("\nUnique dimension codes in use:")
    dse_df.select("DimensionCode").distinct().orderBy("DimensionCode").show(50, truncate=False)
    
except Exception as e:
    print(f"Error reading DimensionSetEntry480 table: {e}")
    # Try to see what columns exist
    try:
        dse_df = spark.table("LH.silver.DimensionSetEntry480")
        print("Available columns in DimensionSetEntry480:")
        print(dse_df.columns)
        dse_df.show(5, truncate=False)
    except:
        pass

# Also check DefaultDimension table
print("\n" + "="*50)
print("Checking BC DefaultDimension...")
try:
    dd_df = spark.table("LH.silver.DefaultDimension352")
    print(f"\nTotal default dimensions: {dd_df.count()}")
    
    # Show unique dimension codes
    print("\nUnique dimension codes in DefaultDimension:")
    dd_df.select("DimensionCode").distinct().orderBy("DimensionCode").show(50, truncate=False)
    
except Exception as e:
    print(f"Error reading DefaultDimension352 table: {e}")
    # Try to see what columns exist
    try:
        dd_df = spark.table("LH.silver.DefaultDimension352")
        print("Available columns in DefaultDimension352:")
        print(dd_df.columns)
        dd_df.show(5, truncate=False)
    except:
        pass