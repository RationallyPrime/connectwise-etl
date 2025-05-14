from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, ArrayType, MapType

# Recursive function to flatten ALL nested structures (structs, arrays, maps)
def flatten_dataframe(df):
    """
    Recursively flatten all nested structures in a DataFrame including 
    nested structs, arrays of structs, and maps.
    
    Args:
        df: The DataFrame to flatten
    
    Returns:
        DataFrame with fully flattened columns
    """
    # Helper function to check if a column needs flattening
    def needs_flattening(field_dtype):
        return isinstance(field_dtype, StructType) or \
               (isinstance(field_dtype, ArrayType) and isinstance(field_dtype.elementType, StructType)) or \
               isinstance(field_dtype, MapType)
    
    # Initial schema check           
    fields = df.schema.fields
    
    # Check if there are nested structures that need flattening
    nested_cols = [field.name for field in fields if needs_flattening(field.dataType)]
    
    # If no nested columns, return the DataFrame as is
    if len(nested_cols) == 0:
        return df
    
    # Process struct columns
    struct_cols = [field.name for field in fields 
                  if isinstance(field.dataType, StructType)]
    
    expanded_cols = []
    
    # Handling regular columns (non-struct)
    for field in fields:
        if field.name not in struct_cols:
            expanded_cols.append(col(field.name))
        else:
            # For struct columns, flatten each field
            struct_fields = field.dataType.fields
            for struct_field in struct_fields:
                expanded_cols.append(
                    col(f"{field.name}.{struct_field.name}").alias(f"{field.name}_{struct_field.name}")
                )
    
    # Create DataFrame with expanded columns
    expanded_df = df.select(expanded_cols)
    
    # Recursively apply flattening until no more nested structures exist
    return flatten_dataframe(expanded_df)

# Create silver schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# Get all tables in bronze schema
bronze_tables = spark.sql("SHOW TABLES IN bronze").select("tableName").collect()

# Process each table
for table_row in bronze_tables:
    table_name = table_row.tableName
    
    # Only process ConnectWise tables
    if table_name.startswith("cw_"):
        print(f"Processing {table_name}...")
        
        # Read the bronze table
        bronze_df = spark.sql(f"SELECT * FROM bronze.{table_name}")
        
        # Apply complete flattening
        silver_df = flatten_dataframe(bronze_df)
        
        # Verify all struct types are gone
        has_struct = False
        for field in silver_df.schema.fields:
            if isinstance(field.dataType, StructType):
                has_struct = True
                print(f"WARNING: Field {field.name} is still a struct after flattening!")
        
        if not has_struct:
            print(f"✓ All struct types successfully flattened in {table_name}")
            
            # Write to silver schema with optimizations
            silver_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .option("delta.autoOptimize.optimizeWrite", "true") \
                .option("delta.autoOptimize.autoCompact", "true") \
                .saveAsTable(f"silver.{table_name}")
                
            print(f"✓ Table silver.{table_name} created successfully")
        else:
            print(f"× Failed to flatten all structs in {table_name}")

print("Processing complete!")