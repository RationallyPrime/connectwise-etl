# Business Central Gold Layer - Fixed for Fabric Tables
from datetime import datetime
from pyspark.sql import SparkSession

# Configuration Setup
batch_id = f"bc_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
lakehouse_root = "/lakehouse/default/Tables/"

print(f"🔧 Gold Layer Processing:")
print(f"  Batch ID: {batch_id}")
print(f"  Lakehouse Root: {lakehouse_root}")

# Get or create Spark session
spark = SparkSession.builder.appName("BC_Gold_Test").getOrCreate()

# Test reading silver tables using Fabric table names
print("\n🔍 Testing Silver Table Access:")

silver_tables_to_test = [
    "customer", "vendor", "item", "glaccount", 
    "currency", "resource", "dimension", "dimensionvalue", "dimensionsetentry"
]

available_silver = []
for table in silver_tables_to_test:
    try:
        df = spark.table(f"LH.silver.{table}")
        count = df.count()
        available_silver.append(table)
        print(f"  ✅ {table}: {count} rows")
    except Exception as e:
        print(f"  ❌ {table}: {e}")

print(f"\n📊 Available Silver Tables: {len(available_silver)}")

# Now let's manually create some key gold dimensions that the orchestration was trying to build
print("\n🥇 Creating Gold Layer Dimensions:")

# 1. Create dim_Customer
if "customer" in available_silver:
    try:
        customer_df = spark.table("LH.silver.customer")
        
        # Add surrogate key and gold metadata
        from pyspark.sql.functions import monotonically_increasing_id, current_timestamp, lit
        from pyspark.sql.window import Window
        from pyspark.sql.functions import dense_rank
        
        # Create surrogate key based on business key
        window = Window.orderBy("No", "$Company")
        dim_customer = customer_df.withColumn("SK_Customer", dense_rank().over(window)) \
                                 .withColumn("_etl_gold_processed_at", current_timestamp()) \
                                 .withColumn("_etl_batch_id", lit(batch_id))
        
        # Write to gold layer
        gold_path = f"{lakehouse_root}gold/dim_Customer"
        dim_customer.write.mode("overwrite").format("delta").save(gold_path)
        
        print(f"  ✅ dim_Customer: {dim_customer.count()} rows created")
        
    except Exception as e:
        print(f"  ❌ dim_Customer: {e}")

# 2. Create dim_Vendor
if "vendor" in available_silver:
    try:
        vendor_df = spark.table("LH.silver.vendor")
        
        window = Window.orderBy("No", "$Company")
        dim_vendor = vendor_df.withColumn("SK_Vendor", dense_rank().over(window)) \
                             .withColumn("_etl_gold_processed_at", current_timestamp()) \
                             .withColumn("_etl_batch_id", lit(batch_id))
        
        gold_path = f"{lakehouse_root}gold/dim_Vendor"
        dim_vendor.write.mode("overwrite").format("delta").save(gold_path)
        
        print(f"  ✅ dim_Vendor: {dim_vendor.count()} rows created")
        
    except Exception as e:
        print(f"  ❌ dim_Vendor: {e}")

# 3. Create dim_Item
if "item" in available_silver:
    try:
        item_df = spark.table("LH.silver.item")
        
        window = Window.orderBy("No", "$Company")
        dim_item = item_df.withColumn("SK_Item", dense_rank().over(window)) \
                         .withColumn("_etl_gold_processed_at", current_timestamp()) \
                         .withColumn("_etl_batch_id", lit(batch_id))
        
        gold_path = f"{lakehouse_root}gold/dim_Item"
        dim_item.write.mode("overwrite").format("delta").save(gold_path)
        
        print(f"  ✅ dim_Item: {dim_item.count()} rows created")
        
    except Exception as e:
        print(f"  ❌ dim_Item: {e}")

# 4. Create dim_GLAccount
if "glaccount" in available_silver:
    try:
        glaccount_df = spark.table("LH.silver.glaccount")
        
        window = Window.orderBy("No", "$Company")
        dim_glaccount = glaccount_df.withColumn("SK_GLAccount", dense_rank().over(window)) \
                                   .withColumn("_etl_gold_processed_at", current_timestamp()) \
                                   .withColumn("_etl_batch_id", lit(batch_id))
        
        gold_path = f"{lakehouse_root}gold/dim_GLAccount"
        dim_glaccount.write.mode("overwrite").format("delta").save(gold_path)
        
        print(f"  ✅ dim_GLAccount: {dim_glaccount.count()} rows created")
        
    except Exception as e:
        print(f"  ❌ dim_GLAccount: {e}")

# 5. Create dim_Currency
if "currency" in available_silver:
    try:
        currency_df = spark.table("LH.silver.currency")
        
        window = Window.orderBy("Code", "$Company")
        dim_currency = currency_df.withColumn("SK_Currency", dense_rank().over(window)) \
                                 .withColumn("_etl_gold_processed_at", current_timestamp()) \
                                 .withColumn("_etl_batch_id", lit(batch_id))
        
        gold_path = f"{lakehouse_root}gold/dim_Currency"
        dim_currency.write.mode("overwrite").format("delta").save(gold_path)
        
        print(f"  ✅ dim_Currency: {dim_currency.count()} rows created")
        
    except Exception as e:
        print(f"  ❌ dim_Currency: {e}")

# 6. Create Date dimension
print("\n📅 Creating Date Dimension:")
try:
    from pyspark.sql.functions import sequence, to_date, date_format, year, month, dayofmonth, dayofweek, weekofyear
    
    # Create date spine for 2020-2030
    date_df = spark.sql("""
        SELECT sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day) as date_array
    """).select(F.explode(F.col("date_array")).alias("Date"))
    
    # Add date attributes
    dim_date = date_df.withColumn("DateKey", F.date_format("Date", "yyyyMMdd").cast("int")) \
                     .withColumn("Year", F.year("Date")) \
                     .withColumn("Month", F.month("Date")) \
                     .withColumn("Day", F.dayofmonth("Date")) \
                     .withColumn("DayOfWeek", F.dayofweek("Date")) \
                     .withColumn("WeekOfYear", F.weekofyear("Date")) \
                     .withColumn("MonthName", F.date_format("Date", "MMMM")) \
                     .withColumn("DayName", F.date_format("Date", "EEEE")) \
                     .withColumn("_etl_gold_processed_at", current_timestamp()) \
                     .withColumn("_etl_batch_id", lit(batch_id))
    
    gold_path = f"{lakehouse_root}gold/dim_Date"
    dim_date.write.mode("overwrite").format("delta").save(gold_path)
    
    print(f"  ✅ dim_Date: {dim_date.count()} rows created")
    
except Exception as e:
    print(f"  ❌ dim_Date: {e}")

# 7. Create a simple fact table - fact_GLEntry
print("\n📊 Creating Fact Tables:")
try:
    glentry_df = spark.table("LH.silver.glentry")
    
    # Basic fact table with surrogate key
    window = Window.orderBy("EntryNo", "$Company")
    fact_glentry = glentry_df.withColumn("SK_GLEntry", dense_rank().over(window)) \
                            .withColumn("_etl_gold_processed_at", current_timestamp()) \
                            .withColumn("_etl_batch_id", lit(batch_id))
    
    gold_path = f"{lakehouse_root}gold/fact_GLEntry"
    fact_glentry.write.mode("overwrite").format("delta").save(gold_path)
    
    print(f"  ✅ fact_GLEntry: {fact_glentry.count()} rows created")
    
except Exception as e:
    print(f"  ❌ fact_GLEntry: {e}")

print(f"\n🎯 Gold Layer Processing Complete!")
print(f"Created key warehouse schema tables that can be used immediately.")