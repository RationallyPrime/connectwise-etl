"""
Incremental data refresh for ConnectWise ETL - Fabric Notebook Version

EFFICIENT INCREMENTAL PROCESSING:
- Bronze: MERGE only new/changed records (not overwrite)
- Silver: Process ONLY the delta records that changed
- Gold: Update ONLY affected fact rows

This avoids the silly approach of reprocessing 500,000 records when only 100 changed!
We track changes using the _etl_timestamp column we thoughtfully added.

Prerequisites:
- Run pip install commands in a separate cell first
- Ensure Key Vault secrets are configured (CW_AUTH_USERNAME, CW_AUTH_PASSWORD, CW_CLIENTID)
"""

# Standard imports
import logging
from datetime import datetime, timedelta

from pyspark.sql import DataFrame

# ETL framework imports
from unified_etl_connectwise import ConnectWiseClient
from unified_etl_connectwise.api_utils import build_condition_string

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def refresh_recent_data(days_back: int = 30) -> dict[str, DataFrame]:
    """
    Refresh only recent data from ConnectWise.

    Strategy:
    - TimeEntry/ExpenseEntry: Only recent entries (by dateEntered)
    - Agreement/PostedInvoice: Only recently updated (by lastUpdated)
    - UnpostedInvoice: ALL records (they're work in progress)

    Args:
        days_back: Number of days to look back (default 30)

    Returns:
        Dictionary mapping entity names to DataFrames
    """
    # Initialize client
    client = ConnectWiseClient()

    # Calculate date threshold
    since_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    print(f"Refreshing data since: {since_date}")

    results = {}

    endpoints = {
        "TimeEntry": "/time/entries",
        "Agreement": "/finance/agreements",
        "UnpostedInvoice": "/finance/invoices",
        "PostedInvoice": "/finance/invoices/posted",
        "ExpenseEntry": "/expense/entries",
    }

    for entity_name, endpoint in endpoints.items():
        print(f"\nRefreshing {entity_name}...")

        # Build appropriate conditions based on entity type
        if entity_name in ["TimeEntry", "ExpenseEntry"]:
            conditions = build_condition_string(date_entered_gte=since_date)
            order_by = "dateEntered desc"
        else:  # Agreement, PostedInvoice, UnpostedInvoice
            conditions = f"(lastUpdated>=[{since_date}])"
            order_by = "lastUpdated desc"

        try:
            df = client.extract(
                endpoint=endpoint, conditions=conditions, order_by=order_by, page_size=1000
            )

            # Add ETL metadata columns
            from pyspark.sql import functions as F  # noqa: N812

            df = df.withColumn("_etl_timestamp", F.current_timestamp())
            df = df.withColumn("_etl_source", F.lit("connectwise"))
            df = df.withColumn("_etl_batch_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))

            # Legacy columns for backward compatibility
            df = df.withColumn("etl_timestamp", F.col("_etl_timestamp").cast("string"))
            df = df.withColumn("etl_entity", F.lit(entity_name))
            df = df.withColumn("etlTimestamp", F.col("_etl_timestamp").cast("string"))
            df = df.withColumn("etlEntity", F.lit(entity_name))

            results[entity_name] = df
            print(f"  Found {df.count()} {entity_name} records")
        except Exception as e:
            print(f"  ERROR extracting {entity_name}: {e}")
            results[entity_name] = None

    return results




def check_latest_records() -> None:
    """Check the most recent records to see what dates we have."""
    client = ConnectWiseClient()

    recent_time = client.paginate(
        endpoint="/time/entries",
        entity_name="time_entries",
        fields="id,dateEntered",
        order_by="dateEntered desc",
        max_pages=1,
        page_size=5,
    )

    print("\nMost recent time entries:")
    for entry in recent_time:
        print(f"  {entry['dateEntered']}")

    recent_invoices = client.paginate(
        endpoint="/finance/invoices",
        entity_name="invoices",
        fields="id,invoiceNumber,lastUpdated",
        order_by="lastUpdated desc",
        max_pages=1,
        page_size=5,
    )

    print("\nMost recent invoices:")
    for inv in recent_invoices:
        print(f"  {inv.get('invoiceNumber', inv.get('id', 'N/A'))}: {inv.get('lastUpdated', 'N/A')}")


# %%
# ==============================================================================
# DEDUPLICATION UTILITIES - Run if you have duplicate issues
# ==============================================================================
"""
Simple deduplication utilities for cleaning up tables with duplicate rows.
"""

def deduplicate_table(table_name: str, unique_keys: list[str], keep: str = "last") -> int:
    """
    Remove duplicate rows from a table based on unique keys.
    
    Args:
        table_name: Name of the table to deduplicate
        unique_keys: List of columns that should be unique (e.g., ['id'])
        keep: Which duplicate to keep - 'first' or 'last' (based on _etl_timestamp)
        
    Returns:
        Number of duplicates removed
    """
    if not spark.catalog.tableExists(table_name):
        print(f"Table {table_name} does not exist")
        return 0
    
    # Read table
    df = spark.table(table_name)
    original_count = df.count()
    
    # Deduplicate
    if keep == "last":
        # Keep the most recent record based on _etl_timestamp
        deduped_df = df.orderBy(*unique_keys, F.desc("_etl_timestamp")) \
                      .dropDuplicates(unique_keys)
    else:
        # Keep the first record
        deduped_df = df.orderBy(*unique_keys, F.asc("_etl_timestamp")) \
                      .dropDuplicates(unique_keys)
    
    final_count = deduped_df.count()
    duplicates_removed = original_count - final_count
    
    if duplicates_removed > 0:
        # Overwrite table with deduplicated data
        deduped_df.write.mode("overwrite").saveAsTable(table_name)
        print(f"Removed {duplicates_removed} duplicates from {table_name}")
        print(f"  Original: {original_count} rows → Final: {final_count} rows")
    else:
        print(f"No duplicates found in {table_name}")
    
    return duplicates_removed



# %%
# ==============================================================================
# CONFIGURATION CHECK CELL - Run this first to verify setup
# ==============================================================================
"""
Check configuration and existing table statistics before running refresh.
"""

print("=== Configuration and Table Status Check ===\n")

# Initialize client to get spark session
from unified_etl_connectwise import ConnectWiseClient
from unified_etl_connectwise.config import SILVER_CONFIG

client = ConnectWiseClient()
spark = client.spark

# Check existing tables
print("Checking existing tables...")
existing_tables = spark.sql("""
    SELECT 
        tableName,
        CASE 
            WHEN tableName LIKE 'bronze_%' THEN 'Bronze'
            WHEN tableName LIKE 'silver_%' THEN 'Silver'
            WHEN tableName LIKE 'gold_%' THEN 'Gold'
            ELSE 'Other'
        END as layer
    FROM (SHOW TABLES)
    WHERE tableName LIKE 'bronze_cw_%' 
       OR tableName LIKE 'silver_cw_%'
       OR tableName LIKE 'gold_%'
    ORDER BY layer, tableName
""")

print("\nExisting ETL Tables:")
existing_tables.show(100, truncate=False)

# Get row counts for key tables
print("\nTable Statistics:")
for row in existing_tables.collect():
    table_name = row.tableName
    try:
        count = spark.table(table_name).count()
        print(f"  {table_name}: {count:,} rows")
    except:
        print(f"  {table_name}: Unable to count")

# Verify Silver configuration
print("\n\nSilver Configuration Summary:")
print(f"Entities configured: {list(SILVER_CONFIG['entities'].keys())}")

for entity, config in SILVER_CONFIG['entities'].items():
    print(f"\n{entity}:")
    print(f"  Bronze: {config['bronze_table']}")
    print(f"  Silver: {config['silver_table']}")
    print(f"  SCD Type: {config.get('scd_type', 'Not specified')}")
    print(f"  Business Keys: {config.get('business_keys', ['id'])}")
    
    # Check if tables exist
    bronze_exists = spark.catalog.tableExists(config['bronze_table'])
    silver_exists = spark.catalog.tableExists(config['silver_table'])
    print(f"  Status: Bronze {'✓' if bronze_exists else '✗'}, Silver {'✓' if silver_exists else '✗'}")

# Check last refresh timestamp if set
try:
    last_refresh = spark.conf.get("spark.unified_etl.last_refresh")
    print(f"\n\nLast incremental refresh: {last_refresh}")
except:
    print("\n\nNo previous incremental refresh timestamp found.")
    print("First run will use 24 hours lookback for Bronze, then incremental for Silver/Gold if tables exist.")

print("\n=== Configuration Check Complete ===")

# %%
# ==============================================================================
# MAIN INCREMENTAL REFRESH CELL
# ==============================================================================

# Configuration - UPDATE THESE BASED ON YOUR ENVIRONMENT
LAKEHOUSE_ROOT = "/lakehouse/default/Tables/"  # Update if different
DAYS_TO_REFRESH = 30  # How many days back to refresh
FORCE_FULL_REFRESH = False  # Set to True to ignore timestamps and reprocess everything

# Check what's the latest data we have
print("=== Checking Latest Records ===")
check_latest_records()

# Refresh recent data from ConnectWise
print(f"\n=== Refreshing Last {DAYS_TO_REFRESH} Days ===")
results = refresh_recent_data(DAYS_TO_REFRESH)
print("\nRefresh Summary:")
for entity, df in results.items():
    if df is not None:
        print(f"  {entity}: {df.count()} records")
    else:
        print(f"  {entity}: ERROR")

# Initialize client to get spark session
client = ConnectWiseClient()
spark = client.spark

# Write the refreshed data to Bronze tables
print("\n=== Writing to Bronze Tables ===")
for entity_name, df in results.items():
    if df is not None and df.count() > 0:
        try:
            bronze_table = f"bronze_cw_{entity_name.lower()}"
            
            if spark.catalog.tableExists(bronze_table):
                # MERGE to handle duplicates
                merge_key = "id"
                temp_view = f"temp_{entity_name.lower()}_updates"
                df.createOrReplaceTempView(temp_view)
                
                # Build MERGE SQL
                update_cols = [col for col in df.columns if col != merge_key]
                update_expr = ", ".join([f"target.{col} = source.{col}" for col in update_cols])
                insert_cols = ", ".join(df.columns)
                insert_values = ", ".join([f"source.{col}" for col in df.columns])
                
                merge_sql = f"""
                MERGE INTO {bronze_table} AS target
                USING {temp_view} AS source
                ON target.{merge_key} = source.{merge_key}
                WHEN MATCHED THEN 
                    UPDATE SET {update_expr}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_cols}) VALUES ({insert_values})
                """
                
                spark.sql(merge_sql)
                print(f"  Merged data into {bronze_table}")
                spark.catalog.dropTempView(temp_view)
            else:
                # First time - create table
                df.write.mode("overwrite").saveAsTable(bronze_table)
                print(f"  Created {bronze_table} with {df.count()} records")

        except Exception as e:
            print(f"  ERROR writing {entity_name}: {e}")
            if "DELTA_FAILED_TO_MERGE_FIELDS" in str(e):
                print("  Schema conflict - consider full refresh with FORCE_FULL_REFRESH = True")

print("\n=== Incremental Refresh Complete ===")
print("Next steps:")
print("1. Verify the data looks correct")
print("2. Run the cascade update cell below to update Silver and Gold layers")
print("3. Monitor for any schema evolution issues")

# Store refreshed entities for the next cell
refreshed_entities = [name for name, df in results.items() if df is not None and df.count() > 0]
print(f"\nRefreshed entities stored: {refreshed_entities}")


# %%
# ==============================================================================
# CASCADE UPDATE CELL - Run this after successful Bronze refresh
# ==============================================================================
"""
This cell INCREMENTALLY updates Silver and Gold layers after Bronze refresh.
Uses Delta Lake change tracking to process only modified records.
Prerequisites:
- Run the incremental refresh cell above first
- Ensure 'refreshed_entities' variable contains the entities to update
"""

from datetime import datetime, timedelta
from pyspark.sql import functions as F

# Get the last refresh timestamp (default to 24 hours ago if not set)
if FORCE_FULL_REFRESH:
    last_refresh = "1900-01-01T00:00:00"  # Force processing all records
    print("FORCE_FULL_REFRESH is True - processing all records")
else:
    try:
        last_refresh = spark.conf.get("spark.unified_etl.last_refresh")
        print(f"Last refresh was at: {last_refresh}")
    except:
        last_refresh = (datetime.now() - timedelta(hours=24)).isoformat()
        print(f"No last refresh found, using: {last_refresh}")

# CASCADE UPDATE - SILVER LAYER (INCREMENTAL)
print("\n=== Incremental Silver Layer Update ===")
from unified_etl_connectwise import models
from unified_etl_connectwise.config import SILVER_CONFIG
from unified_etl_core.silver import apply_silver_transformations

model_mapping = {
    "Agreement": models.get("agreement"),
    "TimeEntry": models.get("timeentry"),
    "ExpenseEntry": models.get("expenseentry"),
    "UnpostedInvoice": models.get("invoice"),
    "PostedInvoice": models.get("invoice"),
    "ProductItem": models.get("productitem"),
}

# Process only entities that had new data
refreshed_entities = [name for name, df in results.items() if df is not None and df.count() > 0]
print(f"Entities to update in Silver: {refreshed_entities}")

# Track what changed for Gold layer
silver_changes = {}

for entity_name in refreshed_entities:
    # Check if entity has Silver config
    if entity_name in SILVER_CONFIG["entities"]:
        entity_config = SILVER_CONFIG["entities"][entity_name]
        bronze_table = f"bronze_cw_{entity_name.lower()}"
        silver_table = entity_config["silver_table"]  # Use config table name

        print(f"\nProcessing {entity_name}: {bronze_table} -> {silver_table}")

        try:
            # INCREMENTAL: Read only NEW/CHANGED Bronze records
            bronze_changes = spark.sql(f"""
                SELECT * FROM {bronze_table}
                WHERE _etl_timestamp >= '{last_refresh}'
            """)
            
            change_count = bronze_changes.count()
            print(f"  Found {change_count} new/changed records in Bronze")
            
            if change_count == 0:
                print(f"  No changes for {entity_name}, skipping")
                continue
            
            # Track changed IDs for Gold processing
            changed_ids = bronze_changes.select("id").distinct()
            silver_changes[entity_name] = changed_ids

            # Get model class
            model_class = model_mapping.get(entity_name)
            if not model_class:
                print(f"  WARNING: No model class found for {entity_name}")
                continue

            # Apply Silver transformations to ONLY changed records
            silver_delta = apply_silver_transformations(
                df=bronze_changes,
                entity_config=entity_config,
                model_class=model_class,
            )

            
            scd_type = entity_config.get("scd_type", 1)
            business_keys = entity_config.get("business_keys", ["id"])
            
            # Check if Silver table exists
            if spark.catalog.tableExists(silver_table):
                if scd_type == 1:
                    # For SCD Type 1, we can use MERGE to update/insert
                    merge_key_conditions = " AND ".join([
                        f"target.{key} = source.{key}" for key in business_keys
                    ])
                    
                    # Create temp view for merge
                    temp_view = f"temp_silver_{entity_name.lower()}_delta"
                    silver_delta.createOrReplaceTempView(temp_view)
                    
                    # Build MERGE statement (UPDATE SET * for all columns)
                    merge_sql = f"""
                    MERGE INTO {silver_table} AS target
                    USING {temp_view} AS source
                    ON {merge_key_conditions}
                    WHEN MATCHED THEN 
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
                    """
                    
                    # Execute merge
                    spark.sql(merge_sql)
                    print(f"  Merged {change_count} records into {silver_table} (SCD Type 1)")
                    
                    # Clean up temp view
                    spark.catalog.dropTempView(temp_view)
                else:
                    # SCD Type 2 - for now, just append (proper implementation would close old records)
                    print(f"  WARNING: SCD Type 2 incremental not fully implemented, appending records")
                    silver_delta.write.mode("append").saveAsTable(silver_table)
            else:
                # First time - create table using the framework function
                print(f"  Silver table doesn't exist, creating with full Bronze data")
                from unified_etl_core.silver import process_bronze_to_silver
                
                # We need full bronze data for first time
                full_bronze_df = spark.table(bronze_table)
                
                # Process using the framework (it handles the write)
                process_bronze_to_silver(
                    entity_name=entity_name.lower(),
                    bronze_table_name=bronze_table,
                    lakehouse_root="/lakehouse/default/Tables/",
                    entity_config=entity_config,
                    model_class=model_class,
                )

        except Exception as e:
            print(f"  ERROR processing {entity_name}: {e}")
    else:
        print(f"\n{entity_name} not found in SILVER_CONFIG, skipping Silver update")

# CASCADE UPDATE - GOLD LAYER (INCREMENTAL)
print("\n\n=== Incremental Gold Layer Update ===")
from unified_etl_connectwise.transforms import (
    create_agreement_period_fact,
    create_expense_entry_fact,
    create_invoice_line_fact,
    create_time_entry_fact,
)

# Check which Gold fact tables need updating based on what changed
print("\nDetermining which fact tables to update based on changed records...")

if "TimeEntry" in silver_changes or "Agreement" in silver_changes:
    print("\nUpdating fact_time_entry (incremental)...")
    try:
        affected_time_ids = []
        
        if "TimeEntry" in silver_changes:
            # Direct time entry changes
            time_ids = silver_changes["TimeEntry"]
            affected_time_ids.append(time_ids)
            
        if "Agreement" in silver_changes:
            # Time entries linked to changed agreements
            agreement_ids = silver_changes["Agreement"]
            linked_times = spark.sql(f"""
                SELECT DISTINCT id 
                FROM silver_cw_timeentry 
                WHERE agreementId IN (SELECT id FROM ({agreement_ids.createOrReplaceTempView("temp_agreement_ids")}))
            """)
            affected_time_ids.append(linked_times)
        
        if affected_time_ids:
            # Union all affected time entry IDs
            all_affected = affected_time_ids[0]
            for df in affected_time_ids[1:]:
                all_affected = all_affected.union(df).distinct()
            
            all_affected.createOrReplaceTempView("temp_affected_time_ids")
            
            # Get only affected time entries
            time_entry_delta = spark.sql("""
                SELECT * FROM silver_cw_timeentry
                WHERE id IN (SELECT id FROM temp_affected_time_ids)
            """)
            
            # Get all agreements (we need full set for lookups)
            agreement_silver = spark.table("silver_cw_agreement")
            member_silver = spark.table("silver_cw_member") if spark.catalog.tableExists("silver_cw_member") else None
            
            # Create facts for affected entries
            fact_delta = create_time_entry_fact(
                spark=spark,
                time_entry_df=time_entry_delta,
                agreement_df=agreement_silver,
                member_df=member_silver,
            )
            
            print(f"  Processing {fact_delta.count()} affected time entries")
            
            # MERGE into existing fact table
            if spark.catalog.tableExists("gold_fact_time_entry"):
                fact_delta.createOrReplaceTempView("temp_fact_time_delta")
                
                merge_sql = """
                MERGE INTO gold_fact_time_entry AS target
                USING temp_fact_time_delta AS source
                ON target.timeEntryId = source.timeEntryId
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
                
                spark.sql(merge_sql)
                print("  Merged changes into fact_time_entry")
            else:
                # First time - create full fact table
                print("  Creating fact_time_entry for first time (full load)")
                time_entry_silver = spark.table("silver_cw_timeentry")
                fact_df = create_time_entry_fact(
                    spark=spark,
                    time_entry_df=time_entry_silver,
                    agreement_df=agreement_silver,
                    member_df=member_silver,
                )
                fact_df.write.mode("overwrite").saveAsTable("gold_fact_time_entry")
                print(f"  Created fact_time_entry with {fact_df.count()} records")
                
    except Exception as e:
        print(f"  ERROR updating fact_time_entry: {e}")

if "Agreement" in silver_changes:
    print("\nUpdating fact_agreement_period (incremental)...")
    try:
        # For agreement periods, we only need to recalculate periods for changed agreements
        changed_agreement_ids = silver_changes["Agreement"]
        changed_agreement_ids.createOrReplaceTempView("temp_changed_agreement_ids")
        
        # Get only changed agreements
        agreement_delta = spark.sql("""
            SELECT * FROM silver_cw_agreement
            WHERE id IN (SELECT id FROM temp_changed_agreement_ids)
        """)
        
        # Create periods for changed agreements
        fact_delta = create_agreement_period_fact(
            spark=spark, 
            agreement_df=agreement_delta, 
            config={"start_date": "2020-01-01", "frequency": "month"}
        )
        
        print(f"  Processing {changed_agreement_ids.count()} changed agreements")
        
        if spark.catalog.tableExists("gold_fact_agreement_period"):
            # Delete old periods for these agreements
            spark.sql("""
                DELETE FROM gold_fact_agreement_period
                WHERE AgreementSK IN (
                    SELECT DISTINCT AgreementSK FROM temp_fact_agreement_delta
                )
            """)
            
            # Insert new periods
            fact_delta.write.mode("append").saveAsTable("gold_fact_agreement_period")
            print("  Updated fact_agreement_period")
        else:
            # First time - create full table
            print("  Creating fact_agreement_period for first time (full load)")
            agreement_silver = spark.table("silver_cw_agreement")
            fact_df = create_agreement_period_fact(
                spark=spark, 
                agreement_df=agreement_silver, 
                config={"start_date": "2020-01-01", "frequency": "month"}
            )
            fact_df.write.mode("overwrite").saveAsTable("gold_fact_agreement_period")
            print(f"  Created fact_agreement_period with {fact_df.count()} records")
            
    except Exception as e:
        print(f"  ERROR updating fact_agreement_period: {e}")

if "ExpenseEntry" in silver_changes:
    print("\nUpdating fact_expense_entry (incremental)...")
    try:
        changed_expense_ids = silver_changes["ExpenseEntry"]
        changed_expense_ids.createOrReplaceTempView("temp_changed_expense_ids")
        
        expense_delta = spark.sql("""
            SELECT * FROM silver_cw_expenseentry
            WHERE id IN (SELECT id FROM temp_changed_expense_ids)
        """)
        
        agreement_silver = spark.table("silver_cw_agreement") if spark.catalog.tableExists("silver_cw_agreement") else None
        
        fact_delta = create_expense_entry_fact(
            spark=spark, 
            expense_df=expense_delta, 
            agreement_df=agreement_silver
        )
        
        print(f"  Processing {fact_delta.count()} expense entries")
        
        if spark.catalog.tableExists("gold_fact_expense_entry"):
            fact_delta.createOrReplaceTempView("temp_fact_expense_delta")
            
            merge_sql = """
            MERGE INTO gold_fact_expense_entry AS target
            USING temp_fact_expense_delta AS source
            ON target.expenseId = source.expenseId
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
            
            spark.sql(merge_sql)
            print("  Merged changes into fact_expense_entry")
        else:
            fact_delta.write.mode("overwrite").saveAsTable("gold_fact_expense_entry")
            print(f"  Created fact_expense_entry with {fact_delta.count()} records")
            
    except Exception as e:
        print(f"  ERROR updating fact_expense_entry: {e}")

# Store the current timestamp for next incremental run
current_timestamp = datetime.now().isoformat()
spark.conf.set("spark.unified_etl.last_refresh", current_timestamp)
print(f"\n=== Incremental Update Complete ===")
print(f"Last refresh timestamp saved: {current_timestamp}")
print("Only processed changed records through Bronze -> Silver -> Gold!")

# %%
# ==============================================================================
# DIMENSION GENERATION CELL - Run after Gold layer update
# ==============================================================================
"""
Generate dimensions from Silver tables for PowerBI consumption.
This creates lightweight dimension tables from enum-like columns.
"""

print("\n=== Generating Dimensions ===")
from unified_etl_core.date_utils import generate_date_dimension
from unified_etl_core.dimensions import create_dimension_from_column

# Generate date dimension if it doesn't exist
if not spark.catalog.tableExists("gold_dim_date"):
    print("\nCreating date dimension...")
    try:
        date_dim = generate_date_dimension(
            spark=spark,
            start_date="2020-01-01",
            end_date="2030-12-31",
            fiscal_year_start_month=7,  # July fiscal year
        )
        date_dim.write.mode("overwrite").saveAsTable("gold_dim_date")
        print(f"  Created dim_date with {date_dim.count()} records")
    except Exception as e:
        print(f"  ERROR creating dim_date: {e}")

# Define dimensions to generate from Silver tables
dimension_configs = [
    # From TimeEntry
    {
        "source_table": "silver_cw_timeentry",
        "column": "billableOption",
        "dimension_name": "dim_billable_option",
    },
    {
        "source_table": "silver_cw_timeentry",
        "column": "status",
        "dimension_name": "dim_time_status",
    },
    {
        "source_table": "silver_cw_timeentry",
        "column": "chargeToType",
        "dimension_name": "dim_charge_type",
    },
    # From Agreement
    {
        "source_table": "silver_cw_agreement",
        "column": "agreementStatus",
        "dimension_name": "dim_agreement_status",
    },
    {
        "source_table": "silver_cw_agreement",
        "column": "billCycleIdentifier",
        "dimension_name": "dim_bill_cycle",
    },
    {
        "source_table": "silver_cw_agreement",
        "column": "periodType",
        "dimension_name": "dim_period_type",
    },
    # From Invoice
    {
        "source_table": "silver_cw_invoice",
        "column": "statusName",
        "dimension_name": "dim_invoice_status",
    },
    # From ExpenseEntry
    {
        "source_table": "silver_cw_expenseentry",
        "column": "typeName",
        "dimension_name": "dim_expense_type",
    },
]

# Generate dimensions
for config in dimension_configs:
    if spark.catalog.tableExists(config["source_table"]):
        print(
            f"\nGenerating {config['dimension_name']} from {config['source_table']}.{config['column']}..."
        )
        try:
            dim_df = create_dimension_from_column(
                spark=spark,
                source_table=config["source_table"],
                column_name=config["column"],
                dimension_name=config["dimension_name"],
                include_counts=True,
            )

            # Write dimension table
            table_name = f"gold_{config['dimension_name']}"
            dim_df.write.mode("overwrite").saveAsTable(table_name)
            print(f"  Created {table_name} with {dim_df.count()} values")
        except Exception as e:
            print(f"  ERROR creating {config['dimension_name']}: {e}")
    else:
        print(
            f"\nSkipping {config['dimension_name']} - source table {config['source_table']} not found"
        )

print("\n=== Dimension Generation Complete ===")

# %%
# ==============================================================================
# COLUMN VALUE ANALYSIS CELL - Identify high-value vs noise columns
# ==============================================================================
"""
Analyze columns to identify which have signal vs noise for PowerBI views.
This helps create focused views that exclude columns with no analytical value.
"""

print("\n=== Column Value Analysis ===")

from pyspark.sql import functions as F  # noqa: E402


def analyze_column_value(df, table_name, sample_size=10000):
    """Analyze columns to provide detailed statistics without arbitrary scoring."""

    # Sample the data if it's large
    total_count = df.count()
    if total_count > sample_size:
        df_sample = df.sample(fraction=sample_size / total_count)
    else:
        df_sample = df

    print(f"\nAnalyzing {table_name} ({total_count} rows, sampled {df_sample.count()})...")

    analysis_results = []

    for col in df.columns:
        # Skip system columns
        if col.startswith("_etl_") or col.startswith("etl"):
            continue

        # Get basic stats
        distinct_count = df_sample.select(col).distinct().count()
        null_count = df_sample.filter(F.col(col).isNull()).count()
        null_percentage = (null_count / df_sample.count()) * 100
        non_null_count = df_sample.count() - null_count

        # Determine column type
        col_type = str(df.schema[col].dataType)
        
        # Calculate cardinality ratio (distinct values / non-null rows)
        cardinality_ratio = distinct_count / non_null_count if non_null_count > 0 else 0
        
        # Get sample values for low-cardinality columns
        sample_values = []
        if distinct_count <= 10 and distinct_count > 0:
            values_df = df_sample.select(col).distinct().filter(F.col(col).isNotNull()).limit(10).collect()
            sample_values = [str(row[0]) for row in values_df]
        
        # For numeric columns, get min/max/mean
        numeric_stats = {}
        if "Int" in col_type or "Double" in col_type or "Decimal" in col_type or "Float" in col_type:
            stats_df = df_sample.select(
                F.min(col).alias("min"),
                F.max(col).alias("max"),
                F.mean(col).alias("mean"),
                F.stddev(col).alias("stddev")
            ).collect()[0]
            numeric_stats = {
                "min": stats_df["min"],
                "max": stats_df["max"],
                "mean": round(stats_df["mean"], 2) if stats_df["mean"] else None,
                "stddev": round(stats_df["stddev"], 2) if stats_df["stddev"] else None
            }

        # Column category based on characteristics
        category = "unknown"
        if col.lower().endswith(("id", "key", "_id", "_key")):
            category = "identifier"
        elif any(kw in col.lower() for kw in ["amount", "cost", "price", "revenue", "total", "sum"]):
            category = "financial"
        elif any(kw in col.lower() for kw in ["hours", "days", "minutes", "duration"]):
            category = "time_measure"
        elif any(kw in col.lower() for kw in ["date", "time", "created", "updated", "modified"]):
            category = "temporal"
        elif any(kw in col.lower() for kw in ["name", "description", "notes", "comment"]):
            category = "text"
        elif any(kw in col.lower() for kw in ["status", "type", "category", "class"]):
            category = "categorical"
        elif "Boolean" in col_type:
            category = "boolean"
        elif distinct_count == 1:
            category = "constant"
        elif cardinality_ratio > 0.95:
            category = "high_cardinality"
        elif cardinality_ratio < 0.01:
            category = "low_cardinality"

        analysis_results.append({
            "column": col,
            "type": col_type,
            "category": category,
            "distinct_values": distinct_count,
            "null_count": null_count,
            "null_percentage": round(null_percentage, 1),
            "cardinality_ratio": round(cardinality_ratio, 3),
            "sample_values": sample_values[:5] if sample_values else [],
            "numeric_stats": numeric_stats
        })

    # Sort by category, then by null percentage
    analysis_results.sort(key=lambda x: (x["category"], x["null_percentage"]))

    # Print analysis by category
    categories = {}
    for result in analysis_results:
        cat = result["category"]
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(result)
    
    print(f"\n  Column Analysis by Category:")
    print(f"  {'Category':<20} {'Count':<8} {'Avg Nulls %':<12}")
    print(f"  {'-'*40}")
    
    for cat, cols in categories.items():
        avg_nulls = sum(c["null_percentage"] for c in cols) / len(cols)
        print(f"  {cat:<20} {len(cols):<8} {avg_nulls:<12.1f}")
    
    # Show detailed analysis for each category
    for cat, cols in categories.items():
        print(f"\n  === {cat.upper()} COLUMNS ===")
        for col in cols[:5]:  # Show first 5 of each category
            print(f"\n  {col['column']} ({col['type']})")
            print(f"    Nulls: {col['null_percentage']}% | Distinct: {col['distinct_values']} | Cardinality: {col['cardinality_ratio']}")
            if col['sample_values']:
                print(f"    Sample values: {col['sample_values']}")
            if col['numeric_stats']:
                stats = col['numeric_stats']
                print(f"    Range: [{stats['min']} - {stats['max']}] | Mean: {stats['mean']} | StdDev: {stats['stddev']}")
        
        if len(cols) > 5:
            print(f"    ... and {len(cols) - 5} more {cat} columns")

    # Identify potentially problematic columns
    print(f"\n  === POTENTIAL ISSUES ===")
    
    constant_cols = [r for r in analysis_results if r["category"] == "constant"]
    if constant_cols:
        print(f"\n  Constant columns (single value):")
        for col in constant_cols:
            print(f"    {col['column']}: '{col['sample_values'][0] if col['sample_values'] else 'NULL'}'")
    
    high_null_cols = [r for r in analysis_results if r["null_percentage"] > 95]
    if high_null_cols:
        print(f"\n  Nearly empty columns (>95% null):")
        for col in high_null_cols:
            print(f"    {col['column']}: {col['null_percentage']}% null")
    
    return analysis_results


# Analyze key tables
tables_to_analyze = [
    "silver_cw_timeentry",
    "silver_cw_agreement",
    "silver_cw_invoice",
    "gold_fact_time_entry",
]

all_analysis = {}
for table in tables_to_analyze:
    if spark.catalog.tableExists(table):
        df = spark.table(table)
        analysis = analyze_column_value(df, table)
        all_analysis[table] = analysis

# %%
# ==============================================================================
# CREATE POWERBI VIEWS CELL - Create optimized views for PowerBI
# ==============================================================================
"""
Create SQL views optimized for PowerBI consumption.
These views exclude low-value columns identified in the analysis above.
"""

print("\n=== Creating PowerBI Optimized Views ===")


# Function to create view based on column analysis
def create_powerbi_view(table_name, analysis_results, view_suffix="_pbi"):
    """Create a view excluding problematic columns based on analysis."""

    # Exclude columns based on objective criteria
    exclude_categories = ["constant", "unknown"]  # Always exclude these
    exclude_columns = set()
    
    for result in analysis_results:
        # Exclude constant columns (single value)
        if result["category"] == "constant":
            exclude_columns.add(result["column"])
        # Exclude nearly empty columns (>95% null) unless they're important categories
        elif result["null_percentage"] > 95 and result["category"] not in ["identifier", "financial"]:
            exclude_columns.add(result["column"])
        # Exclude unknown columns with high nulls
        elif result["category"] == "unknown" and result["null_percentage"] > 80:
            exclude_columns.add(result["column"])
    
    # Get columns to keep
    keep_columns = [r["column"] for r in analysis_results if r["column"] not in exclude_columns]
    
    # Always include certain critical columns even if they have issues
    critical_patterns = ["id", "key", "sk", "date", "amount", "revenue"]
    for result in analysis_results:
        col = result["column"]
        if any(pattern in col.lower() for pattern in critical_patterns) and col not in keep_columns:
            keep_columns.append(col)

    # Build SELECT statement
    select_cols = ", ".join(keep_columns)
    view_name = f"{table_name}{view_suffix}"

    create_view_sql = f"""
    CREATE OR REPLACE VIEW {view_name} AS
    SELECT {select_cols}
    FROM {table_name}
    """

    try:
        spark.sql(create_view_sql)
        print(
            f"  Created view {view_name} with {len(keep_columns)} columns (excluded {len(exclude_columns)} columns)"
        )
        if exclude_columns:
            print(f"    Excluded: {', '.join(sorted(exclude_columns))}")
        return view_name
    except Exception as e:
        print(f"  ERROR creating view {view_name}: {e}")
        return None


# Create views for analyzed tables
for table_name, analysis in all_analysis.items():
    create_powerbi_view(table_name, analysis)

# Create specialized fact views with business logic
print("\n\nCreating specialized business views...")

# Time Entry Analysis View
time_entry_view_sql = """
CREATE OR REPLACE VIEW v_time_entry_analysis AS
SELECT
    TimeEntrySK,
    WorkDateSK,
    memberName,
    agreementName,
    agreementType,
    utilizationType,
    actualHours,
    potentialRevenue,
    actualCost,
    margin,
    marginPercentage,
    isInternalWork,
    isTimapottur,
    effectiveBillingStatus,
    daysSinceWork,
    daysUninvoiced
FROM gold_fact_time_entry
WHERE actualHours > 0  -- Filter out zero-hour entries
"""

try:
    spark.sql(time_entry_view_sql)
    print("  Created v_time_entry_analysis")
except Exception as e:
    print(f"  ERROR creating v_time_entry_analysis: {e}")

print("\n=== PowerBI View Creation Complete ===")
print("\nNext steps:")
print("1. Connect PowerBI to the Lakehouse")
print("2. Use the _pbi views for optimized performance")
print("3. Use v_time_entry_analysis for time tracking analytics")
print("4. Join views with gold_dim_* tables for descriptive labels")
