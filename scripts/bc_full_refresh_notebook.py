# Cell 1: Install Required Wheels
# This cell installs the unified ETL packages from the lakehouse

%pip install /lakehouse/default/Files/unified_etl_core-1.0.0-py3-none-any.whl
%pip install /lakehouse/default/Files/unified_etl_businesscentral-1.0.0-py3-none-any.whl

# Note: After running this cell, you may need to restart the kernel if packages were already loaded

# Cell 2: Business Central Full Data Refresh
# Initialize the refresh process

import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

print("🚀 Starting Business Central Full Data Refresh...")
print(f"📅 Timestamp: {datetime.now().isoformat()}")

# Get active Spark session from Fabric
spark = SparkSession.getActiveSession()
if not spark:
    print("❌ No active Spark session found. This script must run in a Fabric notebook.")
    sys.exit(1)

# Import required modules
from unified_etl_core.main import run_etl_pipeline
from unified_etl_businesscentral import (
    BC_FACT_CONFIGS,
    SILVER_CONFIG,
    create_bc_dimension_bridge,
    create_bc_item_attribute_dimension,
    create_bc_item_attribute_bridge,
    build_bc_account_hierarchy,
    create_purchase_fact,
    create_agreement_fact
)

# Cell 3: Configuration
batch_id = f"bc_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
lakehouse_root = "/lakehouse/default/Tables/"

# Get the actual catalog name from Spark
catalog_name = spark.catalog.currentCatalog()
print(f"  Current catalog: {catalog_name}")

print(f"🔧 Configuration:")
print(f"  Batch ID: {batch_id}")
print(f"  Lakehouse Root: {lakehouse_root}")

# Define all BC entities to process
bc_entities = [
    # Core Entities
    "Customer", "Vendor", "Item", "Resource",
    "GLAccount", "GLEntry", 
    "Currency", "CompanyInformation",
    
    # Dimension Entities
    "Dimension", "DimensionValue", "DimensionSetEntry",
    
    # Purchase Documents
    "PurchInvHeader", "PurchInvLine",
    "PurchCrMemoHeader", "PurchCrMemoLine",
    
    # Sales Documents  
    "SalesInvoiceHeader", "SalesInvoiceLine",
    
    # Ledger Entries
    "CustLedgerEntry", "DetailedCustLedgEntry",
    "VendorLedgerEntry", "DetailedVendorLedgEntry",
    
    # Job/Project
    "Job", "JobLedgerEntry",
    
    # Agreement Management
    "AMSAgreementHeader", "AMSAgreementLine",
    
    # Setup Tables
    "GeneralLedgerSetup", "AccountingPeriod"
]

print(f"\n📊 Processing {len(bc_entities)} Business Central entities")

# Cell 4: Bronze Layer - Extract from BC API
print("\n" + "="*50)
print("🥉 BRONZE LAYER: Extracting from Business Central API...")

try:
    # Run Bronze extraction for all BC entities
    bronze_result = run_etl_pipeline(
        integrations=["businesscentral"],
        layers=["bronze"],
        mode="full",
        config={
            "batch_id": batch_id,
            "lakehouse_root": lakehouse_root,
            "entities": bc_entities
        }
    )
    
    print("✅ Bronze layer extraction complete!")
    
    # Show extraction summary
    if bronze_result and "bronze_stats" in bronze_result:
        print("\n📈 Bronze Extraction Summary:")
        for entity, stats in bronze_result["bronze_stats"].items():
            print(f"  {entity}: {stats.get('row_count', 0)} rows")
            
except Exception as e:
    logger.error(f"Bronze layer extraction failed: {e}")
    print(f"❌ Bronze Error: {e}")
    raise

# Cell 5: Silver Layer - Transform & Standardize
print("\n" + "="*50)
print("🥈 SILVER LAYER: Transforming Business Central data...")

try:
    # Process Silver layer for all entities that have configurations
    silver_entities = [e for e in bc_entities if e in SILVER_CONFIG]
    
    print(f"📋 Processing {len(silver_entities)} entities with silver configurations")
    
    silver_result = run_etl_pipeline(
        integrations=["businesscentral"],
        layers=["silver"],
        mode="full",
        config={
            "batch_id": batch_id,
            "lakehouse_root": lakehouse_root,
            "entities": silver_entities,
            "entity_configs": SILVER_CONFIG
        }
    )
    
    print("✅ Silver layer transformation complete!")
    
    # Verify key silver tables
    print("\n🔍 Verifying Silver Tables:")
    for entity in ["Customer", "Vendor", "Item", "DimensionSetEntry"]:
        try:
            df = spark.table(f"{catalog_name}.silver.{entity}")
            count = df.count()
            print(f"  ✅ {entity}: {count} rows")
        except Exception as e:
            print(f"  ❌ {entity}: Not found or error - {e}")
            
except Exception as e:
    logger.error(f"Silver layer transformation failed: {e}")
    print(f"❌ Silver Error: {e}")
    raise

# Cell 6: Gold Layer - Dimensions
print("\n" + "="*50)
print("🥇 GOLD LAYER - PART 1: Creating Business Central Dimensions...")

gold_path = f"{catalog_name}.gold"
silver_path = f"{catalog_name}.silver"

try:
    # 1. Create BC Account Hierarchy
    print("\n📊 Creating Account Hierarchy...")
    try:
        glaccount_df = spark.table(f"{silver_path}.GLAccount")
        account_hierarchy_df = build_bc_account_hierarchy(
            df=glaccount_df,
            indentation_col="Indentation",
            no_col="No",
            surrogate_key_col="GLAccountKey"
        )
        account_hierarchy_df.write.mode("overwrite").saveAsTable(f"{gold_path}.dim_GLAccountHierarchy")
        print(f"  ✅ Created GL Account Hierarchy: {account_hierarchy_df.count()} accounts")
    except Exception as e:
        print(f"  ⚠️ Account hierarchy skipped: {e}")
    
    # 2. Create Dimension Bridge
    print("\n🌉 Creating Dimension Bridge...")
    try:
        dimension_types = {
            "DEPARTMENT": "DEPARTMENT",
            "PROJECT": "PROJECT", 
            "CUSTOMERGROUP": "CUSTOMERGROUP",
            "AREA": "AREA",
            "EMPLOYEE": "EMPLOYEE",
            "SALESPERSON": "SALESPERSON"
        }
        
        dim_bridge_df = create_bc_dimension_bridge(
            spark=spark,
            silver_path=silver_path,
            gold_path=gold_path,
            dimension_types=dimension_types
        )
        dim_bridge_df.write.mode("overwrite").saveAsTable(f"{gold_path}.dim_DimensionBridge")
        print(f"  ✅ Created Dimension Bridge: {dim_bridge_df.count()} entries")
    except Exception as e:
        print(f"  ⚠️ Dimension bridge skipped: {e}")
    
    # 3. Create Item Attribute Dimensions
    print("\n📦 Creating Item Attribute Dimensions...")
    try:
        # Create attribute dimension
        item_attr_dim_df = create_bc_item_attribute_dimension(
            spark=spark,
            silver_path=silver_path,
            gold_path=gold_path
        )
        item_attr_dim_df.write.mode("overwrite").saveAsTable(f"{gold_path}.dim_ItemAttribute")
        print(f"  ✅ Created Item Attribute Dimension: {item_attr_dim_df.count()} attributes")
        
        # Create attribute bridge
        item_attr_bridge_df = create_bc_item_attribute_bridge(
            spark=spark,
            silver_path=silver_path,
            gold_path=gold_path
        )
        item_attr_bridge_df.write.mode("overwrite").saveAsTable(f"{gold_path}.dim_ItemAttributeBridge")
        print(f"  ✅ Created Item Attribute Bridge: {item_attr_bridge_df.count()} mappings")
    except Exception as e:
        print(f"  ⚠️ Item attributes skipped: {e}")
    
    # 4. Create standard dimensions using generic dimension creator
    print("\n📐 Creating Standard Dimensions...")
    from unified_etl_core.dimensions import create_dimension_from_column
    
    # Define standard dimensions to create
    standard_dimensions = {
        "dim_Customer": {
            "entity": "Customer",
            "enum_column": "Blocked",
            "code_column": "No",
            "description_column": "Name"
        },
        "dim_Vendor": {
            "entity": "Vendor", 
            "enum_column": "Blocked",
            "code_column": "No",
            "description_column": "Name"
        },
        "dim_Item": {
            "entity": "Item",
            "enum_column": "Type",
            "code_column": "No",
            "description_column": "Description"
        },
        "dim_Currency": {
            "entity": "Currency",
            "enum_column": "Code",
            "code_column": "Code",
            "description_column": "Description"
        }
    }
    
    for dim_name, dim_config in standard_dimensions.items():
        try:
            entity_df = spark.table(f"{silver_path}.{dim_config['entity']}")
            dim_df = create_dimension_from_column(
                df=entity_df,
                column_name=dim_config["enum_column"],
                dimension_name=dim_name.replace("dim_", "")
            )
            dim_df.write.mode("overwrite").saveAsTable(f"{gold_path}.{dim_name}")
            print(f"  ✅ Created {dim_name}: {dim_df.count()} rows")
        except Exception as e:
            print(f"  ⚠️ {dim_name} skipped: {e}")
    
    print("\n✅ Gold layer dimensions complete!")
    
except Exception as e:
    logger.error(f"Gold dimension creation failed: {e}")
    print(f"❌ Gold Dimension Error: {e}")
    raise

# Cell 7: Gold Layer - Fact Tables
print("\n" + "="*50)
print("🥇 GOLD LAYER - PART 2: Creating Business Central Fact Tables...")

try:
    # 1. Create Purchase Fact
    print("\n🛒 Creating Purchase Fact Table...")
    try:
        purchase_fact_df = create_purchase_fact(
            spark=spark,
            silver_path=silver_path,
            gold_path=gold_path,
            batch_id=batch_id
        )
        purchase_fact_df.write.mode("overwrite").saveAsTable(f"{gold_path}.fact_Purchase")
        
        # Show fact summary
        total_amount = purchase_fact_df.agg({"LineAmountExclVAT": "sum"}).collect()[0][0]
        print(f"  ✅ Created fact_Purchase: {purchase_fact_df.count()} lines")
        print(f"     Total Purchase Amount: ${total_amount:,.2f}")
    except Exception as e:
        print(f"  ⚠️ Purchase fact skipped: {e}")
    
    # 2. Create Agreement Fact
    print("\n📄 Creating Agreement Fact Table...")
    try:
        agreement_fact_df = create_agreement_fact(
            spark=spark,
            silver_path=silver_path,
            gold_path=gold_path,
            batch_id=batch_id
        )
        agreement_fact_df.write.mode("overwrite").saveAsTable(f"{gold_path}.fact_Agreement")
        print(f"  ✅ Created fact_Agreement: {agreement_fact_df.count()} agreement lines")
    except Exception as e:
        print(f"  ⚠️ Agreement fact skipped: {e}")
    
    # 3. Create GL Entry Fact (using generic fact creator)
    print("\n📊 Creating GL Entry Fact Table...")
    try:
        from unified_etl_core.facts import create_generic_fact_table
        
        glentry_df = spark.table(f"{silver_path}.GLEntry")
        glentry_fact_df = create_generic_fact_table(
            df=glentry_df,
            fact_name="GLEntry",
            surrogate_keys=[{
                "name": "GLEntrySK",
                "business_keys": ["EntryNo", "$Company"]
            }],
            business_keys=[{
                "name": "GLEntryBusinessKey",
                "source_columns": ["EntryNo", "$Company"]
            }],
            calculated_columns={
                "IsDebit": "CASE WHEN Amount > 0 THEN true ELSE false END",
                "IsCredit": "CASE WHEN Amount < 0 THEN true ELSE false END",
                "AbsoluteAmount": "ABS(Amount)"
            },
            batch_id=batch_id
        )
        glentry_fact_df.write.mode("overwrite").saveAsTable(f"{gold_path}.fact_GLEntry")
        print(f"  ✅ Created fact_GLEntry: {glentry_fact_df.count()} entries")
    except Exception as e:
        print(f"  ⚠️ GL Entry fact skipped: {e}")
    
    print("\n✅ Gold layer facts complete!")
    
except Exception as e:
    logger.error(f"Gold fact creation failed: {e}")
    print(f"❌ Gold Fact Error: {e}")
    raise

# Cell 8: Final Summary & Validation
print("\n" + "="*50)
print("📊 BUSINESS CENTRAL REFRESH SUMMARY")
print("="*50)

try:
    # Count records in each layer
    print("\n🥉 Bronze Layer:")
    bronze_tables = spark.catalog.listTables("bronze")
    bc_bronze = [t.name for t in bronze_tables if t.name.startswith("BC_") or t.name in bc_entities]
    for table in bc_bronze[:5]:  # Show first 5
        try:
            count = spark.table(f"bronze.{table}").count()
            print(f"  {table}: {count} rows")
        except:
            pass
    if len(bc_bronze) > 5:
        print(f"  ... and {len(bc_bronze) - 5} more tables")
    
    print("\n🥈 Silver Layer:")
    silver_tables = spark.catalog.listTables("silver")
    bc_silver = [t.name for t in silver_tables if t.name in bc_entities]
    for table in bc_silver[:5]:  # Show first 5
        try:
            count = spark.table(f"silver.{table}").count()
            print(f"  {table}: {count} rows")
        except:
            pass
    if len(bc_silver) > 5:
        print(f"  ... and {len(bc_silver) - 5} more tables")
    
    print("\n🥇 Gold Layer:")
    gold_tables = spark.catalog.listTables("gold")
    
    # Show dimensions
    print("  Dimensions:")
    dims = [t.name for t in gold_tables if t.name.startswith("dim_")]
    for dim in dims:
        try:
            count = spark.table(f"gold.{dim}").count()
            print(f"    {dim}: {count} rows")
        except:
            pass
    
    # Show facts
    print("  Facts:")
    facts = [t.name for t in gold_tables if t.name.startswith("fact_")]
    for fact in facts:
        try:
            count = spark.table(f"gold.{fact}").count()
            print(f"    {fact}: {count} rows")
        except:
            pass
    
    print(f"\n🎯 Business Central Full Refresh Complete!")
    print(f"   Batch ID: {batch_id}")
    print(f"   Timestamp: {datetime.now().isoformat()}")
    print(f"   Duration: Check Fabric monitoring for details")
    
except Exception as e:
    print(f"⚠️ Summary generation had issues: {e}")

print("\n✨ Your Business Central data is now ready for analysis!")
print("   Next steps:")
print("   • Create Power BI reports on the Gold layer")
print("   • Set up incremental refresh schedules")
print("   • Monitor data quality metrics")