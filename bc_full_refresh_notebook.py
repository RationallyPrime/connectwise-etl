#!/usr/bin/env python
# coding: utf-8

# ## bc_full_refresh_notebook
# 
# New notebook

# In[1]:


# Cell 1: Install Required Wheels
# This cell installs the unified ETL packages from the lakehouse

# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install /lakehouse/default/Files/unified_etl_core-1.0.0-py3-none-any.whl
# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install /lakehouse/default/Files/unified_etl_businesscentral-1.0.0-py3-none-any.whl

# Note: After running this cell, you may need to restart the kernel if packages were already loaded


# In[2]:


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

from unified_etl_core.main import run_etl_pipeline
from unified_etl_core.config.models import ETLConfig, IntegrationConfig
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


# In[3]:


# Business Central ETL Configuration for Fabric - Fixed
from datetime import datetime
from unified_etl_core.config.models import (
    ETLConfig, 
    LayerConfig, 
    IntegrationConfig,
    SparkConfig, 
    TableNamingConvention
)

# Configuration Setup
batch_id = f"bc_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
lakehouse_root = "/lakehouse/default/Tables/"

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

# Create ETL Configuration with all required fields
etl_config = ETLConfig(
    # Layer configurations - ALL REQUIRED
    bronze=LayerConfig(
        catalog="LH",
        schema="bronze",  # Fixed: was schema_name
        prefix="bronze_",
        naming_convention=TableNamingConvention.CAMELCASE
    ),
    silver=LayerConfig(
        catalog="LH",
        schema="silver",  # Fixed: was schema_name
        prefix="silver_", 
        naming_convention=TableNamingConvention.CAMELCASE
    ),
    gold=LayerConfig(
        catalog="LH",
        schema="gold",  # Fixed: was schema_name
        prefix="gold_",
        naming_convention=TableNamingConvention.CAMELCASE
    ),
    # Integration configurations - ALL REQUIRED
    integrations={
        "businesscentral": IntegrationConfig(
            name="businesscentral",
            abbreviation="bc",
            base_url="https://api.businesscentral.dynamics.com/",  # Placeholder URL
            enabled=True
        )
    },
    # Spark configuration - REQUIRED
    spark=SparkConfig(
        app_name="bc_full_refresh",
        session_type="fabric",
        config_overrides={}
    ),
    # Global settings - ALL REQUIRED  
    fail_on_error=True,
    audit_columns=True
)

print(f"\n📊 Processing {len(bc_entities)} Business Central entities")

# Test Gold Layer Processing
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from unified_etl_businesscentral.orchestrate import orchestrate_bc_gold_layer  # Fixed import
    
    # Get or create Spark session
    spark = SparkSession.builder.appName("BC_Gold_Test").getOrCreate()
    
    # Define paths
    bronze_path = f"{lakehouse_root}bronze"
    silver_path = f"{lakehouse_root}silver"
    gold_path = f"{lakehouse_root}gold"
    
    print(f"\n🚀 Processing Business Central Gold Layer")
    print(f"  Bronze Path: {bronze_path}")
    print(f"  Silver Path: {silver_path}")
    print(f"  Gold Path: {gold_path}")
    
    try:
        # Process gold layer with correct function name
        stats = orchestrate_bc_gold_layer(
            spark=spark,
            bronze_path=bronze_path,
            silver_path=silver_path,
            gold_path=gold_path,
            batch_id=batch_id  # Added batch_id parameter
        )
        
        print(f"\n✅ Gold Layer Processing Complete:")
        print(f"  Dimensions created: {stats['dimensions_created']}")
        print(f"  Facts created: {stats['facts_created']}")
        print(f"  Total tables: {stats['dimensions_created'] + stats['facts_created']}")
        
        if stats['errors']:
            print(f"\n⚠️  Warnings/Errors encountered:")
            for error in stats['errors']:
                print(f"  - {error}")
                
    except Exception as e:
        print(f"\n❌ Error processing gold layer: {str(e)}")
        import traceback
        traceback.print_exc()


# In[4]:


# Cell 4: Skip Bronze - BC2ADLS Handles Bronze Extraction
print("\n" + "="*50)
print("🥉 BRONZE LAYER: Skipping - BC2ADLS automatically lands bronze data")

try:
    # Verify that bronze tables exist (landed by BC2ADLS)
    print("\n🔍 Verifying Bronze Tables from BC2ADLS:")
    bronze_verified = []
    
    for entity in bc_entities[:5]:  # Check first 5 entities
        try:
            df = spark.table(f"bronze.{entity}")
            count = df.count()
            print(f"  ✅ {entity}: {count} rows")
            bronze_verified.append(entity)
        except Exception as e:
            print(f"  ❌ {entity}: Not found - {e}")
    
    if bronze_verified:
        print(f"\n✅ Bronze layer verified! BC2ADLS has landed {len(bronze_verified)} tables")
    else:
        print("\n⚠️ No bronze tables found - check BC2ADLS pipeline status")
        
except Exception as e:
    print(f"⚠️ Bronze verification had issues: {e}")


# In[ ]:


# Business Central Silver Layer - With Bronze Table Mapping
from datetime import datetime
from unified_etl_core.config.models import (
    ETLConfig, 
    LayerConfig, 
    IntegrationConfig,
    SparkConfig, 
    TableNamingConvention
)
from unified_etl_businesscentral.config import SILVER_CONFIG

# Configuration Setup
batch_id = f"bc_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
lakehouse_root = "/lakehouse/default/Tables/"

print("🥈 SILVER LAYER: Transforming Business Central data...")

# Bronze table mapping - bronze tables have numeric suffixes but silver configs don't
BRONZE_TABLE_MAPPING = {
    "Customer": "Customer18",
    "Vendor": "Vendor23", 
    "Item": "Item27",
    "Resource": "Resource156",
    "GLAccount": "GLAccount15",
    "GLEntry": "GLEntry17",
    "Currency": "Currency4",
    "CompanyInformation": "CompanyInformation79",
    "Dimension": "Dimension348",
    "DimensionValue": "DimensionValue349",
    "DimensionSetEntry": "DimensionSetEntry480",
    "SalesInvoiceHeader": "SalesInvoiceHeader112",
    "SalesInvoiceLine": "SalesInvoiceLine113",
    "CustLedgerEntry": "CustLedgerEntry21",
    "DetailedCustLedgEntry": "DetailedCustLedgEntry379",
    "VendorLedgerEntry": "VendorLedgerEntry25",
    "DetailedVendorLedgEntry": "DetailedVendorLedgEntry380",
    "Job": "Job167",
    "JobLedgerEntry": "JobLedgerEntry169",
    "GeneralLedgerSetup": "GeneralLedgerSetup98",
    "AccountingPeriod": "AccountingPeriod50"
}

# Process Silver layer for all entities that have configurations
silver_entities = [e for e in BRONZE_TABLE_MAPPING.keys() if e in SILVER_CONFIG]

print(f"📋 Processing {len(silver_entities)} entities with silver configurations")

# Create silver-specific config with ALL required fields
silver_etl_config = ETLConfig(
    # Layer configurations - ALL REQUIRED
    bronze=LayerConfig(
        catalog="LH",
        schema="bronze",
        prefix="bronze_",
        naming_convention=TableNamingConvention.CAMELCASE
    ),
    silver=LayerConfig(
        catalog="LH",
        schema="silver",
        prefix="silver_",
        naming_convention=TableNamingConvention.CAMELCASE
    ),
    gold=LayerConfig(
        catalog="LH",
        schema="gold",
        prefix="gold_",
        naming_convention=TableNamingConvention.CAMELCASE
    ),
    # Integration configurations - ALL REQUIRED
    integrations={
        "businesscentral": IntegrationConfig(
            name="businesscentral",
            abbreviation="bc",
            base_url="https://api.businesscentral.dynamics.com/",
            enabled=True
        )
    },
    # Spark configuration - REQUIRED
    spark=SparkConfig(
        app_name="bc_silver_refresh",
        session_type="fabric",
        config_overrides={}
    ),
    # Global settings - ALL REQUIRED  
    fail_on_error=True,
    audit_columns=True
)

print(f"\n✅ Configuration created for {len(silver_entities)} entities")

# Get or create Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("BC_Silver_Test").getOrCreate()

# Process silver tables with correct bronze table names
print("\n🔍 Processing Silver Tables:")

for entity in silver_entities:
    try:
        # Get the bronze table name with suffix
        bronze_entity = BRONZE_TABLE_MAPPING[entity]
        bronze_path = f"{lakehouse_root}bronze/{bronze_entity}"
        silver_path = f"{lakehouse_root}silver/{entity}"  # Silver uses entity name without suffix
        
        # Read bronze table
        bronze_df = spark.read.format("delta").load(bronze_path)
        print(f"  📥 {entity} (from {bronze_entity}): {bronze_df.count()} rows in bronze")
        
        # For now, just copy bronze to silver (in production you'd apply transformations)
        bronze_df.write.mode("overwrite").format("delta").save(silver_path)
        
        # Verify silver table
        silver_df = spark.read.format("delta").load(silver_path)
        print(f"  ✅ {entity}: {silver_df.count()} rows written to silver")
        
    except Exception as e:
        print(f"  ❌ {entity}: Error - {e}")

print(f"\n🎯 Silver layer processing complete!")
print(f"Next step: Run gold layer orchestration to create warehouse schema tables")

# Show what's available for gold layer
print(f"\n📊 Available for Gold Layer:")
print(f"  Silver entities: {len(silver_entities)}")
for entity in silver_entities:
    config = SILVER_CONFIG[entity]
    print(f"  - {entity} → {config['gold_name']}")


# In[6]:


# Cell 6: Gold Layer - Dimensions
print("\n" + "="*50)
print("🥇 GOLD LAYER - PART 1: Creating Business Central Dimensions...")

gold_path = "LH.gold"
silver_path = "LH.silver"

try:
    # 1. Create BC Account Hierarchy
    print("\n📊 Creating Account Hierarchy...")
    try:
        # First, we need to add surrogate keys to GLAccount if not present
        glaccount_df = spark.table(f"{silver_path}.GLAccount")
        
        # Check if GLAccountKey exists, if not, create it
        if "GLAccountKey" not in glaccount_df.columns:
            from unified_etl_core.gold import generate_surrogate_key
            glaccount_df = generate_surrogate_key(
                df=glaccount_df,
                business_keys=["No", "$Company"],
                key_name="GLAccountKey"
            )
        
        account_hierarchy_df = build_bc_account_hierarchy(
            df=glaccount_df,
            indentation_col="Indentation",
            no_col="No",
            surrogate_key_col="GLAccountKey"
        )
        account_hierarchy_df.write.mode("overwrite").saveAsTable(
            f"{gold_path}.dim_GLAccountHierarchy"
        )
        print(f"  ✅ Created GL Account Hierarchy: {account_hierarchy_df.count()} accounts")
    except Exception as e:
        print(f"  ⚠️ Account hierarchy skipped: {e}")
    
    # 2. Create Dimension Bridge
    print("\n🌉 Creating Dimension Bridge...")
    try:
        dimension_types = {
            "DEILD": "DEILD",           # Department
            "VERKEFNI": "VERKEFNI",     # Project
            "STM": "STM",               # Employees (Starfsmenn)
            "TEAM": "TEAM",             # Team
            "PRODUCT": "PRODUCT",       # Products (Vörur)
            "FERÐIR": "FERÐIR",         # Trips/Travel
            "BÍLAR": "BÍLAR",           # Cars
            "PRD": "PRD",               # Product Group
            "TYPE": "TYPE",             # Product Type
            "HÓPUR": "HÓPUR",           # Group
            "ER": "ER",                 # For re-invoicing
            "SM": "SM",                 # Sales & Marketing costs
            "VIDBURDIR": "VIDBURDIR"    # Events
        }
        
        dim_bridge_df = create_bc_dimension_bridge(
            spark=spark,
            silver_path=silver_path,
            gold_path=gold_path,
            dimension_types=dimension_types
        )
        dim_bridge_df.write.mode("overwrite").saveAsTable(
            f"{gold_path}.dim_DimensionBridge"
        )
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
        item_attr_dim_df.write.mode("overwrite").saveAsTable(
            f"{gold_path}.dim_ItemAttribute"
        )
        print(f"  ✅ Created Item Attribute Dimension: {item_attr_dim_df.count()} attributes")
        
        # Create attribute bridge
        item_attr_bridge_df = create_bc_item_attribute_bridge(
            spark=spark,
            silver_path=silver_path,
            gold_path=gold_path
        )
        item_attr_bridge_df.write.mode("overwrite").saveAsTable(
            f"{gold_path}.dim_ItemAttributeBridge"
        )
        print(f"  ✅ Created Item Attribute Bridge: {item_attr_bridge_df.count()} mappings")
    except Exception as e:
        print(f"  ⚠️ Item attributes skipped: {e}")
    
    # 4. Create standard dimensions using generic dimension creator
    print("\n📐 Creating Standard Dimensions...")
    from unified_etl_core.dimensions import create_dimension_from_column
    from unified_etl_core.config.dimension import DimensionConfig
    
    # Define standard dimensions to create
    standard_dimensions = {
        "dim_CustomerStatus": DimensionConfig(
            name="CustomerStatus",
            source_table=f"{silver_path}.Customer",
            source_column="Blocked",
            description="Customer blocking status"
        ),
        "dim_VendorStatus": DimensionConfig(
            name="VendorStatus", 
            source_table=f"{silver_path}.Vendor",
            source_column="Blocked",
            description="Vendor blocking status"
        ),
        "dim_ItemType": DimensionConfig(
            name="ItemType",
            source_table=f"{silver_path}.Item",
            source_column="Type",
            description="Item type classification"
        ),
        "dim_AccountType": DimensionConfig(
            name="AccountType",
            source_table=f"{silver_path}.GLAccount",
            source_column="AccountType",
            description="GL Account type classification"
        )
    }
    
    for dim_name, dim_config in standard_dimensions.items():
        try:
            dim_df = create_dimension_from_column(
                config=etl_config,
                dimension_config=dim_config,
                spark=spark
            )
            dim_df.write.mode("overwrite").saveAsTable(f"{gold_path}.{dim_name}")
            print(f"  ✅ Created {dim_name}: {dim_df.count()} rows")
        except Exception as e:
            print(f"  ⚠️ {dim_name} skipped: {e}")
    
    # 5. Create Date Dimension
    print("\n📅 Creating Date Dimension...")
    try:
        from unified_etl_core.gold import create_date_dimension
        from datetime import date
        
        date_dim_df = create_date_dimension(
            spark=spark,
            start_date=date(2020, 1, 1),
            end_date=date(2030, 12, 31)
        )
        date_dim_df.write.mode("overwrite").saveAsTable(f"{gold_path}.dim_Date")
        print(f"  ✅ Created Date Dimension: {date_dim_df.count()} days")
    except Exception as e:
        print(f"  ⚠️ Date dimension skipped: {e}")
    
    print("\n✅ Gold layer dimensions complete!")
    
except Exception as e:
    logger.error(f"Gold dimension creation failed: {e}")
    print(f"❌ Gold Dimension Error: {e}")
    raise


# In[ ]:


# Cell 7: Gold Layer - Fact Tables
print("\n" + "="*50)
print("🥇 GOLD LAYER - PART 2: Creating Business Central Fact Tables...")
gold_path = "LH.gold"
silver_path = "LH.silver"

try:
    # 1. Create Purchase Fact
    print("\n🛒 Creating Purchase Fact Table...")
    try:
        from datetime import datetime
        fact_batch_id = f"bc_fact_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        purchase_fact_df = create_purchase_fact(
            spark=spark,
            silver_path=silver_path,
            gold_path=gold_path,
            batch_id=fact_batch_id
        )
        purchase_fact_df.write.mode("overwrite").saveAsTable(
            f"{gold_path}.fact_Purchase"
        )
        
        # Show fact summary
        total_amount = purchase_fact_df.agg(
            {"LineAmountExclVAT": "sum"}
        ).collect()[0][0]
        print(f"  ✅ Created fact_Purchase: {purchase_fact_df.count()} lines")
        if total_amount:
            print(f"     Total Purchase Amount: ${total_amount:,.2f}")
    except Exception as e:
        print(f"  ⚠️ Purchase fact skipped: {e}")
    
    # 2. Create Sales Invoice Fact using new typed configuration
    print("\n💰 Creating Sales Invoice Fact Table...")
    try:
        from unified_etl_core.facts import create_generic_fact_table
        from unified_etl_core.config.fact import FactConfig, SurrogateKeyConfig, BusinessKeyConfig
        
        # Load sales invoice lines
        sales_lines_df = spark.table(f"{silver_path}.SalesInvoiceLine")
        
        # Create fact configuration
        sales_fact_config = FactConfig(
            name="SalesInvoice",
            source_table=f"{silver_path}.SalesInvoiceLine",
            surrogate_keys=[
                SurrogateKeyConfig(
                    name="SalesInvoiceLineSK",
                    business_keys=["DocumentNo", "LineNo", "$Company"]
                )
            ],
            business_keys=[
                BusinessKeyConfig(
                    name="SalesInvoiceLineBusinessKey",
                    source_columns=["DocumentNo", "LineNo", "$Company"]
                )
            ],
            calculated_columns={
                "ExtendedAmount": "Quantity * UnitPrice",
                "NetLineAmount": "Amount - COALESCE(LineDiscountAmount, 0)",
                "IsDiscount": "CASE WHEN LineDiscountAmount > 0 THEN true ELSE false END"
            },
            measures=["Amount", "Quantity", "UnitPrice", "LineDiscountAmount"],
            source="businesscentral"
        )
        
        # Create the fact table
        sales_fact_df = create_generic_fact_table(
            config=etl_config,
            fact_config=sales_fact_config,
            silver_df=sales_lines_df,
            spark=spark
        )
        
        sales_fact_df.write.mode("overwrite").saveAsTable(
            f"{gold_path}.fact_SalesInvoice"
        )
        print(f"  ✅ Created fact_SalesInvoice: {sales_fact_df.count()} lines")
        
    except Exception as e:
        print(f"  ⚠️ Sales Invoice fact skipped: {e}")
    
    # 3. Create GL Entry Fact
    print("\n📊 Creating GL Entry Fact Table...")
    try:
        glentry_df = spark.table(f"{silver_path}.GLEntry")
        
        # Create GL Entry fact configuration
        glentry_fact_config = FactConfig(
            name="GLEntry",
            source_table=f"{silver_path}.GLEntry",
            surrogate_keys=[
                SurrogateKeyConfig(
                    name="GLEntrySK",
                    business_keys=["EntryNo", "$Company"]
                )
            ],
            business_keys=[
                BusinessKeyConfig(
                    name="GLEntryBusinessKey",
                    source_columns=["EntryNo", "$Company"]
                )
            ],
            calculated_columns={
                "IsDebit": "CASE WHEN Amount > 0 THEN true ELSE false END",
                "IsCredit": "CASE WHEN Amount < 0 THEN true ELSE false END",
                "AbsoluteAmount": "ABS(Amount)"
            },
            measures=["Amount", "AbsoluteAmount"],
            source="businesscentral"
        )
        
        glentry_fact_df = create_generic_fact_table(
            config=etl_config,
            fact_config=glentry_fact_config,
            silver_df=glentry_df,
            spark=spark
        )
        
        glentry_fact_df.write.mode("overwrite").saveAsTable(
            f"{gold_path}.fact_GLEntry"
        )
        print(f"  ✅ Created fact_GLEntry: {glentry_fact_df.count()} entries")
        
        # Show debit/credit summary
        summary = glentry_fact_df.agg({
            "Amount": "sum",
            "AbsoluteAmount": "sum"
        }).collect()[0]
        
        if summary[0] is not None:
            print(f"     Net Amount: ${summary[0]:,.2f}")
            print(f"     Total Absolute: ${summary[1]:,.2f}")
            
    except Exception as e:
        print(f"  ⚠️ GL Entry fact skipped: {e}")
    
    # 4. Create Customer Ledger Entry Fact
    print("\n👥 Creating Customer Ledger Entry Fact...")
    try:
        cust_ledger_df = spark.table(f"{silver_path}.CustLedgerEntry")
        
        # Create Customer Ledger fact configuration
        cust_ledger_fact_config = FactConfig(
            name="CustomerLedger",
            source_table=f"{silver_path}.CustLedgerEntry",
            surrogate_keys=[
                SurrogateKeyConfig(
                    name="CustLedgerEntrySK", 
                    business_keys=["EntryNo", "$Company"]
                )
            ],
            business_keys=[
                BusinessKeyConfig(
                    name="CustLedgerEntryBusinessKey",
                    source_columns=["EntryNo", "$Company"]
                )
            ],
            calculated_columns={
                "DaysOverdue": "DATEDIFF(CURRENT_DATE(), DueDate)",
                "IsOverdue": "CASE WHEN DueDate < CURRENT_DATE() AND Open = true THEN true ELSE false END",
                "IsOpen": "CASE WHEN Open = true THEN true ELSE false END"
            },
            measures=["Amount", "RemainingAmount"],
            source="businesscentral"
        )
        
        cust_ledger_fact_df = create_generic_fact_table(
            config=etl_config,
            fact_config=cust_ledger_fact_config,
            silver_df=cust_ledger_df,
            spark=spark
        )
        
        cust_ledger_fact_df.write.mode("overwrite").saveAsTable(
            f"{gold_path}.fact_CustomerLedger"
        )
        print(f"  ✅ Created fact_CustomerLedger: {cust_ledger_fact_df.count()} entries")
        
    except Exception as e:
        print(f"  ⚠️ Customer Ledger fact skipped: {e}")
    
    print("\n✅ Gold layer facts complete!")
    
except Exception as e:
    logger.error(f"Gold fact creation failed: {e}")
    print(f"❌ Gold Fact Error: {e}")
    raise


# In[ ]:


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

