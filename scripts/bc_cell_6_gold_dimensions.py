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
                key_name="GLAccountKey",
                business_keys=["No", "$Company"]
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
    
    # Define standard dimensions to create
    standard_dimensions = {
        "dim_CustomerStatus": {
            "source_table": f"{silver_path}.Customer",
            "column": "Blocked",
            "dimension_name": "CustomerStatus"
        },
        "dim_VendorStatus": {
            "source_table": f"{silver_path}.Vendor", 
            "column": "Blocked",
            "dimension_name": "VendorStatus"
        },
        "dim_ItemType": {
            "source_table": f"{silver_path}.Item",
            "column": "Type",
            "dimension_name": "ItemType"
        },
        "dim_AccountType": {
            "source_table": f"{silver_path}.GLAccount",
            "column": "AccountType",
            "dimension_name": "AccountType"
        }
    }
    
    for dim_name, dim_config in standard_dimensions.items():
        try:
            dim_df = create_dimension_from_column(
                spark=spark,
                source_table=dim_config["source_table"],
                column_name=dim_config["column"],
                dimension_name=dim_config["dimension_name"]
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