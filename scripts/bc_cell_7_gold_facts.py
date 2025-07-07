# Cell 7: Gold Layer - Fact Tables
print("\n" + "="*50)
print("🥇 GOLD LAYER - PART 2: Creating Business Central Fact Tables...")

try:
    # 1. Create Purchase Fact
    print("\n🛒 Creating Purchase Fact Table...")
    try:
        from datetime import datetime
        batch_id = f"bc_fact_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        purchase_fact_df = create_purchase_fact(
            spark=spark,
            silver_path=silver_path,
            gold_path=gold_path,
            batch_id=batch_id
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
    
    # 2. Create Sales Invoice Fact (instead of Agreement which has missing columns)
    print("\n💰 Creating Sales Invoice Fact Table...")
    try:
        from unified_etl_core.facts import create_generic_fact_table
        
        # Load sales invoice lines
        sales_lines_df = spark.table(f"{silver_path}.SalesInvoiceLine")
        
        # Create the fact table
        sales_fact_df = create_generic_fact_table(
            silver_df=sales_lines_df,
            entity_name="SalesInvoice",
            surrogate_keys=[{
                "name": "SalesInvoiceLineSK",
                "business_keys": ["DocumentNo", "LineNo", "$Company"]
            }],
            business_keys=[{
                "name": "SalesInvoiceLineBusinessKey",
                "source_columns": ["DocumentNo", "LineNo", "$Company"]
            }],
            calculated_columns={
                "ExtendedAmount": "Quantity * UnitPrice",
                "NetLineAmount": "Amount - COALESCE(LineDiscountAmount, 0)",
                "IsDiscount": "CASE WHEN LineDiscountAmount > 0 THEN true ELSE false END"
            },
            source="businesscentral"
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
        from unified_etl_core.facts import create_generic_fact_table
        
        glentry_df = spark.table(f"{silver_path}.GLEntry")
        
        glentry_fact_df = create_generic_fact_table(
            silver_df=glentry_df,
            entity_name="GLEntry", 
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
            source="businesscentral"
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
        
        cust_ledger_fact_df = create_generic_fact_table(
            silver_df=cust_ledger_df,
            entity_name="CustomerLedger",
            surrogate_keys=[{
                "name": "CustLedgerEntrySK", 
                "business_keys": ["EntryNo", "$Company"]
            }],
            business_keys=[{
                "name": "CustLedgerEntryBusinessKey",
                "source_columns": ["EntryNo", "$Company"]
            }],
            calculated_columns={
                "DaysOverdue": "DATEDIFF(CURRENT_DATE(), DueDate)",
                "IsOverdue": "CASE WHEN DueDate < CURRENT_DATE() AND Open = true THEN true ELSE false END",
                "IsOpen": "CASE WHEN Open = true THEN true ELSE false END"
            },
            source="businesscentral"
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