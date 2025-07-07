"""
Business Central configuration for silver layer processing.

Following CLAUDE.md fail-fast principles and unified-etl-core patterns.
"""

from typing import Any

# Business Central Silver Layer Configuration
# Each entity MUST have ALL required fields - fail-fast philosophy
SILVER_CONFIG: dict[str, dict[str, Any]] = {
    "PurchInvLine": {
        "model": "PurchInvLine",
        "bronze_source": "bronze_bc_PurchInvLine",
        "gold_name": "PurchInvLine",
        "surrogate_keys": [
            {
                "name": "PurchInvLineKey",
                "business_keys": ["DocumentNo", "LineNo", "$Company"]
            }
        ],
        "business_keys": ["DocumentNo", "LineNo", "$Company"],
        "calculated_columns": {
            "ExtendedAmount": "Quantity * DirectUnitCost",
            "LineAmountIncludingVAT": "LineAmount + (LineAmount * VATPercent / 100)"
        },
        "drop_columns": ["_etl_bronze_timestamp"],
        "rename_columns": {}
    },
    "PurchCrMemoLine": {
        "model": "PurchCrMemoLine",
        "bronze_source": "bronze_bc_PurchCrMemoLine",
        "gold_name": "PurchCrMemoLine",
        "surrogate_keys": [
            {
                "name": "PurchCrMemoLineKey",
                "business_keys": ["DocumentNo", "LineNo", "$Company"]
            }
        ],
        "business_keys": ["DocumentNo", "LineNo", "$Company"],
        "calculated_columns": {
            "ExtendedAmount": "Quantity * DirectUnitCost",
            "LineAmountIncludingVAT": "LineAmount + (LineAmount * VATPercent / 100)"
        },
        "drop_columns": ["_etl_bronze_timestamp"],
        "rename_columns": {}
    },
    "Item": {
        "model": "Item",
        "bronze_source": "bronze_bc_Item",
        "gold_name": "dim_Item",
        "surrogate_keys": [
            {
                "name": "ItemKey",
                "business_keys": ["No", "$Company"]
            }
        ],
        "business_keys": ["No", "$Company"],
        "calculated_columns": {},
        "drop_columns": ["_etl_bronze_timestamp"],
        "rename_columns": {}
    },
    "Vendor": {
        "model": "Vendor",
        "bronze_source": "bronze_bc_Vendor",
        "gold_name": "dim_Vendor",
        "surrogate_keys": [
            {
                "name": "VendorKey",
                "business_keys": ["No", "$Company"]
            }
        ],
        "business_keys": ["No", "$Company"],
        "calculated_columns": {},
        "drop_columns": ["_etl_bronze_timestamp"],
        "rename_columns": {}
    },
    "AMSAgreementHeader": {
        "model": "AMSAgreementHeader",
        "bronze_source": "bronze_bc_AMSAgreementHeader",
        "gold_name": "AMSAgreementHeader",
        "surrogate_keys": [
            {
                "name": "AMSAgreementHeaderKey",
                "business_keys": ["No", "$Company"]
            }
        ],
        "business_keys": ["No", "$Company"],
        "calculated_columns": {},
        "drop_columns": ["_etl_bronze_timestamp"],
        "rename_columns": {}
    },
    "AMSAgreementLine": {
        "model": "AMSAgreementLine",
        "bronze_source": "bronze_bc_AMSAgreementLine",
        "gold_name": "AMSAgreementLine",
        "surrogate_keys": [
            {
                "name": "AMSAgreementLineKey",
                "business_keys": ["DocumentNo", "LineNo", "$Company"]
            }
        ],
        "business_keys": ["DocumentNo", "LineNo", "$Company"],
        "calculated_columns": {},
        "drop_columns": ["_etl_bronze_timestamp"],
        "rename_columns": {}
    },
    "Customer": {
        "model": "Customer",
        "bronze_source": "bronze_bc_Customer",
        "gold_name": "dim_Customer",
        "surrogate_keys": [
            {
                "name": "CustomerKey",
                "business_keys": ["No", "$Company"]
            }
        ],
        "business_keys": ["No", "$Company"],
        "calculated_columns": {},
        "drop_columns": ["_etl_bronze_timestamp"],
        "rename_columns": {}
    },
    "DimensionSetEntry": {
        "model": "DimensionSetEntry",
        "bronze_source": "bronze_bc_DimensionSetEntry",
        "gold_name": "DimensionSetEntry",
        "surrogate_keys": [
            {
                "name": "DimensionSetEntryKey",
                "business_keys": ["DimensionSetID", "DimensionCode", "$Company"]
            }
        ],
        "business_keys": ["DimensionSetID", "DimensionCode", "$Company"],
        "calculated_columns": {},
        "drop_columns": ["_etl_bronze_timestamp"],
        "rename_columns": {}
    },
    "GLAccount": {
        "model": "GLAccount",
        "bronze_source": "bronze_bc_GLAccount",
        "gold_name": "dim_GLAccount",
        "surrogate_keys": [
            {
                "name": "GLAccountKey",
                "business_keys": ["No", "$Company"]
            }
        ],
        "business_keys": ["No", "$Company"],
        "calculated_columns": {},
        "drop_columns": ["_etl_bronze_timestamp"],
        "rename_columns": {}
    },
    "GLEntry": {
        "model": "GLEntry",
        "bronze_source": "bronze_bc_GLEntry",
        "gold_name": "GLEntry",
        "surrogate_keys": [
            {
                "name": "GLEntryKey",
                "business_keys": ["EntryNo", "$Company"]
            }
        ],
        "business_keys": ["EntryNo", "$Company"],
        "calculated_columns": {
            "IsDebit": "CASE WHEN Amount > 0 THEN true ELSE false END",
            "IsCredit": "CASE WHEN Amount < 0 THEN true ELSE false END",
            "AbsoluteAmount": "ABS(Amount)"
        },
        "drop_columns": ["_etl_bronze_timestamp"],
        "rename_columns": {}
    },
    "Dimension": {
        "model": "Dimension",
        "bronze_source": "bronze_bc_Dimension",
        "gold_name": "dim_Dimension",
        "surrogate_keys": [
            {
                "name": "DimensionKey",
                "business_keys": ["Code", "$Company"]
            }
        ],
        "business_keys": ["Code", "$Company"],
        "calculated_columns": {},
        "drop_columns": ["_etl_bronze_timestamp"],
        "rename_columns": {}
    },
    "DimensionValue": {
        "model": "DimensionValue",
        "bronze_source": "bronze_bc_DimensionValue",
        "gold_name": "dim_DimensionValue",
        "surrogate_keys": [
            {
                "name": "DimensionValueKey",
                "business_keys": ["DimensionCode", "Code", "$Company"]
            }
        ],
        "business_keys": ["DimensionCode", "Code", "$Company"],
        "calculated_columns": {},
        "drop_columns": ["_etl_bronze_timestamp"],
        "rename_columns": {}
    }
}

# Business Central Fact Table Configurations
BC_FACT_CONFIGS: dict[str, dict[str, Any]] = {
    "fact_Purchase": {
        "name": "fact_Purchase",
        "source_entities": ["PurchInvLine", "PurchCrMemoLine"],
        "dimensions": {
            "Item": {
                "join_keys": {"No": "No"},  # Map fact.No to dim.No
                "required": True
            },
            "Vendor": {
                "join_keys": {"BuyfromVendorNo": "No"},  # Map fact.BuyfromVendorNo to dim.No
                "required": True
            },
            "Date": {
                "join_keys": ["PostingDate"],
                "required": True
            },
            "DimensionBridge": {
                "join_keys": {"DimensionSetID": "DimensionSetID"},
                "required": False
            }
        },
        "measures": [
            "Quantity",
            "DirectUnitCost",
            "LineAmount",
            "ExtendedAmount",
            "LineAmountIncludingVAT",
            "VATPercent"
        ],
        "calculated_columns": {
            "PurchaseType": "CASE WHEN _entity_type = 'PurchInvLine' THEN 'Invoice' ELSE 'Credit Memo' END",
            "NetAmount": "CASE WHEN _entity_type = 'PurchCrMemoLine' THEN -LineAmount ELSE LineAmount END"
        }
    },
    "fact_Agreement": {
        "name": "fact_Agreement",
        "source_entities": ["AMSAgreementLine"],
        "header_entity": "AMSAgreementHeader",
        "join_config": {
            "header_to_line": {"No": "DocumentNo"}  # Map Header.No to Line.DocumentNo
        },
        "dimensions": {
            "Customer": {
                "join_keys": {"CustomerNo": "No"},
                "required": True
            },
            "Date": {
                "join_keys": ["StartingDate"],
                "required": True
            }
        },
        "measures": [
            "Quantity",
            "UnitPrice",
            "LineAmount",
            "DiscountPercent"
        ],
        "calculated_columns": {
            "ContractValue": "Quantity * UnitPrice",
            "NetAmount": "CASE WHEN Type = 'Credit Memo' THEN -Amount ELSE Amount END"
        }
    }
}
