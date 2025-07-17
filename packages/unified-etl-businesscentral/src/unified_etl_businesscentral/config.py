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
        "bronze_source": "purchinvline123",
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
        "bronze_source": "purchcrmemoline125",
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
        "bronze_source": "item27",
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
        "bronze_source": "vendor23",
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
        "bronze_source": "amsagreementheader10007050",
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
        "bronze_source": "amsagreementline10007051",
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
        "bronze_source": "customer18",
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
        "bronze_source": "dimensionsetentry480",
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
        "bronze_source": "glaccount15",
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
        "bronze_source": "glentry17",
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
        "bronze_source": "dimension348",
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
        "bronze_source": "dimensionvalue349",
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
    },
    # Additional dimensions for warehouse schema
    "Currency": {
        "model": "Currency",
        "bronze_source": "currency4",
        "gold_name": "dim_Currency",
        "surrogate_keys": [
            {
                "name": "CurrencyKey",
                "business_keys": ["Code", "$Company"]
            }
        ],
        "business_keys": ["Code", "$Company"],
        "calculated_columns": {
            "Currency": "CONCAT(Code, ' - ', Description)"
        },
        "drop_columns": ["_etl_bronze_timestamp"],
        "rename_columns": {
            "Code": "Currency Code",
            "Description": "Currency Name"
        }
    },
    "Resource": {
        "model": "Resource",
        "bronze_source": "resource156",
        "gold_name": "dim_Resource",
        "surrogate_keys": [
            {
                "name": "ResourceKey",
                "business_keys": ["No", "$Company"]
            }
        ],
        "business_keys": ["No", "$Company"],
        "calculated_columns": {
            "Resource": "CONCAT(No, ' - ', Name)",
            "BlockedDesc": "CASE WHEN Blocked = 1 THEN 'Blocked' ELSE 'Active' END"
        },
        "drop_columns": ["_etl_bronze_timestamp"],
        "rename_columns": {
            "No": "Resource No",
            "Name": "Resource Name"
        }
    },
    "CompanyInformation": {
        "model": "CompanyInformation",
        "bronze_source": "companyinformation79",
        "gold_name": "dim_Company",
        "surrogate_keys": [
            {
                "name": "CompanyKey",
                "business_keys": ["$Company"]
            }
        ],
        "business_keys": ["$Company"],
        "calculated_columns": {},
        "drop_columns": ["_etl_bronze_timestamp"],
        "rename_columns": {
            "Name": "Company Name"
        }
    },
    "SalesInvoiceHeader": {
        "model": "SalesInvoiceHeader",
        "bronze_source": "salesinvoiceheader112",
        "gold_name": "dim_SalesHeader",
        "surrogate_keys": [
            {
                "name": "SalesHeaderKey",
                "business_keys": ["No", "$Company"]
            }
        ],
        "business_keys": ["No", "$Company"],
        "calculated_columns": {
            "DocumentTypeDesc": "CASE DocumentType WHEN 1 THEN 'Quote' WHEN 2 THEN 'Order' WHEN 3 THEN 'Invoice' WHEN 4 THEN 'Credit Memo' WHEN 5 THEN 'Blanket Order' WHEN 6 THEN 'Return Order' ELSE 'Unknown' END",
            "StatusDesc": "CASE Status WHEN 0 THEN 'Open' WHEN 1 THEN 'Released' WHEN 2 THEN 'Pending Approval' WHEN 3 THEN 'Pending Prepayment' ELSE 'Unknown' END"
        },
        "drop_columns": ["_etl_bronze_timestamp"],
        "rename_columns": {
            "No": "Document No"
        }
    },
    "SalesInvoiceLine": {
        "model": "SalesInvoiceLine",
        "bronze_source": "salesinvoiceline113",
        "gold_name": "SalesInvoiceLine",
        "surrogate_keys": [
            {
                "name": "SalesInvoiceLineKey",
                "business_keys": ["DocumentNo", "LineNo", "$Company"]
            }
        ],
        "business_keys": ["DocumentNo", "LineNo", "$Company"],
        "calculated_columns": {
            "ExtendedAmount": "Quantity * UnitPrice",
            "NetAmount": "Amount - LineDiscountAmount"
        },
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
    },
    # Additional facts for warehouse schema
    "fact_SalesInvoiceLines": {
        "name": "fact_SalesInvoiceLines",
        "source_entities": ["SalesInvoiceLine"],
        "dimensions": {
            "Item": {"join_keys": {"No": "No"}, "required": False},
            "GLAccount": {"join_keys": {"No": "No"}, "required": False},
            "Resource": {"join_keys": {"No": "No"}, "required": False},
            "Customer": {"join_keys": {"SelltoCustomerNo": "No"}, "required": True},
            "Date": {"join_keys": ["PostingDate"], "required": True},
            "DimensionBridge": {"join_keys": {"DimensionSetID": "DimensionSetID"}, "required": False}
        },
        "measures": [
            "Quantity", "UnitPrice", "LineAmount", "Amount", "LineDiscountAmount"
        ],
        "calculated_columns": {
            "NetSales": "Amount - LineDiscountAmount",
            "CostAmount": "Quantity * UnitCost"
        }
    },
    "fact_GLEntry": {
        "name": "fact_GLEntry",
        "source_entities": ["GLEntry"],
        "dimensions": {
            "GLAccount": {"join_keys": {"GLAccountNo": "No"}, "required": True},
            "Customer": {"join_keys": {"SourceNo": "No"}, "required": False},
            "Vendor": {"join_keys": {"SourceNo": "No"}, "required": False},
            "Date": {"join_keys": ["PostingDate"], "required": True},
            "DimensionBridge": {"join_keys": {"DimensionSetID": "DimensionSetID"}, "required": False}
        },
        "measures": ["Amount", "DebitAmount", "CreditAmount", "VATAmount"],
        "calculated_columns": {}
    }
}
