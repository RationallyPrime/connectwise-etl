"""ConnectWise configuration - embedded Python dictionaries following CLAUDE.md principles."""

from typing import Any

# Silver layer configuration for ConnectWise entities
SILVER_CONFIG = {
    "entities": {
        "Agreement": {
            "bronze_table": "bronze_cw_agreement",
            "silver_table": "silver_cw_agreement",
            "source": "connectwise",
            "scd_type": 2,
            "incremental_column": "_info.lastUpdated",
            "business_keys": ["id"],
            "column_mappings": {
                "_info.lastUpdated": "lastUpdated",
                "_info.updatedBy": "updatedBy",
                "_info.dateEntered": "dateEntered",
                "_info.enteredBy": "enteredBy",
                "company.id": "companyId",
                "company.identifier": "companyIdentifier",
                "company.name": "companyName",
                "type.id": "typeId",
                "type.name": "typeName",
                "contact.id": "contactId",
                "contact.name": "contactName",
                "opportunity.id": "opportunityId",
                "opportunity.name": "opportunityName",
                "billCycle.id": "billCycleId",
                "billCycle.identifier": "billCycleIdentifier",
                "billCycle.name": "billCycleName",
            },
            "column_types": {
                "id": "integer",
                "companyId": "integer",
                "contactId": "integer",
                "opportunityId": "integer",
                "billCycleId": "integer",
                "typeId": "integer",
                "dateEntered": "timestamp",
                "lastUpdated": "timestamp",
                "startDate": "date",
                "endDate": "date",
                "noEndingDateFlag": "boolean",
                "cancelledFlag": "boolean",
                "billOneTimeFlag": "boolean",
                "restrictDownpaymentFlag": "boolean",
                "invoiceToCompanyFlag": "boolean",
                "applicationUnits": "string",
                "applicationLimit": "decimal",
                "applicationCycle": "string",
                "periodType": "string",
                "billAmount": "decimal",
            },
        },
        "TimeEntry": {
            "bronze_table": "bronze_cw_timeentry",
            "silver_table": "silver_cw_timeentry",
            "source": "connectwise",
            "scd_type": 1,
            "incremental_column": "_info.lastUpdated",
            "business_keys": ["id"],
            "column_mappings": {
                "_info.lastUpdated": "lastUpdated",
                "_info.updatedBy": "updatedBy",
                "_info.dateEntered": "dateEntered",
                "chargeToId": "chargeToId",
                "chargeToType": "chargeToType",
                "company.id": "companyId",
                "company.identifier": "companyIdentifier",
                "member.id": "memberId",
                "member.identifier": "memberIdentifier",
                "member.name": "memberName",
                "member.dailyCapacity": "memberDailyCapacity",
                "workRole.id": "workRoleId",
                "workRole.name": "workRoleName",
                "workType.id": "workTypeId",
                "workType.name": "workTypeName",
                "workType.utilizationFlag": "workTypeUtilizationFlag",
                "agreement.id": "agreementId",
                "ticket.id": "ticketId",
                "ticket.summary": "ticketSummary",
                "project.id": "projectId",
                "project.name": "projectName",
                "phase.id": "phaseId",
                "phase.name": "phaseName",
                "department.id": "departmentId",
                "department.identifier": "departmentIdentifier",
                "department.name": "departmentName",
            },
            "column_types": {
                "id": "integer",
                "chargeToId": "integer",
                "companyId": "integer",
                "memberId": "integer",
                "workRoleId": "integer",
                "workTypeId": "integer",
                "agreementId": "integer",
                "ticketId": "integer",
                "projectId": "integer",
                "phaseId": "integer",
                "departmentId": "integer",
                "businessUnitId": "integer",
                "timeStart": "timestamp",
                "timeEnd": "timestamp",
                "dateEntered": "timestamp",
                "lastUpdated": "timestamp",
                "actualHours": "decimal",
                "billableHours": "decimal",
                "hourlyRate": "decimal",
                "hoursBilled": "decimal",
                "hoursDeduct": "decimal",
                "addToDetailDescriptionFlag": "boolean",
                "addToInternalAnalysisFlag": "boolean",
                "addToResolutionFlag": "boolean",
                "billableOption": "string",
            },
        },
        "ExpenseEntry": {
            "bronze_table": "bronze_cw_expenseentry",
            "silver_table": "silver_cw_expenseentry",
            "source": "connectwise",
            "scd_type": 1,
            "incremental_column": "_info.lastUpdated",
            "business_keys": ["id"],
            "column_mappings": {
                "_info.lastUpdated": "lastUpdated",
                "_info.dateEntered": "dateEntered",
                "company.id": "companyId",
                "company.identifier": "companyIdentifier",
                "member.id": "memberId",
                "member.identifier": "memberIdentifier",
                "member.name": "memberName",
                "type.id": "typeId",
                "type.name": "typeName",
                "agreement.id": "agreementId",
                "project.id": "projectId",
                "phase.id": "phaseId",
                "ticket.id": "ticketId",
                "invoice.id": "invoiceId",
                "classification.id": "classificationId",
                "classification.name": "classificationName",
            },
            "column_types": {
                "id": "integer",
                "companyId": "integer",
                "memberId": "integer",
                "typeId": "integer",
                "agreementId": "integer",
                "projectId": "integer",
                "phaseId": "integer",
                "ticketId": "integer",
                "invoiceId": "integer",
                "classificationId": "integer",
                "date": "date",
                "dateEntered": "timestamp",
                "lastUpdated": "timestamp",
                "amount": "decimal",
                "billAmount": "decimal",
                "agreementAmount": "decimal",
                "distance": "decimal",
                "mileageRate": "decimal",
                "unitQuantity": "decimal",
                "unitCost": "decimal",
                "invoicedFlag": "boolean",
                "exportFlag": "boolean",
                "approvedFlag": "boolean",
                "reimbursableFlag": "boolean",
            },
        },
        "ProductItem": {
            "bronze_table": "bronze_cw_productitem",
            "silver_table": "silver_cw_productitem",
            "source": "connectwise",
            "scd_type": 2,
            "incremental_column": "_info.lastUpdated",
            "business_keys": ["id"],
            "column_mappings": {
                "_info.lastUpdated": "lastUpdated",
                "_info.dateEntered": "dateEntered",
                "catalogItem.id": "catalogItemId",
                "catalogItem.identifier": "catalogItemIdentifier",
                "catalogItem.description": "catalogItemDescription",
                "chargeToId": "chargeToId",
                "chargeToType": "chargeToType",
                "company.id": "companyId",
                "vendor.id": "vendorId",
                "vendor.identifier": "vendorIdentifier",
                "vendor.name": "vendorName",
                "manufacturer.id": "manufacturerId",
                "manufacturer.identifier": "manufacturerIdentifier",
                "manufacturer.name": "manufacturerName",
                "agreement.id": "agreementId",
                "opportunity.id": "opportunityId",
                "invoice.id": "invoiceId",
            },
            "column_types": {
                "id": "integer",
                "catalogItemId": "integer",
                "chargeToId": "integer",
                "companyId": "integer",
                "vendorId": "integer",
                "manufacturerId": "integer",
                "agreementId": "integer",
                "opportunityId": "integer",
                "invoiceId": "integer",
                "quantity": "decimal",
                "price": "decimal",
                "cost": "decimal",
                "discount": "decimal",
                "agreementAmount": "decimal",
                "billableOption": "string",
                "purchaseDate": "date",
                "dateEntered": "timestamp",
                "lastUpdated": "timestamp",
                "cancelledFlag": "boolean",
                "dropshipFlag": "boolean",
                "specialOrderFlag": "boolean",
            },
        },
        "Invoice": {
            "bronze_table": "bronze_cw_invoice",
            "silver_table": "silver_cw_invoice",
            "source": "connectwise",
            "scd_type": 1,
            "incremental_column": "_info.lastUpdated",
            "business_keys": ["id"],
            "column_mappings": {
                "_info.lastUpdated": "lastUpdated",
                "_info.dateEntered": "dateEntered",
                "billing.company.id": "billingCompanyId",
                "billing.company.identifier": "billingCompanyIdentifier",
                "billing.company.name": "billingCompanyName",
                "billing.contact.id": "billingContactId",
                "billing.site.id": "billingSiteId",
                "billing.terms.id": "billingTermsId",
                "billing.terms.name": "billingTermsName",
                "status.id": "statusId",
                "status.name": "statusName",
                "status.isClosed": "statusIsClosed",
                "agreement.id": "agreementId",
                "project.id": "projectId",
                "phase.id": "phaseId",
                "currency.id": "currencyId",
                "currency.symbol": "currencySymbol",
                "currency.isoCode": "currencyIsoCode",
            },
            "column_types": {
                "id": "integer",
                "billingCompanyId": "integer",
                "billingContactId": "integer",
                "billingSiteId": "integer",
                "billingTermsId": "integer",
                "statusId": "integer",
                "agreementId": "integer",
                "projectId": "integer",
                "phaseId": "integer",
                "currencyId": "integer",
                "invoiceNumber": "string",
                "invoiceDate": "date",
                "dueDate": "date",
                "dateEntered": "timestamp",
                "lastUpdated": "timestamp",
                "subtotal": "decimal",
                "total": "decimal",
                "taxTotal": "decimal",
                "adjustmentReason": "string",
                "adjustedBy": "integer",
                "purchaseOrderNumber": "string",
            },
        },
    },
    "global": {
        "preserve_columns": ["id", "customFields"],
        "strip_prefixes": ["_", "api_"],
        "strip_suffixes": ["_ref", "_href"],
        "audit_columns": [
            {"name": "SilverCreatedAt", "type": "timestamp", "default": "current_timestamp()"},
            {"name": "SilverModifiedAt", "type": "timestamp", "default": "current_timestamp()"},
            {"name": "etlEntity", "type": "string"},
            {"name": "etlTimestamp", "type": "timestamp"},
        ],
    },
}

# Gold/fact configuration for agreements
AGREEMENT_CONFIG = {
    "entities": {
        "agreement": {
            "source": "connectwise",
            "endpoint": "/finance/agreements",
            "bronze_table": "bronze_cw_agreements",
            "silver_table": "silver_agreements",
            "gold_tables": ["fact_agreement_period", "fact_agreement_summary", "dim_agreement"],
            "gold_transforms": {
                "fact_agreement_period": {
                    "grain": "monthly",
                    "type": "period_snapshot",
                    "date_spine": {"start": "2020-01-01", "frequency": "month"},
                    "metrics": [
                        "monthly_revenue",
                        "prorated_revenue",
                        "is_active_period",
                        "is_new_agreement",
                        "is_churned_agreement",
                        "months_since_start",
                        "revenue_change",
                        "cumulative_revenue",
                    ],
                    "keys": [
                        {
                            "name": "AgreementPeriodSK",
                            "source_columns": ["id", "period_start"],
                            "type": "hash",
                        },
                        {"name": "AgreementSK", "source_columns": ["id"], "type": "hash"},
                        {"name": "DateSK", "source_columns": ["period_start"], "type": "date_int"},
                    ],
                },
                "fact_agreement_summary": {
                    "grain": "agreement",
                    "type": "aggregate",
                    "metrics": [
                        "lifetime_days",
                        "lifetime_months",
                        "estimated_lifetime_value",
                        "actual_total_revenue",
                        "actual_avg_monthly_revenue",
                        "active_periods",
                    ],
                    "keys": [{"name": "AgreementSK", "source_columns": ["id"], "type": "hash"}],
                },
                "dim_agreement": {
                    "type": "dimension",
                    "scd_type": 2,
                    "natural_key": ["id"],
                    "attributes": [
                        "name",
                        "agreementStatus",
                        "type",
                        "company",
                        "contact",
                        "billCycleId",
                        "applicationUnits",
                        "cancelledFlag",
                    ],
                },
            },
        }
    }
}


def load_silver_config() -> dict[str, Any]:
    """Load the ConnectWise silver configuration.

    Returns:
        Dictionary containing silver configuration for all ConnectWise entities
    """
    return SILVER_CONFIG


def load_agreement_config() -> dict[str, Any]:
    """Load the ConnectWise agreement fact configuration.

    Returns:
        Dictionary containing agreement fact configuration
    """
    return AGREEMENT_CONFIG


def get_entity_config(entity_name: str) -> dict[str, Any]:
    """Get silver configuration for a specific entity.

    Args:
        entity_name: Name of the entity (e.g., 'Agreement', 'TimeEntry')

    Returns:
        Configuration dictionary for the entity

    Raises:
        KeyError: If entity not found in configuration
    """
    config = load_silver_config()

    if entity_name not in config["entities"]:
        raise KeyError(f"Entity '{entity_name}' not found in silver configuration")

    return config["entities"][entity_name]
