"""ConnectWise typed configuration using the new unified configuration system."""

from unified_etl_core.config import (
    ETLConfig,
    LayerConfig,
    IntegrationConfig,
    SparkConfig,
    EntityConfig,
    ColumnMapping,
    SCDConfig,
    FactConfig,
    DimensionMapping,
    CalculatedColumn,
    DataType,
    TableNamingConvention,
)


def create_connectwise_config() -> ETLConfig:
    """Create typed ConnectWise ETL configuration. ALL FIELDS REQUIRED."""
    
    # Layer configurations
    bronze_layer = LayerConfig(
        catalog="Lakehouse",
        schema="bronze",
        prefix="bronze_",
        naming_convention=TableNamingConvention.UNDERSCORE
    )
    
    silver_layer = LayerConfig(
        catalog="Lakehouse",
        schema="silver", 
        prefix="silver_",
        naming_convention=TableNamingConvention.UNDERSCORE
    )
    
    gold_layer = LayerConfig(
        catalog="Lakehouse",
        schema="gold",
        prefix="gold_",
        naming_convention=TableNamingConvention.UNDERSCORE
    )
    
    # ConnectWise integration config
    connectwise_integration = IntegrationConfig(
        name="connectwise",
        abbreviation="cw",
        base_url="https://api-na.myconnectwise.net",
        enabled=True
    )
    
    # Spark configuration
    spark_config = SparkConfig(
        app_name="ConnectWise-ETL",
        session_type="fabric",
        config_overrides={}
    )
    
    return ETLConfig(
        bronze=bronze_layer,
        silver=silver_layer,
        gold=gold_layer,
        integrations={"connectwise": connectwise_integration},
        spark=spark_config,
        fail_on_error=True,
        audit_columns=True
    )


def get_connectwise_entity_configs() -> dict[str, EntityConfig]:
    """Get all ConnectWise entity configurations."""
    return {
        "agreement": create_agreement_entity_config(),
        "timeentry": create_timeentry_entity_config(),
        "expenseentry": create_expenseentry_entity_config(),
        "productitem": create_productitem_entity_config(),
        "invoice": create_invoice_entity_config(),
        "unpostedinvoice": create_unposted_invoice_entity_config(),
    }


def get_connectwise_fact_configs() -> dict[str, FactConfig]:
    """Get all ConnectWise fact configurations."""
    return {
        "agreement": create_agreement_fact_config(),
        "timeentry": create_timeentry_fact_config(),
        "expenseentry": create_expenseentry_fact_config(),
        "invoice_line": create_invoice_line_fact_config(),
    }


def create_agreement_entity_config() -> EntityConfig:
    """Create Agreement entity configuration."""
    
    # Column mappings for nested API fields
    column_mappings = {
        "info_lastUpdated": ColumnMapping(
            source_column="_info.lastUpdated",
            target_column="lastUpdated", 
            target_type=DataType.TIMESTAMP,
            transformation="to_timestamp(_info.lastUpdated)"
        ),
        "info_updatedBy": ColumnMapping(
            source_column="_info.updatedBy",
            target_column="updatedBy",
            target_type=DataType.STRING,
            transformation="_info.updatedBy"
        ),
        "info_dateEntered": ColumnMapping(
            source_column="_info.dateEntered",
            target_column="dateEntered",
            target_type=DataType.TIMESTAMP,
            transformation="to_timestamp(_info.dateEntered)"
        ),
        "company_id": ColumnMapping(
            source_column="company.id",
            target_column="companyId",
            target_type=DataType.INTEGER,
            transformation="company.id"
        ),
        "company_name": ColumnMapping(
            source_column="company.name",
            target_column="companyName",
            target_type=DataType.STRING,
            transformation="company.name"
        ),
        "type_id": ColumnMapping(
            source_column="type.id",
            target_column="typeId",
            target_type=DataType.INTEGER,
            transformation="type.id"
        ),
        "type_name": ColumnMapping(
            source_column="type.name",
            target_column="typeName",
            target_type=DataType.STRING,
            transformation="type.name"
        ),
    }
    
    # SCD Type 2 configuration
    scd_config = SCDConfig(
        type=2,
        business_keys=["id"],
        timestamp_column="lastUpdated"
    )
    
    return EntityConfig(
        name="agreement",
        source="connectwise",
        model_class_name="Agreement",
        flatten_nested=True,
        flatten_max_depth=3,
        preserve_columns=["id", "name", "startDate", "endDate"],
        column_mappings=column_mappings,
        json_columns=[],
        business_keys=["id"],
        scd=scd_config,
        add_audit_columns=True,
        strip_null_columns=True
    )


def create_timeentry_entity_config() -> EntityConfig:
    """Create TimeEntry entity configuration."""
    
    column_mappings = {
        "info_lastUpdated": ColumnMapping(
            source_column="_info.lastUpdated",
            target_column="lastUpdated",
            target_type=DataType.TIMESTAMP,
            transformation="to_timestamp(_info.lastUpdated)"
        ),
        "company_id": ColumnMapping(
            source_column="company.id",
            target_column="companyId",
            target_type=DataType.INTEGER,
            transformation="company.id"
        ),
        "member_id": ColumnMapping(
            source_column="member.id",
            target_column="memberId",
            target_type=DataType.INTEGER,
            transformation="member.id"
        ),
        "agreement_id": ColumnMapping(
            source_column="agreement.id",
            target_column="agreementId",
            target_type=DataType.INTEGER,
            transformation="agreement.id"
        ),
    }
    
    scd_config = SCDConfig(
        type=1,
        business_keys=["id"],
        timestamp_column="lastUpdated"
    )
    
    return EntityConfig(
        name="timeentry",
        source="connectwise",
        model_class_name="TimeEntry",
        flatten_nested=True,
        flatten_max_depth=3,
        preserve_columns=["id", "timeStart", "timeEnd", "hoursActual"],
        column_mappings=column_mappings,
        json_columns=[],
        business_keys=["id"],
        scd=scd_config,
        add_audit_columns=True,
        strip_null_columns=True
    )


def create_expenseentry_entity_config() -> EntityConfig:
    """Create ExpenseEntry entity configuration."""
    
    column_mappings = {
        "info_lastUpdated": ColumnMapping(
            source_column="_info.lastUpdated",
            target_column="lastUpdated",
            target_type=DataType.TIMESTAMP,
            transformation="to_timestamp(_info.lastUpdated)"
        ),
        "company_id": ColumnMapping(
            source_column="company.id",
            target_column="companyId",
            target_type=DataType.INTEGER,
            transformation="company.id"
        ),
        "member_id": ColumnMapping(
            source_column="member.id",
            target_column="memberId",
            target_type=DataType.INTEGER,
            transformation="member.id"
        ),
    }
    
    scd_config = SCDConfig(
        type=1,
        business_keys=["id"],
        timestamp_column="lastUpdated"
    )
    
    return EntityConfig(
        name="expenseentry",
        source="connectwise",
        model_class_name="ExpenseEntry",
        flatten_nested=True,
        flatten_max_depth=3,
        preserve_columns=["id", "date", "amount"],
        column_mappings=column_mappings,
        json_columns=[],
        business_keys=["id"],
        scd=scd_config,
        add_audit_columns=True,
        strip_null_columns=True
    )


def create_productitem_entity_config() -> EntityConfig:
    """Create ProductItem entity configuration."""
    
    column_mappings = {
        "info_lastUpdated": ColumnMapping(
            source_column="_info.lastUpdated",
            target_column="lastUpdated",
            target_type=DataType.TIMESTAMP,
            transformation="to_timestamp(_info.lastUpdated)"
        ),
    }
    
    return EntityConfig(
        name="productitem",
        source="connectwise",
        model_class_name="ProductItem",
        flatten_nested=True,
        flatten_max_depth=2,
        preserve_columns=["id", "identifier", "description"],
        column_mappings=column_mappings,
        json_columns=[],
        business_keys=["id"],
        scd=None,
        add_audit_columns=True,
        strip_null_columns=True
    )


def create_invoice_entity_config() -> EntityConfig:
    """Create Posted Invoice entity configuration."""
    
    column_mappings = {
        "info_lastUpdated": ColumnMapping(
            source_column="_info.lastUpdated",
            target_column="lastUpdated",
            target_type=DataType.TIMESTAMP,
            transformation="to_timestamp(_info.lastUpdated)"
        ),
        "company_id": ColumnMapping(
            source_column="company.id",
            target_column="companyId",
            target_type=DataType.INTEGER,
            transformation="company.id"
        ),
    }
    
    return EntityConfig(
        name="invoice",
        source="connectwise",
        model_class_name="PostedInvoice",
        flatten_nested=True,
        flatten_max_depth=3,
        preserve_columns=["id", "invoiceNumber", "date", "total"],
        column_mappings=column_mappings,
        json_columns=[],
        business_keys=["id"],
        scd=None,
        add_audit_columns=True,
        strip_null_columns=True
    )


def create_unposted_invoice_entity_config() -> EntityConfig:
    """Create Unposted Invoice entity configuration."""
    
    column_mappings = {
        "info_lastUpdated": ColumnMapping(
            source_column="_info.lastUpdated",
            target_column="lastUpdated",
            target_type=DataType.TIMESTAMP,
            transformation="to_timestamp(_info.lastUpdated)"
        ),
        "company_id": ColumnMapping(
            source_column="company.id",
            target_column="companyId",
            target_type=DataType.INTEGER,
            transformation="company.id"
        ),
    }
    
    return EntityConfig(
        name="unpostedinvoice",
        source="connectwise",
        model_class_name="Invoice",
        flatten_nested=True,
        flatten_max_depth=3,
        preserve_columns=["id", "invoiceNumber", "date", "total"],
        column_mappings=column_mappings,
        json_columns=[],
        business_keys=["id"],
        scd=None,
        add_audit_columns=True,
        strip_null_columns=True
    )


def create_agreement_fact_config() -> FactConfig:
    """Create Agreement fact table configuration."""
    
    dimension_mappings = [
        DimensionMapping(
            fact_column="companyId",
            dimension_table="Lakehouse.gold.dim_company",
            dimension_key_column="companyId",
            surrogate_key_column="CompanyKey"
        ),
        DimensionMapping(
            fact_column="typeId", 
            dimension_table="Lakehouse.gold.dim_agreement_type",
            dimension_key_column="typeId",
            surrogate_key_column="AgreementTypeKey"
        ),
    ]
    
    calculated_columns = [
        CalculatedColumn(
            name="AgreementDurationDays",
            expression="datediff(endDate, startDate)",
            data_type="integer"
        ),
        CalculatedColumn(
            name="IsActive",
            expression="case when endDate is null or endDate > current_date() then true else false end",
            data_type="boolean"
        ),
        CalculatedColumn(
            name="AgreementTypeIcelandic",
            expression="""
            case 
                when upper(typeName) like '%TÍMAPOTTUR%' then 'Tímapottur'
                when upper(typeName) like '%ÞJÓNUSTA%' then 'Þjónusta' 
                when upper(typeName) like '%INNRI%' then 'Innri verkefni'
                when upper(typeName) like '%REKSTRAR%' then 'Rekstrarþjónusta'
                when upper(typeName) like '%HUGBÚNAÐUR%' then 'Hugbúnaðarþjónusta'
                else 'Annað'
            end
            """,
            data_type="string"
        ),
    ]
    
    return FactConfig(
        name="agreement",
        source="connectwise",
        source_entities=["agreement"],
        business_keys=["id"],
        surrogate_keys=["AgreementKey"],
        dimension_mappings=dimension_mappings,
        measure_columns=["applicationLimit", "applicationUnits"],
        dimension_columns=["companyId", "typeId", "contactId"],
        calculated_columns=calculated_columns,
        date_column="startDate",
        add_entity_type=False,
        entity_type_column="EntityType",
        add_audit_columns=True
    )


def create_timeentry_fact_config() -> FactConfig:
    """Create TimeEntry fact table configuration."""
    
    dimension_mappings = [
        DimensionMapping(
            fact_column="companyId",
            dimension_table="Lakehouse.gold.dim_company",
            dimension_key_column="companyId",
            surrogate_key_column="CompanyKey"
        ),
        DimensionMapping(
            fact_column="memberId",
            dimension_table="Lakehouse.gold.dim_member",
            dimension_key_column="memberId",
            surrogate_key_column="MemberKey"
        ),
        DimensionMapping(
            fact_column="agreementId",
            dimension_table="Lakehouse.gold.dim_agreement",
            dimension_key_column="agreementId",
            surrogate_key_column="AgreementKey"
        ),
    ]
    
    calculated_columns = [
        CalculatedColumn(
            name="HoursBillable",
            expression="case when billableOption = 'Billable' then hoursActual else 0 end",
            data_type="decimal"
        ),
        CalculatedColumn(
            name="HoursNonBillable", 
            expression="case when billableOption != 'Billable' then hoursActual else 0 end",
            data_type="decimal"
        ),
        CalculatedColumn(
            name="IsTimapottur",
            expression="case when upper(notes) rlike 'TÍMAPOTTUR\\\\s*:?' then true else false end",
            data_type="boolean"
        ),
    ]
    
    return FactConfig(
        name="timeentry",
        source="connectwise",
        source_entities=["timeentry"],
        business_keys=["id"],
        surrogate_keys=["TimeEntryKey"],
        dimension_mappings=dimension_mappings,
        measure_columns=["hoursActual", "hoursBilled", "rateActual"],
        dimension_columns=["companyId", "memberId", "agreementId"],
        calculated_columns=calculated_columns,
        date_column="timeStart",
        add_entity_type=False,
        entity_type_column="EntityType",
        add_audit_columns=True
    )


def create_expenseentry_fact_config() -> FactConfig:
    """Create ExpenseEntry fact table configuration."""
    
    dimension_mappings = [
        DimensionMapping(
            fact_column="companyId",
            dimension_table="Lakehouse.gold.dim_company",
            dimension_key_column="companyId",
            surrogate_key_column="CompanyKey"
        ),
        DimensionMapping(
            fact_column="memberId",
            dimension_table="Lakehouse.gold.dim_member",
            dimension_key_column="memberId",
            surrogate_key_column="MemberKey"
        ),
    ]
    
    calculated_columns = [
        CalculatedColumn(
            name="ExpenseBillable",
            expression="case when billableOption = 'Billable' then amount else 0 end",
            data_type="decimal"
        ),
        CalculatedColumn(
            name="ExpenseNonBillable",
            expression="case when billableOption != 'Billable' then amount else 0 end", 
            data_type="decimal"
        ),
    ]
    
    return FactConfig(
        name="expenseentry",
        source="connectwise",
        source_entities=["expenseentry"],
        business_keys=["id"],
        surrogate_keys=["ExpenseEntryKey"],
        dimension_mappings=dimension_mappings,
        measure_columns=["amount"],
        dimension_columns=["companyId", "memberId"],
        calculated_columns=calculated_columns,
        date_column="date",
        add_entity_type=False,
        entity_type_column="EntityType",
        add_audit_columns=True
    )


def create_invoice_line_fact_config() -> FactConfig:
    """Create Invoice Line fact table configuration."""
    
    dimension_mappings = [
        DimensionMapping(
            fact_column="companyId",
            dimension_table="Lakehouse.gold.dim_company", 
            dimension_key_column="companyId",
            surrogate_key_column="CompanyKey"
        ),
        DimensionMapping(
            fact_column="agreementId",
            dimension_table="Lakehouse.gold.dim_agreement",
            dimension_key_column="agreementId",
            surrogate_key_column="AgreementKey"
        ),
    ]
    
    calculated_columns = [
        CalculatedColumn(
            name="LineType",
            expression="""
            case 
                when upper(description) like '%TIME%' then 'Time'
                when upper(description) like '%EXPENSE%' then 'Expense'
                when upper(description) like '%PRODUCT%' then 'Product'
                else 'Other'
            end
            """,
            data_type="string"
        ),
        CalculatedColumn(
            name="RevenueRecognized",
            expression="quantity * price",
            data_type="decimal"
        ),
    ]
    
    return FactConfig(
        name="invoice_line",
        source="connectwise",
        source_entities=["invoice", "unpostedinvoice"],
        business_keys=["invoiceId", "lineNumber"],
        surrogate_keys=["InvoiceLineKey"],
        dimension_mappings=dimension_mappings,
        measure_columns=["quantity", "price", "cost"],
        dimension_columns=["companyId", "agreementId"],
        calculated_columns=calculated_columns,
        date_column="invoiceDate",
        add_entity_type=True,
        entity_type_column="InvoiceType",
        add_audit_columns=True
    )


# Legacy compatibility - keep the old names but point to new typed configs
SILVER_CONFIG = {
    "entities": {
        entity_name: {
            "source": "connectwise",
            "business_keys": config.business_keys,
            "scd_type": config.scd.type if config.scd else 1,
        }
        for entity_name, config in get_connectwise_entity_configs().items()
    }
}

AGREEMENT_CONFIG = {
    "fact_agreement": create_agreement_fact_config(),
    "fact_timeentry": create_timeentry_fact_config(),
    "fact_expenseentry": create_expenseentry_fact_config(),
}