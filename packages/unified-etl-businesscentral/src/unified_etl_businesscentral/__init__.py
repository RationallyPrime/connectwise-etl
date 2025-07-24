"""
Business Central ETL package - specialized business logic for BC systems.

Contains BC-specific transformations that were moved from unified-etl-core:
- Gold layer dimensional modeling (dimension bridges, item attributes)
- Silver layer global dimension resolution
- Account hierarchy building for BC chart of accounts
- Fact table creation (purchase, agreement)
"""

from .config import BC_FACT_CONFIGS, SILVER_CONFIG
from .transforms.facts import create_agreement_fact, create_purchase_fact
from .transforms.gold_utils import (
    build_bc_account_hierarchy,
    create_bc_dimension_bridge,
    create_bc_item_attribute_bridge,
    create_bc_item_attribute_dimension,
    join_bc_dimension,
)

# Integration interface for core framework
from .processor import BusinessCentralBronzeProcessor

# BC extractor processes BC2ADLS data with Pydantic validation
extractor = BusinessCentralBronzeProcessor

# Import all regenerated models with proper CDM field aliases from consolidated models
from .models import (
    AccountingPeriod,
    CompanyInformation,
    Currency,
    CustLedgerEntry,
    Customer,
    DetailedCustLedgEntry,
    DetailedVendorLedgEntry,
    Dimension,
    DimensionSetEntry,
    DimensionValue,
    GLAccount,
    GLEntry,
    GeneralLedgerSetup,
    Item,
    Job,
    JobLedgerEntry,
    Resource,
    SalesInvoiceHeader,
    SalesInvoiceLine,
    Vendor,
    VendorLedgerEntry,
)

models = {
    # Core entities
    "customer": Customer,
    "vendor": Vendor,
    "item": Item,
    "resource": Resource,
    "companyinformation": CompanyInformation,
    
    # Financial entities
    "glaccount": GLAccount,
    "glentry": GLEntry,
    "currency": Currency,
    "generalledgersetup": GeneralLedgerSetup,
    
    # Dimension entities
    "dimension": Dimension,
    "dimensionvalue": DimensionValue,
    "dimensionsetentry": DimensionSetEntry,
    
    # Ledger entries
    "custledgerentry": CustLedgerEntry,
    "vendorledgerentry": VendorLedgerEntry,
    "detailedcustledgentry": DetailedCustLedgEntry,
    "detailedvendorledgentry": DetailedVendorLedgEntry,
    
    # Sales entities
    "salesinvoiceheader": SalesInvoiceHeader,
    "salesinvoiceline": SalesInvoiceLine,
    
    # Job/Project entities
    "job": Job,
    "jobledgerentry": JobLedgerEntry,
    
    # Setup entities
    "accountingperiod": AccountingPeriod,
}

# Export entity configurations for framework integration
def get_entity_configs():
    """Get Business Central entity configurations for framework integration."""
    from unified_etl_core.config.entity import EntityConfig
    
    entity_configs = {}
    
    for entity_name, config in SILVER_CONFIG.items():
        # Convert to lowercase for framework compatibility
        key = entity_name.lower()
        
        # Get the actual model class for SparkDantic integration
        model_class = models.get(config["model"].lower())
        
        # SKIP entities without model classes (not yet generated)
        if model_class is None:
            continue
        
        # Convert BC rename_columns to proper ColumnMapping objects
        column_mappings = _convert_rename_columns_to_mappings(
            config["model"], config.get("rename_columns", {})
        )
        
        entity_configs[key] = EntityConfig(
            name=key,
            source="businesscentral",
            model_class_name=config["model"],
            model_class=model_class,  # Add actual model class for SparkDantic
            flatten_nested=True,
            flatten_max_depth=3,
            preserve_columns=[],
            json_columns=[],
            column_mappings=column_mappings,
            scd=None,
            business_keys=config.get("business_keys", ["No", "$Company"]),
            add_audit_columns=True,
            strip_null_columns=True
        )
    
    return entity_configs


def _convert_rename_columns_to_mappings(model_class_name: str, rename_columns: dict[str, str]) -> dict:
    """Convert BC SILVER_CONFIG rename_columns to ColumnMapping objects."""
    from unified_etl_core.config.entity import ColumnMapping, DataType
    from pyspark.sql.types import (
        StringType, IntegerType, DoubleType, FloatType, BooleanType, 
        TimestampType, DateType, DecimalType
    )
    
    if not rename_columns:
        return {}
    
    # Get the model class from our models dict
    model_class = models.get(model_class_name.lower())
    if not model_class:
        # Fallback - assume all string types if model not found
        return {
            f"{source_col}_mapping": ColumnMapping(
                source_column=source_col,
                target_column=target_col,
                target_type=DataType.STRING,
                transformation=source_col
            )
            for source_col, target_col in rename_columns.items()
        }
    
    # Generate SparkDantic schema to infer data types
    try:
        spark_schema = model_class.model_spark_schema()
        field_types = {field.name: field.dataType for field in spark_schema.fields}
    except Exception:
        # Fallback if schema generation fails
        field_types = {}
    
    column_mappings = {}
    for source_col, target_col in rename_columns.items():
        # Infer target data type from Spark schema
        spark_type = field_types.get(source_col)
        
        if isinstance(spark_type, StringType):
            target_type = DataType.STRING
        elif isinstance(spark_type, IntegerType):
            target_type = DataType.INTEGER
        elif isinstance(spark_type, (DoubleType, FloatType)):
            target_type = DataType.DOUBLE
        elif isinstance(spark_type, DecimalType):
            target_type = DataType.DECIMAL
        elif isinstance(spark_type, BooleanType):
            target_type = DataType.BOOLEAN
        elif isinstance(spark_type, TimestampType):
            target_type = DataType.TIMESTAMP
        elif isinstance(spark_type, DateType):
            target_type = DataType.DATE
        else:
            # Default to string for unknown types
            target_type = DataType.STRING
        
        # Create mapping with simple pass-through transformation
        mapping_key = f"{source_col}_mapping"
        column_mappings[mapping_key] = ColumnMapping(
            source_column=source_col,
            target_column=target_col,
            target_type=target_type,
            transformation=source_col  # Simple pass-through
        )
    
    return column_mappings

# Provide entity configs to framework
entity_configs = get_entity_configs()

__all__ = [
    "BC_FACT_CONFIGS",
    "SILVER_CONFIG",
    "BusinessCentralBronzeProcessor",
    "build_bc_account_hierarchy",
    "create_agreement_fact",
    "create_bc_dimension_bridge",
    "create_bc_item_attribute_bridge",
    "create_bc_item_attribute_dimension",
    "create_purchase_fact",
    "extractor",
    "join_bc_dimension",
    "models",
    "entity_configs",
]
