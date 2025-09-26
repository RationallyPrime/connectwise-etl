"""Agreement-related utilities and business logic for ConnectWise data.

Handles agreement type mapping, hierarchy resolution, and billing rules.
"""

import logging
import re
from enum import Enum

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class AgreementType(Enum):
    """Standard agreement type classifications."""

    BILLABLE_SERVICE = "billable_service"
    PREPAID_HOURS = "prepaid_hours"
    INTERNAL_PROJECT = "internal_project"
    OPERATIONS = "operations"
    SOFTWARE_SERVICE = "software_service"
    OTHER = "other"


# Agreement type mapping configuration - maps ConnectWise types to standard types
AGREEMENT_TYPE_PATTERNS = {
    # Pattern: (regex pattern, standard type, billing behavior)
    r"yÞjónusta": (AgreementType.BILLABLE_SERVICE, "billable"),
    r"Tímapottur\s*:?": (AgreementType.PREPAID_HOURS, "prepaid"),
    r"Innri verkefni": (AgreementType.INTERNAL_PROJECT, "internal"),
    r"Rekstrarþjónusta": (AgreementType.OPERATIONS, "operations"),
    r"Alrekstur": (AgreementType.OPERATIONS, "operations"),
    r"Hugbúnaðarþjónusta": (AgreementType.SOFTWARE_SERVICE, "billable"),
    r"Office 365": (AgreementType.SOFTWARE_SERVICE, "billable"),
}


def normalize_agreement_type(type_name: str | None) -> tuple[AgreementType, str]:
    """Normalize agreement type names to standard values.

    Args:
        type_name: Raw agreement type name from ConnectWise

    Returns:
        Tuple of (standard type enum, billing behavior)
    """
    if not type_name:
        return AgreementType.OTHER, "unknown"

    # Clean the input - remove trailing spaces and punctuation
    cleaned = type_name.strip().rstrip(":")

    # Try to match against known patterns
    for pattern, (agreement_type, billing) in AGREEMENT_TYPE_PATTERNS.items():
        if re.search(pattern, cleaned, re.IGNORECASE):
            return agreement_type, billing

    logger.warning(f"Unknown agreement type: '{type_name}'")
    return AgreementType.OTHER, "unknown"


def extract_agreement_number(df: DataFrame, custom_fields_col: str = "customFields") -> DataFrame:
    """Extract agreement number from customFields with validation.

    Handles both JSON string and already-parsed struct formats.
    """
    # Check if customFields is already parsed (struct/array) or needs JSON parsing
    from pyspark.sql.types import StringType, ArrayType, StructType

    custom_fields_type = None
    for field in df.schema.fields:
        if field.name == custom_fields_col:
            custom_fields_type = field.dataType
            break

    # Use customFields directly if it's already an array/struct, otherwise parse JSON
    if isinstance(custom_fields_type, (ArrayType, StructType)):
        # Already parsed - use directly
        df_parsed = df.withColumn("_parsed_custom_fields", F.col(custom_fields_col))
    else:
        # String type - needs JSON parsing
        df_parsed = df.withColumn(
            "_parsed_custom_fields",
            F.when(
                F.col(custom_fields_col).isNotNull(),
                F.from_json(
                    F.col(custom_fields_col),
                    "array<struct<id:string,caption:string,type:string,value:string>>",
                ),
            ).otherwise(None),
        )

    # Extract agreement number with validation
    return df_parsed.withColumn(
        "agreementNumber",
        F.when(
            (F.col("_parsed_custom_fields").isNotNull()) & (F.size("_parsed_custom_fields") > 0),
            F.col("_parsed_custom_fields").getItem(0).getField("value"),
        ).otherwise(None),
    ).drop("_parsed_custom_fields")


def resolve_agreement_hierarchy(
    df: DataFrame,
    agreements_df: DataFrame,
    entity_agreement_col: str = "agreementId",
    entity_type: str = "entity",
) -> DataFrame:
    """Resolve agreement hierarchy with parent fallback and type normalization.

    This function:
    1. Joins entities with agreements
    2. Extracts agreement numbers with parent fallback
    3. Normalizes agreement types
    4. Adds billing classification
    """
    logger.info(f"Resolving agreement hierarchy for {entity_type}")

    # Get agreement numbers and normalize types
    agreements_enhanced = extract_agreement_number(agreements_df)

    # Create normalized type columns using UDF would be better, but let's use when/otherwise
    agreements_enhanced = agreements_enhanced.withColumn(
        "_type_normalized",
        # Check each pattern
        F.when(F.col("typeName").rlike(r"(?i)yÞjónusta"), "billable_service")
        .when(F.col("typeName").rlike(r"(?i)Tímapottur\s*:?"), "prepaid_hours")
        .when(F.col("typeName").rlike(r"(?i)Innri verkefni"), "internal_project")
        .when(F.col("typeName").rlike(r"(?i)Rekstrarþjónusta"), "operations")
        .when(F.col("typeName").rlike(r"(?i)Alrekstur"), "operations")
        .when(F.col("typeName").rlike(r"(?i)Hugbúnaðarþjónusta"), "software_service")
        .when(F.col("typeName").rlike(r"(?i)Office 365"), "software_service")
        .otherwise("other"),
    ).withColumn(
        "_billing_behavior",
        F.when(F.col("_type_normalized").isin("billable_service", "software_service"), "billable")
        .when(F.col("_type_normalized") == "prepaid_hours", "prepaid")
        .when(F.col("_type_normalized") == "internal_project", "internal")
        .when(F.col("_type_normalized") == "operations", "operations")
        .otherwise("unknown"),
    )

    # Join entity with agreements
    df_with_agreement = df.join(
        agreements_enhanced.select(
            F.col("id").alias("agr_id"),
            F.col("typeName").alias("agreement_type"),
            F.col("_type_normalized").alias("agreement_type_normalized"),
            F.col("_billing_behavior").alias("agreement_billing_behavior"),
            F.col("agreementNumber").alias("direct_agreement_number"),
            F.col("parentAgreementId"),
        ),
        df[entity_agreement_col] == F.col("agr_id"),
        "left",
    )

    # If no direct agreement number, check parent
    df_with_parent = df_with_agreement.join(
        agreements_enhanced.select(
            F.col("id").alias("parent_id"),
            F.col("agreementNumber").alias("parent_agreement_number"),
        ),
        F.col("parentAgreementId") == F.col("parent_id"),
        "left",
    )

    # Final agreement number with fallback logic
    result = df_with_parent.withColumn(
        "final_agreement_number", F.coalesce("direct_agreement_number", "parent_agreement_number")
    ).drop("agr_id", "parent_id", "direct_agreement_number", "parent_agreement_number")

    return result


def filter_billable_time_entries(time_df: DataFrame) -> DataFrame:
    """Filter time entries for invoice creation, excluding prepaid and internal work.

    This implements the Business Central logic where Tímapottur entries are excluded
    from invoice line creation.
    """
    return time_df.filter(
        # Must have an invoice ID
        F.col("invoiceId").isNotNull()
        &
        # Exclude prepaid hours (Tímapottur)
        (F.col("agreement_type_normalized") != "prepaid_hours")
        &
        # Exclude internal work
        (F.col("agreement_type_normalized") != "internal_project")
    )


def calculate_effective_billing_status(df: DataFrame) -> DataFrame:
    """Calculate the effective billing status combining multiple factors.

    Considers:
    - Invoice status (already invoiced?)
    - Agreement type (billable, prepaid, internal)
    - Billable option (Billable, NoCharge, DoNotBill)
    """
    return df.withColumn(
        "effectiveBillingStatus",
        F.when(F.col("invoiceId").isNotNull(), "Invoiced")
        .when(F.col("agreement_type_normalized") == "internal_project", "Internal")
        .when(F.col("agreement_type_normalized") == "prepaid_hours", "Prepaid")
        .when(
            (F.col("agreement_billing_behavior") == "billable")
            & (F.col("billableOption") == "Billable"),
            "Billable",
        )
        .when(F.col("billableOption") == "NoCharge", "NoCharge")
        .when(F.col("billableOption") == "DoNotBill", "DoNotBill"),
    )


def add_surrogate_keys(df: DataFrame, key_config: dict[str, dict]) -> DataFrame:
    """Add surrogate keys based on configuration.

    Args:
        df: Source DataFrame
        key_config: Dictionary mapping key names to generation config

    Returns:
        DataFrame with surrogate keys added
    """
    result_df = df

    for key_name, config in key_config.items():
        key_type = config.get("type", "hash")
        source_columns = config["source_columns"]

        if key_type == "hash":
            # Create hash-based surrogate key
            result_df = result_df.withColumn(
                key_name, F.sha2(F.concat_ws("_", *[F.col(c) for c in source_columns]), 256)
            )
        elif key_type == "date_int":
            # Create integer date key (YYYYMMDD)
            result_df = result_df.withColumn(
                key_name, F.date_format(F.col(source_columns[0]), "yyyyMMdd").cast("int")
            )

    return result_df
