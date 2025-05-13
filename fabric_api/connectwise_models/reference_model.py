"""
Custom reference type models for ConnectWise API entities.
These models handle the reference types like Company, Site, etc. that appear in responses.
"""
import json
from typing import Dict, Any, Optional, ClassVar, Dict, Type, Union
from pydantic import Field, model_validator
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from sparkdantic import SparkModel


class ReferenceModel(SparkModel):
    """
    Base class for ConnectWise reference fields.

    This model handles references to other entities in the ConnectWise API.
    Instead of using RootModel (which causes schema issues), we use defined fields.
    """
    class Config:
        validate_by_name = True

    # Define the fields that all reference models have
    id: int
    name: Optional[str] = None
    identifier: Optional[str] = None

    # Class variable to cache the Spark schema
    # This helps sparkdantic avoid recursion issues
    _spark_schema_cache: ClassVar[Dict[str, StructType]] = {}

    @classmethod
    def model_spark_schema(
        cls,
        safe_casting: bool = False,
        by_alias: bool = True,
        mode: str = 'validation',
        exclude_fields: bool = False,
    ) -> StructType:
        """
        Custom implementation of model_spark_schema to handle reference models correctly.

        This method creates a simple StructType schema for reference models
        to avoid the TypeConversionError in sparkdantic.
        """
        # Check if we have a cached schema for this class
        class_name = cls.__name__
        if class_name in cls._spark_schema_cache:
            return cls._spark_schema_cache[class_name]

        # Create a simple schema for reference models
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("identifier", StringType(), True)
        ])

        # Cache the schema for future use
        cls._spark_schema_cache[class_name] = schema

        return schema

    @model_validator(mode='before')
    @classmethod
    def parse_reference(cls, data: Any) -> Dict[str, Any]:
        """
        Pre-validate the reference data to handle various formats.

        ConnectWise API sometimes returns references with different structures:
        - Some have id + name
        - Some have id + identifier
        - Some have id + name + identifier

        This validator normalizes them to a consistent format.
        """
        if not isinstance(data, dict):
            # If we get a non-dict, try to convert or use a default
            if data is None:
                return {"id": 0}
            return {"id": data}

        # Ensure we have the minimum required field
        if "id" not in data:
            data["id"] = 0

        return data


class MemberReference(ReferenceModel):
    """Reference to a Member (user) in ConnectWise"""
    dailyCapacity: Optional[float] = None
    
    @classmethod
    def model_spark_schema(
        cls,
        safe_casting: bool = False,
        by_alias: bool = True,
        mode: str = 'validation',
        exclude_fields: bool = False,
    ) -> StructType:
        """
        Custom implementation for MemberReference with dailyCapacity field.
        """
        # Check if we have a cached schema for this class
        class_name = cls.__name__
        if class_name in cls._spark_schema_cache:
            return cls._spark_schema_cache[class_name]

        # Create a schema for MemberReference
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("identifier", StringType(), True),
            StructField("dailyCapacity", FloatType(), True)
        ])

        # Cache the schema for future use
        cls._spark_schema_cache[class_name] = schema

        return schema


class LocationReference(ReferenceModel):
    """Reference to a Location in ConnectWise"""
    pass


class DepartmentReference(ReferenceModel):
    """Reference to a Department in ConnectWise"""
    pass


class WorkTypeReference(ReferenceModel):
    """Reference to a Work Type in ConnectWise"""
    pass


class WorkRoleReference(ReferenceModel):
    """Reference to a Work Role in ConnectWise"""
    pass


class TimeSheetReference(ReferenceModel):
    """Reference to a Time Sheet in ConnectWise"""
    pass


class ProjectReference(ReferenceModel):
    """Reference to a Project in ConnectWise"""
    pass


class PhaseReference(ReferenceModel):
    """Reference to a Project Phase in ConnectWise"""
    pass


class TaxCodeReference(ReferenceModel):
    """Reference to a Tax Code in ConnectWise"""
    pass


class ClassificationReference(ReferenceModel):
    """Reference to a Classification in ConnectWise"""
    pass


class CurrencyReference(ReferenceModel):
    """Reference to a Currency in ConnectWise"""
    symbol: Optional[str] = None
    currencyCode: Optional[str] = None
    currencyIdentifier: Optional[str] = None


class ExpenseReportReference(ReferenceModel):
    """Reference to an Expense Report in ConnectWise"""
    pass


class PaymentMethodReference(ReferenceModel):
    """Reference to a Payment Method in ConnectWise"""
    pass


class TypeReference(ReferenceModel):
    """Reference to a Type in ConnectWise"""
    pass


class BusinessUnitReference(ReferenceModel):
    """Reference to a Business Unit in ConnectWise"""
    pass


class CatalogItemReference(ReferenceModel):
    """Reference to a Catalog Item in ConnectWise"""
    pass


class OpportunityReference(ReferenceModel):
    """Reference to an Opportunity in ConnectWise"""
    pass


class UnitOfMeasureReference(ReferenceModel):
    """Reference to a Unit of Measure in ConnectWise"""
    pass


class BillingTermsReference(ReferenceModel):
    """Reference to Billing Terms in ConnectWise"""
    pass


class SiteReference(ReferenceModel):
    """Reference to a Site in ConnectWise"""
    pass


class CompanyReference(ReferenceModel):
    """Reference to a Company in ConnectWise"""
    pass