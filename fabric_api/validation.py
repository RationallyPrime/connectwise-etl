#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Common validation layer for ConnectWise data.
This module centralizes validation logic to ensure consistent handling across all entity types.
"""

import logging
from typing import Dict, List, Any, Type, Tuple, TypeVar, Optional, Generic
from datetime import datetime
from pydantic import ValidationError, BaseModel

# Models are imported from the connectwise_models package via schemas
from fabric_api import schemas

# Initialize logger with minimal verbosity for production environments
logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)  # Only show errors by default (even more restrictive)

# Log configuration flags
VERBOSE_ERROR_LOGGING = False  # Set to True for detailed validation error messages
SILENCE_VALIDATION_ERRORS = True  # Set to True to completely silence validation errors in console
                                  # Errors will still be captured in ValidationResult objects

# Generic type variable for model types
T = TypeVar('T', bound=BaseModel)

class ValidationResult(Generic[T]):
    """
    Container for validation results, including both valid objects and errors.
    
    Attributes:
        valid_objects: List of successfully validated objects
        errors: List of validation errors
        total_processed: Total number of records processed
        success_rate: Percentage of successfully validated objects
    """
    
    def __init__(self, valid_objects: List[T], errors: List[Dict[str, Any]], entity_name: str):
        """
        Initialize a ValidationResult.
        
        Args:
            valid_objects: List of successfully validated objects
            errors: List of validation errors
            entity_name: Name of the entity type being validated
        """
        self.valid_objects = valid_objects
        self.errors = errors
        self.entity_name = entity_name
        self.total_processed = len(valid_objects) + len(errors)
        self.success_rate = (len(valid_objects) / self.total_processed) * 100 if self.total_processed > 0 else 0
    
    def log_summary(self) -> None:
        """Log a summary of the validation results."""
        # Only log summary if we're not silencing validation outputs
        if not SILENCE_VALIDATION_ERRORS:
            logger.info(
                f"Validation summary for {self.entity_name}: "
                f"{len(self.valid_objects)} valid, {len(self.errors)} invalid "
                f"({self.success_rate:.1f}% success rate)"
            )
            
        # Always track errors in a structured way for later analysis
        # This doesn't output to console but ensures we have error data available
        if len(self.errors) > 0:
            error_pattern = {}
            for error_dict in self.errors:
                for error in error_dict.get("errors", []):
                    if "loc" in error and len(error["loc"]) > 0:
                        field = error["loc"][0]
                        error_pattern[field] = error_pattern.get(field, 0) + 1
            
            # This will be captured in logs but not printed to console when SILENCE_VALIDATION_ERRORS is True
            if not SILENCE_VALIDATION_ERRORS and error_pattern:
                logger.info(f"Validation error patterns for {self.entity_name}: {error_pattern}")
    
    def __repr__(self) -> str:
        """String representation of the validation results."""
        return (
            f"ValidationResult(entity_name={self.entity_name}, "
            f"valid_objects={len(self.valid_objects)}, "
            f"errors={len(self.errors)}, "
            f"success_rate={self.success_rate:.1f}%)"
        )


def validate_data(
    raw_data: List[Dict[str, Any]],
    model_class: Type[T],
    entity_name: str,
    id_field: str = "id"
) -> ValidationResult[T]:
    """
    Validate a list of raw data dictionaries against a Pydantic model.
    
    Args:
        raw_data: List of raw data dictionaries
        model_class: Pydantic model class to validate against
        entity_name: Name of the entity type (for error reporting)
        id_field: Field to use for identifying records in error messages
    
    Returns:
        ValidationResult containing valid objects and errors
    """
    valid_objects: List[T] = []
    errors: List[Dict[str, Any]] = []
    
    logger.info(f"Validating {len(raw_data)} {entity_name} records")
    
    for i, raw_dict in enumerate(raw_data):
        # Get ID for error reporting (or create a generic one if not available)
        record_id = raw_dict.get(id_field, f"Unknown-{i}")
        
        try:
            # Validate using model_validate method from Pydantic v2
            validated_obj = model_class.model_validate(raw_dict)
            valid_objects.append(validated_obj)
        except ValidationError as e:
            # Extract just the essential error information to avoid huge outputs
            error_fields = [f"{err['type']} on {'.'.join(str(loc) for loc in err['loc'])}" for err in e.errors()]
            concise_error = ", ".join(error_fields)
            
            # Control validation error output based on configuration flags
            if not SILENCE_VALIDATION_ERRORS:
                if VERBOSE_ERROR_LOGGING:
                    logger.warning(f"❌ Validation failed for {entity_name} ID {record_id}: {e.json()}")
                else:
                    logger.warning(f"❌ Validation failed for {entity_name} ID {record_id}: {concise_error}")
            
            # For debugging purposes only - use this in a local debug environment
            # Uncomment if needed: print(f"DEBUG: Validation error for {entity_name} ID {record_id}")
                
            errors.append({
                "entity": entity_name,
                "raw_data_id": record_id,
                "errors": e.errors(),
                "timestamp": datetime.utcnow().isoformat()
            })
    
    # Create and return the validation result
    result = ValidationResult(valid_objects, errors, entity_name)
    result.log_summary()
    return result


# Convenience functions for each entity type
def validate_agreements(raw_data: List[Dict[str, Any]]) -> ValidationResult[schemas.Agreement]:
    """Validate Agreement data."""
    return validate_data(raw_data, schemas.Agreement, "Agreement")


def validate_posted_invoices(raw_data: List[Dict[str, Any]]) -> ValidationResult[schemas.PostedInvoice]:
    """Validate PostedInvoice data."""
    return validate_data(raw_data, schemas.PostedInvoice, "PostedInvoice")


def validate_unposted_invoices(raw_data: List[Dict[str, Any]]) -> ValidationResult[schemas.UnpostedInvoice]:
    """Validate UnpostedInvoice data."""
    return validate_data(raw_data, schemas.UnpostedInvoice, "UnpostedInvoice")


def validate_time_entries(raw_data: List[Dict[str, Any]]) -> ValidationResult[schemas.TimeEntry]:
    """Validate TimeEntry data."""
    return validate_data(raw_data, schemas.TimeEntry, "TimeEntry")


def validate_expense_entries(raw_data: List[Dict[str, Any]]) -> ValidationResult[schemas.ExpenseEntry]:
    """Validate ExpenseEntry data."""
    return validate_data(raw_data, schemas.ExpenseEntry, "ExpenseEntry")


def validate_product_items(raw_data: List[Dict[str, Any]]) -> ValidationResult[schemas.ProductItem]:
    """Validate ProductItem data."""
    return validate_data(raw_data, schemas.ProductItem, "ProductItem")