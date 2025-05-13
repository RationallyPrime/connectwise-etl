#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Logging utilities for the ETL process.
Provides structured error reporting, validation error tracking, and configurable logging.
"""

import logging
import json
import os
import sys
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Union, Callable

from pydantic import ValidationError

# Custom log levels for different types of ETL events
API_CALL = 15  # Between DEBUG and INFO - for API operations
VALIDATION = 25  # Between INFO and WARNING - for validation issues
ETL_PROGRESS = 35  # Between WARNING and ERROR - for ETL progress

# Register custom log levels
logging.addLevelName(API_CALL, "API_CALL")
logging.addLevelName(VALIDATION, "VALIDATION")
logging.addLevelName(ETL_PROGRESS, "ETL_PROGRESS")

# Configure default logging format
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

class ETLLogger:
    """
    Enhanced logger for ETL operations with structured error reporting.
    """
    
    def __init__(
        self, 
        name: str,
        log_level: int = logging.INFO,
        log_file: Optional[str] = None,
        console: bool = True,
        format_str: str = DEFAULT_LOG_FORMAT
    ):
        """
        Initialize the ETL logger.
        
        Args:
            name: Logger name
            log_level: Minimum log level to record
            log_file: Optional file path to write logs to
            console: Whether to output logs to console
            format_str: Log message format string
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)
        self.logger.propagate = False  # Don't propagate to parent loggers
        
        # Set up formatters
        formatter = logging.Formatter(format_str)
        
        # Clear any existing handlers
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
        
        # Add console handler if requested
        if console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        
        # Add file handler if log_file is provided
        if log_file:
            # Make sure the directory exists
            os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
    
    # Standard logging methods
    def debug(self, msg: str, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)
    
    def info(self, msg: str, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)
    
    def warning(self, msg: str, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)
    
    def error(self, msg: str, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)
    
    def critical(self, msg: str, *args, **kwargs):
        self.logger.critical(msg, *args, **kwargs)
    
    # Custom log levels
    def api_call(self, msg: str, *args, **kwargs):
        """Log API call information at API_CALL level"""
        self.logger.log(API_CALL, msg, *args, **kwargs)
    
    def validation(self, msg: str, *args, **kwargs):
        """Log validation issues at VALIDATION level"""
        self.logger.log(VALIDATION, msg, *args, **kwargs)
    
    def etl_progress(self, msg: str, *args, **kwargs):
        """Log ETL progress at ETL_PROGRESS level"""
        self.logger.log(ETL_PROGRESS, msg, *args, **kwargs)
    
    # Special error logging methods
    def validation_error(
        self, 
        error: ValidationError, 
        record_id: Optional[Union[str, int]] = None, 
        entity_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Log a validation error and return structured error information.
        
        Args:
            error: ValidationError from Pydantic
            record_id: Optional identifier for the record
            entity_type: Optional entity type name
            
        Returns:
            Structured error information dictionary
        """
        # Extract error details
        error_details = error.errors()
        
        # Create structured error entry
        error_entry = {
            "entity": entity_type or "Unknown",
            "raw_data_id": str(record_id) if record_id is not None else "Unknown",
            "errors": error_details,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Log the error
        self.validation(
            f"Validation error for {entity_type or 'entity'} ID {record_id or 'unknown'}: "
            f"{len(error_details)} issues found"
        )
        
        # Log detailed error information at debug level
        self.debug(f"Validation error details: {json.dumps(error_details, default=str)}")
        
        return error_entry
    
    def api_error(
        self, 
        endpoint: str, 
        error: Exception, 
        status_code: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Log an API error and return structured error information.
        
        Args:
            endpoint: API endpoint that was called
            error: Exception that occurred
            status_code: Optional HTTP status code
            
        Returns:
            Structured error information dictionary
        """
        # Create structured error entry
        error_entry = {
            "type": "api_error",
            "endpoint": endpoint,
            "status_code": status_code,
            "error_message": str(error),
            "error_type": type(error).__name__,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Log the error
        self.error(
            f"API error on {endpoint}: {type(error).__name__}"
            f"{f' (Status: {status_code})' if status_code else ''} - {str(error)}"
        )
        
        return error_entry
    
    def etl_error(
        self, 
        stage: str, 
        error: Exception, 
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Log an ETL processing error and return structured error information.
        
        Args:
            stage: ETL stage where the error occurred (e.g., 'extraction', 'transformation', 'loading')
            error: Exception that occurred
            context: Optional contextual information about the error
            
        Returns:
            Structured error information dictionary
        """
        # Create structured error entry
        error_entry = {
            "type": "etl_error",
            "stage": stage,
            "error_message": str(error),
            "error_type": type(error).__name__,
            "context": context or {},
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Log the error
        self.error(
            f"ETL error during {stage}: {type(error).__name__} - {str(error)}"
        )
        
        # Log context at debug level if provided
        if context:
            self.debug(f"ETL error context: {json.dumps(context, default=str)}")
        
        return error_entry
    
    # Utility methods
    def timed_operation(self, operation_name: str) -> Callable:
        """
        Context manager for timing operations.

        Usage:
            with logger.timed_operation("fetch_data") as timer:
                # Do something
                timer.add_context({"record_count": 42})

        Args:
            operation_name: Name of the operation for logging

        Returns:
            Context manager
        """
        from contextlib import contextmanager
        import time

        class TimedOperationContext:
            def __init__(self):
                self.context = {}

            def add_context(self, ctx: Dict[str, Any]):
                """Add context information for the timed operation"""
                self.context.update(ctx)

        @contextmanager
        def _timer():
            ctx = TimedOperationContext()
            start_time = time.time()
            try:
                self.info(f"Starting operation: {operation_name}")
                yield ctx
            finally:
                elapsed = time.time() - start_time
                context_str = f" | Context: {ctx.context}" if ctx.context else ""
                self.info(f"Completed operation: {operation_name} | Time: {elapsed:.2f}s{context_str}")

        return _timer()
    
    def summarize_validation_errors(self, validation_errors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Summarize validation errors by entity and error type.
        
        Args:
            validation_errors: List of validation error dictionaries
            
        Returns:
            Summary of validation errors
        """
        if not validation_errors:
            return {"total_errors": 0, "entities": {}}
        
        # Group errors by entity
        entities = {}
        for error in validation_errors:
            entity = error.get("entity", "Unknown")
            if entity not in entities:
                entities[entity] = []
            entities[entity].append(error)
        
        # Summarize errors by entity
        summary = {
            "total_errors": len(validation_errors),
            "entities": {}
        }
        
        for entity, errors in entities.items():
            # Group errors by field location
            field_counts = {}
            for error in errors:
                for err_detail in error.get("errors", []):
                    loc = ".".join(str(x) for x in err_detail.get("loc", []))
                    if not loc:
                        loc = "general"
                    
                    if loc not in field_counts:
                        field_counts[loc] = 0
                    field_counts[loc] += 1
            
            summary["entities"][entity] = {
                "count": len(errors),
                "fields": field_counts
            }
        
        # Log the summary
        self.validation(f"Validation error summary: {json.dumps(summary, indent=2)}")
        
        return summary

# Default logger instance for the ETL process
etl_logger = ETLLogger("fabric_api.etl")

# Helper functions to use the default logger
def debug(msg: str, *args, **kwargs):
    etl_logger.debug(msg, *args, **kwargs)

def info(msg: str, *args, **kwargs):
    etl_logger.info(msg, *args, **kwargs)

def warning(msg: str, *args, **kwargs):
    etl_logger.warning(msg, *args, **kwargs)

def error(msg: str, *args, **kwargs):
    etl_logger.error(msg, *args, **kwargs)

def critical(msg: str, *args, **kwargs):
    etl_logger.critical(msg, *args, **kwargs)

def api_call(msg: str, *args, **kwargs):
    etl_logger.api_call(msg, *args, **kwargs)

def validation(msg: str, *args, **kwargs):
    etl_logger.validation(msg, *args, **kwargs)

def etl_progress(msg: str, *args, **kwargs):
    etl_logger.etl_progress(msg, *args, **kwargs)

def configure_logging(
    log_level: int = logging.INFO,
    log_file: Optional[str] = None,
    console: bool = True,
    format_str: str = DEFAULT_LOG_FORMAT
):
    """
    Configure the default ETL logger.
    
    Args:
        log_level: Minimum log level to record
        log_file: Optional file path to write logs to
        console: Whether to output logs to console
        format_str: Log message format string
    """
    global etl_logger
    etl_logger = ETLLogger(
        "fabric_api.etl",
        log_level=log_level,
        log_file=log_file,
        console=console,
        format_str=format_str
    )