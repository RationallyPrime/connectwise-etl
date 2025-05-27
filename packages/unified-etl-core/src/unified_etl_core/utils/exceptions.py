"""Custom exceptions for the unified ETL framework."""


class TableNotFoundExceptionError(Exception):
    """Raised when a table cannot be found in the expected location."""
    pass


class ConfigurationError(Exception):
    """Raised when there's an issue with configuration."""
    pass


class ValidationError(Exception):
    """Raised when data validation fails."""
    pass


class TransformationError(Exception):
    """Raised when data transformation fails."""
    pass