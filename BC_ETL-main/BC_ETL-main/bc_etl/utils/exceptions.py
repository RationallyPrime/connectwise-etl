# bc_etl/utils/exceptions.py
class BCETLExceptionError(Exception):
    """Base exception for all BC ETL errors."""

    pass


class TableNotFoundExceptionError(BCETLExceptionError):
    """Raised when a required table cannot be found."""

    pass


class ColumnStandardizationError(BCETLExceptionError):
    """Raised when column standardization fails."""

    pass


class DimensionResolutionError(BCETLExceptionError):
    """Raised when dimension resolution fails."""

    pass


class HierarchyBuildError(BCETLExceptionError):
    """Raised when building a hierarchy structure fails."""

    pass


class SurrogateKeyError(BCETLExceptionError):
    """Raised when surrogate key generation fails."""

    pass


class DataTypeConversionError(BCETLExceptionError):
    """Raised when data type conversion fails."""

    pass


class ConfigurationError(BCETLExceptionError):
    """Raised when configuration loading fails."""

    pass


class SCDHandlingError(BCETLExceptionError):
    """Raised when SCD handling fails."""

    pass


class DimensionError(BCETLExceptionError):
    """Raised when dimension operations fail."""

    pass


class DimensionJoinError(BCETLExceptionError):
    """Raised when dimension join operations fail."""

    pass


class FactTableError(BCETLExceptionError):
    """Raised when fact table creation fails."""

    pass


class AnalysisException(BCETLExceptionError):  # noqa: N818
    """Raised when an analysis exception occurs."""

    pass
