# unified_etl/utils/exceptions.py
class UnifiedETLExceptionError(Exception):
    """Base exception for all Unified ETL errors."""

    pass


class TableNotFoundExceptionError(UnifiedETLExceptionError):
    """Raised when a required table cannot be found."""

    pass


class ColumnStandardizationError(UnifiedETLExceptionError):
    """Raised when column standardization fails."""

    pass


class DimensionResolutionError(UnifiedETLExceptionError):
    """Raised when dimension resolution fails."""

    pass


class HierarchyBuildError(UnifiedETLExceptionError):
    """Raised when building a hierarchy structure fails."""

    pass


class SurrogateKeyError(UnifiedETLExceptionError):
    """Raised when surrogate key generation fails."""

    pass


class DataTypeConversionError(UnifiedETLExceptionError):
    """Raised when data type conversion fails."""

    pass


class ConfigurationError(UnifiedETLExceptionError):
    """Raised when configuration loading fails."""

    pass


class SCDHandlingError(UnifiedETLExceptionError):
    """Raised when SCD handling fails."""

    pass


class DimensionError(UnifiedETLExceptionError):
    """Raised when dimension operations fail."""

    pass


class DimensionJoinError(UnifiedETLExceptionError):
    """Raised when dimension join operations fail."""

    pass


class FactTableError(UnifiedETLExceptionError):
    """Raised when fact table creation fails."""

    pass


class AnalysisException(UnifiedETLExceptionError):  # noqa: N818
    """Raised when an analysis exception occurs."""

    pass
