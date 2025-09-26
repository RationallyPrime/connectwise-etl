"""ETL-specific error handling using error codes."""

from .base import ApplicationError, ErrorCode, ErrorLevel


# Three main exception classes for ETL framework
class ETLConfigError(ApplicationError):
    """Configuration and setup errors (1xxx codes).

    Use for:
    - Missing required configuration
    - Invalid configuration values
    - Schema mismatches
    - Validation failures during setup
    """

    def __init__(self, message: str, code: ErrorCode = ErrorCode.CONFIG_INVALID, **kwargs):
        super().__init__(
            message=message, code=code, level=kwargs.pop("level", ErrorLevel.ERROR), **kwargs
        )


class ETLProcessingError(ApplicationError):
    """Runtime processing errors (2xxx-5xxx codes).

    Use for:
    - API/source system failures
    - Bronze layer extraction/validation
    - Silver layer transformations
    - Gold layer dimensional modeling
    """

    def __init__(self, message: str, code: ErrorCode, **kwargs):
        super().__init__(
            message=message, code=code, level=kwargs.pop("level", ErrorLevel.ERROR), **kwargs
        )


class ETLInfrastructureError(ApplicationError):
    """Infrastructure/platform errors (6xxx codes).

    Use for:
    - Spark session failures
    - Storage access issues
    - Memory/resource limits
    - Platform-level problems
    """

    def __init__(self, message: str, code: ErrorCode = ErrorCode.SPARK_SESSION_FAILED, **kwargs):
        super().__init__(
            message=message, code=code, level=kwargs.pop("level", ErrorLevel.CRITICAL), **kwargs
        )


# Legacy aliases for backward compatibility (to be deprecated)
DimensionResolutionError = ETLProcessingError
DimensionJoinError = ETLProcessingError
HierarchyBuildError = ETLProcessingError
