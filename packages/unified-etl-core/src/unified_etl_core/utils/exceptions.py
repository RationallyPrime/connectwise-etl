"""ETL-specific error handling using error codes."""

from ..base import ApplicationError, ErrorCode, ErrorLevel


# Convenience exception aliases that use specific ETL error codes
class DimensionResolutionError(ApplicationError):
    """Raised when dimension resolution fails."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message=message,
            code=ErrorCode.DIMENSION_RESOLUTION_FAILED,
            level=ErrorLevel.ERROR,
            **kwargs
        )


class DimensionJoinError(ApplicationError):
    """Raised when dimension join operations fail."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message=message,
            code=ErrorCode.DIMENSION_JOIN_FAILED,
            level=ErrorLevel.ERROR,
            **kwargs
        )


class HierarchyBuildError(ApplicationError):
    """Raised when hierarchy building fails."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message=message,
            code=ErrorCode.DIMENSION_HIERARCHY_FAILED,
            level=ErrorLevel.ERROR,
            **kwargs
        )