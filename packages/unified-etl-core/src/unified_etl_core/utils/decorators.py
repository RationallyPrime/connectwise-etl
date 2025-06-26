"""Error handling decorators adapted for ETL framework."""

import asyncio
import inspect
from functools import wraps
from typing import Awaitable, Callable, TypeVar, cast

import structlog
from typing_extensions import ParamSpec

from ..utils.exceptions import ApplicationError
from ..base import ErrorLevel

# Type variables for generic function signatures
P = ParamSpec("P")
T = TypeVar("T")

logger = structlog.get_logger(__name__)


def with_etl_error_handling(
    error_level: ErrorLevel = ErrorLevel.ERROR,
    reraise: bool = True,
    operation: str | None = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator for handling errors in ETL functions.
    
    Simplified version of the error handler that logs structured errors
    and optionally re-raises them.

    Args:
        error_level: Severity level for error logging
        reraise: Whether to re-raise the error after handling
        operation: Optional operation name (defaults to function name)

    Returns:
        Decorated function with error handling
    """

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        # Capture the original signature to preserve it
        original_signature = inspect.signature(func)
        op_name = operation or func.__name__

        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                try:
                    return await cast("Callable[P, Awaitable[T]]", func)(*args, **kwargs)
                except ApplicationError as e:
                    logger.log(
                        e.level.value,
                        f"ETL error in {op_name}",
                        error_code=e.code.value,
                        error_message=str(e),
                        operation=op_name,
                        error_details=e.details.model_dump() if hasattr(e.details, 'model_dump') else e.details,
                        exc_info=True,
                    )
                    if reraise:
                        raise
                    return cast("T", None)
                except Exception as e:
                    logger.log(
                        error_level.value,
                        f"Unexpected error in {op_name}",
                        operation=op_name,
                        error_type=type(e).__name__,
                        error_message=str(e),
                        exc_info=True,
                    )
                    if reraise:
                        raise
                    return cast("T", None)

            # Explicitly set the signature on the wrapper
            async_wrapper.__signature__ = original_signature  # type: ignore
            return cast("Callable[P, T]", async_wrapper)
        else:

            @wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                try:
                    return func(*args, **kwargs)
                except ApplicationError as e:
                    logger.log(
                        e.level.value,
                        f"ETL error in {op_name}",
                        error_code=e.code.value,
                        error_message=str(e),
                        operation=op_name,
                        error_details=e.details.model_dump() if hasattr(e.details, 'model_dump') else e.details,
                        exc_info=True,
                    )
                    if reraise:
                        raise
                    return cast("T", None)
                except Exception as e:
                    logger.log(
                        error_level.value,
                        f"Unexpected error in {op_name}",
                        operation=op_name,
                        error_type=type(e).__name__,
                        error_message=str(e),
                        exc_info=True,
                    )
                    if reraise:
                        raise
                    return cast("T", None)

            # Explicitly set the signature on the wrapper
            sync_wrapper.__signature__ = original_signature  # type: ignore
            return cast("Callable[P, T]", sync_wrapper)

    return decorator