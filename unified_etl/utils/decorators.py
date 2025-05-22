# unified_etl/utils/decorators.py
import functools
import inspect
import time
from collections.abc import Callable
from typing import Any, TypeVar

from unified_etl.utils import logging
from unified_etl.utils.exceptions import UnifiedETLExceptionError

# Type variable for the return type of the decorated function
T = TypeVar("T")


def handle_errors(
    error_class: type[Exception] = UnifiedETLExceptionError,
    error_msg: str | None = None,
    default_return: Any = None,
    reraise: bool = True,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator for standardized error handling in ETL functions.

    Args:
        error_class: Exception class to raise if an error occurs
        error_msg: Optional error message template
        default_return: Value to return if an error occurs and reraise=False
        reraise: Whether to re-raise the exception (if False, returns default_return)

    Returns:
        Decorated function with standardized error handling
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            # Get function metadata for logging
            func_name = func.__name__
            module_name = func.__module__

            # Extract arguments for context logging
            # Skip first arg if it's 'self' or 'cls'
            arg_spec = inspect.getfullargspec(func)
            arg_names = arg_spec.args

            if arg_names and arg_names[0] in ("self", "cls"):
                arg_names = arg_names[1:]
                args_to_log = args[1:]
            else:
                args_to_log = args

            # Create context dict with arg names and values
            context = {}
            for name, value in zip(arg_names, args_to_log, strict=False):
                # Avoid logging potentially large objects
                if name not in ("df", "spark", "dataframe"):
                    try:
                        # Try to convert to string, but don't fail if not serializable
                        context[name] = str(value)
                    except Exception:
                        context[name] = f"<{type(value).__name__}>"

            # Add kwargs, also skipping dataframes
            for name, value in kwargs.items():
                if name not in ("df", "spark", "dataframe"):
                    try:
                        context[name] = str(value)
                    except Exception:
                        context[name] = f"<{type(value).__name__}>"

            # Generate custom error message if none provided
            msg = error_msg or f"Error in {module_name}.{func_name}"

            # Log function entry with context
            logging.info(f"Executing {module_name}.{func_name}", **context)

            start_time = time.time()

            try:
                result = func(*args, **kwargs)

                # Log success and execution time
                elapsed = time.time() - start_time
                logging.info(
                    f"Successfully completed {module_name}.{func_name} in {elapsed:.2f}s",
                    execution_time=elapsed,
                    **context,
                )

                return result

            except Exception as e:
                # Log the error with function context
                elapsed = time.time() - start_time
                logging.error(
                    f"{msg}: {e!s}",
                    error_type=type(e).__name__,
                    execution_time=elapsed,
                    function=f"{module_name}.{func_name}",
                    **context,
                    exc_info=True,
                )

                if reraise:
                    # Re-raise as the specified error class if it's not already that type
                    if not isinstance(e, error_class):
                        raise error_class(f"{msg}: {e!s}") from e
                    # Otherwise, re-raise the original exception
                    raise

                # Return default value if not re-raising
                return default_return

        return wrapper

    return decorator


def fact_table_handler(
    error_msg: str | None = None,
    reraise: bool = True,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator specifically for fact table creation functions.
    Uses handle_errors with FactTableError as the exception class.

    Args:
        error_msg: Optional error message template
        reraise: Whether to re-raise the exception

    Returns:
        Decorated function with standardized error handling
    """
    from unified_etl.utils.exceptions import FactTableError

    return handle_errors(
        error_class=FactTableError,
        error_msg=error_msg,
        reraise=reraise,
    )


def dimension_handler(
    error_msg: str | None = None,
    reraise: bool = True,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator specifically for dimension creation/processing functions.
    Uses handle_errors with DimensionError as the exception class.

    Args:
        error_msg: Optional error message template
        reraise: Whether to re-raise the exception

    Returns:
        Decorated function with standardized error handling
    """
    from unified_etl.utils.exceptions import DimensionError

    return handle_errors(
        error_class=DimensionError,
        error_msg=error_msg,
        reraise=reraise,
    )


def timing_decorator(func: Callable[..., T]) -> Callable[..., T]:
    """
    Simple decorator to log execution time of a function.

    Args:
        func: Function to decorate

    Returns:
        Decorated function with timing
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start_time
        logging.info(f"{func.__name__} executed in {elapsed:.2f} seconds")
        return result

    return wrapper


def retry(
    max_attempts: int = 3,
    delay_seconds: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Retry decorator for handling transient errors.

    Args:
        max_attempts: Maximum number of retry attempts
        delay_seconds: Initial delay between retries in seconds
        backoff_factor: Backoff multiplier for subsequent retries
        exceptions: Tuple of exceptions to catch and retry

    Returns:
        Decorated function with retry logic
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception = None
            current_delay = delay_seconds

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    if attempt < max_attempts:
                        logging.warning(
                            f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {e!s}. "
                            f"Retrying in {current_delay:.2f} seconds..."
                        )

                        # Sleep with backoff
                        time.sleep(current_delay)
                        current_delay *= backoff_factor
                    else:
                        logging.error(
                            f"All {max_attempts} attempts failed for {func.__name__}: {e!s}"
                        )

            # If we get here, all attempts failed
            if last_exception:
                raise last_exception

            # This should never happen, but needed to satisfy type checking
            raise RuntimeError(
                "Unexpected error in retry decorator: no exception was raised but all attempts failed"
            )

        return wrapper

    return decorator
