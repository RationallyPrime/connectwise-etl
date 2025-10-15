"""Main integration plugin protocol."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from .client import DataFetcherProtocol, DataValidatorProtocol
from .processor import BronzeProcessorProtocol, GoldProcessorProtocol, SilverProcessorProtocol
from .registry import ModelRegistryProtocol

if TYPE_CHECKING:
    from etl_core.utils.incremental import IncrementalHandler


@dataclass(frozen=True)
class Processors:
    """Container for medallion layer processors.

    Using a named dataclass instead of tuple provides:
    - Clear field names
    - Type safety
    - Better IDE support
    - Easier refactoring
    """

    bronze: BronzeProcessorProtocol
    silver: SilverProcessorProtocol
    gold: GoldProcessorProtocol


@runtime_checkable
class IntegrationPluginProtocol(Protocol):
    """
    Main protocol defining an integration plugin (e.g., ConnectWise, Jira).

    Each integration provides:
    - A model registry (SparkDantic models for entities)
    - A data fetcher (integration-specific extraction logic)
    - Three processors (bronze, silver, gold)

    Note: Each integration gets its own lakehouse, so table naming
    collisions between integrations are not a concern.

    Example:
        ```python
        class ConnectWisePlugin:
            @property
            def name(self) -> str:
                return "connectwise"

            def initialize_registry(self) -> ModelRegistryProtocol:
                return ConnectWiseRegistry()

            def initialize_fetcher(self) -> DataFetcherProtocol:
                return ConnectWiseAPIClient()

            def initialize_processors(...) -> Processors:
                return Processors(
                    bronze=ConnectWiseBronzeProcessor(...),
                    silver=ConnectWiseSilverProcessor(...),
                    gold=ConnectWiseGoldProcessor(...),
                )
        ```
    """

    @property
    def name(self) -> str:
        """
        Unique identifier for the integration (e.g., 'connectwise', 'jira').

        Used for:
        - Lakehouse naming
        - Logging context
        - Configuration lookup
        """
        ...

    def initialize_registry(self) -> ModelRegistryProtocol:
        """
        Initialize and return the model registry.

        The registry provides:
        - SparkDantic models for each entity
        - Entity-specific configuration
        - Merge keys and timestamp columns

        Returns:
            Initialized model registry for this integration.
        """
        ...

    def initialize_fetcher(self) -> DataFetcherProtocol:
        """
        Initialize and return the data fetcher.

        The fetcher handles:
        - Authentication with data source
        - API pagination or file reading
        - Incremental extraction logic

        Returns:
            Initialized data fetcher (integration-specific).
        """
        ...

    def initialize_processors(
        self,
        fetcher: DataFetcherProtocol,
        validator: DataValidatorProtocol,
        registry: ModelRegistryProtocol,
        incremental_handler: "IncrementalHandler",
    ) -> Processors:
        """
        Initialize and return the processors for this integration.

        Args:
            fetcher: The initialized data fetcher (integration-specific).
            validator: The data validator (provided by core, typically reused).
            registry: The initialized model registry.
            incremental_handler: Core incremental handler utility (merge logic).

        Returns:
            Processors container with bronze, silver, and gold processors.

        Example:
            ```python
            def initialize_processors(self, fetcher, validator, registry, incremental):
                bronze = BronzeProcessor(fetcher, validator, registry)
                silver = SilverProcessor(registry, incremental)
                gold = GoldProcessor(registry)
                return Processors(bronze=bronze, silver=silver, gold=gold)
            ```
        """
        ...
