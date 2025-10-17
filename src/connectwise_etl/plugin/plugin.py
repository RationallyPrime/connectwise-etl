"""ConnectWise integration plugin."""

from etl_core.domain.protocols import (
    DataFetcherProtocol,
    DataValidatorProtocol,
    ModelRegistryProtocol,
    Processors,
)
from etl_core.utils.incremental import IncrementalHandler

from .fetcher import ConnectWiseFetcher
from .processors import ConnectWiseBronzeProcessor, ConnectWiseGoldProcessor, ConnectWiseSilverProcessor
from .registry import ConnectWiseRegistry


class ConnectWisePlugin:
    """
    ConnectWise integration plugin implementing IntegrationPluginProtocol.

    Provides complete ConnectWise-to-Lakehouse ETL functionality with:
    - API data extraction
    - Pydantic validation
    - Bronze/Silver/Gold medallion architecture
    - Icelandic agreement type handling
    """

    @property
    def name(self) -> str:
        """Unique identifier for the integration."""
        return "connectwise"

    def initialize_registry(self) -> ModelRegistryProtocol:
        """Initialize and return the model registry."""
        return ConnectWiseRegistry()

    def initialize_fetcher(self) -> DataFetcherProtocol:
        """Initialize and return the data fetcher."""
        return ConnectWiseFetcher()

    def initialize_processors(
        self,
        fetcher: DataFetcherProtocol,
        validator: DataValidatorProtocol,
        registry: ModelRegistryProtocol,
        incremental_handler: IncrementalHandler,
    ) -> Processors:
        """
        Initialize and return the processors for ConnectWise.

        Args:
            fetcher: The initialized ConnectWise data fetcher.
            validator: The data validator (provided by core).
            registry: The initialized ConnectWise registry.
            incremental_handler: Core incremental handler utility.

        Returns:
            Processors container with bronze, silver, and gold processors.
        """
        bronze = ConnectWiseBronzeProcessor(
            fetcher=fetcher,
            validator=validator,
            registry=registry,
        )

        silver = ConnectWiseSilverProcessor(
            registry=registry,
            incremental_handler=incremental_handler,
        )

        gold = ConnectWiseGoldProcessor(
            registry=registry,
        )

        return Processors(bronze=bronze, silver=silver, gold=gold)
