"""ConnectWise data fetcher implementing DataFetcherProtocol."""

from typing import Any, Iterator

from etl_core.domain.protocols import TConfigDict, TLoadMode
from etl_core.utils.errors import FetchError

from ..client import ConnectWiseClient


class ConnectWiseFetcher:
    """
    ConnectWise data fetcher implementing DataFetcherProtocol.

    Adapts the existing ConnectWiseClient to the protocol interface.
    """

    def __init__(self):
        """Initialize the fetcher with ConnectWise client."""
        self.client = ConnectWiseClient()

    def fetch_raw(
        self,
        entity_name: str,
        mode: TLoadMode,
        config: TConfigDict,
        **kwargs: Any,
    ) -> Iterator[dict[str, Any]]:
        """
        Fetch raw records from ConnectWise API.

        Args:
            entity_name: Name of the entity to fetch.
            mode: Load mode (full, incremental, append).
            config: Entity-specific configuration with 'endpoint' key.
            **kwargs: Additional parameters (page_size, lookback_days, etc.).

        Returns:
            Iterator of raw dictionaries (unvalidated).

        Raises:
            FetchError: If data extraction fails.
        """
        try:
            # Get endpoint from config
            endpoint = config.get("endpoint")
            if not endpoint:
                raise FetchError(
                    f"No endpoint configured for entity: {entity_name}",
                    details={
                        "source": "ConnectWiseFetcher",
                        "operation": "fetch_raw",
                        "entity_name": entity_name,
                    },
                )

            # Get optional parameters
            page_size = kwargs.get("page_size", 1000)
            conditions = None

            # Handle incremental mode
            if mode == "incremental":
                lookback_days = kwargs.get("lookback_days", 7)
                # Build incremental conditions
                from datetime import datetime, timedelta

                since_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
                # ConnectWise uses lastUpdated for most entities
                conditions = f"lastUpdated > [{since_date}]"

            # Get fields from config if specified
            fields_str = config.get("fields")

            # Use existing client's paginate method
            records = self.client.paginate(
                endpoint=endpoint,
                entity_name=entity_name,
                fields=fields_str,
                conditions=conditions,
                page_size=page_size,
            )

            # Convert list to iterator
            return iter(records)

        except Exception as e:
            raise FetchError(
                f"Failed to fetch {entity_name} from ConnectWise: {e}",
                details={
                    "source": "ConnectWiseFetcher",
                    "operation": "fetch_raw",
                    "entity_name": entity_name,
                    "endpoint": config.get("endpoint"),
                },
            ) from e

    def test_connection(self) -> bool:
        """
        Verify connectivity with ConnectWise API.

        Returns:
            True if connection successful, False otherwise.
        """
        try:
            # Simple test: fetch 1 record from system info
            self.client.get("/system/info")
            return True
        except Exception:
            return False

    def close(self) -> None:
        """
        Clean up resources (close HTTP session).

        The requests.Session will be closed when the client is garbage collected.
        """
        if hasattr(self.client, "session"):
            self.client.session.close()
