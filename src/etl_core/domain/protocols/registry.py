"""Protocol for model registry and entity metadata."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .types import TConfigDict, TLayer, TSparkModelClass


@runtime_checkable
class ModelRegistryProtocol(Protocol):
    """
    Protocol for accessing integration-specific models and entity metadata.

    This allows the core framework to look up models and operational metadata
    without hard-coding entity-specific logic.

    Responsibilities:
    - Provide SparkDantic model classes for entities
    - Expose entity-specific configuration (endpoints, transformations)
    - Define merge keys for incremental processing
    - Specify timestamp columns for watermarking
    """

    def get_model(self, entity_name: str) -> TSparkModelClass:
        """
        Retrieve the SparkDantic model class for a given entity.

        Args:
            entity_name: Name of the entity (e.g., "agreement", "timeentry").

        Returns:
            SparkDantic model class for validation and schema.

        Raises:
            KeyError: If the entity name is not found.
        """
        ...

    def list_entities(self) -> list[str]:
        """
        List all available entity names managed by this integration.

        Returns:
            List of entity names (e.g., ["agreement", "timeentry", "invoice"]).
        """
        ...

    def get_entity_config(self, entity_name: str) -> TConfigDict:
        """
        Retrieve specific configuration for an entity.

        This includes integration-specific settings like:
        - API endpoints
        - Field mappings
        - Transformation rules
        - Business logic parameters

        Args:
            entity_name: Name of the entity.

        Returns:
            Configuration dictionary for the entity.

        Raises:
            KeyError: If the entity name is not found.
        """
        ...

    def get_merge_keys(self, entity_name: str) -> list[str]:
        """
        Get the business keys used for merge operations on this entity.

        These keys identify unique records during incremental loads.
        Typically includes natural keys like ["id"] or composite keys.

        Args:
            entity_name: Name of the entity.

        Returns:
            List of column names that form the merge key.

        Raises:
            KeyError: If the entity name is not found.
        """
        ...

    def get_timestamp_column(self, entity_name: str, layer: TLayer) -> str:
        """
        Get the timestamp column used for watermarking in a specific layer.

        Different layers may use different timestamp columns:
        - Bronze: "etl_timestamp" (extraction time)
        - Silver/Gold: "_etl_processed_at" (processing time)

        Args:
            entity_name: Name of the entity.
            layer: Medallion layer (bronze, silver, gold).

        Returns:
            Name of the timestamp column for watermarking.

        Raises:
            KeyError: If the entity name is not found.
        """
        ...
