"""ConnectWise model registry implementing ModelRegistryProtocol."""

from etl_core.domain.protocols import TConfigDict, TLayer, TSparkModelClass

from ..models import Agreement, Company, ExpenseEntry, Invoice, Member, ProductItem, TimeEntry


class ConnectWiseRegistry:
    """
    ConnectWise model registry implementing ModelRegistryProtocol.

    Provides access to SparkDantic models and operational metadata for all
    ConnectWise entities.
    """

    # Entity name to model class mapping
    _models: dict[str, TSparkModelClass] = {  # type: ignore[assignment]
        "agreement": Agreement,
        "timeentry": TimeEntry,
        "expenseentry": ExpenseEntry,
        "productitem": ProductItem,
        "invoice": Invoice,
        "member": Member,
        "company": Company,
    }

    # Entity configurations with endpoints and field mappings
    _entity_configs: dict[str, TConfigDict] = {
        "agreement": {
            "endpoint": "/finance/agreements",
            "fields": None,  # Use model introspection
        },
        "timeentry": {
            "endpoint": "/time/entries",
            "fields": None,
        },
        "expenseentry": {
            "endpoint": "/expense/entries",
            "fields": None,
        },
        "productitem": {
            "endpoint": "/procurement/products",
            "fields": None,
        },
        "invoice": {
            "endpoint": "/finance/invoices",
            "fields": None,
        },
        "member": {
            "endpoint": "/system/members",
            "fields": None,  # Member has too many fields, let API return all
        },
        "company": {
            "endpoint": "/company/companies",
            "fields": None,
        },
    }

    # Merge keys for each entity (for incremental loads)
    _merge_keys: dict[str, list[str]] = {
        "agreement": ["id"],
        "timeentry": ["id"],
        "expenseentry": ["id"],
        "productitem": ["id"],
        "invoice": ["id"],
        "member": ["id"],
        "company": ["id"],
    }

    # Timestamp columns by layer
    _timestamp_columns: dict[str, dict[TLayer, str]] = {
        "agreement": {
            "bronze": "etl_timestamp",
            "silver": "_etl_processed_at",
            "gold": "_etl_processed_at",
        },
        "timeentry": {
            "bronze": "etl_timestamp",
            "silver": "_etl_processed_at",
            "gold": "_etl_processed_at",
        },
        "expenseentry": {
            "bronze": "etl_timestamp",
            "silver": "_etl_processed_at",
            "gold": "_etl_processed_at",
        },
        "productitem": {
            "bronze": "etl_timestamp",
            "silver": "_etl_processed_at",
            "gold": "_etl_processed_at",
        },
        "invoice": {
            "bronze": "etl_timestamp",
            "silver": "_etl_processed_at",
            "gold": "_etl_processed_at",
        },
        "member": {
            "bronze": "etl_timestamp",
            "silver": "_etl_processed_at",
            "gold": "_etl_processed_at",
        },
        "company": {
            "bronze": "etl_timestamp",
            "silver": "_etl_processed_at",
            "gold": "_etl_processed_at",
        },
    }

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
        if entity_name not in self._models:
            raise KeyError(
                f"Unknown entity: {entity_name}. Available: {list(self._models.keys())}"
            )
        return self._models[entity_name]

    def list_entities(self) -> list[str]:
        """
        List all available entity names.

        Returns:
            List of entity names (e.g., ["agreement", "timeentry", "invoice"]).
        """
        return list(self._models.keys())

    def get_entity_config(self, entity_name: str) -> TConfigDict:
        """
        Retrieve specific configuration for an entity.

        Args:
            entity_name: Name of the entity.

        Returns:
            Configuration dictionary with endpoint, fields, etc.

        Raises:
            KeyError: If the entity name is not found.
        """
        if entity_name not in self._entity_configs:
            raise KeyError(
                f"Unknown entity: {entity_name}. Available: {list(self._entity_configs.keys())}"
            )
        return self._entity_configs[entity_name].copy()

    def get_merge_keys(self, entity_name: str) -> list[str]:
        """
        Get the business keys used for merge operations.

        Args:
            entity_name: Name of the entity.

        Returns:
            List of column names that form the merge key.

        Raises:
            KeyError: If the entity name is not found.
        """
        if entity_name not in self._merge_keys:
            raise KeyError(
                f"Unknown entity: {entity_name}. Available: {list(self._merge_keys.keys())}"
            )
        return self._merge_keys[entity_name].copy()

    def get_timestamp_column(self, entity_name: str, layer: TLayer) -> str:
        """
        Get the timestamp column for a specific layer.

        Args:
            entity_name: Name of the entity.
            layer: Medallion layer (bronze, silver, gold).

        Returns:
            Name of the timestamp column for watermarking.

        Raises:
            KeyError: If the entity name is not found.
        """
        if entity_name not in self._timestamp_columns:
            raise KeyError(
                f"Unknown entity: {entity_name}. Available: {list(self._timestamp_columns.keys())}"
            )
        return self._timestamp_columns[entity_name][layer]
