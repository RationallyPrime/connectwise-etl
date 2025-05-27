"""ConnectWise entity extractor."""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame
from unified_etl_core.extract.base_extractor import BaseExtractor

from unified_etl_connectwise.api.client import ConnectWiseClient
from unified_etl_connectwise.utils.api_utils import get_fields_for_api_call

logger = logging.getLogger(__name__)


class ConnectWiseExtractor(BaseExtractor):
    """Extractor for ConnectWise PSA data."""

    def __init__(self, config: dict[str, Any]):
        """Initialize ConnectWise extractor.

        Args:
            config: Source configuration containing auth credentials
        """
        super().__init__(config)
        # ConnectWiseClient reads credentials from environment variables
        self.client = ConnectWiseClient()
        self._spark = None

    @property
    def spark(self):
        """Get SparkSession - assumes running in Fabric with global spark."""
        if self._spark is None:
            # In Fabric notebooks, spark is available globally
            import sys

            self._spark = sys.modules["__main__"].spark
        return self._spark

    def get_model_class(self, entity_name: str) -> type:
        """Get Pydantic model class for entity.

        Args:
            entity_name: Name of the entity

        Returns:
            Pydantic model class
        """
        # Dynamic import to avoid importing all models upfront
        import importlib

        models_module = importlib.import_module("unified_etl_connectwise.models")

        # Map entity names to model class names
        model_name_mapping = {
            "Agreement": "Agreement",
            "TimeEntry": "TimeEntry",
            "ExpenseEntry": "ExpenseEntry",
            "ProductItem": "ProductItem",
            "PostedInvoice": "PostedInvoice",
            "UnpostedInvoice": "Invoice",  # Note: UnpostedInvoice uses Invoice model
        }

        model_name = model_name_mapping.get(entity_name)
        if not model_name:
            raise ValueError(
                f"Unknown entity: {entity_name}. Available entities: {list(model_name_mapping.keys())}"
            )

        # Get the model class from the module
        model_class = getattr(models_module, model_name, None)
        if not model_class:
            raise ValueError(f"Model class {model_name} not found in models module")

        return model_class

    def extract(
        self,
        endpoint: str,
        page_size: int = 1000,
        conditions: str | None = None,
        fields: list[str] | None = None,
        **kwargs,
    ) -> DataFrame:
        """Extract data from ConnectWise API.

        Args:
            endpoint: API endpoint (e.g., "/finance/agreements")
            page_size: Number of records per page
            conditions: API query conditions
            fields: Fields to request (if None, derives from model)
            **kwargs: Additional parameters

        Returns:
            Spark DataFrame with extracted data
        """
        # Determine entity name from endpoint
        entity_name = self._get_entity_name(endpoint)

        # Get model class
        model_class = self.get_model_class(entity_name)

        # Generate fields string if not provided
        if fields is None:
            fields_str = get_fields_for_api_call(model_class, max_depth=2)
        else:
            fields_str = ",".join(fields)

        logger.info(f"Extracting {entity_name} from {endpoint}")

        # Use the client's paginate method
        all_data = self.client.paginate(
            endpoint=endpoint,
            entity_name=f"{entity_name}s",  # Pluralize for API logging
            fields=fields_str,
            conditions=conditions,
            page_size=page_size,
            **kwargs,  # Pass through any additional parameters
        )

        logger.info(f"Extracted {len(all_data)} {entity_name} records")

        # Validate data through Pydantic models
        validated_data = []
        for record in all_data:
            try:
                # Pydantic handles datetime parsing automatically!
                model_instance = model_class(**record)
                validated_data.append(model_instance.model_dump())
            except Exception as e:
                logger.warning(f"Validation error for {entity_name} record: {e}")
                # Optionally skip invalid records or handle differently
                continue

        # Create Spark DataFrame using SparkDantic schema
        spark_schema = model_class.model_spark_schema()
        return self.spark.createDataFrame(validated_data, schema=spark_schema)

    def _get_entity_name(self, endpoint: str) -> str:
        """Derive entity name from endpoint.

        Args:
            endpoint: API endpoint

        Returns:
            Entity name
        """
        # Comprehensive endpoint mapping
        endpoint_mapping = {
            "/finance/agreements": "Agreement",
            "/finance/invoices": "UnpostedInvoice",  # Unposted invoices
            "/finance/invoices/posted": "PostedInvoice",  # Posted invoices
            "/time/entries": "TimeEntry",
            "/expense/entries": "ExpenseEntry",
            "/procurement/products": "ProductItem",
        }

        # Try exact match first
        if endpoint in endpoint_mapping:
            return endpoint_mapping[endpoint]

        # Try to extract entity name from endpoint
        # e.g., "/finance/invoices/123" -> "invoice"
        for ep, entity in endpoint_mapping.items():
            if endpoint.startswith(ep):
                return entity

        raise ValueError(f"Cannot determine entity name from endpoint: {endpoint}")
