"""ConnectWise entity extractor."""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
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
        self.client = ConnectWiseClient(
            base_url=config["base_url"],
            company=config["auth"]["credentials"]["company"],
            public_key=config["auth"]["credentials"]["public_key"],
            private_key=config["auth"]["credentials"]["private_key"],
        )
        self._spark = None
        
    @property
    def spark(self) -> SparkSession:
        """Get or create SparkSession."""
        if self._spark is None:
            self._spark = SparkSession.builder.appName("ConnectWiseExtractor").getOrCreate()
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
        
        # Paginate through API results
        all_data = []
        page = 1
        
        while True:
            response = self.client.get(
                endpoint=endpoint,
                params={
                    "pageSize": page_size,
                    "page": page,
                    "fields": fields_str,
                    "conditions": conditions,
                }
            )
            
            if response.status_code != 200:
                raise Exception(f"API error: {response.status_code} - {response.text}")
                
            data = response.json()
            if not data:
                break
                
            all_data.extend(data)
            
            # Check if we got a full page
            if len(data) < page_size:
                break
                
            page += 1
            
        logger.info(f"Extracted {len(all_data)} {entity_name} records")
        
        # Convert to pandas then Spark DataFrame
        if all_data:
            df = pd.DataFrame(all_data)
            return self._pandas_to_spark(df, self.spark)
        else:
            # Return empty DataFrame with correct schema
            return self.spark.createDataFrame([], model_class.spark_schema())
            
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