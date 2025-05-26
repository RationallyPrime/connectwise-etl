"""Base extractor class for all data sources."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Base class for all source extractors."""
    
    def __init__(self, config: dict[str, Any]):
        """Initialize extractor with source configuration.
        
        Args:
            config: Source configuration from pipeline config
        """
        self.config = config
        
    @abstractmethod
    def extract(self, endpoint: str, **kwargs) -> DataFrame:
        """Extract data from source.
        
        Args:
            endpoint: API endpoint or table name
            **kwargs: Additional parameters specific to the source
            
        Returns:
            Spark DataFrame with extracted data
        """
        pass
        
    @abstractmethod
    def get_model_class(self, entity_name: str) -> type:
        """Get Pydantic model class for entity.
        
        Args:
            entity_name: Name of the entity
            
        Returns:
            Pydantic model class
        """
        pass
        
    def _pandas_to_spark(self, df: pd.DataFrame, spark: SparkSession) -> DataFrame:
        """Convert pandas DataFrame to Spark DataFrame.
        
        Args:
            df: Pandas DataFrame
            spark: SparkSession
            
        Returns:
            Spark DataFrame
        """
        return spark.createDataFrame(df)