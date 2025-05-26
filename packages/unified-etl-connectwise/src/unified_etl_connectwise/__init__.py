"""ConnectWise PSA adapter for Unified ETL Framework."""

from .api.client import ConnectWiseClient
from .extract.extractor import ConnectWiseExtractor

__version__ = "1.0.0"

__all__ = [
    "ConnectWiseClient",
    "ConnectWiseExtractor",
]
