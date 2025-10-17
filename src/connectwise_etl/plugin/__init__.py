"""ConnectWise integration plugin implementing etl_core protocols."""

from .fetcher import ConnectWiseFetcher
from .plugin import ConnectWisePlugin
from .registry import ConnectWiseRegistry

__all__ = ["ConnectWisePlugin", "ConnectWiseFetcher", "ConnectWiseRegistry"]
