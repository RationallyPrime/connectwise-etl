"""
Unified ETL Core - The foundation for all ETL operations.

Consolidated architecture following CLAUDE.md principles:
- Bronze: Simple data reading (bronze.py)
- Silver: Pydantic validation + transforms (silver.py)
- Gold: Universal utilities (gold.py)
- Facts: Generic fact creation (facts.py)
- Pipeline: Orchestration (pipeline.py)

Business-specific logic delegated to individual packages.
"""

__version__ = "1.0.0"

# Core consolidated modules
from . import facts, gold, main, silver

__all__ = [
    "facts",
    "gold",
    "main",
    "silver",
]
