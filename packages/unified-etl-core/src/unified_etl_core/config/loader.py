"""Configuration loader."""
import os
from pathlib import Path

import yaml

from .models import PipelineConfig


def expand_env_vars(obj: any) -> any:
    """Recursively expand environment variables in strings."""
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    elif isinstance(obj, dict):
        return {k: expand_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [expand_env_vars(item) for item in obj]
    return obj


def load_config(config_path: str | Path) -> PipelineConfig:
    """Load pipeline configuration from YAML file.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        Loaded and validated configuration
    """
    config_path = Path(config_path).expanduser()
    
    with open(config_path) as f:
        raw_config = yaml.safe_load(f)
    
    # Expand environment variables
    expanded_config = expand_env_vars(raw_config)
    
    return PipelineConfig(**expanded_config)