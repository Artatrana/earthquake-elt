# ============================================================================
# FILE: src/config.py
# ============================================================================
import os
import tomli
from typing import Any, Dict
from pathlib import Path

# Load .env file
def load_env():
    """Load environment variables from .env file."""
    env_path = Path('.env')
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

def load_config(config_path: str = "config/config.toml") -> Dict[str, Any]:
    """Load configuration from TOML file with environment variable substitution."""

    # Load .env first
    load_env()

    with open(config_path, 'rb') as f:
        config = tomli.load(f)

    return _substitute_env_vars(config)

def _substitute_env_vars(obj: Any) -> Any:
    """Recursively substitute environment variables in config."""
    if isinstance(obj, dict):
        return {k: _substitute_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_substitute_env_vars(item) for item in obj]
    elif isinstance(obj, str) and obj.startswith("${") and obj.endswith("}"):
        var_name = obj[2:-1]
        value = os.getenv(var_name)
        if value is None:
            raise ValueError(f"Environment variable {var_name} not found")
        return value
    return obj