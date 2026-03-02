"""
MCP Configuration Loader
========================
Loads configuration for the MCP server from .env file or environment variables.
"""

import os
from pathlib import Path
from typing import Optional


MCP_DIR = Path(__file__).parent
PROJECT_ROOT = MCP_DIR.parent


def _load_env_file() -> dict:
    """Load key-value pairs from mcp/.env file."""
    env_file = MCP_DIR / ".env"
    config = {}
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    config[key.strip()] = value.strip()
    return config


# Load .env file on import
_env = _load_env_file()


def get(key: str, default: Optional[str] = None) -> Optional[str]:
    """Get config value from environment first, then .env file."""
    return os.environ.get(key, _env.get(key, default))


def get_required(key: str) -> str:
    """Get a required config value, raising error if not found."""
    value = get(key)
    if not value:
        raise ValueError(
            f"{key} not set. Copy mcp/.env.example to mcp/.env and fill in your values."
        )
    return value


# ── Derived config ──────────────────────────────────────
def get_backend() -> str:
    """Get the configured MCP backend: 'databricks', 's3', or 'auto'."""
    return get("MCP_BACKEND", "auto").lower()


def get_databricks_config() -> dict:
    """Get Databricks connection config."""
    return {
        "host": get_required("DATABRICKS_HOST"),
        "token": get_required("DATABRICKS_TOKEN"),
        "warehouse_id": get_required("DATABRICKS_WAREHOUSE_ID"),
        "catalog": get("DATABRICKS_CATALOG", "sparkling"),
        "schema": get("DATABRICKS_SCHEMA", "banking"),
    }


def get_s3_config() -> dict:
    """Get S3 connection config."""
    return {
        "bucket": get_required("S3_BUCKET"),
        "region": get("AWS_REGION", "ap-southeast-1"),
        "data_prefix": "data",
    }


def get_server_name() -> str:
    """Get the MCP server name."""
    return get("MCP_SERVER_NAME", "sparkling-data-explorer")


def get_log_level() -> str:
    """Get the log level."""
    return get("MCP_LOG_LEVEL", "INFO")
