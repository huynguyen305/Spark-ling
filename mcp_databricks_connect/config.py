"""
Configuration for the Databricks Connect MCP Server
=====================================================
Loads connection settings from environment variables or .env file.

Required:
  DATABRICKS_HOST   — workspace URL (e.g. https://dbc-xxx.cloud.databricks.com)
  DATABRICKS_TOKEN  — personal access token

Optional:
  DATABRICKS_CLUSTER_ID  — attach to a specific cluster (omit for serverless)
  DATABRICKS_CATALOG     — Unity Catalog catalog (default: sparkling)
  DATABRICKS_SCHEMA      — default schema (default: banking)
  S3_BUCKET              — S3 bucket for raw data reads
  MCP_TRANSPORT          — stdio | sse | streamable-http (default: stdio)
  MCP_PORT               — port for sse/streamable-http (default: 8081)
  MCP_HOST               — bind address (default: 0.0.0.0)
  MCP_LOG_LEVEL          — DEBUG | INFO | WARNING (default: INFO)
  MCP_SERVER_NAME        — server name (default: sparkling-spark-engine)
"""

import os
from pathlib import Path
from typing import Optional

MCP_DIR = Path(__file__).parent
PROJECT_ROOT = MCP_DIR.parent


def _load_env_file() -> dict:
    """Load key=value pairs from mcp_databricks_connect/.env."""
    env_file = MCP_DIR / ".env"
    config: dict[str, str] = {}
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    config[key.strip()] = value.strip()
    return config


_env = _load_env_file()


def get(key: str, default: Optional[str] = None) -> Optional[str]:
    """Get config: env var > .env file > default."""
    return os.environ.get(key, _env.get(key, default))


def get_required(key: str) -> str:
    """Get a required config value, raising if missing."""
    value = get(key)
    if not value:
        raise ValueError(
            f"{key} not set. "
            f"Copy mcp_databricks_connect/.env.example → .env and fill values, "
            f"or set the environment variable."
        )
    return value


# ── Databricks Connect config ───────────────────────────────

def get_spark_connect_config() -> dict:
    """Return config dict for SparkConnectBackend."""
    return {
        "host": get_required("DATABRICKS_HOST"),
        "token": get_required("DATABRICKS_TOKEN"),
        "cluster_id": get("DATABRICKS_CLUSTER_ID"),  # None → serverless
        "catalog": get("DATABRICKS_CATALOG", "sparkling"),
        "schema": get("DATABRICKS_SCHEMA", "banking"),
        "s3_bucket": get("S3_BUCKET", ""),
    }


# ── Server / transport config ───────────────────────────────

def get_server_name() -> str:
    return get("MCP_SERVER_NAME", "sparkling-spark-engine")


def get_transport() -> str:
    return get("MCP_TRANSPORT", "stdio").lower()


def get_port() -> int:
    return int(get("MCP_PORT", "8081"))


def get_host() -> str:
    return get("MCP_HOST", "0.0.0.0")


def get_log_level() -> str:
    return get("MCP_LOG_LEVEL", "INFO").upper()
