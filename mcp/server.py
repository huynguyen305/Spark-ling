"""
Spark-ling MCP Server
======================
Model Context Protocol server for AI-assisted data exploration.

Exposes tools that allow AI assistants (Claude, Gemini, etc.) to:
- List available tables/datasets
- Inspect table schemas
- Sample data
- Run read-only SQL queries
- Get data profiles (stats, nulls, distinct counts)

Supports two backends:
- Databricks SQL warehouse (rich, Unity Catalog-aware)
- S3 direct via local PySpark (cost-effective, no cloud compute needed)

Usage:
    # Start the server (stdio mode for IDE integration)
    python -m mcp.server

    # Or via MCP CLI
    mcp run mcp/server.py
"""

import json
import logging
import sys
from typing import Any

from mcp.server import FastMCP

# Import config and backends
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
from config import (
    get_backend,
    get_databricks_config,
    get_s3_config,
    get_server_name,
    get_log_level,
)

# ── Logging ──────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, get_log_level(), logging.INFO),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("sparkling-mcp")

# ── Initialize MCP server ───────────────────────────────
app = FastMCP(get_server_name())

# ── Backend selection ────────────────────────────────────
_backend = None


def _get_backend():
    """Lazy-initialize the appropriate backend."""
    global _backend
    if _backend is not None:
        return _backend

    mode = get_backend()
    logger.info(f"Initializing MCP backend: {mode}")

    if mode == "databricks":
        from databricks_backend import DatabricksBackend
        _backend = DatabricksBackend(get_databricks_config())
    elif mode == "s3":
        from s3_backend import S3Backend
        _backend = S3Backend(get_s3_config())
    elif mode == "auto":
        # Try Databricks first, fall back to S3
        try:
            config = get_databricks_config()
            from databricks_backend import DatabricksBackend
            _backend = DatabricksBackend(config)
            logger.info("Auto-selected: Databricks backend")
        except (ValueError, ImportError) as e:
            logger.info(f"Databricks not available ({e}), falling back to S3")
            from s3_backend import S3Backend
            _backend = S3Backend(get_s3_config())
            logger.info("Auto-selected: S3 backend")
    else:
        raise ValueError(f"Unknown MCP_BACKEND: {mode}. Use 'databricks', 's3', or 'auto'.")

    return _backend


def _format_results(data: Any) -> str:
    """Format results for MCP response."""
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        # Format as markdown table
        if not data:
            return "No results."
        headers = list(data[0].keys())
        lines = ["| " + " | ".join(headers) + " |"]
        lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
        for row in data:
            values = [str(row.get(h, "")) for h in headers]
            lines.append("| " + " | ".join(values) + " |")
        return "\n".join(lines)
    return json.dumps(data, indent=2, default=str)


# ═══════════════════════════════════════════════════════════
# MCP Tools
# ═══════════════════════════════════════════════════════════


@app.tool()
def list_tables() -> str:
    """
    List all available tables/datasets in the Spark-ling project.
    Returns table names, formats, and paths.
    """
    backend = _get_backend()
    tables = backend.list_tables()
    return _format_results(tables)


@app.tool()
def describe_table(table_name: str) -> str:
    """
    Get the schema (columns and data types) for a specific table.

    Args:
        table_name: Name of the table (e.g., 'customers', 'transactions')
    """
    backend = _get_backend()
    schema = backend.describe_table(table_name)
    return _format_results(schema)


@app.tool()
def sample_data(table_name: str, limit: int = 10) -> str:
    """
    Preview the first N rows of a table.

    Args:
        table_name: Name of the table (e.g., 'customers', 'transactions')
        limit: Number of rows to return (max 100)
    """
    backend = _get_backend()
    data = backend.sample_data(table_name, limit=limit)
    return _format_results(data)


@app.tool()
def query_sql(sql: str) -> str:
    """
    Execute a read-only SQL query against the banking data.
    Only SELECT statements are allowed. DDL/DML is blocked for safety.

    Args:
        sql: SQL query to execute (SELECT only)

    Examples:
        - "SELECT COUNT(*) FROM customers"
        - "SELECT segment, COUNT(*) as cnt FROM customers GROUP BY segment"
        - "SELECT * FROM transactions WHERE amount > 10000 LIMIT 10"
    """
    backend = _get_backend()
    try:
        results = backend.query_sql(sql)
        return _format_results(results)
    except ValueError as e:
        return f"❌ Query rejected: {e}"
    except Exception as e:
        return f"❌ Query error: {e}"


@app.tool()
def get_data_profile(table_name: str) -> str:
    """
    Get a statistical profile of a table: row count, null counts,
    distinct value counts per column.

    Args:
        table_name: Name of the table (e.g., 'customers', 'transactions')
    """
    backend = _get_backend()
    profile = backend.get_data_profile(table_name)
    # Format as readable summary
    lines = [
        f"## Data Profile: {profile['table']}",
        f"- **Row count**: {profile['row_count']:,}",
        f"- **Column count**: {profile['column_count']}",
        "",
        "### Column Statistics",
    ]
    lines.append(_format_results(profile["columns"]))
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════
# MCP Resources (contextual info)
# ═══════════════════════════════════════════════════════════


@app.resource("sparkling://info")
def get_project_info() -> str:
    """Overview of the Spark-ling banking analytics project."""
    return """
# Spark-ling: Banking Analytics Project

## Available Datasets
- **customers** (10,000 rows): Personal info, segment, KYC status
- **accounts** (15,000 rows): Balance, type (Savings/Current/Deposit), status
- **transactions** (500,000+ rows): 1 year of banking transactions
- **branches** (100 rows): Regional branch data

## Key Columns
- Customer segments: Premium, Gold, Silver, Bronze
- Transaction types: Deposit, Withdrawal, Transfer In/Out, Payment, Fee, Interest
- Channels: Branch, ATM, Mobile App, Internet Banking, POS, API
- Regions: Hanoi, Ho Chi Minh, Da Nang, Hai Phong, Can Tho, etc.

## Example Queries
```sql
-- Top customers by transaction volume
SELECT c.name, COUNT(t.txn_id) as txn_count, SUM(t.amount) as total
FROM customers c
JOIN accounts a ON c.customer_id = a.customer_id
JOIN transactions t ON a.account_id = t.account_id
GROUP BY c.name ORDER BY total DESC LIMIT 10

-- Segment distribution
SELECT segment, COUNT(*) as count FROM customers GROUP BY segment

-- Monthly transaction trends
SELECT DATE_FORMAT(txn_datetime, 'yyyy-MM') as month, COUNT(*) as txn_count
FROM transactions GROUP BY month ORDER BY month
```
"""


# ═══════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    import os
    transport = os.environ.get("MCP_TRANSPORT", "stdio").lower()
    port = int(os.environ.get("MCP_PORT", "8080"))

    logger.info(f"Starting {get_server_name()} MCP server (transport={transport})...")

    if transport == "sse":
        # SSE transport for remote access (EC2 deployment)
        app.run(transport="sse", host="0.0.0.0", port=port)
    else:
        # stdio transport for local IDE integration
        app.run(transport="stdio")
