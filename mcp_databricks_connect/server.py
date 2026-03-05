"""
Spark-ling MCP Server — Databricks Connect Edition
====================================================
A Model Context Protocol server backed by Databricks Connect,
giving AI assistants full Spark computing power on remote
serverless (or cluster-attached) compute.

Compared to the original MCP server (mcp/server.py):
  ┌──────────────────────────┬──────────────────────────────────┐
  │ mcp/server.py            │ mcp_databricks_connect/server.py │
  ├──────────────────────────┼──────────────────────────────────┤
  │ SQL warehouse (JDBC)     │ DatabricksSession (gRPC)         │
  │ SQL only                 │ SQL + full DataFrame API         │
  │ Multi-backend (s3/athena)│ Databricks-only, Spark-native    │
  │ Port 8080                │ Port 8081                        │
  └──────────────────────────┴──────────────────────────────────┘

Tools exposed:
  ● list_tables / describe_table / table_detail
  ● sample_data / query_sql / explain_sql
  ● dataframe_operation (filter, groupby, orderby, select, …)
  ● read_s3_path (ad-hoc Parquet/CSV/JSON/Delta reads)
  ● get_data_profile (comprehensive column statistics)
  ● cache_table / uncache_table / list_cached_tables
  ● list_catalogs / list_schemas
  ● spark_session_info / server_status

Usage:
    # Local (stdio — IDE integration, default)
    python mcp_databricks_connect/server.py

    # Remote (SSE — team-shared endpoint)
    MCP_TRANSPORT=sse python mcp_databricks_connect/server.py
"""

import json
import logging
import sys
from typing import Any

from mcp.server import FastMCP

# Ensure our package is importable
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
from config import (
    get_spark_connect_config,
    get_server_name,
    get_log_level,
    get_transport,
    get_port,
    get_host,
)

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, get_log_level(), logging.INFO),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("sparkling-spark-connect")

# ── MCP app ──────────────────────────────────────────────────
app = FastMCP(get_server_name(), host=get_host(), port=get_port())

# ── Backend singleton ────────────────────────────────────────
_backend = None


def _get_backend():
    global _backend
    if _backend is None:
        from spark_connect_backend import SparkConnectBackend

        config = get_spark_connect_config()
        logger.info(
            f"Initialising SparkConnectBackend "
            f"(host={config['host']}, cluster={config.get('cluster_id', 'serverless')})"
        )
        _backend = SparkConnectBackend(config)
    return _backend


def _fmt(data: Any) -> str:
    """Render data as a Markdown table (list[dict]) or JSON."""
    if isinstance(data, list) and data and isinstance(data[0], dict):
        headers = list(data[0].keys())
        lines = [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join(["---"] * len(headers)) + " |",
        ]
        for row in data:
            vals = [str(row.get(h, "")) for h in headers]
            lines.append("| " + " | ".join(vals) + " |")
        return "\n".join(lines)
    return json.dumps(data, indent=2, default=str)


# ═════════════════════════════════════════════════════════════
# MCP Tools — Catalog exploration
# ═════════════════════════════════════════════════════════════


@app.tool()
def list_catalogs() -> str:
    """
    List all Unity Catalog catalogs accessible from this workspace.
    """
    return _fmt(_get_backend().list_catalogs())


@app.tool()
def list_schemas(catalog: str | None = None) -> str:
    """
    List schemas in a catalog (defaults to the configured catalog).

    Args:
        catalog: Optional catalog name. Uses default if omitted.
    """
    return _fmt(_get_backend().list_schemas(catalog))


@app.tool()
def list_tables() -> str:
    """
    List all tables in the current catalog.schema.
    Returns fully qualified names.
    """
    return _fmt(_get_backend().list_tables())


@app.tool()
def describe_table(table_name: str) -> str:
    """
    Show the schema (columns, types, comments) for a table.

    Args:
        table_name: Short name (e.g. 'customers') or fully-qualified name.
    """
    return _fmt(_get_backend().describe_table(table_name))


@app.tool()
def table_detail(table_name: str) -> str:
    """
    Get detailed metadata for a table: format, location, size,
    partition columns, table properties (Delta / Parquet).

    Args:
        table_name: Table name.
    """
    detail = _get_backend().table_detail(table_name)
    return _fmt([detail]) if detail else "No detail available."


# ═════════════════════════════════════════════════════════════
# MCP Tools — Data reading
# ═════════════════════════════════════════════════════════════


@app.tool()
def sample_data(table_name: str, limit: int = 10) -> str:
    """
    Preview the first N rows of a table (max 100).

    Args:
        table_name: Table name.
        limit: Number of rows (default 10, max 100).
    """
    return _fmt(_get_backend().sample_data(table_name, limit))


@app.tool()
def read_s3_path(path: str, fmt: str = "parquet") -> str:
    """
    Read data from an S3 path and return schema + 20-row preview.
    Useful for exploring raw files not yet registered as tables.

    Args:
        path: S3 path (s3a://bucket/prefix) or relative path like 'raw/customers'.
        fmt:  Format — parquet | csv | json | delta.
    """
    result = _get_backend().read_s3_path(path, fmt)
    lines = [
        f"## S3 Read: `{result['path']}` ({result['format']})",
        "",
        "### Schema",
        _fmt(result["schema"]),
        "",
        "### Preview (up to 20 rows)",
        _fmt(result["preview_rows"]),
    ]
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════
# MCP Tools — SQL & DataFrame operations
# ═════════════════════════════════════════════════════════════


@app.tool()
def query_sql(sql: str) -> str:
    """
    Execute a read-only Spark SQL query.
    Runs on Databricks remote compute. DDL/DML is blocked.

    Args:
        sql: SELECT query.

    Examples:
        - "SELECT COUNT(*) FROM customers"
        - "SELECT segment, COUNT(*) cnt FROM customers GROUP BY segment"
        - "SELECT * FROM transactions WHERE amount > 1000000 LIMIT 10"
    """
    try:
        return _fmt(_get_backend().query_sql(sql))
    except ValueError as e:
        return f"❌ Query rejected: {e}"
    except Exception as e:
        return f"❌ Query error: {e}"


@app.tool()
def explain_sql(sql: str, mode: str = "extended") -> str:
    """
    Show the Spark execution plan for a query without running it.
    Helpful for performance analysis and query tuning.

    Args:
        sql:  The SQL query to explain (SELECT only).
        mode: Plan detail — simple | extended | codegen | cost | formatted.
    """
    try:
        plan = _get_backend().explain_sql(sql, mode)
        return f"```\n{plan}\n```"
    except ValueError as e:
        return f"❌ Rejected: {e}"
    except Exception as e:
        return f"❌ Error: {e}"


@app.tool()
def dataframe_operation(
    table_name: str,
    operation: str,
    params: str = "{}",
) -> str:
    """
    Run a PySpark DataFrame operation on a table using remote Spark compute.

    Args:
        table_name: Table name.
        operation:  One of: filter, groupby_agg, orderby, select, distinct, describe, corr.
        params:     JSON string with operation parameters.

    Operation examples:
        filter      → {"condition": "amount > 1000000"}
        groupby_agg → {"group_cols": ["segment"], "agg": {"amount": "sum", "customer_id": "count"}}
        orderby     → {"columns": ["amount"], "ascending": false}
        select      → {"columns": ["customer_id", "name", "segment"]}
        distinct    → {"columns": ["segment"]}
        describe    → {}
        corr        → {"col1": "balance", "col2": "amount"}
    """
    try:
        parsed = json.loads(params) if isinstance(params, str) else params
        result = _get_backend().dataframe_operation(table_name, operation, parsed)
        return _fmt(result)
    except (ValueError, json.JSONDecodeError) as e:
        return f"❌ Invalid operation: {e}"
    except Exception as e:
        return f"❌ Error: {e}"


# ═════════════════════════════════════════════════════════════
# MCP Tools — Profiling & performance
# ═════════════════════════════════════════════════════════════


@app.tool()
def get_data_profile(table_name: str) -> str:
    """
    Produce a comprehensive data profile: row count, column-level
    distinct counts, null counts, min/max values.

    Args:
        table_name: Table name.
    """
    profile = _get_backend().get_data_profile(table_name)
    lines = [
        f"## Data Profile: {profile['table']}",
        f"- **Row count**: {profile['row_count']:,}",
        f"- **Column count**: {profile['column_count']}",
        "",
        "### Column Statistics",
        _fmt(profile["columns"]),
    ]
    return "\n".join(lines)


@app.tool()
def cache_table(table_name: str) -> str:
    """
    Cache a table in Spark memory for faster repeated access.
    Useful when you plan to run many queries against the same table.

    Args:
        table_name: Table name.
    """
    return _get_backend().cache_table(table_name)


@app.tool()
def uncache_table(table_name: str) -> str:
    """
    Remove a table from Spark cache to free memory.

    Args:
        table_name: Table name.
    """
    return _get_backend().uncache_table(table_name)


@app.tool()
def list_cached_tables() -> str:
    """
    List all tables currently cached in Spark memory.
    """
    cached = _get_backend().list_cached_tables()
    if not cached:
        return "No tables are currently cached."
    return "\n".join(f"- {t}" for t in cached)


# ═════════════════════════════════════════════════════════════
# MCP Tools — Session & health
# ═════════════════════════════════════════════════════════════


@app.tool()
def spark_session_info() -> str:
    """
    Show Spark session details: version, catalog, schema, cluster mode, uptime.
    """
    info = _get_backend().session_info()
    lines = ["## Spark Session Info"]
    for k, v in info.items():
        lines.append(f"- **{k}**: {v}")
    return "\n".join(lines)


@app.tool()
def server_status() -> str:
    """
    Health check: shows server config, backend connectivity, and table count.
    """
    import platform

    transport = get_transport()
    status = {
        "server": get_server_name(),
        "status": "healthy",
        "backend": "databricks-connect",
        "transport": transport,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
    }

    try:
        backend = _get_backend()
        info = backend.session_info()
        status["spark_version"] = info["spark_version"]
        status["cluster"] = info["cluster_id"]
        tables = backend.list_tables()
        status["tables_available"] = len(tables)
        status["backend_status"] = "connected"
    except Exception as e:
        status["backend_status"] = f"error: {e}"
        status["tables_available"] = 0

    lines = ["## MCP Spark-Connect Server Status"]
    for k, v in status.items():
        lines.append(f"- **{k}**: {v}")
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════
# MCP Resources
# ═════════════════════════════════════════════════════════════


@app.resource("sparkling://spark-connect/info")
def project_info() -> str:
    """Overview of the Spark-ling project and Databricks Connect MCP server."""
    return """
# Spark-ling: Databricks Connect MCP Server

## What is this?
An MCP server that gives AI assistants **full Spark computing power**
via Databricks Connect. Unlike the SQL-warehouse-based server, this one uses
a remote DatabricksSession — meaning you can run arbitrary PySpark
DataFrame operations, not just SQL.

## Available Datasets (catalog: sparkling, schema: banking)
- **customers** (10,000 rows): Personal info, segment, KYC status
- **accounts** (15,000 rows): Balance, type, status
- **transactions** (500,000+ rows): 1 year of banking transactions
- **branches** (100 rows): Regional branch data

## Capabilities
- **SQL queries**: Full Spark SQL (read-only)
- **DataFrame ops**: filter, groupBy, agg, orderBy, select, distinct, describe, corr
- **Execution plans**: Explain queries without running them
- **S3 reads**: Read Parquet/CSV/JSON/Delta from any S3 path
- **Caching**: Cache/uncache tables in Spark memory
- **Profiling**: Row counts, distinct counts, null analysis, min/max
- **Unity Catalog**: Browse catalogs and schemas

## Example Queries
```sql
-- Transaction volume by channel
SELECT channel, COUNT(*) cnt, SUM(amount) total
FROM transactions GROUP BY channel ORDER BY total DESC

-- Customer segment distribution
SELECT segment, COUNT(*) FROM customers GROUP BY segment
```

## Example DataFrame Operations
```json
{"operation": "groupby_agg",
 "params": {"group_cols": ["segment"],
            "agg": {"customer_id": "count", "balance": "avg"}}}
```
"""


@app.resource("sparkling://spark-connect/architecture")
def architecture_info() -> str:
    """Architecture and data flow for the Databricks Connect MCP server."""
    return """
# Architecture: Databricks Connect MCP Server

```
┌─────────────────────────────┐
│  AI Assistant (Claude, etc.)│
│  ↕  MCP Protocol (stdio)   │
├─────────────────────────────┤
│  MCP Server (this process)  │
│  FastMCP + tool handlers    │
│  ↕  DatabricksSession       │
├─────────────────────────────┤
│  Databricks Connect (gRPC)  │
│  ↕  Spark Connect Protocol  │
├─────────────────────────────┤
│  Databricks Serverless      │
│  Spark Engine (remote)      │
│  ↕  S3 / Unity Catalog      │
├─────────────────────────────┤
│  S3: s3://sparkling-data    │
│  Delta / Parquet / CSV      │
└─────────────────────────────┘
```

## Key Points
- Code runs **locally** (MCP server process)
- Spark engine runs **on Databricks** (serverless or attached cluster)
- Data lives in **S3** (accessed via Unity Catalog external locations)
- Connection is via **gRPC** (Spark Connect protocol), not JDBC
- No local JVM or Spark installation needed
"""


# ═════════════════════════════════════════════════════════════
# Entry point
# ═════════════════════════════════════════════════════════════

if __name__ == "__main__":
    transport = get_transport()

    logger.info(
        f"Starting {get_server_name()} "
        f"(transport={transport}, port={get_port()})…"
    )

    if transport == "sse":
        app.run(transport="sse")
    elif transport == "streamable-http":
        app.run(transport="streamable-http")
    else:
        app.run(transport="stdio")
