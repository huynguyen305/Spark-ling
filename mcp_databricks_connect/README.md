# Spark-ling MCP Server — Databricks Connect Edition

> **Full Spark computing for AI assistants** via Databricks Connect + MCP.

This is the **second MCP server** in the Spark-ling project. While the original
server (`mcp/`) uses a Databricks SQL warehouse (JDBC), AWS Athena, or local
PySpark to run SQL queries, this server uses **Databricks Connect** to give AI
assistants a full remote SparkSession — enabling DataFrame operations, execution
plan analysis, caching, and more.

---

## Architecture

```
┌────────────────────────┐
│ AI Assistant (Claude…) │
│ ↕ MCP Protocol         │
├────────────────────────┤
│ This MCP Server        │ ← runs locally (or on EC2)
│ FastMCP + tools        │
│ ↕ DatabricksSession    │
├────────────────────────┤
│ Databricks Connect     │ ← gRPC / Spark Connect protocol
│ ↕ Serverless Compute   │
├────────────────────────┤
│ Spark Engine           │ ← runs on Databricks
│ ↕ S3 / Unity Catalog   │
└────────────────────────┘
```

**Key difference**: code runs locally, Spark runs remotely. No local JVM needed.

---

## Comparison: Original vs. Databricks Connect MCP Server

| Feature | `mcp/server.py` (original) | `mcp_databricks_connect/server.py` |
|---------|---------------------------|-------------------------------------|
| Connection | SQL warehouse (JDBC) | DatabricksSession (gRPC) |
| Query type | SQL only | SQL + DataFrame API |
| Backends | Databricks / Athena / S3 | Databricks Connect only |
| Spark engine | SQL warehouse or local | Serverless or cluster-attached |
| Default port | 8080 | 8081 |
| Use case | Multi-cloud data exploration | Spark-native computing & analysis |

---

## Quick Start

### 1. Prerequisites

- Python 3.10+
- Databricks workspace with Unity Catalog
- Personal access token ([how to generate](https://docs.databricks.com/en/dev-tools/auth/pat.html))
- `databricks-connect` matching your DBR version

### 2. Install

```bash
cd ~/sparking_repo/Spark-ling
source .venv/bin/activate

pip install -r mcp_databricks_connect/requirements.txt
```

### 3. Configure

```bash
cp mcp_databricks_connect/.env.example mcp_databricks_connect/.env
# Edit .env with your Databricks host, token, catalog, etc.
```

### 4. Test connection

```bash
python -c "
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.serverless().getOrCreate()
print(f'✅ Spark {spark.version}')
spark.stop()
"
```

### 5. Run the server

```bash
# stdio mode (local IDE integration — recommended)
python mcp_databricks_connect/server.py

# SSE mode (remote access)
MCP_TRANSPORT=sse python mcp_databricks_connect/server.py
```

---

## IDE Integration

### VS Code — Claude / Copilot (MCP settings)

Add to your MCP client configuration:

```json
{
  "mcpServers": {
    "sparkling-spark-engine": {
      "command": "python",
      "args": ["mcp_databricks_connect/server.py"],
      "cwd": "/home/huynguyenle/sparking_repo/Spark-ling",
      "env": {
        "DATABRICKS_HOST": "https://dbc-XXXXXXXX.cloud.databricks.com",
        "DATABRICKS_TOKEN": "dapi..."
      }
    }
  }
}
```

Or if using the `.env` file, just:

```json
{
  "mcpServers": {
    "sparkling-spark-engine": {
      "command": "python",
      "args": ["mcp_databricks_connect/server.py"],
      "cwd": "/home/huynguyenle/sparking_repo/Spark-ling"
    }
  }
}
```

---

## Available Tools

### Catalog Exploration
| Tool | Description |
|------|-------------|
| `list_catalogs` | List all Unity Catalog catalogs |
| `list_schemas` | List schemas in a catalog |
| `list_tables` | List tables in current catalog.schema |
| `describe_table` | Show column names, types, comments |
| `table_detail` | Format, location, size, partitions (DESCRIBE DETAIL) |

### Data Access
| Tool | Description |
|------|-------------|
| `sample_data` | Preview first N rows (max 100) |
| `read_s3_path` | Read Parquet/CSV/JSON/Delta from S3 path |
| `query_sql` | Run read-only Spark SQL (SELECT only) |

### Spark Computing
| Tool | Description |
|------|-------------|
| `dataframe_operation` | Run PySpark DataFrame ops (filter, groupby_agg, orderby, select, distinct, describe, corr) |
| `explain_sql` | Show execution plan without running query |
| `cache_table` | Cache table in Spark memory |
| `uncache_table` | Remove table from cache |
| `list_cached_tables` | Show cached tables |

### Profiling & Health
| Tool | Description |
|------|-------------|
| `get_data_profile` | Row count, distinct/null counts, min/max per column |
| `spark_session_info` | Spark version, catalog, cluster, uptime |
| `server_status` | Full health check |

---

## Example Tool Calls

### SQL query
```
query_sql("SELECT segment, COUNT(*) cnt FROM customers GROUP BY segment ORDER BY cnt DESC")
```

### DataFrame operation — group by with aggregation
```
dataframe_operation(
  table_name="transactions",
  operation="groupby_agg",
  params='{"group_cols": ["channel"], "agg": {"amount": "sum", "txn_id": "count"}}'
)
```

### DataFrame operation — filter
```
dataframe_operation(
  table_name="transactions",
  operation="filter",
  params='{"condition": "amount > 100000000"}'
)
```

### Explain a query
```
explain_sql("SELECT c.segment, AVG(a.balance) FROM customers c JOIN accounts a ON c.customer_id = a.customer_id GROUP BY c.segment")
```

### Read raw S3 files
```
read_s3_path("raw/customers", "parquet")
```

---

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABRICKS_HOST` | ✅ | — | Workspace URL |
| `DATABRICKS_TOKEN` | ✅ | — | Personal access token |
| `DATABRICKS_CLUSTER_ID` | ❌ | *(serverless)* | Cluster ID (omit for serverless) |
| `DATABRICKS_CATALOG` | ❌ | `sparkling` | Default catalog |
| `DATABRICKS_SCHEMA` | ❌ | `banking` | Default schema |
| `S3_BUCKET` | ❌ | — | S3 bucket for relative path reads |
| `MCP_TRANSPORT` | ❌ | `stdio` | `stdio` / `sse` / `streamable-http` |
| `MCP_PORT` | ❌ | `8081` | Port for remote transports |
| `MCP_SERVER_NAME` | ❌ | `sparkling-spark-engine` | Server display name |
| `MCP_LOG_LEVEL` | ❌ | `INFO` | Log verbosity |

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `No module named 'databricks.connect'` | `pip install "databricks-connect>=17.0,<18.0"` |
| `PERMISSION_DENIED` | Check token in `~/.databrickscfg` or `.env` |
| Token expired | Regenerate: Databricks → Settings → Developer → Access tokens |
| `Cluster not found` | Verify `DATABRICKS_CLUSTER_ID` or remove it to use serverless |
| Serverless timeout | First connection may take 30–60s for cold start; retry |
| Version mismatch | Match `databricks-connect` version to your DBR version |
