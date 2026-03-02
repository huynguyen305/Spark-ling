"""
Databricks Backend for MCP Server
===================================
Connects to a Databricks SQL warehouse to execute queries and explore data.
Uses the Databricks SQL Connector for Python.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class DatabricksBackend:
    """Backend that queries data via Databricks SQL warehouse."""

    def __init__(self, config: dict):
        """
        Initialize Databricks backend.

        Args:
            config: Dict with keys: host, token, warehouse_id, catalog, schema
        """
        self.host = config["host"]
        self.token = config["token"]
        self.warehouse_id = config["warehouse_id"]
        self.catalog = config["catalog"]
        self.schema = config["schema"]
        self._connection = None

    def _get_connection(self):
        """Lazy-initialize Databricks SQL connection."""
        if self._connection is None:
            from databricks import sql as dbsql

            self.host_clean = self.host.replace("https://", "").rstrip("/")
            self._connection = dbsql.connect(
                server_hostname=self.host_clean,
                http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
                access_token=self.token,
            )
            logger.info(f"Connected to Databricks: {self.host_clean}")
        return self._connection

    def _execute(self, sql: str) -> list[dict[str, Any]]:
        """Execute SQL and return results as list of dicts."""
        conn = self._get_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]

    def list_tables(self) -> list[dict[str, str]]:
        """List all tables in the configured catalog.schema."""
        sql = f"SHOW TABLES IN {self.catalog}.{self.schema}"
        results = self._execute(sql)
        tables = []
        for row in results:
            table_name = row.get("tableName", row.get("name", ""))
            tables.append({
                "name": f"{self.catalog}.{self.schema}.{table_name}",
                "short_name": table_name,
            })
        return tables

    def describe_table(self, table_name: str) -> list[dict[str, str]]:
        """Get schema (columns, types) for a table."""
        full_name = self._resolve_table_name(table_name)
        sql = f"DESCRIBE TABLE {full_name}"
        results = self._execute(sql)
        return [
            {
                "column": row.get("col_name", ""),
                "type": row.get("data_type", ""),
                "comment": row.get("comment", ""),
            }
            for row in results
            if row.get("col_name", "").strip() and not row.get("col_name", "").startswith("#")
        ]

    def sample_data(self, table_name: str, limit: int = 10) -> list[dict]:
        """Return first N rows from a table."""
        full_name = self._resolve_table_name(table_name)
        limit = min(limit, 100)  # Cap at 100 for safety
        sql = f"SELECT * FROM {full_name} LIMIT {limit}"
        return self._execute(sql)

    def query_sql(self, sql: str) -> list[dict]:
        """
        Execute a read-only SQL query.
        Rejects DDL/DML statements for safety.
        """
        sql_upper = sql.strip().upper()
        blocked = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE", "MERGE"]
        for keyword in blocked:
            if sql_upper.startswith(keyword):
                raise ValueError(f"Only read-only queries are allowed. '{keyword}' is not permitted.")
        return self._execute(sql)

    def get_data_profile(self, table_name: str) -> dict:
        """Get basic statistics for a table."""
        full_name = self._resolve_table_name(table_name)

        # Row count
        count_result = self._execute(f"SELECT COUNT(*) as row_count FROM {full_name}")
        row_count = count_result[0]["row_count"] if count_result else 0

        # Column stats
        columns = self.describe_table(table_name)
        col_stats = []
        for col in columns[:20]:  # Limit to first 20 columns
            col_name = col["column"]
            try:
                stats = self._execute(f"""
                    SELECT
                        COUNT(DISTINCT `{col_name}`) as distinct_count,
                        COUNT(*) - COUNT(`{col_name}`) as null_count
                    FROM {full_name}
                """)
                col_stats.append({
                    "column": col_name,
                    "type": col["type"],
                    "distinct_count": stats[0]["distinct_count"] if stats else None,
                    "null_count": stats[0]["null_count"] if stats else None,
                })
            except Exception as e:
                col_stats.append({
                    "column": col_name,
                    "type": col["type"],
                    "error": str(e),
                })

        return {
            "table": full_name,
            "row_count": row_count,
            "column_count": len(columns),
            "columns": col_stats,
        }

    def _resolve_table_name(self, name: str) -> str:
        """Resolve short table name to fully qualified name."""
        if "." not in name:
            return f"{self.catalog}.{self.schema}.{name}"
        return name

    def close(self):
        """Close the connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
