"""
Databricks Connect Backend for Spark-ling MCP Server
=====================================================
Uses databricks-connect SDK to obtain a full remote SparkSession
(DatabricksSession) that runs Spark jobs on Databricks serverless
compute while code is authored locally.

Key difference from the SQL-warehouse backend:
  - SQL warehouse → JDBC/ODBC, SQL-only, fixed warehouse
  - Databricks Connect → gRPC, full DataFrame/SparkSQL, serverless compute

Capabilities exposed:
  - Spark SQL queries (read-only)
  - DataFrame operations (filter, groupBy, agg, join)
  - Schema inspection via Spark catalog
  - Execution plan analysis (explain)
  - Table caching / uncaching
  - Data profiling via Spark aggregations
  - Read from S3, Unity Catalog tables, Delta Lake
"""

import logging
import time
from typing import Any, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# Safety: blocked DDL/DML keywords for read-only enforcement
_BLOCKED_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
    "CREATE", "TRUNCATE", "MERGE", "GRANT", "REVOKE",
]

# Known banking datasets in S3 — auto-registered as temp views
# when Unity Catalog tables are not yet available.
KNOWN_S3_TABLES = {
    "customers":    {"format": "parquet", "path": "raw/customers/"},
    "accounts":     {"format": "parquet", "path": "raw/accounts/"},
    "transactions": {"format": "parquet", "path": "raw/transactions/"},
    "branches":     {"format": "parquet", "path": "raw/branches/"},
}


class SparkConnectBackend:
    """
    Backend powered by Databricks Connect (DatabricksSession).

    Provides full Spark DataFrame + SQL capabilities on remote
    serverless compute while keeping the coding experience local.
    """

    def __init__(self, config: dict):
        """
        Args:
            config: Dict with keys:
                - host:        Databricks workspace URL
                - token:       Personal access token (optional if using ~/.databrickscfg)
                - cluster_id:  Cluster ID (optional; omit to use serverless)
                - catalog:     Unity Catalog catalog (default: sparkling)
                - schema:      Default schema (default: banking)
                - s3_bucket:   S3 bucket for raw data (optional)
        """
        self.host = config.get("host", "")
        self.token = config.get("token", "")
        self.cluster_id = config.get("cluster_id")
        self.catalog = config.get("catalog", "sparkling")
        self.schema = config.get("schema", "banking")
        self.s3_bucket = config.get("s3_bucket", "")
        self._spark = None
        self._session_start: Optional[float] = None
        self._s3_views_registered = False
        self._use_s3_views = False

    # ── Session management ───────────────────────────────────

    def _get_spark(self):
        """Lazy-init DatabricksSession (serverless or cluster-attached)."""
        if self._spark is not None:
            return self._spark

        from databricks.connect import DatabricksSession

        builder = DatabricksSession.builder

        if self.host:
            builder = builder.host(self.host)
        if self.token:
            builder = builder.token(self.token)

        if self.cluster_id:
            builder = builder.clusterId(self.cluster_id)
            logger.info(f"Connecting to cluster: {self.cluster_id}")
        else:
            builder = builder.serverless()
            logger.info("Connecting to Databricks serverless compute")

        self._spark = builder.getOrCreate()
        self._session_start = time.time()

        # Set default catalog/schema
        if self.catalog:
            self._spark.sql(f"USE CATALOG {self.catalog}")
        if self.schema:
            self._spark.sql(f"USE SCHEMA {self.schema}")

        logger.info(
            f"DatabricksSession ready — Spark {self._spark.version} "
            f"(catalog={self.catalog}, schema={self.schema})"
        )

        # Auto-register S3 data as temp views when UC tables are absent
        self._ensure_s3_views()

        return self._spark

    # ── S3 auto-registration ─────────────────────────────────

    def _ensure_s3_views(self):
        """
        Auto-register known S3 datasets as Spark temp views when the
        Unity Catalog schema is empty.  This lets every tool (SQL, DataFrame,
        profiling, caching) work transparently without requiring the user
        to first create UC tables or use raw S3 paths.

        Runs once per session; idempotent.
        """
        if self._s3_views_registered:
            return

        spark = self._spark

        # 1. Check whether Unity Catalog already has user tables
        try:
            rows = spark.sql(
                f"SHOW TABLES IN {self.catalog}.{self.schema}"
            ).collect()
            # Filter out temp views that *we* might have created in a
            # previous call (safety for re-entrance).
            uc_tables = [
                r for r in rows
                if not (hasattr(r, "isTemporary") and r["isTemporary"])
            ]
            if uc_tables:
                logger.info(
                    f"Found {len(uc_tables)} UC table(s) in "
                    f"{self.catalog}.{self.schema} — using Unity Catalog"
                )
                self._s3_views_registered = True
                return
        except Exception as exc:
            logger.warning(f"Could not inspect UC tables: {exc}")

        # 2. No UC tables — register S3 data as session-scoped temp views
        if not self.s3_bucket:
            logger.warning(
                "S3_BUCKET not configured; cannot auto-register S3 views"
            )
            self._s3_views_registered = True
            return

        registered = 0
        for table_name, info in KNOWN_S3_TABLES.items():
            s3_path = f"s3://{self.s3_bucket}/data/{info['path']}"
            try:
                df = spark.read.format(info["format"]).load(s3_path)
                df.createOrReplaceTempView(table_name)
                logger.info(
                    f"  ✔ auto-registered view: {table_name} → {s3_path}"
                )
                registered += 1
            except Exception as exc:
                logger.warning(
                    f"  ✘ failed to register '{table_name}' from "
                    f"{s3_path}: {exc}"
                )

        if registered:
            self._use_s3_views = True
            logger.info(
                f"Auto-registered {registered}/{len(KNOWN_S3_TABLES)} "
                f"S3 datasets as temp views (queries will appear in "
                f"Databricks SQL history)"
            )

        self._s3_views_registered = True

    def close(self):
        """Shut down the Spark session."""
        if self._spark:
            self._spark.stop()
            self._spark = None
            self._session_start = None
            self._s3_views_registered = False
            self._use_s3_views = False
            logger.info("DatabricksSession stopped")

    # ── Catalog / schema exploration ─────────────────────────

    def list_catalogs(self) -> list[dict[str, str]]:
        """List all available catalogs in Unity Catalog."""
        spark = self._get_spark()
        rows = spark.sql("SHOW CATALOGS").collect()
        return [{"catalog": row[0]} for row in rows]

    def list_schemas(self, catalog: Optional[str] = None) -> list[dict[str, str]]:
        """List schemas in a catalog (defaults to current catalog)."""
        spark = self._get_spark()
        cat = catalog or self.catalog
        rows = spark.sql(f"SHOW SCHEMAS IN {cat}").collect()
        return [{"schema": row[0]} for row in rows]

    def list_tables(self) -> list[dict[str, str]]:
        """List tables — from Unity Catalog or auto-registered S3 views."""
        spark = self._get_spark()

        if self._use_s3_views:
            tables = []
            for name, info in KNOWN_S3_TABLES.items():
                tables.append({
                    "name": name,
                    "short_name": name,
                    "source": f"s3://{self.s3_bucket}/data/{info['path']}",
                })
            return tables

        # UC-based listing
        rows = spark.sql(
            f"SHOW TABLES IN {self.catalog}.{self.schema}"
        ).collect()
        tables = []
        for row in rows:
            table_name = row["tableName"] if "tableName" in row.asDict() else row[1]
            tables.append({
                "name": f"{self.catalog}.{self.schema}.{table_name}",
                "short_name": table_name,
            })
        return tables

    def describe_table(self, table_name: str) -> list[dict[str, str]]:
        """Get column names, types, and comments for a table."""
        spark = self._get_spark()
        full = self._resolve(table_name)

        # For S3-backed temp views, use DataFrame schema directly
        # (DESCRIBE TABLE may not return comments for temp views).
        if self._use_s3_views and full in KNOWN_S3_TABLES:
            df = spark.table(full)
            return [
                {"column": f.name, "type": str(f.dataType), "comment": ""}
                for f in df.schema.fields
            ]

        rows = spark.sql(f"DESCRIBE TABLE {full}").collect()
        return [
            {
                "column": r["col_name"],
                "type": r["data_type"],
                "comment": r.get("comment", ""),
            }
            for r in rows
            if r["col_name"].strip() and not r["col_name"].startswith("#")
        ]

    def table_detail(self, table_name: str) -> dict[str, Any]:
        """
        DESCRIBE DETAIL — gives format, location, size, partition cols, etc.
        For S3-backed temp views, returns synthetic metadata.
        """
        spark = self._get_spark()
        full = self._resolve(table_name)

        # Temp views don't support DESCRIBE DETAIL — return S3 metadata
        short = table_name.split(".")[-1] if "." in table_name else table_name
        if self._use_s3_views and short in KNOWN_S3_TABLES:
            info = KNOWN_S3_TABLES[short]
            s3_path = f"s3://{self.s3_bucket}/data/{info['path']}"
            df = spark.table(full)
            return {
                "name": short,
                "format": info["format"],
                "location": s3_path,
                "numColumns": len(df.schema.fields),
                "source": "S3 auto-registered temp view",
            }

        rows = spark.sql(f"DESCRIBE DETAIL {full}").collect()
        if rows:
            return rows[0].asDict()
        return {}

    # ── Data reading ─────────────────────────────────────────

    def sample_data(self, table_name: str, limit: int = 10) -> list[dict]:
        """Return first N rows of a table as list of dicts."""
        spark = self._get_spark()
        full = self._resolve(table_name)
        limit = min(limit, 100)
        df = spark.sql(f"SELECT * FROM {full} LIMIT {limit}")
        return [row.asDict() for row in df.collect()]

    def read_s3_path(self, path: str, fmt: str = "parquet") -> list[dict]:
        """
        Read data directly from an S3 path.
        Useful for ad-hoc exploration of files not yet registered as tables.

        Args:
            path: S3 path (s3a://bucket/prefix or relative like raw/customers)
            fmt:  parquet | csv | json | delta
        """
        spark = self._get_spark()
        if not path.startswith("s3"):
            if not self.s3_bucket:
                raise ValueError(
                    "Relative path given but no S3_BUCKET configured. "
                    "Provide a full s3a:// path or set S3_BUCKET."
                )
            path = f"s3a://{self.s3_bucket}/data/{path}"

        reader = spark.read
        if fmt == "csv":
            df = reader.csv(path, header=True, inferSchema=True)
        elif fmt == "json":
            df = reader.json(path)
        elif fmt == "delta":
            df = reader.format("delta").load(path)
        else:
            df = reader.parquet(path)

        preview = df.limit(20)
        schema_info = [
            {"column": f.name, "type": str(f.dataType)}
            for f in df.schema.fields
        ]
        return {
            "path": path,
            "format": fmt,
            "schema": schema_info,
            "preview_rows": [row.asDict() for row in preview.collect()],
        }

    # ── SQL execution ────────────────────────────────────────

    def query_sql(self, sql: str) -> list[dict]:
        """
        Execute a read-only Spark SQL query.
        DDL/DML is blocked for safety.
        """
        self._guard_read_only(sql)
        spark = self._get_spark()
        df = spark.sql(sql)
        return [row.asDict() for row in df.collect()]

    def explain_sql(self, sql: str, mode: str = "extended") -> str:
        """
        Return the query execution plan without running the query.

        Args:
            sql:  SQL query
            mode: simple | extended | codegen | cost | formatted
        """
        self._guard_read_only(sql)
        spark = self._get_spark()
        df = spark.sql(sql)
        return df._jdf.queryExecution().toString()

    # ── DataFrame-style operations ───────────────────────────

    def dataframe_operation(
        self,
        table_name: str,
        operation: str,
        params: dict | None = None,
    ) -> list[dict]:
        """
        Execute a PySpark DataFrame operation on a table.

        Supported operations:
            filter      — params: {"condition": "amount > 1000000"}
            groupby_agg — params: {"group_cols": ["segment"], "agg": {"amount": "sum", "customer_id": "count"}}
            orderby     — params: {"columns": ["amount"], "ascending": false}
            select      — params: {"columns": ["customer_id", "name", "segment"]}
            distinct    — params: {"columns": ["segment"]}  (optional subset)
            describe    — params: {} (Spark describe — basic stats)
            corr        — params: {"col1": "balance", "col2": "amount"}

        Returns at most 200 rows.
        """
        spark = self._get_spark()
        full = self._resolve(table_name)
        df = spark.table(full)
        params = params or {}

        if operation == "filter":
            condition = params.get("condition", "1=1")
            df = df.filter(condition)

        elif operation == "groupby_agg":
            group_cols = params.get("group_cols", [])
            aggs = params.get("agg", {})
            if not group_cols or not aggs:
                raise ValueError("groupby_agg requires 'group_cols' and 'agg' params")
            agg_exprs = []
            for col_name, func_name in aggs.items():
                fn = getattr(F, func_name, None)
                if fn is None:
                    raise ValueError(f"Unknown aggregation function: {func_name}")
                agg_exprs.append(fn(col_name).alias(f"{func_name}_{col_name}"))
            df = df.groupBy(*group_cols).agg(*agg_exprs)

        elif operation == "orderby":
            cols = params.get("columns", [])
            asc = params.get("ascending", True)
            if not cols:
                raise ValueError("orderby requires 'columns' param")
            df = df.orderBy(*cols, ascending=asc)

        elif operation == "select":
            cols = params.get("columns", [])
            if not cols:
                raise ValueError("select requires 'columns' param")
            df = df.select(*cols)

        elif operation == "distinct":
            cols = params.get("columns")
            if cols:
                df = df.select(*cols).distinct()
            else:
                df = df.distinct()

        elif operation == "describe":
            df = df.describe()

        elif operation == "corr":
            col1 = params.get("col1")
            col2 = params.get("col2")
            if not col1 or not col2:
                raise ValueError("corr requires 'col1' and 'col2' params")
            corr_val = df.stat.corr(col1, col2)
            return [{"col1": col1, "col2": col2, "correlation": corr_val}]

        else:
            raise ValueError(
                f"Unknown operation: {operation}. "
                f"Supported: filter, groupby_agg, orderby, select, distinct, describe, corr"
            )

        # Cap output
        return [row.asDict() for row in df.limit(200).collect()]

    # ── Profiling ────────────────────────────────────────────

    def get_data_profile(self, table_name: str) -> dict:
        """
        Comprehensive data profile using Spark aggregations.
        Returns row count, column stats (distinct, nulls, min, max).
        """
        spark = self._get_spark()
        full = self._resolve(table_name)
        df = spark.table(full)

        row_count = df.count()
        schema_fields = df.schema.fields
        col_stats = []

        for field in schema_fields[:30]:  # cap at 30 columns
            col_name = field.name
            dtype = str(field.dataType)
            try:
                stats = df.agg(
                    F.countDistinct(F.col(col_name)).alias("distinct"),
                    (F.count("*") - F.count(F.col(col_name))).alias("nulls"),
                ).collect()[0]
                entry = {
                    "column": col_name,
                    "type": dtype,
                    "distinct_count": stats["distinct"],
                    "null_count": stats["nulls"],
                }
                # Add min/max for numeric and string types
                try:
                    minmax = df.agg(
                        F.min(F.col(col_name)).alias("min_val"),
                        F.max(F.col(col_name)).alias("max_val"),
                    ).collect()[0]
                    entry["min"] = str(minmax["min_val"])
                    entry["max"] = str(minmax["max_val"])
                except Exception:
                    pass

                col_stats.append(entry)
            except Exception as e:
                col_stats.append({
                    "column": col_name,
                    "type": dtype,
                    "error": str(e),
                })

        return {
            "table": full,
            "row_count": row_count,
            "column_count": len(schema_fields),
            "columns": col_stats,
        }

    # ── Cache management ─────────────────────────────────────

    def cache_table(self, table_name: str) -> str:
        """Cache a table in Spark memory for faster repeated access."""
        spark = self._get_spark()
        full = self._resolve(table_name)
        spark.sql(f"CACHE TABLE {full}")
        return f"Cached: {full}"

    def uncache_table(self, table_name: str) -> str:
        """Remove a table from Spark cache."""
        spark = self._get_spark()
        full = self._resolve(table_name)
        spark.sql(f"UNCACHE TABLE IF EXISTS {full}")
        return f"Uncached: {full}"

    def list_cached_tables(self) -> list[str]:
        """List currently cached tables."""
        spark = self._get_spark()
        catalog = spark.catalog
        cached = []
        try:
            tables = catalog.listTables(self.schema)
            for t in tables:
                if catalog.isCached(t.name):
                    cached.append(f"{self.catalog}.{self.schema}.{t.name}")
        except Exception as e:
            logger.warning(f"Could not list cached tables: {e}")
        return cached

    # ── Session info ─────────────────────────────────────────

    def session_info(self) -> dict[str, Any]:
        """Return info about the current Spark session."""
        spark = self._get_spark()
        uptime = time.time() - self._session_start if self._session_start else 0
        info = {
            "spark_version": spark.version,
            "catalog": self.catalog,
            "schema": self.schema,
            "cluster_id": self.cluster_id or "serverless",
            "host": self.host,
            "session_uptime_seconds": round(uptime, 1),
        }
        if self._use_s3_views:
            info["data_source"] = "S3 auto-registered temp views"
            info["s3_tables"] = list(KNOWN_S3_TABLES.keys())
        else:
            info["data_source"] = "Unity Catalog"
        return info

    # ── Helpers ──────────────────────────────────────────────

    def _resolve(self, name: str) -> str:
        """Resolve short name → catalog.schema.table (or temp view name)."""
        if "." not in name:
            # When S3 views are active, keep short names so Spark resolves
            # them to the session-scoped temp views instead of UC tables.
            if self._use_s3_views and name in KNOWN_S3_TABLES:
                return name
            return f"{self.catalog}.{self.schema}.{name}"
        # Fully-qualified name referencing a UC table that doesn't exist
        # but has an S3 temp view → rewrite to the short name.
        if self._use_s3_views:
            parts = name.split(".")
            if len(parts) == 3:
                _, _, short = parts
                if short in KNOWN_S3_TABLES:
                    return short
        return name

    @staticmethod
    def _guard_read_only(sql: str):
        """Reject DDL/DML statements."""
        first_word = sql.strip().split()[0].upper() if sql.strip() else ""
        if first_word in _BLOCKED_KEYWORDS:
            raise ValueError(
                f"Only read-only queries are allowed. '{first_word}' is not permitted."
            )
