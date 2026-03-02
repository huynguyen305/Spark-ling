"""
S3 Backend for MCP Server
==========================
Reads Parquet/CSV data directly from S3 using PySpark locally.
Good for cost-effective exploration without a running Databricks warehouse.
"""

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


class S3Backend:
    """Backend that reads data from S3 directly using local PySpark."""

    # Known datasets in the Spark-ling project
    KNOWN_TABLES = {
        "customers": {"format": "csv", "path": "raw/customers.csv"},
        "accounts": {"format": "csv", "path": "raw/accounts.csv"},
        "transactions": {"format": "csv", "path": "raw/transactions.csv"},
        "branches": {"format": "csv", "path": "raw/branches.csv"},
    }

    def __init__(self, config: dict):
        """
        Initialize S3 backend.

        Args:
            config: Dict with keys: bucket, region, data_prefix
        """
        self.bucket = config["bucket"]
        self.region = config["region"]
        self.data_prefix = config.get("data_prefix", "data")
        self._spark = None

    def _get_spark(self):
        """Lazy-initialize a local SparkSession with S3 access."""
        if self._spark is None:
            import sys
            from pathlib import Path

            # Add project src to path
            project_root = Path(__file__).parent.parent
            sys.path.insert(0, str(project_root))

            from configs.spark_config import get_spark_session
            self._spark = get_spark_session("MCP-S3-Explorer", mode="aws")
            logger.info(f"SparkSession initialized for S3 backend (bucket: {self.bucket})")
        return self._spark

    def _get_s3_path(self, relative_path: str) -> str:
        """Build full s3a:// path."""
        return f"s3a://{self.bucket}/{self.data_prefix}/{relative_path}"

    def _read_table(self, table_name: str):
        """Read a table from S3 as a Spark DataFrame."""
        spark = self._get_spark()

        if table_name in self.KNOWN_TABLES:
            info = self.KNOWN_TABLES[table_name]
            path = self._get_s3_path(info["path"])
            fmt = info["format"]
        else:
            # Try common formats
            for fmt in ["parquet", "csv"]:
                for suffix in [f"raw/{table_name}", f"processed/{table_name}", f"raw/{table_name}.csv"]:
                    path = self._get_s3_path(suffix)
                    try:
                        if fmt == "csv":
                            return spark.read.csv(path, header=True, inferSchema=True)
                        else:
                            return spark.read.parquet(path)
                    except Exception:
                        continue
            raise ValueError(f"Table '{table_name}' not found in S3 bucket s3://{self.bucket}")

        if fmt == "csv":
            return spark.read.csv(path, header=True, inferSchema=True)
        elif fmt == "parquet":
            return spark.read.parquet(path)
        elif fmt == "delta":
            return spark.read.format("delta").load(path)
        else:
            raise ValueError(f"Unsupported format: {fmt}")

    def list_tables(self) -> list[dict[str, str]]:
        """List known tables/datasets."""
        tables = []
        for name, info in self.KNOWN_TABLES.items():
            tables.append({
                "name": name,
                "short_name": name,
                "format": info["format"],
                "path": self._get_s3_path(info["path"]),
            })

        # Also scan for processed data
        try:
            spark = self._get_spark()
            hadoop = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jsc.hadoopConfiguration()
            )
            # This is optional — may fail if S3 isn't accessible
        except Exception:
            pass

        return tables

    def describe_table(self, table_name: str) -> list[dict[str, str]]:
        """Get schema for a table by reading it from S3."""
        df = self._read_table(table_name)
        return [
            {"column": field.name, "type": str(field.dataType), "comment": ""}
            for field in df.schema.fields
        ]

    def sample_data(self, table_name: str, limit: int = 10) -> list[dict]:
        """Return first N rows from a table."""
        limit = min(limit, 100)
        df = self._read_table(table_name)
        rows = df.limit(limit).collect()
        return [row.asDict() for row in rows]

    def query_sql(self, sql: str) -> list[dict]:
        """
        Execute a SQL query against tables loaded from S3.
        Tables must be registered first via temp views.
        """
        sql_upper = sql.strip().upper()
        blocked = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE"]
        for keyword in blocked:
            if sql_upper.startswith(keyword):
                raise ValueError(f"Only read-only queries are allowed. '{keyword}' is not permitted.")

        spark = self._get_spark()

        # Auto-register known tables as temp views
        for name in self.KNOWN_TABLES:
            if name.upper() in sql.upper():
                try:
                    df = self._read_table(name)
                    df.createOrReplaceTempView(name)
                except Exception as e:
                    logger.warning(f"Could not load table {name}: {e}")

        result = spark.sql(sql)
        rows = result.limit(1000).collect()  # Safety cap
        return [row.asDict() for row in rows]

    def get_data_profile(self, table_name: str) -> dict:
        """Get basic statistics for a table."""
        df = self._read_table(table_name)
        row_count = df.count()

        col_stats = []
        for field in df.schema.fields[:20]:
            try:
                from pyspark.sql.functions import countDistinct, count, col, when, isnull
                stats = df.select(
                    countDistinct(col(field.name)).alias("distinct_count"),
                    count(when(isnull(col(field.name)), 1)).alias("null_count"),
                ).collect()[0]
                col_stats.append({
                    "column": field.name,
                    "type": str(field.dataType),
                    "distinct_count": stats["distinct_count"],
                    "null_count": stats["null_count"],
                })
            except Exception as e:
                col_stats.append({
                    "column": field.name,
                    "type": str(field.dataType),
                    "error": str(e),
                })

        return {
            "table": table_name,
            "row_count": row_count,
            "column_count": len(df.schema.fields),
            "columns": col_stats,
        }

    def close(self):
        """Stop the SparkSession."""
        if self._spark:
            self._spark.stop()
            self._spark = None
