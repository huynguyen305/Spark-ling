"""
AWS Athena Backend for MCP Server
==================================
Serverless SQL queries against S3 data via AWS Athena.

This is the recommended AWS backend: no Spark cluster needed,
pay-per-query pricing, and fast for interactive exploration.

Prerequisites:
    - AWS credentials configured (aws configure or IAM role)
    - S3 bucket with Spark-ling data uploaded
    - Athena workgroup (default or custom)
"""

import logging
import time
from typing import Any

import boto3

logger = logging.getLogger(__name__)


class AthenaBackend:
    """Backend that queries S3 data via AWS Athena (serverless SQL)."""

    # Known datasets — mapped to S3 paths within the bucket
    KNOWN_TABLES = {
        "customers": {"format": "csv", "path": "raw/customers.csv"},
        "accounts": {"format": "csv", "path": "raw/accounts.csv"},
        "transactions": {"format": "csv", "path": "raw/transactions.csv"},
        "branches": {"format": "csv", "path": "raw/branches.csv"},
    }

    # CSV schema definitions for CREATE EXTERNAL TABLE
    TABLE_SCHEMAS = {
        "customers": """
            customer_id STRING,
            name STRING,
            email STRING,
            phone STRING,
            date_of_birth STRING,
            address STRING,
            city STRING,
            region STRING,
            segment STRING,
            kyc_status STRING,
            created_date STRING,
            updated_date STRING
        """,
        "accounts": """
            account_id STRING,
            customer_id STRING,
            account_type STRING,
            balance DOUBLE,
            currency STRING,
            status STRING,
            branch_id STRING,
            opened_date STRING,
            updated_date STRING
        """,
        "transactions": """
            txn_id STRING,
            account_id STRING,
            txn_type STRING,
            amount DOUBLE,
            currency STRING,
            txn_datetime STRING,
            channel STRING,
            status STRING,
            description STRING,
            counterparty_account STRING,
            reference STRING
        """,
        "branches": """
            branch_id STRING,
            branch_name STRING,
            region STRING,
            city STRING,
            address STRING,
            manager STRING,
            opened_date STRING,
            status STRING,
            phone STRING,
            email STRING
        """,
    }

    def __init__(self, config: dict):
        """
        Initialize Athena backend.

        Args:
            config: Dict with keys:
                - bucket: S3 bucket name
                - region: AWS region
                - database: Athena database name (default: sparkling)
                - workgroup: Athena workgroup (default: primary)
                - data_prefix: S3 prefix for data (default: data)
                - output_location: S3 path for query results
        """
        self.bucket = config["bucket"]
        self.region = config.get("region", "ap-southeast-1")
        self.database = config.get("database", "sparkling")
        self.workgroup = config.get("workgroup", "primary")
        self.data_prefix = config.get("data_prefix", "data")
        self.output_location = config.get(
            "output_location",
            f"s3://{self.bucket}/athena-results/",
        )

        self._athena = boto3.client("athena", region_name=self.region)
        self._s3 = boto3.client("s3", region_name=self.region)
        self._glue = boto3.client("glue", region_name=self.region)
        self._initialized = False

    def _ensure_database(self):
        """Create the Athena database if it doesn't exist."""
        if self._initialized:
            return

        sql = f"CREATE DATABASE IF NOT EXISTS {self.database}"
        self._execute_athena(sql, wait=True)
        self._initialized = True
        logger.info(f"Athena database '{self.database}' ready")

    def _ensure_table(self, table_name: str):
        """Create an external table in Athena pointing to S3 data."""
        if table_name not in self.TABLE_SCHEMAS:
            raise ValueError(
                f"Unknown table '{table_name}'. "
                f"Known tables: {', '.join(self.TABLE_SCHEMAS.keys())}"
            )

        # Check if table already exists via Glue Catalog
        try:
            self._glue.get_table(
                DatabaseName=self.database,
                Name=table_name,
            )
            return  # Table exists
        except self._glue.exceptions.EntityNotFoundException:
            pass

        schema = self.TABLE_SCHEMAS[table_name]
        info = self.KNOWN_TABLES[table_name]
        s3_path = f"s3://{self.bucket}/{self.data_prefix}/{info['path']}"

        # For CSV files, we need a folder not a file — Athena reads directory
        # Use the parent folder and set up SerDe for CSV
        if info["format"] == "csv":
            # Point to the file's parent directory or the file itself
            sql = f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS {self.database}.{table_name} (
                    {schema}
                )
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                WITH SERDEPROPERTIES (
                    'separatorChar' = ',',
                    'quoteChar' = '"',
                    'escapeChar' = '\\\\'
                )
                STORED AS TEXTFILE
                LOCATION '{s3_path.rsplit('/', 1)[0]}/'
                TBLPROPERTIES ('skip.header.line.count'='1')
            """
        elif info["format"] == "parquet":
            s3_dir = s3_path.rstrip("/") + "/"
            sql = f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS {self.database}.{table_name} (
                    {schema}
                )
                STORED AS PARQUET
                LOCATION '{s3_dir}'
            """
        else:
            raise ValueError(f"Unsupported format: {info['format']}")

        self._execute_athena(sql, wait=True)
        logger.info(f"Created Athena table: {self.database}.{table_name}")

    def _execute_athena(
        self, sql: str, wait: bool = True, max_wait_seconds: int = 60
    ) -> str:
        """
        Execute a query on Athena and return the query execution ID.

        Args:
            sql: SQL statement
            wait: If True, block until query completes
            max_wait_seconds: Max time to wait for query completion

        Returns:
            Query execution ID
        """
        response = self._athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.output_location},
            WorkGroup=self.workgroup,
        )
        query_id = response["QueryExecutionId"]
        logger.debug(f"Query started: {query_id}")

        if wait:
            self._wait_for_query(query_id, max_wait_seconds)

        return query_id

    def _wait_for_query(self, query_id: str, max_wait_seconds: int = 60):
        """Wait for an Athena query to complete."""
        elapsed = 0
        poll_interval = 0.5

        while elapsed < max_wait_seconds:
            response = self._athena.get_query_execution(QueryExecutionId=query_id)
            state = response["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                return
            elif state in ("FAILED", "CANCELLED"):
                reason = response["QueryExecution"]["Status"].get(
                    "StateChangeReason", "Unknown error"
                )
                raise RuntimeError(f"Athena query {state}: {reason}")

            time.sleep(poll_interval)
            elapsed += poll_interval
            # Back off polling interval
            poll_interval = min(poll_interval * 1.5, 5.0)

        raise TimeoutError(
            f"Athena query {query_id} did not complete within {max_wait_seconds}s"
        )

    def _get_query_results(self, query_id: str) -> list[dict[str, Any]]:
        """Fetch results from a completed Athena query."""
        results = []
        paginator = self._athena.get_paginator("get_query_results")

        first_page = True
        for page in paginator.paginate(QueryExecutionId=query_id):
            rows = page["ResultSet"]["Rows"]
            if first_page and rows:
                # First row is column headers
                headers = [col["VarCharValue"] for col in rows[0]["Data"]]
                rows = rows[1:]
                first_page = False

            for row in rows:
                values = [
                    col.get("VarCharValue", None) for col in row["Data"]
                ]
                results.append(dict(zip(headers, values)))

        return results

    def list_tables(self) -> list[dict[str, str]]:
        """List all known tables/datasets."""
        self._ensure_database()

        tables = []
        for name, info in self.KNOWN_TABLES.items():
            tables.append({
                "name": f"{self.database}.{name}",
                "short_name": name,
                "format": info["format"],
                "path": f"s3://{self.bucket}/{self.data_prefix}/{info['path']}",
                "engine": "athena",
            })

        # Also check Glue catalog for any extra tables
        try:
            response = self._glue.get_tables(DatabaseName=self.database)
            glue_tables = {t["Name"] for t in response.get("TableList", [])}
            known_names = set(self.KNOWN_TABLES.keys())
            for extra in glue_tables - known_names:
                tables.append({
                    "name": f"{self.database}.{extra}",
                    "short_name": extra,
                    "format": "unknown",
                    "path": "see Glue catalog",
                    "engine": "athena",
                })
        except Exception as e:
            logger.debug(f"Could not list Glue tables: {e}")

        return tables

    def describe_table(self, table_name: str) -> list[dict[str, str]]:
        """Get schema for a table."""
        self._ensure_database()
        self._ensure_table(table_name)

        query_id = self._execute_athena(
            f"DESCRIBE {self.database}.{table_name}", wait=True
        )
        results = self._get_query_results(query_id)

        columns = []
        for row in results:
            # Athena DESCRIBE returns col_name and data_type
            col_name = row.get("col_name", "")
            if not col_name or col_name.startswith("#"):
                continue
            columns.append({
                "column": col_name,
                "type": row.get("data_type", "string"),
                "comment": row.get("comment", ""),
            })

        # Fallback: parse from TABLE_SCHEMAS if DESCRIBE didn't work well
        if not columns and table_name in self.TABLE_SCHEMAS:
            schema_str = self.TABLE_SCHEMAS[table_name]
            for line in schema_str.strip().split("\n"):
                line = line.strip().rstrip(",")
                if line:
                    parts = line.split()
                    if len(parts) >= 2:
                        columns.append({
                            "column": parts[0],
                            "type": parts[1],
                            "comment": "",
                        })

        return columns

    def sample_data(self, table_name: str, limit: int = 10) -> list[dict]:
        """Return first N rows from a table."""
        self._ensure_database()
        self._ensure_table(table_name)

        limit = min(limit, 100)
        query_id = self._execute_athena(
            f"SELECT * FROM {self.database}.{table_name} LIMIT {limit}",
            wait=True,
        )
        return self._get_query_results(query_id)

    def query_sql(self, sql: str) -> list[dict]:
        """
        Execute a read-only SQL query via Athena.

        Tables must be referenced as {database}.{table} or just {table}
        (Athena uses the database context set during execution).
        """
        sql_upper = sql.strip().upper()
        blocked = [
            "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
            "CREATE", "TRUNCATE", "MERGE",
        ]
        for keyword in blocked:
            if sql_upper.startswith(keyword):
                raise ValueError(
                    f"Only read-only queries are allowed. "
                    f"'{keyword}' is not permitted."
                )

        self._ensure_database()

        # Auto-register tables referenced in the query
        for name in self.KNOWN_TABLES:
            if name.upper() in sql.upper():
                try:
                    self._ensure_table(name)
                except Exception as e:
                    logger.warning(f"Could not ensure table {name}: {e}")

        query_id = self._execute_athena(sql, wait=True)
        return self._get_query_results(query_id)

    def get_data_profile(self, table_name: str) -> dict:
        """Get basic statistics for a table."""
        self._ensure_database()
        self._ensure_table(table_name)

        full_name = f"{self.database}.{table_name}"

        # Row count
        query_id = self._execute_athena(
            f"SELECT COUNT(*) as row_count FROM {full_name}", wait=True
        )
        count_results = self._get_query_results(query_id)
        row_count = int(count_results[0]["row_count"]) if count_results else 0

        # Get columns
        columns = self.describe_table(table_name)

        # Column stats — run a single aggregation query for efficiency
        col_stats = []
        stat_exprs = []
        col_names = []
        for col in columns[:20]:
            col_name = col["column"]
            col_names.append(col_name)
            stat_exprs.append(
                f'COUNT(DISTINCT "{col_name}") as "{col_name}_distinct"'
            )
            stat_exprs.append(
                f'SUM(CASE WHEN "{col_name}" IS NULL THEN 1 ELSE 0 END) '
                f'as "{col_name}_nulls"'
            )

        if stat_exprs:
            stats_sql = f"SELECT {', '.join(stat_exprs)} FROM {full_name}"
            try:
                query_id = self._execute_athena(stats_sql, wait=True)
                stats_result = self._get_query_results(query_id)
                stats_row = stats_result[0] if stats_result else {}
            except Exception as e:
                logger.warning(f"Could not get column stats: {e}")
                stats_row = {}

            for col in columns[:20]:
                col_name = col["column"]
                col_stats.append({
                    "column": col_name,
                    "type": col["type"],
                    "distinct_count": int(
                        stats_row.get(f"{col_name}_distinct", 0)
                    ),
                    "null_count": int(
                        stats_row.get(f"{col_name}_nulls", 0)
                    ),
                })
        else:
            col_stats = [
                {"column": c["column"], "type": c["type"]}
                for c in columns[:20]
            ]

        return {
            "table": full_name,
            "row_count": row_count,
            "column_count": len(columns),
            "columns": col_stats,
        }

    def close(self):
        """Clean up resources. Athena is serverless, nothing to close."""
        pass
