"""
RDS Incremental CDC Pipeline — Daily Load
==========================================
Reads only new/changed rows from PostgreSQL RDS since the last watermark
and merges them into the existing Unity Catalog Delta tables.

Tables with CDC support (last_modified column):
  - dim_customer         (SCD Type 2 — append new versions)
  - dim_account          (SCD Type 1 — MERGE/UPSERT)
  - fact_transaction     (append-only — INSERT new rows)
  - fact_daily_balance   (MERGE on customer_id + date_key)

Dimension tables without CDC (full refresh, small tables):
  - dim_date, dim_branch, dim_account_type

CDC Watermark Strategy:
  - Watermarks are stored in Unity Catalog as a Delta table
    `{catalog}.{schema}.cdc_watermark`
  - Each table's last_watermark is updated after a successful load
  - Idempotent: re-running with the same watermark is safe

Usage (via Databricks Asset Bundle):
    databricks bundle run rds_data_migration_1 --target prod \
        --task rds_incremental_task

Usage (direct):
    python -m rds_data_migration_1_etl.transformations.rds_incremental \
        --catalog sparkling --schema prod \
        --rds-host <host> --rds-port 5432 \
        --rds-database sparkdb \
        --rds-username sparkadmin --rds-password <password>
"""

import argparse
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tables that support incremental load via last_modified watermark
# ---------------------------------------------------------------------------
CDC_TABLES = [
    {
        "name": "dim_customer",
        "cdc_column": "last_modified",
        # SCD2: new versions are appended; never overwrite history
        "strategy": "append",
    },
    {
        "name": "dim_account",
        "cdc_column": "last_modified",
        # SCD1: upsert by business key
        "strategy": "merge",
        "merge_key": "account_id",
    },
    {
        "name": "fact_transaction",
        "cdc_column": "last_modified",
        # Fact table: append new transactions
        "strategy": "append",
        "dedup_key": "txn_id",
    },
    {
        "name": "fact_daily_balance",
        "cdc_column": "last_modified",
        # Snapshot: merge on (customer_id, date_key)
        "strategy": "merge",
        "merge_key": "customer_id,date_key",
    },
]

# Small dimension tables — full refresh each run (row count is tiny)
FULL_REFRESH_TABLES = ["dim_date", "dim_branch", "dim_account_type"]

# Default watermark for first run (process all history)
DEFAULT_WATERMARK = "2020-01-01 00:00:00"

# Unity Catalog table that stores watermarks
WATERMARK_TABLE = "cdc_watermark"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _jdbc_url(host: str, port: str, database: str) -> str:
    return f"jdbc:postgresql://{host}:{port}/{database}"


def _jdbc_props(username: str, password: str) -> dict:
    return {
        "user": username,
        "password": password,
        "driver": "org.postgresql.Driver",
    }


def _fq(catalog: str, schema: str, table: str) -> str:
    """Fully-qualified Unity Catalog table name."""
    return f"{catalog}.{schema}.{table}"


def _ensure_watermark_table(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the CDC watermark tracking table if it does not exist."""
    fq = _fq(catalog, schema, WATERMARK_TABLE)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fq} (
            table_name      STRING NOT NULL,
            last_watermark  TIMESTAMP,
            last_row_count  BIGINT,
            last_run_status STRING,
            updated_at      TIMESTAMP
        )
        USING DELTA
    """)
    logger.info("Watermark table ready: %s", fq)


def _get_watermark(spark: SparkSession, catalog: str, schema: str, table_name: str) -> str:
    """Return the last processed watermark for a table, or DEFAULT_WATERMARK."""
    fq = _fq(catalog, schema, WATERMARK_TABLE)
    rows = (
        spark.sql(f"SELECT last_watermark FROM {fq} WHERE table_name = '{table_name}'")
        .collect()
    )
    if rows and rows[0]["last_watermark"] is not None:
        ts = rows[0]["last_watermark"]
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    return DEFAULT_WATERMARK


def _update_watermark(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table_name: str,
    new_watermark: str,
    row_count: int,
    status: str = "SUCCESS",
) -> None:
    """Upsert the watermark for a table."""
    fq = _fq(catalog, schema, WATERMARK_TABLE)
    spark.sql(f"""
        MERGE INTO {fq} AS target
        USING (
            SELECT
                '{table_name}'   AS table_name,
                TIMESTAMP('{new_watermark}') AS last_watermark,
                {row_count}      AS last_row_count,
                '{status}'       AS last_run_status,
                CURRENT_TIMESTAMP AS updated_at
        ) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# ---------------------------------------------------------------------------
# Incremental extraction helpers
# ---------------------------------------------------------------------------

def _extract_cdc(
    spark: SparkSession,
    jdbc_url: str,
    jdbc_props: dict,
    table_name: str,
    cdc_column: str,
    watermark: str,
) -> DataFrame:
    """
    Extract rows from RDS where cdc_column > watermark.
    Uses a push-down predicate so only changed rows are transferred.
    """
    query = f"(SELECT * FROM {table_name} WHERE {cdc_column} > '{watermark}') AS incremental"
    logger.info("Extracting %s WHERE %s > '%s'", table_name, cdc_column, watermark)
    df = spark.read.jdbc(url=jdbc_url, table=query, properties=jdbc_props)
    count = df.count()
    logger.info("  → %d new/changed rows", count)
    return df


def _load_append(
    spark: SparkSession,
    df: DataFrame,
    fq_table: str,
    dedup_key: str | None = None,
) -> int:
    """Append new rows. Optional dedup_key removes exact duplicates before writing."""
    if dedup_key and df.count() > 0:
        df = df.dropDuplicates([dedup_key])
    count = df.count()
    if count == 0:
        logger.info("  No rows to append for %s", fq_table)
        return 0
    df.write.format("delta").mode("append").saveAsTable(fq_table)
    logger.info("  Appended %d rows → %s", count, fq_table)
    return count


def _load_merge(
    spark: SparkSession,
    df: DataFrame,
    fq_table: str,
    merge_keys: list[str],
) -> int:
    """MERGE (upsert) new rows into the target Delta table."""
    count = df.count()
    if count == 0:
        logger.info("  No rows to merge for %s", fq_table)
        return 0

    # Register temp view for MERGE statement
    tmp_view = fq_table.replace(".", "_") + "_incremental"
    df.createOrReplaceTempView(tmp_view)

    key_cond = " AND ".join(
        f"target.{k} = source.{k}" for k in merge_keys
    )
    all_cols = df.columns
    update_set = ", ".join(
        f"target.{c} = source.{c}" for c in all_cols if c not in merge_keys
    )
    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join(f"source.{c}" for c in all_cols)

    spark.sql(f"""
        MERGE INTO {fq_table} AS target
        USING {tmp_view} AS source
        ON {key_cond}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)
    logger.info("  Merged %d rows → %s", count, fq_table)
    return count


def _load_full_refresh(
    spark: SparkSession,
    jdbc_url: str,
    jdbc_props: dict,
    table_name: str,
    fq_table: str,
) -> int:
    """Full overwrite for small dimension tables."""
    logger.info("Full refresh: %s → %s", table_name, fq_table)
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_props)
    count = df.count()
    df.write.format("delta").mode("overwrite").saveAsTable(fq_table)
    logger.info("  Written %d rows → %s", count, fq_table)
    return count


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="RDS → Unity Catalog incremental CDC load")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    parser.add_argument("--schema", required=True, help="Schema / database name")
    parser.add_argument("--rds-host", required=True)
    parser.add_argument("--rds-port", default="5432")
    parser.add_argument("--rds-database", default="sparkdb")
    parser.add_argument("--rds-username", required=True)
    parser.add_argument("--rds-password", required=True)
    parser.add_argument(
        "--full-refresh-dims",
        action="store_true",
        default=False,
        help="Also refresh small dimension tables (dim_date, dim_branch, dim_account_type)",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    jdbc_url = _jdbc_url(args.rds_host, args.rds_port, args.rds_database)
    jdbc_props = _jdbc_props(args.rds_username, args.rds_password)

    # Ensure schema and watermark table exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")
    _ensure_watermark_table(spark, args.catalog, args.schema)

    pipeline_start = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    summary: list[dict] = []

    # ── 1. Incremental CDC tables ──────────────────────────────────────────
    for table_cfg in CDC_TABLES:
        tname = table_cfg["name"]
        cdc_col = table_cfg["cdc_column"]
        strategy = table_cfg["strategy"]
        fq_table = _fq(args.catalog, args.schema, tname)

        watermark = _get_watermark(spark, args.catalog, args.schema, tname)
        logger.info("Processing %s | watermark=%s | strategy=%s", tname, watermark, strategy)

        try:
            df = _extract_cdc(spark, jdbc_url, jdbc_props, tname, cdc_col, watermark)
            new_max_ts = df.agg(F.max(cdc_col)).collect()[0][0]

            if strategy == "append":
                dedup = table_cfg.get("dedup_key")
                count = _load_append(spark, df, fq_table, dedup_key=dedup)
            else:  # merge
                keys = [k.strip() for k in table_cfg["merge_key"].split(",")]
                count = _load_merge(spark, df, fq_table, merge_keys=keys)

            new_wm = (
                new_max_ts.strftime("%Y-%m-%d %H:%M:%S")
                if new_max_ts
                else watermark
            )
            _update_watermark(spark, args.catalog, args.schema, tname, new_wm, count)
            summary.append({"table": tname, "rows": count, "status": "OK", "watermark": new_wm})

        except Exception as exc:
            logger.error("Failed to process %s: %s", tname, exc)
            _update_watermark(spark, args.catalog, args.schema, tname, watermark, 0, "FAILED")
            summary.append({"table": tname, "rows": 0, "status": f"FAILED: {exc}", "watermark": watermark})

    # ── 2. Optional full-refresh of small dimensions ───────────────────────
    if args.full_refresh_dims:
        for tname in FULL_REFRESH_TABLES:
            fq_table = _fq(args.catalog, args.schema, tname)
            try:
                count = _load_full_refresh(spark, jdbc_url, jdbc_props, tname, fq_table)
                _update_watermark(spark, args.catalog, args.schema, tname, pipeline_start, count)
                summary.append({"table": tname, "rows": count, "status": "OK (full refresh)"})
            except Exception as exc:
                logger.error("Full refresh failed for %s: %s", tname, exc)
                summary.append({"table": tname, "rows": 0, "status": f"FAILED: {exc}"})

    # ── Summary ───────────────────────────────────────────────────────────
    logger.info("\n=== Incremental Load Summary ===")
    for row in summary:
        logger.info("  %-30s rows=%-8d  %s", row["table"], row["rows"], row["status"])
    logger.info("=================================")

    failed = [r for r in summary if r["status"].startswith("FAILED")]
    if failed:
        raise RuntimeError(f"Incremental load finished with {len(failed)} failure(s): {[f['table'] for f in failed]}")

    logger.info("Incremental load completed successfully.")


if __name__ == "__main__":
    main()
