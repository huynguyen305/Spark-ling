"""
Incremental Load Pipeline: RDS PostgreSQL → Databricks Bronze (CDC)
===================================================================
Extracts only CHANGED rows since the last pipeline run using the
CDC watermark pattern, then MERGEs them into Bronze Delta tables.

LEARNING NOTES:
---------------
1. CDC (Change Data Capture) STRATEGIES — from simplest to most advanced:

   a) TIMESTAMP-BASED (this pipeline):
      WHERE last_modified > :last_watermark
      ✅ Simple to implement
      ❌ Misses DELETEs (need soft-delete flag)
      ❌ Clock skew can cause missed records

   b) LOG-BASED (production-grade):
      Read database WAL (Write-Ahead Log) or change events
      PostgreSQL: Use logical replication / pgoutput / Debezium
      Oracle: Use LogMiner or GoldenGate
      ✅ Captures all changes including DELETEs
      ✅ Very low latency (near real-time)
      ❌ Complex setup

   c) AWS DMS (managed service):
      AWS Database Migration Service reads change logs automatically
      ✅ Managed, no coding needed
      ❌ Costs money, limited transformation capability

   At Techcombank, you'd likely use AWS DMS or a log-based CDC tool.
   This pipeline teaches the CONCEPT of CDC using the timestamp approach.

2. MERGE (UPSERT) PATTERN:
   Delta Lake MERGE is like SQL MERGE INTO:
   - If row EXISTS in target (matched on business key): UPDATE it
   - If row is NEW (not matched): INSERT it
   This ensures idempotency — running the same batch twice is safe.

3. WATERMARK TRACKING:
   After each successful run, we update cdc_watermark table with:
   - The MAX(last_modified) from the extracted batch
   - Row count for auditing
   This becomes the starting point for the next run.

USAGE:
    # Run incremental load:
    python pipelines/migration/incremental_load_pipeline.py

    # Run for specific tables:
    python pipelines/migration/incremental_load_pipeline.py --tables fact_transaction dim_customer

    # Check watermarks:
    python pipelines/migration/incremental_load_pipeline.py --show-watermarks
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "configs"))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from full_load_pipeline import (
    load_rds_config, get_jdbc_url, get_spark_session, TABLE_CONFIGS
)


# ── Watermark Management ────────────────────────────────────────────────────

def get_watermarks(spark, jdbc_url: str, rds_config: dict) -> Dict[str, str]:
    """
    Read current CDC watermarks from PostgreSQL RDS.

    LEARNING: The watermark table stores the "high-water mark" for each table.
    It answers: "When was the last time we successfully extracted this table?"
    """
    jdbc_properties = {
        "user": rds_config["username"],
        "password": rds_config["password"],
        "driver": "org.postgresql.Driver",
    }

    df = spark.read.jdbc(
        url=jdbc_url,
        table="cdc_watermark",
        properties=jdbc_properties,
    )

    watermarks = {}
    for row in df.collect():
        watermarks[row["table_name"]] = {
            "last_watermark": str(row["last_watermark"]),
            "last_row_count": row["last_row_count"],
            "last_status": row["last_run_status"],
        }

    return watermarks


def update_watermark(spark, jdbc_url: str, rds_config: dict,
                     table_name: str, new_watermark: str,
                     row_count: int):
    """
    Update the watermark after successful extraction.

    LEARNING: Watermark update MUST happen AFTER successful processing.
    If the pipeline fails mid-way, the watermark stays at the old value,
    and the next run will re-extract the same batch (idempotent recovery).
    """
    import psycopg2

    config = rds_config
    conn = psycopg2.connect(
        host=config["host"],
        port=int(config["port"]),
        dbname=config["database"],
        user=config["username"],
        password=config["password"],
    )
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE cdc_watermark
        SET last_watermark = %s::TIMESTAMP,
            last_row_count = %s,
            last_run_status = 'SUCCESS',
            updated_at = CURRENT_TIMESTAMP
        WHERE table_name = %s
    """, (new_watermark, row_count, table_name))
    conn.commit()
    conn.close()


# ── Incremental Extraction ──────────────────────────────────────────────────

def extract_incremental(spark, jdbc_url: str, table_name: str,
                         watermark: str, rds_config: dict):
    """
    Extract rows changed since the last watermark.

    LEARNING: The WHERE clause uses LAST_MODIFIED > :watermark.
    This captures:
    - Newly INSERTed rows (LAST_MODIFIED = creation time)
    - UPDATEd rows (LAST_MODIFIED = update time via trigger)
    - Does NOT capture DELETEs (need soft-delete or log-based CDC)
    """
    print(f"\n   📖 Incremental extract: {table_name}")
    print(f"      Watermark: {watermark}")

    jdbc_properties = {
        "user": rds_config["username"],
        "password": rds_config["password"],
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000",
    }

    # Use a pushdown query to filter at the PostgreSQL level
    # LEARNING: Pushdown predicates run on the SOURCE database,
    # reducing the amount of data transferred over the network.
    query = f"""
        (SELECT *
         FROM {table_name}
         WHERE last_modified > '{watermark}'::TIMESTAMP
         ORDER BY last_modified
        ) AS incremental_query
    """

    df = spark.read.jdbc(
        url=jdbc_url,
        table=query,
        properties=jdbc_properties,
    )

    count = df.count()
    print(f"      ✅ Extracted {count:,} changed rows")
    return df, count


def merge_to_bronze(spark, incremental_df, s3_bucket: str,
                     table_name: str, config: dict):
    """
    MERGE incremental data into Bronze Delta table.

    LEARNING: Delta Lake MERGE is the key operation for CDC:

    MERGE INTO bronze_table AS target
    USING incremental_data AS source
    ON target.business_key = source.business_key
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *

    This handles both:
    - UPDATE: If the row exists → update all columns
    - INSERT: If the row is new → insert it

    The merge key is the BUSINESS KEY (not surrogate key):
    - DIM_CUSTOMER: CUSTOMER_KEY (surrogate, because SCD Type 2)
    - FACT_TRANSACTION: TXN_ID (business key, unique per transaction)
    """
    from pyspark.sql.functions import current_timestamp, lit
    from delta.tables import DeltaTable

    bronze_path = f"s3a://{s3_bucket}/migration/bronze/{table_name.lower()}"
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Add Bronze metadata
    df_with_meta = incremental_df \
        .withColumn("_ingestion_timestamp", current_timestamp()) \
        .withColumn("_source_system", lit("postgresql_rds_cdc")) \
        .withColumn("_batch_id", lit(batch_id))

    # Determine merge key
    # LEARNING: Merge key selection is critical:
    # - DIM_CUSTOMER uses CUSTOMER_KEY (because SCD Type 2 creates new keys)
    # - FACT_TRANSACTION uses TXN_ID (unique business identifier)
    merge_keys = {
        "dim_date": "date_key",
        "dim_branch": "branch_id",
        "dim_account_type": "account_type_code",
        "dim_customer": "customer_key",
        "fact_transaction": "txn_id",
        "fact_daily_balance": "balance_key",
    }
    merge_key = merge_keys.get(table_name, "txn_id")

    try:
        # Try to load existing Delta table
        delta_table = DeltaTable.forPath(spark, bronze_path)

        print(f"      🔀 Merging into existing Bronze table on {merge_key}...")
        delta_table.alias("target").merge(
            df_with_meta.alias("source"),
            f"target.{merge_key} = source.{merge_key}"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

        # Get updated count
        updated_count = spark.read.format("delta").load(bronze_path).count()
        print(f"      ✅ Bronze table now has {updated_count:,} total rows")

    except Exception as e:
        # LEARNING: If Delta table doesn't exist yet, fall back to full write.
        # This happens on the very first incremental run.
        if "is not a Delta table" in str(e) or "Path does not exist" in str(e):
            print(f"      ⚠️  Bronze table not found, creating new...")
            df_with_meta.write.format("delta").mode("overwrite").save(bronze_path)
            print(f"      ✅ Bronze table created with {df_with_meta.count():,} rows")
        else:
            raise


# ── Main Pipeline ───────────────────────────────────────────────────────────

def run_incremental_load(tables: Optional[List[str]] = None,
                          show_watermarks: bool = False):
    """
    Execute the incremental CDC pipeline.

    LEARNING: Pipeline execution flow:
    1. Read watermarks (last successful extraction timestamp per table)
    2. Extract changed rows (WHERE LAST_MODIFIED > watermark)
    3. MERGE into Bronze Delta tables
    4. Update watermarks (for next run)
    5. Log metrics
    """
    rds_config = load_rds_config()
    jdbc_url = get_jdbc_url(rds_config)

    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  Incremental CDC Pipeline: RDS PostgreSQL → Bronze Delta   ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print(f"  JDBC URL: {jdbc_url}")
    print(f"  Timestamp: {datetime.now().isoformat()}")

    spark = get_spark_session()

    try:
        # Step 1: Get current watermarks
        watermarks = get_watermarks(spark, jdbc_url, rds_config)

        if show_watermarks:
            print("\n📊 Current CDC Watermarks:")
            for table, info in watermarks.items():
                print(f"   {table}: {info['last_watermark']} "
                      f"(rows: {info['last_row_count']}, "
                      f"status: {info['last_status']})")
            return

        # Step 2: Process each table
        target_tables = tables or [
            "dim_customer", "fact_transaction", "fact_daily_balance"
        ]

        metrics = {}
        for table_name in target_tables:
            if table_name not in TABLE_CONFIGS:
                print(f"   ⚠️  Unknown table: {table_name}")
                continue

            config = TABLE_CONFIGS[table_name]
            watermark_info = watermarks.get(table_name, {})
            last_watermark = watermark_info.get(
                "last_watermark", "2020-01-01 00:00:00.000000"
            )

            print(f"\n{'─'*60}")
            print(f"  📦 Table: {table_name} (CDC)")
            print(f"     Last watermark: {last_watermark}")
            print(f"{'─'*60}")

            try:
                # Extract incremental
                inc_df, row_count = extract_incremental(
                    spark, jdbc_url, table_name, last_watermark, rds_config
                )

                if row_count == 0:
                    print(f"      ℹ️  No changes since last extraction")
                    metrics[table_name] = {"status": "no_changes", "rows": 0}
                    continue

                # Get max watermark from extracted data
                from pyspark.sql.functions import max as spark_max
                new_watermark = inc_df.agg(
                    spark_max("last_modified").alias("max_ts")
                ).first()["max_ts"]
                new_watermark_str = str(new_watermark)

                # Merge to Bronze
                merge_to_bronze(spark, inc_df, rds_config["s3_bucket"],
                                 table_name, config)

                # Update watermark
                update_watermark(spark, jdbc_url, rds_config,
                                  table_name, new_watermark_str, row_count)

                metrics[table_name] = {
                    "status": "success",
                    "rows_extracted": row_count,
                    "new_watermark": new_watermark_str,
                }

            except Exception as e:
                print(f"   ❌ FAILED: {e}")
                metrics[table_name] = {"status": "failed", "error": str(e)}

        # Save metrics
        metrics_path = PROJECT_ROOT / "data" / "incremental_metrics.json"
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        with open(metrics_path, "w") as f:
            json.dump({
                "pipeline": "incremental_cdc",
                "timestamp": datetime.now().isoformat(),
                "tables": metrics,
            }, f, indent=2, default=str)

        print(f"\n📊 Metrics saved to: {metrics_path}")

        # Summary
        total = sum(
            m.get("rows_extracted", 0) for m in metrics.values()
            if m.get("status") == "success"
        )
        print(f"\n✅ Incremental load complete! Rows processed: {total:,}")

    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(
        description="Incremental CDC pipeline: RDS → Bronze Delta",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
LEARNING — CDC INCREMENTAL PROCESS:
  1. Reads last watermark from CDC_WATERMARK table
  2. Queries RDS: WHERE LAST_MODIFIED > :last_watermark
  3. MERGEs changed rows into Bronze Delta table
  4. Updates watermark to MAX(LAST_MODIFIED) of extracted batch
  5. Next run will pick up from the new watermark

This is the DAILY pipeline that runs after rds_daily_generator.py
simulates new banking activity.
        """
    )
    parser.add_argument("--tables", nargs="+",
                        help="Specific tables to process")
    parser.add_argument("--show-watermarks", action="store_true",
                        help="Display current watermarks and exit")

    args = parser.parse_args()
    run_incremental_load(tables=args.tables,
                          show_watermarks=args.show_watermarks)


if __name__ == "__main__":
    main()
