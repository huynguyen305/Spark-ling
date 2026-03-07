"""
Full Load Migration Pipeline: RDS PostgreSQL → Databricks Bronze
================================================================
Performs the initial full extraction of all tables from PostgreSQL RDS
and loads them into Databricks as Bronze (raw) Delta tables via S3.

LEARNING NOTES:
---------------
1. FULL LOAD vs INCREMENTAL:
   - Full load: Extract ALL rows from source (used for initial migration)
   - Incremental: Extract only CHANGED rows (used for daily updates)
   - This pipeline does FULL LOAD — run it once at the start of migration.

2. THE PIPELINE FLOW:
   ┌────────────┐    JDBC     ┌────────────┐   S3 Write   ┌──────────────┐
   │ PostgreSQL │──────────►  │  Spark     │─────────────► │ S3 Landing   │
   │ RDS        │             │  (local)   │               │ Zone (Parquet│
   └────────────┘             └────────────┘               └──────┬───────┘
                                                                  │
                                                        Databricks COPY INTO
                                                                  │
                                                           ┌──────▼───────┐
                                                           │  Databricks  │
                                                           │  Bronze      │
                                                           │  (Delta)     │
                                                           └──────────────┘

3. WHY S3 AS INTERMEDIATE?
   - JDBC direct to Databricks is slow for large tables (500K+ rows)
   - S3 is cheap, fast storage that both Spark and Databricks can access
   - Parquet format preserves schema and is columnar (fast reads)
   - In production, AWS DMS does this automatically

4. SPARK JDBC:
   spark.read.jdbc() parallelizes reads using partitionColumn:
   - Splits the table into N partitions for parallel extraction
   - Each partition reads a range of the partitionColumn
   - numPartitions=8 means 8 concurrent JDBC connections

5. ORACLE vs POSTGRESQL JDBC:
   - Oracle:      jdbc:oracle:thin:@host:1521/SERVICE  (driver: oracle.jdbc.OracleDriver)
   - PostgreSQL:  jdbc:postgresql://host:5432/dbname   (driver: org.postgresql.Driver)
   - The Spark pipeline logic is IDENTICAL — only the URL and driver change!

6. VALIDATION:
   After loading, we compare row counts between RDS and Bronze.
   Any mismatch indicates data loss during migration.

USAGE:
    # Full load all tables:
    python pipelines/migration/full_load_pipeline.py

    # Full load specific tables:
    python pipelines/migration/full_load_pipeline.py --tables dim_customer fact_transaction

    # Validate only (compare counts):
    python pipelines/migration/full_load_pipeline.py --validate-only
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "configs"))
sys.path.insert(0, str(PROJECT_ROOT / "src"))


# ── Configuration ────────────────────────────────────────────────────────────

def load_rds_config() -> dict:
    """Load RDS connection configuration."""
    env_file = PROJECT_ROOT / "aws" / ".env"
    config = {}
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    config[key.strip()] = value.strip()

    return {
        "host": os.environ.get("RDS_HOST", config.get("RDS_HOST", "")),
        "port": os.environ.get("RDS_PORT", config.get("RDS_PORT", "5432")),
        "database": os.environ.get("RDS_DATABASE", config.get("RDS_DATABASE", "sparkdb")),
        "username": os.environ.get("RDS_USERNAME", config.get("RDS_USERNAME", "sparkadmin")),
        "password": os.environ.get("RDS_PASSWORD", config.get("RDS_PASSWORD", "")),
        "s3_bucket": os.environ.get("S3_BUCKET", config.get("S3_BUCKET", "sparkling-data-test")),
    }


# Table metadata: defines extraction strategy per table
# LEARNING: Each table has different characteristics that affect extraction:
# - partition_column: Column used to parallelize JDBC reads
# - num_partitions: Number of parallel JDBC connections
# - is_dimension: Dimensions are small; facts are large
TABLE_CONFIGS = {
    "dim_date": {
        "partition_column": "date_key",
        "num_partitions": 4,
        "is_dimension": True,
        "description": "Calendar dimension (2020-2026)",
    },
    "dim_branch": {
        "partition_column": "branch_key",
        "num_partitions": 2,
        "is_dimension": True,
        "description": "Bank branch locations across Vietnam",
    },
    "dim_account_type": {
        "partition_column": "account_type_key",
        "num_partitions": 1,
        "is_dimension": True,
        "description": "Account type reference data",
    },
    "dim_customer": {
        "partition_column": "customer_key",
        "num_partitions": 8,
        "is_dimension": True,
        "description": "Customer master with SCD Type 2 history",
    },
    "fact_transaction": {
        "partition_column": "txn_key",
        "num_partitions": 16,
        "is_dimension": False,
        "description": "Banking transactions (largest table)",
    },
    "fact_daily_balance": {
        "partition_column": "balance_key",
        "num_partitions": 8,
        "is_dimension": False,
        "description": "End-of-day account balance snapshots",
    },
}


# ── Pipeline Steps ──────────────────────────────────────────────────────────

def get_jdbc_url(config: dict) -> str:
    """
    Build PostgreSQL JDBC URL.

    LEARNING: JDBC URL formats by database:
      - PostgreSQL: jdbc:postgresql://host:5432/dbname
      - Oracle:     jdbc:oracle:thin:@host:1521/service
      - MySQL:      jdbc:mysql://host:3306/dbname
    The format differs, but Spark's API is identical for all.
    """
    return f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database']}"


def get_spark_session():
    """
    Create SparkSession with JDBC and S3 support.

    LEARNING: Key Spark configs for this pipeline:
    - postgresql driver: Open-source, no license needed (unlike Oracle ojdbc)
    - hadoop-aws: Enables Spark to write to S3
    - fs.s3a.*: S3 authentication (uses EC2 instance profile or env vars)
    """
    from pyspark.sql import SparkSession

    builder = SparkSession.builder \
        .appName("FullLoadMigration-RDS-to-Bronze") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.jars.packages",
                "org.postgresql:postgresql:42.7.3,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # S3 credentials (from environment or IAM role)
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        builder = builder \
            .config("spark.hadoop.fs.s3a.access.key",
                    os.environ["AWS_ACCESS_KEY_ID"]) \
            .config("spark.hadoop.fs.s3a.secret.key",
                    os.environ["AWS_SECRET_ACCESS_KEY"]) \
            .config("spark.hadoop.fs.s3a.endpoint",
                    f"s3.{os.environ.get('AWS_REGION', 'ap-southeast-1')}.amazonaws.com")
    else:
        # Use Default credentials chain (works with ~/.aws/credentials, ENV vars, or EC2/EMR roles)
        builder = builder \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

    return builder.getOrCreate()


def extract_table(spark, jdbc_url: str, table_name: str,
                  config: dict, rds_config: dict) -> "DataFrame":
    """
    Extract a table from PostgreSQL RDS using JDBC.

    LEARNING: spark.read.jdbc() parameters:
    - url: JDBC connection string
    - table: Table name (or a subquery in parentheses)
    - properties: JDBC connection properties (user, password, driver)
    - partitionColumn: Column to split parallel reads on
    - lowerBound/upperBound: Range of partitionColumn values
    - numPartitions: Number of parallel JDBC connections

    IMPORTANT: lowerBound/upperBound don't filter data!
    They only determine how to split the partitionColumn range.
    All rows are always extracted.

    DRIVER COMPARISON:
    - Oracle:      oracle.jdbc.OracleDriver  (requires ojdbc11.jar, license!)
    - PostgreSQL:  org.postgresql.Driver      (open-source, free)
    """
    print(f"\n   📖 Extracting {table_name}...")
    print(f"      Partitions: {config['num_partitions']}, "
          f"Column: {config['partition_column']}")

    jdbc_properties = {
        "user": rds_config["username"],
        "password": rds_config["password"],
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000",  # LEARNING: Larger fetchsize = fewer round-trips
    }

    # Get bounds for partitioning
    # LEARNING: We query min/max of the partition column to determine bounds.
    # Without this, Spark would use a single JDBC connection (very slow).
    bounds_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"(SELECT MIN({config['partition_column']}) as min_val, "
              f"MAX({config['partition_column']}) as max_val "
              f"FROM {table_name}) AS bounds_query",
        properties=jdbc_properties,
    )
    bounds = bounds_df.first()
    lower = bounds["min_val"] or 0
    upper = bounds["max_val"] or 1

    # Read with parallel partitions
    df = spark.read.jdbc(
        url=jdbc_url,
        table=table_name,
        column=config["partition_column"],
        lowerBound=int(lower),
        upperBound=int(upper) + 1,
        numPartitions=config["num_partitions"],
        properties=jdbc_properties,
    )

    count = df.count()
    print(f"      ✅ Extracted {count:,} rows")
    return df


def load_to_s3_landing(df, s3_bucket: str, table_name: str,
                        is_dimension: bool):
    """
    Write extracted data to S3 landing zone as Parquet.

    LEARNING: Landing zone is "raw" storage — data exactly as extracted.
    - Dimensions: overwrite (small, always full-refreshed)
    - Facts: append with date partitioning (for incremental support)
    - Parquet format: columnar, compressed, schema-preserving
    """
    s3_path = f"s3a://{s3_bucket}/migration/landing/{table_name.lower()}"
    print(f"      📤 Writing to {s3_path}")

    if is_dimension:
        # LEARNING: Dimensions are small enough to overwrite entirely
        df.write.mode("overwrite").parquet(s3_path)
    else:
        # LEARNING: Facts get date-partitioned for efficient downstream reads
        if "txn_date_key" in df.columns:
            df.write.mode("overwrite").partitionBy("txn_date_key").parquet(s3_path)
        elif "date_key" in df.columns:
            df.write.mode("overwrite").partitionBy("date_key").parquet(s3_path)
        else:
            df.write.mode("overwrite").parquet(s3_path)

    print(f"      ✅ Landed to S3")


def register_bronze_delta(spark, s3_bucket: str, table_name: str):
    """
    Convert landing Parquet to Bronze Delta table.

    LEARNING: Why Delta format?
    1. ACID transactions — no partial writes
    2. Time travel — query historical versions
    3. Schema evolution — add columns without rewriting
    4. MERGE support — upsert for incremental loads
    5. Z-ordering — optimize query performance

    In Databricks, you'd use CREATE TABLE ... USING DELTA.
    Here we write Delta locally for the simulation.
    """
    landing_path = f"s3a://{s3_bucket}/migration/landing/{table_name.lower()}"
    bronze_path = f"s3a://{s3_bucket}/migration/bronze/{table_name.lower()}"

    print(f"      🔄 Converting to Bronze Delta: {bronze_path}")

    df = spark.read.parquet(landing_path)

    # Add Bronze metadata columns
    # LEARNING: Bronze tables always include:
    # - _ingestion_timestamp: When this row was loaded
    # - _source_system: Which system the data came from
    # - _batch_id: For tracking which pipeline run loaded it
    from pyspark.sql.functions import current_timestamp, lit
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    df_bronze = df \
        .withColumn("_ingestion_timestamp", current_timestamp()) \
        .withColumn("_source_system", lit("postgresql_rds")) \
        .withColumn("_batch_id", lit(batch_id))

    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .save(bronze_path)

    count = df_bronze.count()
    print(f"      ✅ Bronze table created: {count:,} rows")
    return count


def validate_migration(spark, jdbc_url: str, rds_config: dict,
                        s3_bucket: str) -> dict:
    """
    Compare row counts between RDS source and Bronze destination.

    LEARNING: Data validation is NON-NEGOTIABLE in migrations.
    At minimum, verify:
    1. Row counts match (no data loss)
    2. Column counts match (no schema drift)
    3. Key columns are non-null
    4. Checksums match (no data corruption) — advanced
    """
    print("\n🔍 Validating migration (source vs Bronze counts)...")

    jdbc_properties = {
        "user": rds_config["username"],
        "password": rds_config["password"],
        "driver": "org.postgresql.Driver",
    }

    results = {}
    for table_name in TABLE_CONFIGS:
        try:
            # Source count
            count_df = spark.read.jdbc(
                url=jdbc_url,
                table=f"(SELECT COUNT(*) as cnt FROM {table_name}) AS q",
                properties=jdbc_properties,
            )
            source_count = count_df.first()["cnt"]

            # Bronze count
            bronze_path = f"s3a://{s3_bucket}/migration/bronze/{table_name.lower()}"
            try:
                bronze_df = spark.read.format("delta").load(bronze_path)
                bronze_count = bronze_df.count()
            except Exception:
                bronze_count = 0

            match = "✅" if source_count == bronze_count else "❌ MISMATCH"
            print(f"   {match} {table_name}: "
                  f"RDS={source_count:,} → Bronze={bronze_count:,}")

            results[table_name] = {
                "source_count": source_count,
                "bronze_count": bronze_count,
                "match": source_count == bronze_count,
            }
        except Exception as e:
            print(f"   ❌ {table_name}: {e}")
            results[table_name] = {"error": str(e)}

    return results


# ── Main ────────────────────────────────────────────────────────────────────

def run_full_load(tables: Optional[List[str]] = None,
                  validate_only: bool = False):
    """
    Execute the full load pipeline.

    LEARNING: Pipeline execution order matters:
    1. Dimensions FIRST (they're referenced by facts)
    2. Facts SECOND (they reference dimensions)
    3. Validate LAST (after all tables are loaded)
    """
    rds_config = load_rds_config()
    jdbc_url = get_jdbc_url(rds_config)

    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  Full Load Pipeline: RDS PostgreSQL → Databricks Bronze    ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print(f"  JDBC URL: {jdbc_url}")
    print(f"  S3 Bucket: {rds_config['s3_bucket']}")
    print(f"  Timestamp: {datetime.now().isoformat()}")

    spark = get_spark_session()

    try:
        if validate_only:
            validate_migration(spark, jdbc_url, rds_config,
                               rds_config["s3_bucket"])
            return

        # Determine which tables to process
        target_tables = tables or list(TABLE_CONFIGS.keys())

        # Sort: dimensions first, then facts
        target_tables.sort(
            key=lambda t: (0 if TABLE_CONFIGS[t]["is_dimension"] else 1, t)
        )

        print(f"\n  Tables to extract: {', '.join(target_tables)}")

        metrics = {}
        for table_name in target_tables:
            config = TABLE_CONFIGS[table_name]
            print(f"\n{'─'*60}")
            print(f"  📦 Table: {table_name}")
            print(f"     {config['description']}")
            print(f"{'─'*60}")

            try:
                # Step 1: Extract from RDS
                df = extract_table(spark, jdbc_url, table_name,
                                   config, rds_config)

                # Step 2: Land to S3
                load_to_s3_landing(df, rds_config["s3_bucket"],
                                    table_name, config["is_dimension"])

                # Step 3: Convert to Bronze Delta
                count = register_bronze_delta(spark, rds_config["s3_bucket"],
                                               table_name)

                metrics[table_name] = {
                    "status": "success",
                    "row_count": count,
                    "timestamp": datetime.now().isoformat(),
                }

            except Exception as e:
                print(f"   ❌ FAILED: {e}")
                metrics[table_name] = {
                    "status": "failed",
                    "error": str(e),
                }

        # Validate
        print("\n" + "=" * 60)
        validation = validate_migration(spark, jdbc_url, rds_config,
                                         rds_config["s3_bucket"])

        # Save metrics
        metrics_path = PROJECT_ROOT / "data" / "migration_metrics.json"
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        with open(metrics_path, "w") as f:
            json.dump({
                "pipeline": "full_load",
                "timestamp": datetime.now().isoformat(),
                "tables": metrics,
                "validation": {k: v for k, v in validation.items()},
            }, f, indent=2, default=str)
        print(f"\n📊 Metrics saved to: {metrics_path}")

        # Summary
        print("\n" + "=" * 60)
        total_rows = sum(
            m.get("row_count", 0) for m in metrics.values()
            if m.get("status") == "success"
        )
        print(f"✅ Full load complete! Total rows migrated: {total_rows:,}")
        print("=" * 60)

    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(
        description="Full load migration: RDS PostgreSQL → Databricks Bronze",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
LEARNING — FULL LOAD PROCESS:
  1. Connects to PostgreSQL RDS via JDBC
  2. Extracts each table in parallel (using partitionColumn)
  3. Writes to S3 landing zone as Parquet
  4. Converts to Bronze Delta tables (adds metadata)
  5. Validates row counts match between source and destination

EXAMPLES:
  # Full load all tables:
  python pipelines/migration/full_load_pipeline.py

  # Load specific tables:
  python pipelines/migration/full_load_pipeline.py --tables dim_customer fact_transaction

  # Validate existing migration:
  python pipelines/migration/full_load_pipeline.py --validate-only
        """
    )
    parser.add_argument("--tables", nargs="+",
                        help="Specific tables to extract (default: all)")
    parser.add_argument("--validate-only", action="store_true",
                        help="Only validate counts, don't extract")

    args = parser.parse_args()
    run_full_load(tables=args.tables, validate_only=args.validate_only)


if __name__ == "__main__":
    main()
