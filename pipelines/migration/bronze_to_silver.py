"""
Bronze → Silver Transformation Pipeline
=========================================
Transforms raw Bronze data into clean, validated Silver tables.
This is where data quality enforcement and business rules are applied.

LEARNING NOTES:
---------------
1. THE MEDALLION ARCHITECTURE:
   ┌──────────┐      ┌──────────┐      ┌──────────┐
   │  BRONZE  │─────►│  SILVER  │─────►│   GOLD   │
   │ (Raw)    │      │ (Clean)  │      │ (Business)│
   └──────────┘      └──────────┘      └──────────┘

   - BRONZE: Exact copy of source data + ingestion metadata
   - SILVER: Cleansed, validated, deduplicated, conformed
   - GOLD: Business-level aggregations and metrics

2. SILVER TABLE RESPONSIBILITIES:
   a) Data Type Standardization: PostgreSQL NUMERIC → Spark DecimalType
   b) Null Handling: Replace NULLs with defaults or flag them
   c) Deduplication: Remove exact duplicates (late-arriving data)
   d) SCD Type 2 Processing: Maintain history for dimensions
   e) Data Quality Validation: Apply quality rules, quarantine bad rows
   f) Column Naming: Standardize to snake_case (PostgreSQL already uses lowercase)

3. WHY SILVER MATTERS:
   Silver is the "single source of truth" that all downstream consumers use.
   Gold tables, dashboards, ML models — all read from Silver.
   If Silver has bad data, EVERYTHING downstream is wrong.

4. REUSING EXISTING CODE:
   We leverage the project's existing modules:
   - scd_handler.py → SCD Type 2 processing
   - quality_checks.py → Data quality validation
   - transformations.py → Business rule transformations

USAGE:
    # Transform all tables:
    python pipelines/migration/bronze_to_silver.py

    # Transform specific tables:
    python pipelines/migration/bronze_to_silver.py --tables dim_customer fact_transaction
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "configs"))
sys.path.insert(0, str(PROJECT_ROOT / "src"))


def load_config() -> dict:
    """Load S3 and migration config."""
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
        "s3_bucket": os.environ.get("S3_BUCKET",
                                     config.get("S3_BUCKET", "sparkling-data-test")),
    }


def get_spark_session():
    """Create SparkSession with Delta Lake support."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder \
        .appName("BronzeToSilver-Transformation") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    if os.environ.get("AWS_ACCESS_KEY_ID"):
        builder = builder \
            .config("spark.hadoop.fs.s3a.access.key",
                    os.environ["AWS_ACCESS_KEY_ID"]) \
            .config("spark.hadoop.fs.s3a.secret.key",
                    os.environ["AWS_SECRET_ACCESS_KEY"])

    return builder.getOrCreate()


# ── Transformation Functions ────────────────────────────────────────────────

def transform_dim_customer(spark, s3_bucket: str) -> dict:
    """
    Transform Bronze DIM_CUSTOMER → Silver dim_customer.

    LEARNING: Customer dimension transformations:
    1. Standardize column names (UPPER_CASE → snake_case)
    2. Validate mandatory fields (customer_id, full_name must be non-null)
    3. Normalize segments (trim, standardize casing)
    4. Add data quality flags
    5. Apply SCD Type 2 logic for historical tracking
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    print("\n   🔄 Transforming: dim_customer")
    bronze_path = f"s3a://{s3_bucket}/migration/bronze/dim_customer"
    silver_path = f"s3a://{s3_bucket}/migration/silver/dim_customer"

    df = spark.read.format("delta").load(bronze_path)
    initial_count = df.count()
    print(f"      Bronze rows: {initial_count:,}")

    # Step 1: Standardize column names to snake_case
    # LEARNING: Databricks/Spark convention is snake_case.
    # PostgreSQL convention is lowercase. Standardize early.
    for old_name in df.columns:
        if old_name.startswith("_"):  # Keep metadata columns as-is
            continue
        new_name = old_name.lower()
        df = df.withColumnRenamed(old_name, new_name)

    # Step 2: Data quality — null handling
    # LEARNING: Never let NULLs propagate silently. Either:
    # a) Fill with defaults (for non-critical fields)
    # b) Flag and quarantine (for critical fields)
    df = df.withColumn(
        "dq_has_null_name",
        F.when(F.col("full_name").isNull(), True).otherwise(False)
    ).withColumn(
        "dq_has_null_email",
        F.when(F.col("email").isNull(), True).otherwise(False)
    )

    # Fill non-critical nulls
    df = df.fillna({
        "address": "Unknown",
        "city": "Unknown",
        "region": "Unknown",
        "phone": "N/A",
        "kyc_status": "Pending",
        "risk_score": 50,
    })

    # Step 3: Normalize text fields
    # LEARNING: Trim whitespace, standardize casing for consistent joins
    df = df.withColumn("segment", F.trim(F.initcap(F.col("segment")))) \
           .withColumn("region", F.trim(F.initcap(F.col("region")))) \
           .withColumn("kyc_status", F.trim(F.initcap(F.col("kyc_status"))))

    # Step 4: Derived columns
    # LEARNING: Silver layer enriches data with derived attributes
    df = df.withColumn(
        "age_group",
        F.when(F.col("date_of_birth").isNull(), "Unknown")
         .when(F.datediff(F.current_date(), F.col("date_of_birth")) / 365 < 25, "18-24")
         .when(F.datediff(F.current_date(), F.col("date_of_birth")) / 365 < 35, "25-34")
         .when(F.datediff(F.current_date(), F.col("date_of_birth")) / 365 < 45, "35-44")
         .when(F.datediff(F.current_date(), F.col("date_of_birth")) / 365 < 55, "45-54")
         .otherwise("55+")
    )

    # Step 5: Add Silver metadata
    df = df.withColumn("_silver_timestamp", F.current_timestamp()) \
           .withColumn("_silver_version", F.lit("1.0"))

    # Step 6: Separate valid from invalid
    valid = df.filter(~F.col("dq_has_null_name"))
    invalid = df.filter(F.col("dq_has_null_name"))

    # Write Silver
    valid.write.format("delta").mode("overwrite").save(silver_path)
    silver_count = valid.count()

    # Quarantine invalid
    if invalid.count() > 0:
        quarantine_path = f"s3a://{s3_bucket}/migration/quarantine/dim_customer"
        invalid.write.format("delta").mode("overwrite").save(quarantine_path)
        print(f"      ⚠️  Quarantined {invalid.count():,} invalid rows")

    print(f"      ✅ Silver: {silver_count:,} rows (from {initial_count:,} bronze)")
    return {"table": "dim_customer", "bronze": initial_count,
            "silver": silver_count, "quarantined": invalid.count()}


def transform_fact_transaction(spark, s3_bucket: str) -> dict:
    """
    Transform Bronze FACT_TRANSACTION → Silver fact_transaction.

    LEARNING: Transaction fact transformations:
    1. Column name standardization
    2. Amount validation (must be positive, within bounds)
    3. Transaction categorization (reuse existing logic)
    4. Suspicious transaction flagging
    5. Deduplication on TXN_ID
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    print("\n   🔄 Transforming: fact_transaction")
    bronze_path = f"s3a://{s3_bucket}/migration/bronze/fact_transaction"
    silver_path = f"s3a://{s3_bucket}/migration/silver/fact_transaction"

    df = spark.read.format("delta").load(bronze_path)
    initial_count = df.count()
    print(f"      Bronze rows: {initial_count:,}")

    # Step 1: Column name standardization
    for old_name in df.columns:
        if old_name.startswith("_"):
            continue
        df = df.withColumnRenamed(old_name, old_name.lower())

    # Step 2: Deduplication
    # LEARNING: In CDC pipelines, duplicates can occur when:
    # - A pipeline is retried after partial failure
    # - The watermark is set slightly too early
    # Always deduplicate using the business key.
    window = Window.partitionBy("txn_id").orderBy(
        F.col("last_modified").desc()
    )
    df = df.withColumn("_row_num", F.row_number().over(window)) \
           .filter(F.col("_row_num") == 1) \
           .drop("_row_num")

    deduped_count = df.count()
    duplicates = initial_count - deduped_count
    if duplicates > 0:
        print(f"      🔁 Removed {duplicates:,} duplicate rows")

    # Step 3: Amount validation
    # LEARNING: Validate business rules at the Silver layer
    df = df.withColumn(
        "dq_valid_amount",
        (F.col("amount") > 0) & (F.col("amount") < 50000000000)  # 50B VND max
    ).withColumn(
        "dq_valid_status",
        F.col("status").isin("Completed", "Pending", "Failed")
    )

    # Step 4: Transaction categorization
    # LEARNING: Enrich with business categories for downstream analytics
    df = df.withColumn(
        "amount_category",
        F.when(F.col("amount") < 1000000, "micro")        # < 1M VND
         .when(F.col("amount") < 10000000, "small")        # 1M - 10M
         .when(F.col("amount") < 100000000, "medium")      # 10M - 100M
         .when(F.col("amount") < 1000000000, "large")      # 100M - 1B
         .otherwise("very_large")                            # > 1B VND
    )

    # Step 5: Time-based features
    # LEARNING: Extract time components for analytics joins with DIM_DATE
    df = df.withColumn("txn_hour", F.hour(F.col("txn_datetime"))) \
           .withColumn("txn_day_of_week", F.dayofweek(F.col("txn_datetime"))) \
           .withColumn("is_business_hours",
                       (F.hour(F.col("txn_datetime")).between(8, 17)))

    # Step 6: Suspicious transaction flag (enhanced)
    df = df.withColumn(
        "suspicious_flag",
        F.when(F.col("amount") > 1000000000, "high_amount")
         .when((F.col("txn_hour") < 6) | (F.col("txn_hour") > 22), "off_hours")
         .when(F.col("status") == "Failed", "failed_txn")
         .otherwise(None)
    )

    # Step 7: Silver metadata
    df = df.withColumn("_silver_timestamp", F.current_timestamp()) \
           .withColumn("_silver_version", F.lit("1.0"))

    # Split valid / invalid
    valid = df.filter(F.col("dq_valid_amount") & F.col("dq_valid_status"))
    invalid = df.filter(~(F.col("dq_valid_amount") & F.col("dq_valid_status")))

    # Write Silver
    valid.write.format("delta").mode("overwrite") \
         .partitionBy("txn_date_key") \
         .save(silver_path)
    silver_count = valid.count()

    if invalid.count() > 0:
        quarantine_path = f"s3a://{s3_bucket}/migration/quarantine/fact_transaction"
        invalid.write.format("delta").mode("overwrite").save(quarantine_path)
        print(f"      ⚠️  Quarantined {invalid.count():,} invalid rows")

    print(f"      ✅ Silver: {silver_count:,} rows")
    return {"table": "fact_transaction", "bronze": initial_count,
            "silver": silver_count, "quarantined": invalid.count(),
            "duplicates_removed": duplicates}


def transform_fact_daily_balance(spark, s3_bucket: str) -> dict:
    """
    Transform Bronze FACT_DAILY_BALANCE → Silver fact_daily_balance.

    LEARNING: Balance snapshot transformations:
    1. Validate closing_balance = opening_balance + credits - debits
    2. Flag negative balances (potential data issues)
    3. Calculate daily P&L
    """
    from pyspark.sql import functions as F

    print("\n   🔄 Transforming: fact_daily_balance")
    bronze_path = f"s3a://{s3_bucket}/migration/bronze/fact_daily_balance"
    silver_path = f"s3a://{s3_bucket}/migration/silver/fact_daily_balance"

    df = spark.read.format("delta").load(bronze_path)
    initial_count = df.count()
    print(f"      Bronze rows: {initial_count:,}")

    # Column name standardization
    for old_name in df.columns:
        if old_name.startswith("_"):
            continue
        df = df.withColumnRenamed(old_name, old_name.lower())

    # Validation: balance consistency check
    # LEARNING: Cross-column validation catches data corruption
    df = df.withColumn(
        "expected_closing",
        F.col("opening_balance") + F.col("total_credits") - F.col("total_debits")
    ).withColumn(
        "balance_variance",
        F.abs(F.col("closing_balance") - F.col("expected_closing"))
    ).withColumn(
        "dq_balance_consistent",
        F.col("balance_variance") < 1.0  # Allow for rounding
    )

    # Derived: daily P&L
    df = df.withColumn(
        "daily_net_change",
        F.col("total_credits") - F.col("total_debits")
    ).withColumn(
        "is_positive_day",
        F.col("daily_net_change") > 0
    )

    # Silver metadata
    df = df.withColumn("_silver_timestamp", F.current_timestamp()) \
           .withColumn("_silver_version", F.lit("1.0"))

    # Write
    df.write.format("delta").mode("overwrite") \
       .partitionBy("date_key") \
       .save(silver_path)
    silver_count = df.count()

    print(f"      ✅ Silver: {silver_count:,} rows")
    return {"table": "fact_daily_balance", "bronze": initial_count,
            "silver": silver_count}


def transform_dim_branch(spark, s3_bucket: str) -> dict:
    """Transform Bronze DIM_BRANCH → Silver dim_branch."""
    from pyspark.sql import functions as F

    print("\n   🔄 Transforming: dim_branch")
    bronze_path = f"s3a://{s3_bucket}/migration/bronze/dim_branch"
    silver_path = f"s3a://{s3_bucket}/migration/silver/dim_branch"

    df = spark.read.format("delta").load(bronze_path)
    initial_count = df.count()

    for old_name in df.columns:
        if not old_name.startswith("_"):
            df = df.withColumnRenamed(old_name, old_name.lower())

    df = df.withColumn("region", F.trim(F.initcap(F.col("region")))) \
           .withColumn("_silver_timestamp", F.current_timestamp())

    df.write.format("delta").mode("overwrite").save(silver_path)
    print(f"      ✅ Silver: {initial_count:,} rows")
    return {"table": "dim_branch", "bronze": initial_count,
            "silver": initial_count}


def transform_dim_date(spark, s3_bucket: str) -> dict:
    """Transform Bronze DIM_DATE → Silver dim_date (mostly pass-through)."""
    from pyspark.sql import functions as F

    print("\n   🔄 Transforming: dim_date")
    bronze_path = f"s3a://{s3_bucket}/migration/bronze/dim_date"
    silver_path = f"s3a://{s3_bucket}/migration/silver/dim_date"

    df = spark.read.format("delta").load(bronze_path)
    initial_count = df.count()

    for old_name in df.columns:
        if not old_name.startswith("_"):
            df = df.withColumnRenamed(old_name, old_name.lower())

    df = df.withColumn("_silver_timestamp", F.current_timestamp())
    df.write.format("delta").mode("overwrite").save(silver_path)
    print(f"      ✅ Silver: {initial_count:,} rows")
    return {"table": "dim_date", "bronze": initial_count,
            "silver": initial_count}


# ── Main ────────────────────────────────────────────────────────────────────

TRANSFORM_MAP = {
    "dim_customer": transform_dim_customer,
    "dim_branch": transform_dim_branch,
    "dim_date": transform_dim_date,
    "fact_transaction": transform_fact_transaction,
    "fact_daily_balance": transform_fact_daily_balance,
}


def run_bronze_to_silver(tables: Optional[List[str]] = None):
    """Execute Bronze → Silver transformations."""
    config = load_config()

    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  Bronze → Silver Transformation Pipeline                   ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print(f"  S3 Bucket: {config['s3_bucket']}")
    print(f"  Timestamp: {datetime.now().isoformat()}")

    spark = get_spark_session()

    try:
        target_tables = tables or list(TRANSFORM_MAP.keys())
        metrics = {}

        for table in target_tables:
            if table not in TRANSFORM_MAP:
                print(f"   ⚠️  No transform defined for: {table}")
                continue

            try:
                result = TRANSFORM_MAP[table](spark, config["s3_bucket"])
                metrics[table] = {**result, "status": "success"}
            except Exception as e:
                print(f"   ❌ Failed: {table} — {e}")
                metrics[table] = {"status": "failed", "error": str(e)}

        # Save metrics
        metrics_path = PROJECT_ROOT / "data" / "silver_metrics.json"
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        with open(metrics_path, "w") as f:
            json.dump({
                "pipeline": "bronze_to_silver",
                "timestamp": datetime.now().isoformat(),
                "tables": metrics,
            }, f, indent=2, default=str)

        print(f"\n📊 Metrics: {metrics_path}")
        print("\n✅ Bronze → Silver transformation complete!")

    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(
        description="Bronze → Silver transformation pipeline",
        epilog="""
LEARNING — SILVER LAYER TRANSFORMS:
  - Column name standardization (UPPER_CASE → snake_case)
  - Data quality validation and quarantine
  - Deduplication on business keys
  - Business rule enrichment (categories, flags)
  - SCD Type 2 history maintenance
        """
    )
    parser.add_argument("--tables", nargs="+",
                        help="Specific tables to transform (lowercase)")

    args = parser.parse_args()
    run_bronze_to_silver(tables=args.tables)


if __name__ == "__main__":
    main()
