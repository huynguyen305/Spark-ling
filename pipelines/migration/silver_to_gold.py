"""
Silver → Gold Aggregation Pipeline
====================================
Creates business-level aggregations from Silver tables.
Gold tables are optimized for analytics, dashboards, and reporting.

LEARNING NOTES:
---------------
1. GOLD LAYER PURPOSE:
   Gold tables answer specific business questions:
   - "What's the daily revenue by branch?" → gold_daily_branch_summary
   - "What does each customer's full profile look like?" → gold_customer_360
   - "What are the monthly balance trends?" → gold_monthly_balance_trends
   - "Which transactions need investigation?" → gold_risk_alerts

2. DESIGN PRINCIPLES:
   a) Pre-aggregated: Avoid expensive JOINs at query time
   b) Denormalized: Flat tables that combine dims + facts
   c) Business-aligned: Named after business concepts, not technical tables
   d) Slowly changing: Updated daily/hourly, not real-time

3. WHO CONSUMES GOLD:
   - BI dashboards (Power BI, Tableau, Databricks SQL)
   - Data scientists (ML feature stores)
   - Business analysts (ad-hoc SQL queries)
   - Downstream applications (APIs, alerts)

4. AT TECHCOMBANK:
   Gold tables would feed into:
   - Daily management dashboards
   - Regulatory reporting (SBV, Basel III)
   - Customer 360 for relationship managers
   - Anti-money laundering (AML) alerts

USAGE:
    python pipelines/migration/silver_to_gold.py
    python pipelines/migration/silver_to_gold.py --tables daily_branch_summary customer_360
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


def load_config() -> dict:
    """Load S3 config."""
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
    """Create SparkSession for Gold layer processing."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder \
        .appName("SilverToGold-Aggregation") \
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


# ── Gold Table Builders ─────────────────────────────────────────────────────

def build_daily_branch_summary(spark, s3_bucket: str) -> dict:
    """
    Gold: Daily transaction summary by branch and region.

    LEARNING: This is a classic "aggregate fact" table:
    - Grain: One row per branch per day
    - Measures: total_amount, txn_count, avg_amount
    - Joined with dim_branch for region context

    SQL equivalent:
      SELECT b.region, b.branch_name, t.txn_date_key,
             SUM(t.amount) as total_amount,
             COUNT(*) as txn_count,
             AVG(t.amount) as avg_amount
      FROM silver.fact_transaction t
      JOIN silver.dim_branch b ON t.branch_id = b.branch_id
      GROUP BY b.region, b.branch_name, t.txn_date_key
    """
    from pyspark.sql import functions as F

    print("\n   🏆 Building: gold_daily_branch_summary")

    silver_txn = spark.read.format("delta").load(
        f"s3a://{s3_bucket}/migration/silver/fact_transaction"
    )
    silver_branch = spark.read.format("delta").load(
        f"s3a://{s3_bucket}/migration/silver/dim_branch"
    )

    # Join transactions with branches
    # LEARNING: Star schema JOIN — fact table joins dimension on business key
    gold = silver_txn.alias("t").join(
        silver_branch.alias("b"),
        F.col("t.branch_id") == F.col("b.branch_id"),
        "left"
    ).groupBy(
        F.col("b.region"),
        F.col("b.branch_name"),
        F.col("b.branch_id"),
        F.col("t.txn_date_key"),
    ).agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("txn_count"),
        F.avg("amount").alias("avg_amount"),
        F.max("amount").alias("max_amount"),
        F.min("amount").alias("min_amount"),
        F.sum(F.when(F.col("t.txn_type") == "Deposit", F.col("amount"))
              .otherwise(0)).alias("total_deposits"),
        F.sum(F.when(F.col("t.txn_type") == "Withdrawal", F.col("amount"))
              .otherwise(0)).alias("total_withdrawals"),
        F.sum(F.when(F.col("t.suspicious_flag").isNotNull(), 1)
              .otherwise(0)).alias("suspicious_count"),
        F.countDistinct("t.customer_id").alias("unique_customers"),
    ).withColumn("_gold_timestamp", F.current_timestamp())

    gold_path = f"s3a://{s3_bucket}/migration/gold/daily_branch_summary"
    gold.write.format("delta").mode("overwrite") \
        .partitionBy("txn_date_key") \
        .save(gold_path)

    count = gold.count()
    print(f"      ✅ {count:,} rows")
    return {"table": "daily_branch_summary", "rows": count}


def build_customer_360(spark, s3_bucket: str) -> dict:
    """
    Gold: Customer 360 — Complete customer profile with aggregated metrics.

    LEARNING: Customer 360 is a MUST-HAVE in banking analytics:
    - Combines customer demographics with transaction behavior
    - Used by relationship managers for personalized service
    - Feeds into credit scoring and risk models
    - Updated daily in the Gold layer

    This is a WIDE TABLE (many columns) — typical for Gold layer.
    """
    from pyspark.sql import functions as F

    print("\n   🏆 Building: gold_customer_360")

    silver_cust = spark.read.format("delta").load(
        f"s3a://{s3_bucket}/migration/silver/dim_customer"
    ).filter(F.col("is_current") == 1)  # Only current version

    silver_txn = spark.read.format("delta").load(
        f"s3a://{s3_bucket}/migration/silver/fact_transaction"
    )

    # Aggregate transaction metrics per customer
    txn_metrics = silver_txn.groupBy("customer_id").agg(
        F.count("*").alias("total_transactions"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_transaction_amount"),
        F.max("amount").alias("max_transaction_amount"),
        F.min("txn_datetime").alias("first_transaction_date"),
        F.max("txn_datetime").alias("last_transaction_date"),
        F.countDistinct("channel").alias("channels_used"),
        F.sum(F.when(F.col("txn_type") == "Deposit", F.col("amount"))
              .otherwise(0)).alias("total_deposits"),
        F.sum(F.when(F.col("txn_type") == "Withdrawal", F.col("amount"))
              .otherwise(0)).alias("total_withdrawals"),
        F.sum(F.when(F.col("status") == "Failed", 1)
              .otherwise(0)).alias("failed_transactions"),
        F.sum(F.when(F.col("suspicious_flag").isNotNull(), 1)
              .otherwise(0)).alias("suspicious_transactions"),
        # Channel preference
        F.mode("channel").alias("preferred_channel"),
        F.mode("txn_type").alias("most_common_txn_type"),
    )

    # Join customer demographics with transaction metrics
    gold = silver_cust.alias("c").join(
        txn_metrics.alias("m"),
        F.col("c.customer_id") == F.col("m.customer_id"),
        "left"
    ).select(
        F.col("c.customer_id"),
        F.col("c.full_name"),
        F.col("c.email"),
        F.col("c.phone"),
        F.col("c.region"),
        F.col("c.segment"),
        F.col("c.kyc_status"),
        F.col("c.risk_score"),
        F.col("c.registration_date"),
        F.col("c.age_group"),
        # Transaction metrics
        F.coalesce(F.col("m.total_transactions"), F.lit(0)).alias("total_transactions"),
        F.col("m.total_amount"),
        F.col("m.avg_transaction_amount"),
        F.col("m.max_transaction_amount"),
        F.col("m.first_transaction_date"),
        F.col("m.last_transaction_date"),
        F.col("m.channels_used"),
        F.col("m.total_deposits"),
        F.col("m.total_withdrawals"),
        F.col("m.failed_transactions"),
        F.col("m.suspicious_transactions"),
        F.col("m.preferred_channel"),
        F.col("m.most_common_txn_type"),
        # Derived: customer lifetime value proxy
        F.datediff(F.current_date(), F.col("c.registration_date")).alias("customer_tenure_days"),
        # LEARNING: CLV approximation using transaction history
        (F.coalesce(F.col("m.total_amount"), F.lit(0)) /
         F.greatest(
             F.datediff(F.current_date(), F.col("c.registration_date")),
             F.lit(1)
         ) * 365).alias("annualized_value"),
    ).withColumn("_gold_timestamp", F.current_timestamp())

    gold_path = f"s3a://{s3_bucket}/migration/gold/customer_360"
    gold.write.format("delta").mode("overwrite").save(gold_path)

    count = gold.count()
    print(f"      ✅ {count:,} rows")
    return {"table": "customer_360", "rows": count}


def build_monthly_balance_trends(spark, s3_bucket: str) -> dict:
    """
    Gold: Monthly balance trends — aggregated by customer and month.

    LEARNING: Time-series aggregation for trend analysis:
    - Grain: One row per customer per month
    - Useful for: credit risk modeling, churn prediction, portfolio management
    """
    from pyspark.sql import functions as F

    print("\n   🏆 Building: gold_monthly_balance_trends")

    silver_bal = spark.read.format("delta").load(
        f"s3a://{s3_bucket}/migration/silver/fact_daily_balance"
    )

    # Extract year-month from date_key
    gold = silver_bal.withColumn(
        "year_month", F.concat(
            F.substring(F.col("date_key").cast("string"), 1, 4),
            F.lit("-"),
            F.substring(F.col("date_key").cast("string"), 5, 2)
        )
    ).groupBy("customer_id", "year_month").agg(
        F.avg("closing_balance").alias("avg_monthly_balance"),
        F.max("closing_balance").alias("max_balance"),
        F.min("closing_balance").alias("min_balance"),
        F.last("closing_balance").alias("eom_balance"),
        F.sum("total_credits").alias("monthly_credits"),
        F.sum("total_debits").alias("monthly_debits"),
        F.sum("txn_count").alias("monthly_txn_count"),
        F.avg("daily_net_change").alias("avg_daily_net_change"),
    ).withColumn(
        "monthly_net_flow",
        F.col("monthly_credits") - F.col("monthly_debits")
    ).withColumn(
        "balance_volatility",
        F.col("max_balance") - F.col("min_balance")
    ).withColumn("_gold_timestamp", F.current_timestamp())

    gold_path = f"s3a://{s3_bucket}/migration/gold/monthly_balance_trends"
    gold.write.format("delta").mode("overwrite") \
        .partitionBy("year_month") \
        .save(gold_path)

    count = gold.count()
    print(f"      ✅ {count:,} rows")
    return {"table": "monthly_balance_trends", "rows": count}


def build_risk_alerts(spark, s3_bucket: str) -> dict:
    """
    Gold: Risk alerts — suspicious transactions and high-risk customers.

    LEARNING: AML (Anti-Money Laundering) is a regulatory requirement.
    Banks must flag and report suspicious transactions.
    This table feeds into the compliance dashboard.
    """
    from pyspark.sql import functions as F

    print("\n   🏆 Building: gold_risk_alerts")

    silver_txn = spark.read.format("delta").load(
        f"s3a://{s3_bucket}/migration/silver/fact_transaction"
    )
    silver_cust = spark.read.format("delta").load(
        f"s3a://{s3_bucket}/migration/silver/dim_customer"
    ).filter(F.col("is_current") == 1)

    # Filter suspicious + failed transactions
    alerts = silver_txn.filter(
        (F.col("suspicious_flag").isNotNull()) | (F.col("status") == "Failed")
    ).alias("t").join(
        silver_cust.alias("c"),
        F.col("t.customer_id") == F.col("c.customer_id"),
        "left"
    ).select(
        F.col("t.txn_id"),
        F.col("t.customer_id"),
        F.col("c.full_name"),
        F.col("c.segment"),
        F.col("c.risk_score"),
        F.col("t.amount"),
        F.col("t.txn_type"),
        F.col("t.channel"),
        F.col("t.txn_datetime"),
        F.col("t.status"),
        F.col("t.suspicious_flag"),
        # Risk level
        F.when(F.col("c.risk_score") > 80, "critical")
         .when(F.col("c.risk_score") > 60, "high")
         .when(F.col("c.risk_score") > 40, "medium")
         .otherwise("low").alias("risk_level"),
        F.current_timestamp().alias("_gold_timestamp"),
    )

    gold_path = f"s3a://{s3_bucket}/migration/gold/risk_alerts"
    alerts.write.format("delta").mode("overwrite").save(gold_path)

    count = alerts.count()
    print(f"      ✅ {count:,} alert rows")
    return {"table": "risk_alerts", "rows": count}


# ── Main ────────────────────────────────────────────────────────────────────

GOLD_BUILDERS = {
    "daily_branch_summary": build_daily_branch_summary,
    "customer_360": build_customer_360,
    "monthly_balance_trends": build_monthly_balance_trends,
    "risk_alerts": build_risk_alerts,
}


def run_silver_to_gold(tables: Optional[List[str]] = None):
    """Execute Silver → Gold aggregations."""
    config = load_config()

    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  Silver → Gold Aggregation Pipeline                        ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print(f"  S3 Bucket: {config['s3_bucket']}")
    print(f"  Timestamp: {datetime.now().isoformat()}")

    spark = get_spark_session()

    try:
        target_tables = tables or list(GOLD_BUILDERS.keys())
        metrics = {}

        for table in target_tables:
            if table not in GOLD_BUILDERS:
                print(f"   ⚠️  Unknown gold table: {table}")
                continue
            try:
                result = GOLD_BUILDERS[table](spark, config["s3_bucket"])
                metrics[table] = {**result, "status": "success"}
            except Exception as e:
                print(f"   ❌ Failed: {table} — {e}")
                metrics[table] = {"status": "failed", "error": str(e)}

        # Save metrics
        metrics_path = PROJECT_ROOT / "data" / "gold_metrics.json"
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        with open(metrics_path, "w") as f:
            json.dump({
                "pipeline": "silver_to_gold",
                "timestamp": datetime.now().isoformat(),
                "tables": metrics,
            }, f, indent=2, default=str)

        print(f"\n📊 Metrics: {metrics_path}")
        print("\n✅ Silver → Gold aggregation complete!")

    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(
        description="Silver → Gold aggregation pipeline",
        epilog="""
LEARNING — GOLD TABLE DESCRIPTIONS:
  daily_branch_summary   — Transaction volume by branch/region/day
  customer_360           — Complete customer profile with behavior metrics
  monthly_balance_trends — Monthly balance aggregations for trend analysis
  risk_alerts            — Suspicious transaction alerts for compliance
        """
    )
    parser.add_argument("--tables", nargs="+",
                        help="Specific gold tables to build")

    args = parser.parse_args()
    run_silver_to_gold(tables=args.tables)


if __name__ == "__main__":
    main()
