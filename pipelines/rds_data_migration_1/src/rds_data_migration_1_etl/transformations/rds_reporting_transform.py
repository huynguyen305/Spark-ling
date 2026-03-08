"""
RDS Reporting Transformations — Databricks PySpark Migration
=============================================================
Migrates the 5 PostgreSQL stored procedures from
``aws/rds/rds_reporting_procedures.sql`` to PySpark jobs that run
inside a Databricks Job and write results to Unity Catalog.

Reporting tables produced (in the ``{catalog}.{schema}`` namespace):

  rpt_monthly_txn_summary       Monthly transaction volume per customer / account type
  rpt_account_balance_snapshot  End-of-day account balances with VND balance tiering
  rpt_customer_segment_kpi      Segment-level KPIs for management reporting
  rpt_channel_analysis          Digital vs traditional channel mix
  rpt_dormant_watchlist         Inactive accounts for risk / compliance team

Refresh strategy:
  Full replace per reporting period (idempotent — safe to re-run).

Execution order matches the master PostgreSQL procedure
``sp_run_daily_reporting()``:
  1. rpt_account_balance_snapshot   — depends on dim_account
  2. rpt_monthly_txn_summary        — depends on fact_transaction
  3. rpt_customer_segment_kpi       — depends on dim_account + fact_transaction
  4. rpt_channel_analysis           — depends on fact_transaction
  5. rpt_dormant_watchlist          — depends on dim_account

Usage (via Databricks Asset Bundle):
    databricks bundle run rds_data_migration_1 --target prod \\
        --task rds_reporting_task

Usage (direct):
    python -m rds_data_migration_1_etl.transformations.rds_reporting_transform \\
        --catalog sparkling --schema prod
"""

import argparse
import logging
from datetime import date, datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def _fq(catalog: str, schema: str, table: str) -> str:
    """Fully-qualified Unity Catalog table name."""
    return f"{catalog}.{schema}.{table}"


def _yyyymm(d: date) -> int:
    """Return YYYYMM integer from a date."""
    return d.year * 100 + d.month


# ============================================================================
# 1. rpt_account_balance_snapshot
# ============================================================================
# Migrated from: sp_populate_rpt_account_balance_snapshot
#
# Grain  : One row per (snapshot_date × account_id)
# Sources: dim_account, dim_customer, fact_daily_balance (fallback: dim_account.current_balance)
# ============================================================================

def populate_account_balance_snapshot(
    spark: SparkSession,
    catalog: str,
    schema: str,
    snapshot_date: date,
) -> int:
    """
    Build the end-of-day account balance snapshot for ``snapshot_date``.

    Mirrors ``sp_populate_rpt_account_balance_snapshot`` from rds_reporting_procedures.sql.
    Accounts with status = 'Closed' are excluded.
    """
    snap_str = snapshot_date.strftime("%Y-%m-%d")
    date_key = int(snapshot_date.strftime("%Y%m%d"))
    logger.info("[rpt_account_balance_snapshot] Snapshot date: %s", snap_str)

    dim_account = spark.table(_fq(catalog, schema, "dim_account"))
    dim_customer = spark.table(_fq(catalog, schema, "dim_customer")).filter(F.col("is_current") == True)
    fact_daily_balance = spark.table(_fq(catalog, schema, "fact_daily_balance"))

    # End-of-day balance — prefer fact_daily_balance; fall back to dim_account.current_balance
    eod_balance = (
        fact_daily_balance
        .filter(F.col("date_key") == date_key)
        .select("customer_id", "account_type_code", "closing_balance")
    )

    snapshot = (
        dim_account
        .filter(F.col("status") != "Closed")
        .join(dim_customer.select("customer_id", "segment"), on="customer_id", how="inner")
        .join(
            eod_balance,
            on=["customer_id", "account_type_code"],
            how="left",
        )
        .withColumn(
            "current_balance",
            F.coalesce(F.col("closing_balance"), F.col("current_balance")),
        )
        .withColumn(
            "balance_tier",
            F.when(F.col("current_balance") < 1_000_000, "Micro")
             .when(F.col("current_balance") < 100_000_000, "Retail")
             .when(F.col("current_balance") < 1_000_000_000, "Affluent")
             .otherwise("Private"),
        )
        .withColumn(
            "days_since_activity",
            F.when(
                F.col("last_activity_date").isNotNull(),
                F.datediff(F.lit(snap_str).cast("date"), F.col("last_activity_date")),
            ).otherwise(F.lit(None).cast("int")),
        )
        .withColumn("snapshot_date", F.lit(snap_str).cast("date"))
        .withColumn("last_modified", F.current_timestamp())
        .select(
            "snapshot_date",
            "account_id",
            "customer_id",
            "account_type_code",
            "segment",
            "branch_id",
            "current_balance",
            "balance_tier",
            "status",
            "opened_date",
            "days_since_activity",
            "last_modified",
        )
    )

    fq_target = _fq(catalog, schema, "rpt_account_balance_snapshot")
    _replace_partition(spark, snapshot, fq_target, "snapshot_date", snap_str)
    count = snapshot.count()
    logger.info("[rpt_account_balance_snapshot] %d rows written for %s", count, snap_str)
    return count


# ============================================================================
# 2. rpt_monthly_txn_summary
# ============================================================================
# Migrated from: sp_populate_rpt_monthly_txn_summary
#
# Grain  : One row per (report_month × customer_id × account_type_code)
# Sources: fact_transaction, dim_customer
# ============================================================================

def populate_monthly_txn_summary(
    spark: SparkSession,
    catalog: str,
    schema: str,
    report_month: int,
) -> int:
    """
    Build the monthly transaction summary for ``report_month`` (YYYYMM).

    Mirrors ``sp_populate_rpt_monthly_txn_summary`` from rds_reporting_procedures.sql.
    """
    logger.info("[rpt_monthly_txn_summary] Month: %d", report_month)

    year = report_month // 100
    month = report_month % 100
    start_date = f"{year}-{month:02d}-01"
    # Last day of month
    end_date = (
        datetime(year, month, 1).__class__(
            year + (month // 12), (month % 12) + 1, 1
        ) - __import__("datetime").timedelta(days=1)
    ).strftime("%Y-%m-%d")

    fact_txn = spark.table(_fq(catalog, schema, "fact_transaction"))
    dim_customer = spark.table(_fq(catalog, schema, "dim_customer")).filter(F.col("is_current") == True)

    txn_in_month = (
        fact_txn
        .filter(
            (F.col("txn_datetime") >= start_date)
            & (F.col("txn_datetime") <= end_date + " 23:59:59")
            & (F.col("status") == "Completed")
        )
    )

    # Net flow: credit types add, debit types subtract
    txn_with_sign = txn_in_month.withColumn(
        "signed_amount",
        F.when(
            F.col("txn_type").isin("Deposit", "Transfer In", "Interest"),
            F.col("amount"),
        ).when(
            F.col("txn_type").isin("Withdrawal", "Transfer Out", "Payment", "Fee"),
            -F.col("amount"),
        ).otherwise(F.lit(0.0)),
    )

    # Preferred channel = most frequent channel for this customer+account_type this month
    channel_count_window = Window.partitionBy("customer_id", "account_type_code", "channel")
    channel_ranked = (
        txn_in_month
        .withColumn("ch_cnt", F.count("*").over(channel_count_window))
        .withColumn(
            "ch_rank",
            F.row_number().over(
                Window.partitionBy("customer_id", "account_type_code").orderBy(F.desc("ch_cnt"))
            ),
        )
        .filter(F.col("ch_rank") == 1)
        .select("customer_id", "account_type_code", F.col("channel").alias("preferred_channel"))
        .distinct()
    )

    summary = (
        txn_with_sign
        .groupBy("customer_id", "account_type_code")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum(F.when(F.col("signed_amount") > 0, F.col("amount")).otherwise(0)).alias("total_credit_vnd"),
            F.sum(F.when(F.col("signed_amount") < 0, F.col("amount")).otherwise(0)).alias("total_debit_vnd"),
            F.sum("signed_amount").alias("net_flow_vnd"),
            F.round(F.avg("amount"), 2).alias("avg_txn_amount"),
            F.max("amount").alias("max_single_txn"),
        )
        .join(
            dim_customer.select("customer_id", "segment"),
            on="customer_id",
            how="left",
        )
        .join(channel_ranked, on=["customer_id", "account_type_code"], how="left")
        .withColumn("report_month", F.lit(report_month))
        .withColumn("last_modified", F.current_timestamp())
        .select(
            "report_month",
            "customer_id",
            "segment",
            "account_type_code",
            "txn_count",
            "total_credit_vnd",
            "total_debit_vnd",
            "net_flow_vnd",
            "avg_txn_amount",
            "max_single_txn",
            "preferred_channel",
            "last_modified",
        )
    )

    fq_target = _fq(catalog, schema, "rpt_monthly_txn_summary")
    _replace_partition(spark, summary, fq_target, "report_month", str(report_month))
    count = summary.count()
    logger.info("[rpt_monthly_txn_summary] %d rows written for month %d", count, report_month)
    return count


# ============================================================================
# 3. rpt_customer_segment_kpi
# ============================================================================
# Migrated from: sp_populate_rpt_customer_segment_kpi
#
# Grain  : One row per (report_month × segment)
# Sources: dim_customer, dim_account, fact_transaction
# ============================================================================

def populate_customer_segment_kpi(
    spark: SparkSession,
    catalog: str,
    schema: str,
    report_month: int,
) -> int:
    """
    Build monthly segment KPIs for management reporting.

    Mirrors ``sp_populate_rpt_customer_segment_kpi`` from rds_reporting_procedures.sql.
    """
    logger.info("[rpt_customer_segment_kpi] Month: %d", report_month)

    year = report_month // 100
    month = report_month % 100
    start_date = f"{year}-{month:02d}-01"
    end_date_plus1 = (
        datetime(year + (month // 12), (month % 12) + 1, 1)
        .strftime("%Y-%m-%d")
    )

    dim_customer = spark.table(_fq(catalog, schema, "dim_customer")).filter(F.col("is_current") == True)
    dim_account = spark.table(_fq(catalog, schema, "dim_account")).filter(F.col("status") != "Closed")
    fact_txn = spark.table(_fq(catalog, schema, "fact_transaction"))

    # Account statistics (current snapshot — not month-bound)
    acct_with_seg = dim_account.join(
        dim_customer.select("customer_id", "segment"), on="customer_id", how="inner"
    )
    account_stats = acct_with_seg.groupBy("segment").agg(
        F.countDistinct("customer_id").alias("total_customers"),
        F.count("account_id").alias("total_accounts"),
        F.count(F.when(F.col("status") == "Active", 1)).alias("active_accounts"),
        F.count(F.when(F.col("status").isin("Dormant", "Frozen"), 1)).alias("dormant_accounts"),
        F.sum("current_balance").alias("total_balance_vnd"),
        F.avg("current_balance").alias("avg_balance_per_acct"),
    ).withColumn(
        "avg_balance_per_cust",
        F.col("total_balance_vnd") / F.when(F.col("total_customers") > 0, F.col("total_customers")).otherwise(1),
    ).withColumn(
        "dormant_account_pct",
        F.round(
            F.col("dormant_accounts").cast("double")
            / F.when(F.col("total_accounts") > 0, F.col("total_accounts")).otherwise(1)
            * 100,
            2,
        ),
    )

    # Transaction statistics (month-scoped, Completed only)
    txn_in_month = (
        fact_txn
        .filter(
            (F.col("txn_datetime") >= start_date)
            & (F.col("txn_datetime") < end_date_plus1)
            & (F.col("status") == "Completed")
        )
        .join(dim_customer.select("customer_id", "segment"), on="customer_id", how="inner")
    )
    txn_stats = txn_in_month.groupBy("segment").agg(
        F.sum("amount").alias("total_txn_volume_vnd"),
        F.count("*").alias("txn_count"),
        F.round(
            F.count("*").cast("double") / F.countDistinct("customer_id").cast("double"),
            2,
        ).alias("avg_txn_per_customer"),
    )

    kpi = (
        account_stats
        .join(txn_stats, on="segment", how="left")
        .fillna({"total_txn_volume_vnd": 0, "txn_count": 0, "avg_txn_per_customer": 0})
        .withColumn("report_month", F.lit(report_month))
        .withColumn("avg_balance_per_acct", F.round("avg_balance_per_acct", 2))
        .withColumn("avg_balance_per_cust", F.round("avg_balance_per_cust", 2))
        .withColumn("last_modified", F.current_timestamp())
        .select(
            "report_month",
            "segment",
            "total_customers",
            "total_accounts",
            "active_accounts",
            "total_balance_vnd",
            "avg_balance_per_acct",
            "avg_balance_per_cust",
            "total_txn_volume_vnd",
            "txn_count",
            "avg_txn_per_customer",
            "dormant_account_pct",
            "last_modified",
        )
    )

    fq_target = _fq(catalog, schema, "rpt_customer_segment_kpi")
    _replace_partition(spark, kpi, fq_target, "report_month", str(report_month))
    count = kpi.count()
    logger.info("[rpt_customer_segment_kpi] %d segment rows for month %d", count, report_month)
    return count


# ============================================================================
# 4. rpt_channel_analysis
# ============================================================================
# Migrated from: sp_populate_rpt_channel_analysis
#
# Grain  : One row per (report_month × channel)
# Sources: fact_transaction
# ============================================================================

def populate_channel_analysis(
    spark: SparkSession,
    catalog: str,
    schema: str,
    report_month: int,
) -> int:
    """
    Build channel mix analysis (digital vs traditional) for ``report_month``.

    Mirrors ``sp_populate_rpt_channel_analysis`` from rds_reporting_procedures.sql.
    Digital channels: Mobile App, Internet Banking, API.
    """
    logger.info("[rpt_channel_analysis] Month: %d", report_month)

    year = report_month // 100
    month = report_month % 100
    start_date = f"{year}-{month:02d}-01"
    end_date_plus1 = (
        datetime(year + (month // 12), (month % 12) + 1, 1)
        .strftime("%Y-%m-%d")
    )

    fact_txn = spark.table(_fq(catalog, schema, "fact_transaction"))
    txn_in_month = fact_txn.filter(
        (F.col("txn_datetime") >= start_date)
        & (F.col("txn_datetime") < end_date_plus1)
        & (F.col("status") == "Completed")
    )

    channel_base = txn_in_month.groupBy("channel").agg(
        F.count("*").alias("txn_count"),
        F.sum("amount").alias("total_volume_vnd"),
        F.avg("amount").alias("avg_txn_amount"),
    )

    # Grand totals for percentage calculations
    totals_row = channel_base.agg(
        F.sum("txn_count").alias("grand_txn_count"),
        F.sum("total_volume_vnd").alias("grand_volume"),
    )
    grand_txn_count = totals_row.collect()[0]["grand_txn_count"] or 1
    grand_volume = totals_row.collect()[0]["grand_volume"] or 1

    channel_analysis = (
        channel_base
        .withColumn("report_month", F.lit(report_month))
        .withColumn("avg_txn_amount", F.round("avg_txn_amount", 2))
        .withColumn(
            "pct_of_total_txns",
            F.round(F.col("txn_count").cast("double") / grand_txn_count * 100, 2),
        )
        .withColumn(
            "pct_of_total_vol",
            F.round(F.col("total_volume_vnd").cast("double") / grand_volume * 100, 2),
        )
        .withColumn(
            "digital_flag",
            F.col("channel").isin("Mobile App", "Internet Banking", "API"),
        )
        .withColumn("last_modified", F.current_timestamp())
        .select(
            "report_month",
            "channel",
            "txn_count",
            "total_volume_vnd",
            "avg_txn_amount",
            "pct_of_total_txns",
            "pct_of_total_vol",
            "digital_flag",
            "last_modified",
        )
    )

    fq_target = _fq(catalog, schema, "rpt_channel_analysis")
    _replace_partition(spark, channel_analysis, fq_target, "report_month", str(report_month))
    count = channel_analysis.count()
    logger.info("[rpt_channel_analysis] %d channel rows for month %d", count, report_month)
    return count


# ============================================================================
# 5. rpt_dormant_watchlist
# ============================================================================
# Migrated from: sp_populate_rpt_dormant_watchlist
#
# Grain  : One row per (snapshot_date × account_id)
# Sources: dim_account, dim_customer
# SBV Circular 14/2017 — 180-day threshold for official dormancy
# ============================================================================

def populate_dormant_watchlist(
    spark: SparkSession,
    catalog: str,
    schema: str,
    snapshot_date: date,
    min_days_inactive: int = 60,
) -> int:
    """
    Build the dormant account watchlist for ``snapshot_date``.

    Dormancy tiers:
      At Risk      :  60–89 days inactive  → send reactivation nudge
      Pre-Dormant  :  90–179 days inactive → RM outreach; fee waiver offer
      Dormant      :  180+ days inactive   → initiate SBV dormant account process

    Mirrors ``sp_populate_rpt_dormant_watchlist`` from rds_reporting_procedures.sql.
    """
    snap_str = snapshot_date.strftime("%Y-%m-%d")
    logger.info(
        "[rpt_dormant_watchlist] Snapshot=%s, min_days_inactive=%d",
        snap_str, min_days_inactive,
    )

    dim_account = spark.table(_fq(catalog, schema, "dim_account"))
    dim_customer = spark.table(_fq(catalog, schema, "dim_customer")).filter(F.col("is_current") == True)

    watchlist = (
        dim_account
        .filter(F.col("status").isin("Active", "Dormant"))
        .filter(F.col("last_activity_date").isNotNull())
        .join(
            dim_customer.select("customer_id", "full_name", "segment"),
            on="customer_id",
            how="inner",
        )
        .withColumn(
            "days_inactive",
            F.datediff(F.lit(snap_str).cast("date"), F.col("last_activity_date")),
        )
        .filter(F.col("days_inactive") >= min_days_inactive)
        .withColumn(
            "dormancy_risk",
            F.when(
                (F.col("days_inactive") >= min_days_inactive) & (F.col("days_inactive") <= 89),
                "At Risk",
            ).when(
                (F.col("days_inactive") >= 90) & (F.col("days_inactive") <= 179),
                "Pre-Dormant",
            ).when(
                F.col("days_inactive") >= 180,
                "Dormant",
            ),
        )
        .withColumn(
            "recommended_action",
            F.when(
                (F.col("days_inactive") >= min_days_inactive) & (F.col("days_inactive") <= 89),
                "Send reactivation SMS / push notification",
            ).when(
                (F.col("days_inactive") >= 90) & (F.col("days_inactive") <= 179),
                "Assign RM outreach; offer fee waiver for next cycle",
            ).when(
                F.col("days_inactive") >= 180,
                "Initiate SBV dormant account process; consider full fee waiver",
            ),
        )
        .withColumn("snapshot_date", F.lit(snap_str).cast("date"))
        .withColumn("last_modified", F.current_timestamp())
        .select(
            "snapshot_date",
            "account_id",
            "customer_id",
            F.col("full_name").alias("customer_name"),
            "segment",
            "account_type_code",
            "branch_id",
            "current_balance",
            "last_activity_date",
            "days_inactive",
            "dormancy_risk",
            "recommended_action",
            "last_modified",
        )
    )

    fq_target = _fq(catalog, schema, "rpt_dormant_watchlist")
    _replace_partition(spark, watchlist, fq_target, "snapshot_date", snap_str)
    count = watchlist.count()
    logger.info("[rpt_dormant_watchlist] %d accounts flagged for %s", count, snap_str)
    return count


# ============================================================================
# Utility: replace a single partition (idempotent full-replace pattern)
# ============================================================================

def _replace_partition(
    spark: SparkSession,
    df: DataFrame,
    fq_table: str,
    partition_col: str,
    partition_val: str,
) -> None:
    """
    Write ``df`` to ``fq_table``, replacing any existing rows where
    ``partition_col = partition_val``.  Creates the table on first run.
    """
    try:
        # Delete existing rows for this partition
        spark.sql(f"DELETE FROM {fq_table} WHERE {partition_col} = '{partition_val}'")
    except Exception:
        # Table does not exist yet — create it on first write
        pass

    df.write.format("delta").mode("append").saveAsTable(fq_table)


# ============================================================================
# Master orchestrator — equivalent to sp_run_daily_reporting()
# ============================================================================

def run_daily_reporting(
    spark: SparkSession,
    catalog: str,
    schema: str,
    run_date: date,
) -> None:
    """
    Run all 5 reporting transformations in the correct dependency order.

    Equivalent to ``CALL sp_run_daily_reporting()`` in PostgreSQL.
    """
    report_month = _yyyymm(run_date)
    logger.info(
        "=== run_daily_reporting START | date=%s | month=%d ===",
        run_date.isoformat(),
        report_month,
    )

    # Step 1: Balance snapshot (depends on dim_account)
    populate_account_balance_snapshot(spark, catalog, schema, run_date)

    # Steps 2–4: Monthly aggregates (safe to re-run mid-month)
    populate_monthly_txn_summary(spark, catalog, schema, report_month)
    populate_customer_segment_kpi(spark, catalog, schema, report_month)
    populate_channel_analysis(spark, catalog, schema, report_month)

    # Step 5: Dormant risk watchlist (depends on dim_account)
    populate_dormant_watchlist(spark, catalog, schema, run_date)

    logger.info(
        "=== run_daily_reporting COMPLETE at %s ===",
        datetime.utcnow().isoformat(),
    )


# ============================================================================
# CLI entry point
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Daily reporting transformations (PostgreSQL SP migration)")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    parser.add_argument("--schema", required=True, help="Schema name")
    parser.add_argument(
        "--run-date",
        default=None,
        help="Reporting date YYYY-MM-DD (default: today UTC)",
    )
    parser.add_argument(
        "--min-days-inactive",
        type=int,
        default=60,
        help="Minimum days inactive for dormant watchlist (default: 60)",
    )
    args = parser.parse_args()

    run_date = (
        datetime.strptime(args.run_date, "%Y-%m-%d").date()
        if args.run_date
        else datetime.utcnow().date()
    )

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    report_month = _yyyymm(run_date)
    logger.info("Catalog=%s  Schema=%s  RunDate=%s  Month=%d", args.catalog, args.schema, run_date, report_month)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")

    populate_account_balance_snapshot(spark, args.catalog, args.schema, run_date)
    populate_monthly_txn_summary(spark, args.catalog, args.schema, report_month)
    populate_customer_segment_kpi(spark, args.catalog, args.schema, report_month)
    populate_channel_analysis(spark, args.catalog, args.schema, report_month)
    populate_dormant_watchlist(spark, args.catalog, args.schema, run_date, args.min_days_inactive)

    logger.info("All reporting tables updated successfully.")


if __name__ == "__main__":
    main()
