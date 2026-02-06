"""
Banking Transformations Module
==============================
Reusable transformation functions for banking data pipelines.
Production-style code with type hints and documentation.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, sum as spark_sum, avg, count, max as spark_max,
    min as spark_min, round as spark_round, lit, current_date,
    to_date, date_format, datediff
)
from typing import List, Dict


def enrich_customer_segment(df: DataFrame, balance_col: str = "total_balance") -> DataFrame:
    """
    Re-segment customers based on actual balance thresholds (VND).
    
    Thresholds:
    - UHNW: >= 5 billion
    - HNW: >= 500 million
    - Affluent: >= 100 million
    - Mass Affluent: >= 20 million
    - Mass: < 20 million
    """
    return df.withColumn(
        "calculated_segment",
        when(col(balance_col) >= 5_000_000_000, "UHNW")
        .when(col(balance_col) >= 500_000_000, "HNW")
        .when(col(balance_col) >= 100_000_000, "Affluent")
        .when(col(balance_col) >= 20_000_000, "Mass Affluent")
        .otherwise("Mass")
    )


def categorize_transaction(df: DataFrame, amount_col: str = "amount") -> DataFrame:
    """
    Categorize transactions by amount size.
    
    Categories:
    - Large: >= 100M VND
    - Medium: 10M - 100M
    - Small: 1M - 10M
    - Micro: < 1M
    """
    return df.withColumn(
        "amount_category",
        when(col(amount_col) >= 100_000_000, "Large")
        .when(col(amount_col) >= 10_000_000, "Medium")
        .when(col(amount_col) >= 1_000_000, "Small")
        .otherwise("Micro")
    )


def flag_suspicious_transactions(
    df: DataFrame,
    amount_threshold: float = 1_000_000_000,
    flag_failed: bool = True
) -> DataFrame:
    """
    Flag transactions that may require compliance review.
    
    Flags:
    - Amount exceeds threshold (default 1B VND)
    - Failed transactions (optional)
    """
    condition = col("amount") >= amount_threshold
    if flag_failed:
        condition = condition | (col("status") == "Failed")
    
    return df.withColumn("needs_review", condition)


def calculate_customer_metrics(
    accounts_df: DataFrame,
    transactions_df: DataFrame
) -> DataFrame:
    """
    Calculate comprehensive customer metrics by joining accounts and transactions.
    
    Returns:
    - account_count, total_balance
    - txn_count, total_txn_volume
    - avg_txn_amount, deposit_count, withdrawal_count
    """
    # Account-level aggregation
    account_summary = accounts_df.groupBy("customer_id").agg(
        count("account_id").alias("account_count"),
        spark_round(spark_sum("balance"), 2).alias("total_balance")
    )
    
    # Transaction-level aggregation
    txn_with_customer = transactions_df.join(
        accounts_df.select("account_id", "customer_id"),
        "account_id"
    )
    
    txn_summary = txn_with_customer.groupBy("customer_id").agg(
        count("txn_id").alias("txn_count"),
        spark_round(spark_sum("amount"), 0).alias("total_txn_volume"),
        spark_round(avg("amount"), 0).alias("avg_txn_amount"),
        count(when(col("txn_type") == "Deposit", 1)).alias("deposit_count"),
        count(when(col("txn_type") == "Withdrawal", 1)).alias("withdrawal_count")
    )
    
    return account_summary.join(txn_summary, "customer_id", "left").fillna(0)


def build_customer_360(
    customers_df: DataFrame,
    accounts_df: DataFrame,
    transactions_df: DataFrame,
    branches_df: DataFrame
) -> DataFrame:
    """
    Build comprehensive Customer 360 view with all metrics and enrichments.
    
    This is a denormalized view suitable for analytics and BI tools.
    """
    # Get customer metrics
    metrics = calculate_customer_metrics(accounts_df, transactions_df)
    
    # Get primary branch (most accounts)
    from pyspark.sql.window import Window
    branch_window = Window.partitionBy("customer_id").orderBy(col("account_count").desc())
    
    primary_branch = accounts_df.groupBy("customer_id", "branch_id").agg(
        count("account_id").alias("account_count")
    ).withColumn(
        "rank", count("*").over(branch_window)
    ).filter(col("rank") == 1).drop("rank", "account_count")
    
    # Build 360 view
    customer_360 = customers_df \
        .join(metrics, "customer_id", "left") \
        .join(primary_branch, "customer_id", "left") \
        .join(branches_df.select("branch_id", "branch_name", "region"), "branch_id", "left")
    
    # Add calculated segment
    customer_360 = enrich_customer_segment(customer_360)
    
    # Add customer tenure
    customer_360 = customer_360.withColumn(
        "tenure_days",
        datediff(current_date(), to_date(col("registration_date")))
    )
    
    return customer_360.fillna({
        "account_count": 0,
        "total_balance": 0,
        "txn_count": 0,
        "total_txn_volume": 0
    })


def aggregate_by_period(
    df: DataFrame,
    date_col: str,
    group_cols: List[str],
    agg_specs: Dict[str, str],
    period: str = "month"
) -> DataFrame:
    """
    Generic period-based aggregation.
    
    Args:
        df: Input DataFrame
        date_col: Date column name
        group_cols: Additional grouping columns
        agg_specs: Dict of {output_col: "agg_func(input_col)"}
        period: "day", "week", "month", "quarter", "year"
    
    Example:
        aggregate_by_period(txn_df, "txn_date", ["channel"], 
                           {"total_amount": "sum(amount)", "txn_count": "count(*)"})
    """
    period_formats = {
        "day": "yyyy-MM-dd",
        "week": "yyyy-ww",
        "month": "yyyy-MM",
        "quarter": "yyyy-QQ",
        "year": "yyyy"
    }
    
    df_with_period = df.withColumn(
        "period",
        date_format(to_date(col(date_col)), period_formats.get(period, "yyyy-MM"))
    )
    
    # Build aggregation expressions
    from pyspark.sql.functions import expr
    agg_exprs = [expr(f"{agg_expr} as {out_col}") for out_col, agg_expr in agg_specs.items()]
    
    return df_with_period.groupBy(["period"] + group_cols).agg(*agg_exprs)
