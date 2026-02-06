"""
Data Quality Framework
======================
Production-ready data quality validation for banking data.
Generates quality reports and flags problematic records.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, count, sum as spark_sum, lit, isnan, isnull,
    length, regexp_extract, mean, stddev, current_timestamp
)
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass


@dataclass
class QualityRule:
    """Represents a data quality rule."""
    name: str
    column: str
    condition: str  # SQL expression
    severity: str = "ERROR"  # ERROR, WARNING, INFO


class DataQualityChecker:
    """
    Data Quality validation framework.
    
    Usage:
        checker = DataQualityChecker(spark)
        checker.add_rule(QualityRule("amount_positive", "amount", "amount > 0"))
        report = checker.validate(transactions_df)
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.rules: List[QualityRule] = []
    
    def add_rule(self, rule: QualityRule) -> "DataQualityChecker":
        """Add a validation rule."""
        self.rules.append(rule)
        return self
    
    def add_not_null_rule(self, column: str, severity: str = "ERROR") -> "DataQualityChecker":
        """Add a not-null check for a column."""
        self.rules.append(QualityRule(
            name=f"{column}_not_null",
            column=column,
            condition=f"{column} IS NOT NULL",
            severity=severity
        ))
        return self
    
    def add_range_rule(self, column: str, min_val: float, max_val: float) -> "DataQualityChecker":
        """Add a range check for a numeric column."""
        self.rules.append(QualityRule(
            name=f"{column}_in_range",
            column=column,
            condition=f"{column} BETWEEN {min_val} AND {max_val}"
        ))
        return self
    
    def add_allowed_values_rule(self, column: str, values: List[str]) -> "DataQualityChecker":
        """Add an allowed values check."""
        values_str = ", ".join([f"'{v}'" for v in values])
        self.rules.append(QualityRule(
            name=f"{column}_allowed_values",
            column=column,
            condition=f"{column} IN ({values_str})"
        ))
        return self
    
    def validate(self, df: DataFrame) -> Dict:
        """
        Run all validation rules and return a quality report.
        
        Returns:
            Dict with overall score and per-rule results
        """
        total_rows = df.count()
        results = []
        
        for rule in self.rules:
            # Count passing rows
            passed = df.filter(rule.condition).count()
            failed = total_rows - passed
            pass_rate = (passed / total_rows * 100) if total_rows > 0 else 0
            
            results.append({
                "rule_name": rule.name,
                "column": rule.column,
                "severity": rule.severity,
                "total_rows": total_rows,
                "passed": passed,
                "failed": failed,
                "pass_rate": round(pass_rate, 2)
            })
        
        # Calculate overall score
        error_rules = [r for r in results if r["severity"] == "ERROR"]
        if error_rules:
            overall_score = sum(r["pass_rate"] for r in error_rules) / len(error_rules)
        else:
            overall_score = 100.0
        
        return {
            "overall_score": round(overall_score, 2),
            "total_rows": total_rows,
            "rules_checked": len(self.rules),
            "rule_results": results,
            "timestamp": str(current_timestamp())
        }
    
    def flag_invalid_records(self, df: DataFrame) -> DataFrame:
        """
        Return DataFrame with validation flags for each rule.
        Useful for quarantining bad records.
        """
        result = df
        for rule in self.rules:
            result = result.withColumn(
                f"valid_{rule.name}",
                when(col(rule.condition) if isinstance(rule.condition, str) else rule.condition, True).otherwise(False)
            )
        
        # Overall valid flag
        valid_cols = [f"valid_{r.name}" for r in self.rules if r.severity == "ERROR"]
        if valid_cols:
            from functools import reduce
            from operator import and_
            result = result.withColumn(
                "is_valid",
                reduce(and_, [col(c) for c in valid_cols])
            )
        
        return result


def null_analysis(df: DataFrame) -> DataFrame:
    """
    Analyze null values across all columns.
    
    Returns DataFrame with column, null_count, null_percentage.
    """
    from pyspark.sql import Row
    
    total = df.count()
    results = []
    
    for c in df.columns:
        null_count = df.filter(col(c).isNull() | isnan(col(c))).count()
        results.append(Row(
            column=c,
            null_count=null_count,
            null_percentage=round((null_count / total) * 100, 2) if total > 0 else 0
        ))
    
    spark = df.sparkSession
    return spark.createDataFrame(results)


def detect_anomalies(
    df: DataFrame,
    column: str,
    method: str = "zscore",
    threshold: float = 3.0
) -> DataFrame:
    """
    Detect statistical anomalies in a numeric column.
    
    Methods:
    - zscore: Flag values > threshold standard deviations from mean
    - iqr: Flag values outside IQR * threshold
    """
    if method == "zscore":
        stats = df.select(mean(column).alias("mean"), stddev(column).alias("std")).collect()[0]
        mean_val, std_val = stats["mean"], stats["std"]
        
        return df.withColumn(
            f"{column}_is_anomaly",
            (abs(col(column) - mean_val) / std_val) > threshold
        )
    
    elif method == "iqr":
        quantiles = df.approxQuantile(column, [0.25, 0.75], 0.05)
        q1, q3 = quantiles[0], quantiles[1]
        iqr = q3 - q1
        lower = q1 - (threshold * iqr)
        upper = q3 + (threshold * iqr)
        
        return df.withColumn(
            f"{column}_is_anomaly",
            (col(column) < lower) | (col(column) > upper)
        )
    
    else:
        raise ValueError(f"Unknown method: {method}")


def calculate_completeness_score(df: DataFrame) -> float:
    """Calculate data completeness score (0-100)."""
    total_cells = df.count() * len(df.columns)
    null_cells = 0
    
    for c in df.columns:
        null_cells += df.filter(col(c).isNull()).count()
    
    return round((1 - null_cells / total_cells) * 100, 2) if total_cells > 0 else 0


# Pre-built rule sets for banking data
TRANSACTION_RULES = [
    QualityRule("amount_positive", "amount", "amount > 0"),
    QualityRule("status_valid", "status", "status IN ('Completed', 'Pending', 'Failed', 'Reversed')"),
    QualityRule("channel_valid", "channel", "channel IN ('Branch', 'ATM', 'Mobile App', 'Internet Banking', 'POS', 'API')"),
]

CUSTOMER_RULES = [
    QualityRule("customer_id_not_null", "customer_id", "customer_id IS NOT NULL"),
    QualityRule("name_not_empty", "name", "name IS NOT NULL AND length(name) > 0"),
    QualityRule("kyc_valid", "kyc_status", "kyc_status IN ('Verified', 'Pending', 'Expired', 'Rejected')"),
]
