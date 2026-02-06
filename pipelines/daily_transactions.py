"""
Daily Transactions Pipeline
===========================
Production-style pipeline for processing daily banking transactions.

This pipeline:
1. Reads raw transaction data
2. Validates data quality
3. Enriches with transformations
4. Writes to processed layer

Usage:
    python pipelines/daily_transactions.py --date 2025-01-15
"""

import argparse
from datetime import datetime, date
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp, lit

from transformations import categorize_transaction, flag_suspicious_transactions
from quality_checks import DataQualityChecker, TRANSACTION_RULES


def create_spark_session(app_name: str) -> SparkSession:
    """Create configured SparkSession for pipeline."""
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def run_pipeline(process_date: str, data_path: Path) -> dict:
    """
    Run the daily transactions pipeline.
    
    Args:
        process_date: Date to process (YYYY-MM-DD)
        data_path: Base path to data directory
    
    Returns:
        Dict with pipeline metrics
    """
    print(f"\n{'='*60}")
    print(f"Daily Transactions Pipeline - {process_date}")
    print(f"{'='*60}")
    
    spark = create_spark_session(f"DailyTxnPipeline-{process_date}")
    
    try:
        # === STEP 1: Read raw data ===
        print("\n📖 Step 1: Reading raw transactions...")
        raw_path = data_path / "raw" / "transactions.csv"
        
        transactions = spark.read.csv(str(raw_path), header=True, inferSchema=True)
        transactions = transactions.withColumn("txn_date", to_date(col("txn_datetime")))
        
        # Filter for process date
        daily_txn = transactions.filter(col("txn_date") == process_date)
        initial_count = daily_txn.count()
        print(f"   Raw records for {process_date}: {initial_count:,}")
        
        if initial_count == 0:
            print(f"   ⚠️ No transactions found for {process_date}")
            return {"status": "no_data", "records": 0}
        
        # === STEP 2: Data Quality Validation ===
        print("\n🔍 Step 2: Validating data quality...")
        checker = DataQualityChecker(spark)
        for rule in TRANSACTION_RULES:
            checker.add_rule(rule)
        
        quality_report = checker.validate(daily_txn)
        print(f"   Quality Score: {quality_report['overall_score']}/100")
        
        for result in quality_report['rule_results']:
            status = "✅" if result['pass_rate'] >= 99 else "⚠️"
            print(f"   {status} {result['rule_name']}: {result['pass_rate']}%")
        
        # Flag invalid records
        validated = checker.flag_invalid_records(daily_txn)
        valid_records = validated.filter(col("is_valid") == True)
        invalid_records = validated.filter(col("is_valid") == False)
        
        print(f"   Valid: {valid_records.count():,} | Invalid: {invalid_records.count():,}")
        
        # === STEP 3: Transformations ===
        print("\n🔄 Step 3: Applying transformations...")
        
        # Categorize by amount
        enriched = categorize_transaction(valid_records)
        
        # Flag suspicious transactions
        enriched = flag_suspicious_transactions(enriched)
        
        # Add audit columns
        enriched = enriched \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("pipeline_version", lit("1.0.0"))
        
        suspicious_count = enriched.filter(col("needs_review")).count()
        print(f"   Flagged for review: {suspicious_count:,}")
        
        # === STEP 4: Write outputs ===
        print("\n💾 Step 4: Writing outputs...")
        
        # Write valid processed records
        processed_path = data_path / "processed" / "transactions" / f"date={process_date}"
        enriched.drop("is_valid").write.mode("overwrite").parquet(str(processed_path))
        print(f"   ✅ Processed: {processed_path}")
        
        # Write invalid records to quarantine
        if invalid_records.count() > 0:
            quarantine_path = data_path / "quarantine" / "transactions" / f"date={process_date}"
            quarantine_path.parent.mkdir(parents=True, exist_ok=True)
            invalid_records.write.mode("overwrite").parquet(str(quarantine_path))
            print(f"   ⚠️ Quarantined: {quarantine_path}")
        
        # === Summary ===
        metrics = {
            "status": "success",
            "process_date": process_date,
            "raw_records": initial_count,
            "valid_records": valid_records.count(),
            "invalid_records": invalid_records.count(),
            "quality_score": quality_report['overall_score'],
            "suspicious_flagged": suspicious_count
        }
        
        print(f"\n{'='*60}")
        print("✅ Pipeline completed successfully!")
        print(f"   Records processed: {metrics['valid_records']:,}")
        print(f"   Quality score: {metrics['quality_score']}")
        print(f"{'='*60}\n")
        
        return metrics
        
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Daily Transactions Pipeline")
    parser.add_argument(
        "--date",
        type=str,
        default=date.today().strftime("%Y-%m-%d"),
        help="Process date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--data-path",
        type=str,
        default=str(Path(__file__).parent.parent / "data"),
        help="Base data directory"
    )
    
    args = parser.parse_args()
    
    metrics = run_pipeline(args.date, Path(args.data_path))
    print(f"Pipeline metrics: {metrics}")


if __name__ == "__main__":
    main()
