"""
Customer Dimension SCD Pipeline
===============================
Maintains customer dimension with SCD Type 2 history tracking.

This pipeline:
1. Reads current dimension and new/updated customer data
2. Applies SCD Type 2 logic
3. Updates dimension table

Usage:
    python pipelines/customer_dim_scd.py
"""

from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from scd_handler import SCDHandler


def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("CustomerDimSCD") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()


def run_scd_pipeline(data_path: Path) -> dict:
    """
    Run SCD Type 2 update for customer dimension.
    """
    print(f"\n{'='*60}")
    print("Customer Dimension SCD Pipeline")
    print(f"{'='*60}")
    
    spark = create_spark_session()
    
    try:
        dim_path = data_path / "processed" / "dim_customer"
        raw_path = data_path / "raw" / "customers.csv"
        
        # Initialize SCD handler
        handler = SCDHandler(
            spark=spark,
            business_key="customer_id",
            tracked_columns=["name", "segment", "kyc_status"],
            surrogate_key="customer_key"
        )
        
        # Read source data
        print("\n📖 Reading source customer data...")
        source_customers = spark.read.csv(str(raw_path), header=True, inferSchema=True)
        source_count = source_customers.count()
        print(f"   Source records: {source_count:,}")
        
        # Check if dimension exists
        if dim_path.exists():
            print("\n📖 Reading existing dimension...")
            existing_dim = spark.read.parquet(str(dim_path))
            existing_count = existing_dim.count()
            current_count = existing_dim.filter(col("is_current")).count()
            print(f"   Existing records: {existing_count:,} (current: {current_count:,})")
            
            # Apply SCD Type 2
            print("\n🔄 Applying SCD Type 2...")
            updated_dim = handler.apply_scd2(existing_dim, source_customers)
            
        else:
            print("\n🆕 Initializing new dimension...")
            updated_dim = handler.initialize_dimension(source_customers)
        
        # Write updated dimension
        print("\n💾 Writing dimension...")
        updated_dim.write.mode("overwrite").parquet(str(dim_path))
        
        new_count = updated_dim.count()
        new_current = updated_dim.filter(col("is_current")).count()
        historical = new_count - new_current
        
        metrics = {
            "status": "success",
            "total_records": new_count,
            "current_records": new_current,
            "historical_records": historical
        }
        
        print(f"\n{'='*60}")
        print("✅ SCD Pipeline completed!")
        print(f"   Total: {new_count:,} | Current: {new_current:,} | Historical: {historical:,}")
        print(f"{'='*60}\n")
        
        return metrics
        
    finally:
        spark.stop()


if __name__ == "__main__":
    data_path = Path(__file__).parent.parent / "data"
    run_scd_pipeline(data_path)
