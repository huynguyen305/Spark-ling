"""
SCD Type 2 Handler
==================
Production-ready Slowly Changing Dimension implementation.
Supports Type 1 (overwrite) and Type 2 (history tracking).
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, when, current_date, current_timestamp,
    monotonically_increasing_id, md5, concat_ws, coalesce
)
from typing import List, Optional
from datetime import date


class SCDHandler:
    """
    Handler for Slowly Changing Dimensions.
    
    Usage:
        handler = SCDHandler(spark, "customer_id", ["name", "segment", "kyc_status"])
        result = handler.apply_scd2(existing_dim, new_data)
    """
    
    def __init__(
        self,
        spark: SparkSession,
        business_key: str,
        tracked_columns: List[str],
        surrogate_key: str = "surrogate_key"
    ):
        self.spark = spark
        self.business_key = business_key
        self.tracked_columns = tracked_columns
        self.surrogate_key = surrogate_key
    
    def initialize_dimension(
        self,
        source_df: DataFrame,
        additional_cols: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Initialize a new SCD Type 2 dimension table from source data.
        
        Adds: surrogate_key, effective_date, expiration_date, is_current, hash_diff
        """
        cols_to_select = [self.business_key] + self.tracked_columns
        if additional_cols:
            cols_to_select.extend(additional_cols)
        
        return source_df.select(*cols_to_select).withColumn(
            self.surrogate_key, monotonically_increasing_id()
        ).withColumn(
            "hash_diff", md5(concat_ws("|", *[col(c) for c in self.tracked_columns]))
        ).withColumn(
            "effective_date", current_date()
        ).withColumn(
            "expiration_date", lit("9999-12-31").cast("date")
        ).withColumn(
            "is_current", lit(True)
        ).withColumn(
            "load_timestamp", current_timestamp()
        )
    
    def apply_scd1(self, existing_df: DataFrame, updates_df: DataFrame) -> DataFrame:
        """
        Apply SCD Type 1 logic (overwrite, no history).
        
        Simple: update existing records with new values.
        """
        # Get non-updated records
        unchanged = existing_df.join(
            updates_df.select(self.business_key),
            self.business_key,
            "left_anti"
        )
        
        # Get updated records (new values)
        updated = existing_df.select(self.surrogate_key, self.business_key).join(
            updates_df,
            self.business_key
        ).withColumn("load_timestamp", current_timestamp())
        
        return unchanged.unionByName(updated, allowMissingColumns=True)
    
    def apply_scd2(
        self,
        existing_df: DataFrame,
        updates_df: DataFrame,
        effective_date: Optional[date] = None
    ) -> DataFrame:
        """
        Apply SCD Type 2 logic (full history tracking).
        
        Steps:
        1. Identify changed records (hash comparison)
        2. Expire old versions
        3. Insert new versions
        4. Keep unchanged records
        """
        eff_date = effective_date or date.today()
        
        # Add hash to updates
        updates_with_hash = updates_df.withColumn(
            "new_hash", md5(concat_ws("|", *[col(c) for c in self.tracked_columns]))
        )
        
        # Current records from existing dimension
        current_records = existing_df.filter(col("is_current"))
        
        # Join to find changes
        joined = current_records.alias("existing").join(
            updates_with_hash.alias("updates"),
            col(f"existing.{self.business_key}") == col(f"updates.{self.business_key}"),
            "inner"
        )
        
        # Identify changed records (different hash)
        changed_keys = joined.filter(
            col("existing.hash_diff") != col("updates.new_hash")
        ).select(col(f"existing.{self.business_key}").alias("changed_key"))
        
        # 1. Expire old versions of changed records
        expired = current_records.join(
            changed_keys,
            current_records[self.business_key] == changed_keys["changed_key"]
        ).drop("changed_key").withColumn(
            "expiration_date", lit(str(eff_date)).cast("date")
        ).withColumn(
            "is_current", lit(False)
        )
        
        # 2. Create new versions from updates
        new_versions = updates_with_hash.join(
            changed_keys,
            updates_with_hash[self.business_key] == changed_keys["changed_key"]
        ).drop("changed_key").withColumn(
            self.surrogate_key, monotonically_increasing_id() + 1000000000  # Offset to avoid collision
        ).withColumn(
            "hash_diff", col("new_hash")
        ).drop("new_hash").withColumn(
            "effective_date", lit(str(eff_date)).cast("date")
        ).withColumn(
            "expiration_date", lit("9999-12-31").cast("date")
        ).withColumn(
            "is_current", lit(True)
        ).withColumn(
            "load_timestamp", current_timestamp()
        )
        
        # 3. Keep unchanged records (both current and historical)
        all_changed_keys = changed_keys.union(
            expired.select(col(self.business_key).alias("changed_key"))
        )
        unchanged = existing_df.join(
            all_changed_keys,
            existing_df[self.business_key] == all_changed_keys["changed_key"],
            "left_anti"
        )
        
        # 4. Handle new records (not in existing)
        existing_keys = existing_df.select(self.business_key).distinct()
        truly_new = updates_with_hash.join(
            existing_keys,
            self.business_key,
            "left_anti"
        ).withColumn(
            self.surrogate_key, monotonically_increasing_id() + 2000000000
        ).withColumn(
            "hash_diff", col("new_hash")
        ).drop("new_hash").withColumn(
            "effective_date", lit(str(eff_date)).cast("date")
        ).withColumn(
            "expiration_date", lit("9999-12-31").cast("date")
        ).withColumn(
            "is_current", lit(True)
        ).withColumn(
            "load_timestamp", current_timestamp()
        )
        
        # Combine all parts
        return unchanged.unionByName(expired, allowMissingColumns=True) \
            .unionByName(new_versions, allowMissingColumns=True) \
            .unionByName(truly_new, allowMissingColumns=True)
    
    def get_current_state(self, scd_df: DataFrame) -> DataFrame:
        """Get current state of all records."""
        return scd_df.filter(col("is_current"))
    
    def get_state_as_of(self, scd_df: DataFrame, as_of_date: str) -> DataFrame:
        """Get state as of a specific date."""
        return scd_df.filter(
            (col("effective_date") <= as_of_date) &
            (col("expiration_date") >= as_of_date)
        )
    
    def get_change_history(self, scd_df: DataFrame, business_key_value: str) -> DataFrame:
        """Get full change history for a specific record."""
        return scd_df.filter(
            col(self.business_key) == business_key_value
        ).orderBy("effective_date")


def merge_with_delta(
    spark: SparkSession,
    target_path: str,
    source_df: DataFrame,
    merge_key: str,
    update_columns: List[str]
) -> None:
    """
    Merge pattern for Delta Lake (if available).
    
    This is the preferred pattern for production SCD implementations.
    """
    try:
        from delta.tables import DeltaTable
        
        target = DeltaTable.forPath(spark, target_path)
        
        target.alias("target").merge(
            source_df.alias("source"),
            f"target.{merge_key} = source.{merge_key}"
        ).whenMatchedUpdate(set={
            c: f"source.{c}" for c in update_columns
        }).whenNotMatchedInsertAll().execute()
        
    except ImportError:
        raise ImportError("Delta Lake not available. Install delta-spark package.")
