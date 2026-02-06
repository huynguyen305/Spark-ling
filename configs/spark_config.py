"""
Spark Configuration for Local Development
==========================================
Optimized settings for local machine with 8-16GB RAM.
Adjust based on your machine's resources.
"""

import os
from pyspark.sql import SparkSession
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent

# Set HADOOP_HOME for Windows (required for Parquet writes)
HADOOP_HOME = PROJECT_ROOT / "hadoop"
os.environ["HADOOP_HOME"] = str(HADOOP_HOME)
os.environ["PATH"] = str(HADOOP_HOME / "bin") + os.pathsep + os.environ.get("PATH", "")

# Data paths
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DATA_ANALYTICS = PROJECT_ROOT / "data" / "analytics"


def get_spark_session(app_name: str = "Spark-ling", enable_delta: bool = False) -> SparkSession:
    """
    Create a SparkSession with optimized local settings.
    
    Args:
        app_name: Name of the Spark application
        enable_delta: Whether to enable Delta Lake support
    
    Returns:
        Configured SparkSession
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.warehouse.dir", str(PROJECT_ROOT / "spark-warehouse")) \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties")
    
    if enable_delta:
        builder = builder \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    return builder.getOrCreate()


def get_spark_session_minimal(app_name: str = "Spark-ling-Minimal") -> SparkSession:
    """
    Lightweight SparkSession for quick experiments.
    Uses less memory, suitable for small data exploration.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[2]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


# Common Spark configurations explained
SPARK_CONFIG_GUIDE = """
╔══════════════════════════════════════════════════════════════════════════════╗
║                         SPARK CONFIGURATION GUIDE                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  spark.driver.memory         Memory for the driver (your local machine)     ║
║  spark.executor.memory       Memory per executor (local = same as driver)   ║
║  spark.sql.shuffle.partitions  Number of partitions after shuffle           ║
║                               Default 200 is too high for local dev         ║
║  spark.sql.adaptive.enabled  Enable Adaptive Query Execution (AQE)          ║
║                               Auto-optimizes shuffle partitions             ║
║  spark.serializer            Kryo is faster than default Java serializer    ║
║                                                                              ║
║  LOCAL TIPS:                                                                 ║
║  • Use local[*] to use all CPU cores                                        ║
║  • Use local[2] for lightweight testing                                     ║
║  • Reduce shuffle.partitions to 4-8 for small data                         ║
║  • Monitor at http://localhost:4040 when Spark is running                   ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

if __name__ == "__main__":
    print(SPARK_CONFIG_GUIDE)
    
    # Test configuration
    spark = get_spark_session("ConfigTest")
    print(f"\n✅ Spark {spark.version} initialized successfully!")
    print(f"📁 Project Root: {PROJECT_ROOT}")
    print(f"🌐 Spark UI: http://localhost:4040")
    spark.stop()
    print("✅ Spark session stopped cleanly.")
