"""
Spark Configuration for Local & GCP Development
==================================================
Supports two modes:
  - "local"  → Runs on your machine with local[*] (default, unchanged)
  - "gcp"    → Runs on GCP Dataproc with GCS data paths

Usage:
    # Local development (unchanged from before)
    from configs.spark_config import get_spark_session
    spark = get_spark_session("MyApp")

    # GCP Dataproc (submitted via gcp/submit_job.sh)
    spark = get_spark_session("MyApp", mode="gcp")

    # Get data path that works in both modes
    from configs.spark_config import get_data_path
    raw = get_data_path("raw")  # local: data/raw, gcp: gs://bucket/data/raw
"""

import os
from pyspark.sql import SparkSession
from pathlib import Path

# ── Project paths ────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent

# Set HADOOP_HOME for Windows (required for Parquet writes)
HADOOP_HOME = PROJECT_ROOT / "hadoop"
os.environ["HADOOP_HOME"] = str(HADOOP_HOME)
os.environ["PATH"] = str(HADOOP_HOME / "bin") + os.pathsep + os.environ.get("PATH", "")

# Data paths (local)
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DATA_ANALYTICS = PROJECT_ROOT / "data" / "analytics"


# ── GCP configuration ───────────────────────────────────────
def _load_gcp_config() -> dict:
    """Load GCP config from gcp/.env file if it exists."""
    env_file = PROJECT_ROOT / "gcp" / ".env"
    config = {}
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    config[key.strip()] = value.strip()
    return config


def get_gcs_bucket() -> str:
    """Get the GCS bucket name from config."""
    config = _load_gcp_config()
    bucket = config.get("GCS_BUCKET", "")
    if not bucket:
        raise ValueError(
            "GCS_BUCKET not set. Copy gcp/.env.example to gcp/.env and fill in your values."
        )
    return bucket


def get_data_path(layer: str = "raw", mode: str = "local") -> str:
    """
    Get the data path for a given layer, based on execution mode.

    Args:
        layer: Data layer - "raw", "processed", or "analytics"
        mode: "local" or "gcp"

    Returns:
        Local file path or gs:// URI
    """
    if mode == "gcp":
        bucket = get_gcs_bucket()
        return f"gs://{bucket}/data/{layer}"
    else:
        return str(PROJECT_ROOT / "data" / layer)


# ── Spark session builders ───────────────────────────────────
def get_spark_session(
    app_name: str = "Spark-ling",
    mode: str = "local",
    enable_delta: bool = False,
) -> SparkSession:
    """
    Create a SparkSession configured for the given execution mode.

    Args:
        app_name: Name of the Spark application
        mode: "local" (default) or "gcp"
        enable_delta: Whether to enable Delta Lake support

    Returns:
        Configured SparkSession
    """
    if mode == "gcp":
        return _build_gcp_session(app_name, enable_delta)
    else:
        return _build_local_session(app_name, enable_delta)


def _build_local_session(app_name: str, enable_delta: bool) -> SparkSession:
    """Build SparkSession for local development (unchanged from original)."""
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


def _build_gcp_session(app_name: str, enable_delta: bool) -> SparkSession:
    """
    Build SparkSession for GCP Dataproc.

    On Dataproc, the cluster already has Spark installed and configured.
    We do NOT set master() — Dataproc handles that via YARN.
    Delta Lake is pre-configured via cluster properties in setup_gcp.sh.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    # Delta Lake — on Dataproc it's set at the cluster level,
    # but setting it here too for completeness / standalone use
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


# ── Configuration guide ──────────────────────────────────────
SPARK_CONFIG_GUIDE = """
╔══════════════════════════════════════════════════════════════════════════════╗
║                         SPARK CONFIGURATION GUIDE                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  MODES:                                                                      ║
║  • local  → Uses local[*], data from local filesystem (default)             ║
║  • gcp    → Uses YARN on Dataproc, data from GCS                            ║
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
║  GCP TIPS:                                                                   ║
║  • Use ./gcp/setup_gcp.sh to create the cluster                            ║
║  • Use ./gcp/submit_job.sh to submit jobs                                   ║
║  • Use ./gcp/teardown_gcp.sh when done to save costs                        ║
║  • Cluster auto-deletes after 30min idle (configurable in .env)             ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

if __name__ == "__main__":
    print(SPARK_CONFIG_GUIDE)

    # Test local configuration
    spark = get_spark_session("ConfigTest")
    print(f"\n✅ Spark {spark.version} initialized successfully! (local mode)")
    print(f"📁 Project Root: {PROJECT_ROOT}")
    print(f"🌐 Spark UI: http://localhost:4040")
    print(f"📂 Data paths:")
    print(f"   Raw:       {get_data_path('raw')}")
    print(f"   Processed: {get_data_path('processed')}")
    print(f"   Analytics: {get_data_path('analytics')}")
    spark.stop()
    print("✅ Spark session stopped cleanly.")
