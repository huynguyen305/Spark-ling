"""
Spark Configuration for Local, GCP & Databricks Development
==============================================================
Supports three modes:
  - "local"       → Runs on your machine with local[*] (default)
  - "gcp"         → Runs on GCP Dataproc with GCS data paths
  - "databricks"  → Runs on Databricks serverless, data on GCS

Usage:
    # Auto-detect environment (recommended)
    from configs.spark_config import get_spark_session, get_data_path, detect_mode
    mode = detect_mode()  # "local", "gcp", or "databricks"
    spark = get_spark_session("MyApp", mode=mode)
    raw = get_data_path("raw", mode=mode)

    # Or explicitly set mode
    spark = get_spark_session("MyApp", mode="databricks")
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


# ── Environment detection ───────────────────────────────────
def detect_mode() -> str:
    """
    Auto-detect which execution environment we're running in.

    Returns:
        "databricks", "gcp", or "local"
    """
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return "databricks"
    elif os.environ.get("DATAPROC_CLUSTER"):
        return "gcp"
    else:
        return "local"


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
        mode: "local", "gcp", or "databricks"

    Returns:
        Local file path or gs:// URI
    """
    if mode in ("gcp", "databricks"):
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
        mode: "local", "gcp", or "databricks"
        enable_delta: Whether to enable Delta Lake support

    Returns:
        Configured SparkSession
    """
    if mode == "databricks":
        return _build_databricks_session(app_name)
    elif mode == "gcp":
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


def _build_databricks_session(app_name: str) -> SparkSession:
    """
    Build SparkSession for Databricks (serverless or cluster).

    On Databricks, a SparkSession is pre-created and fully managed.
    We just get the existing session — no need to set master, memory,
    or Delta config (all handled by the runtime).
    """
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()


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
║  • local      → Uses local[*], data from local filesystem (default)         ║
║  • gcp        → Uses YARN on Dataproc, data from GCS                        ║
║  • databricks → Uses Databricks serverless, data from GCS                   ║
║                                                                              ║
║  AUTO-DETECT:                                                                ║
║  • detect_mode() auto-selects the right mode based on environment           ║
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
║  DATABRICKS TIPS:                                                            ║
║  • SparkSession is pre-created — just use getOrCreate()                     ║
║  • Delta Lake is built-in — no extra config needed                          ║
║  • Use Unity Catalog External Location for GCS access                       ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

if __name__ == "__main__":
    print(SPARK_CONFIG_GUIDE)

    # Test local configuration
    print(f"  Detected mode: {detect_mode()}")
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
