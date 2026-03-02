"""
Spark Configuration for Local, GCP, AWS & Databricks Development
=================================================================
Supports four modes:
  - "local"       → Runs on your machine with local[*] (default)
  - "gcp"         → Runs on GCP Dataproc with GCS data paths
  - "aws"         → Runs on AWS EMR with S3 data paths
  - "databricks"  → Runs on Databricks serverless, data on S3

Usage:
    # Auto-detect environment (recommended)
    from configs.spark_config import get_spark_session, get_data_path, detect_mode
    mode = detect_mode()  # "local", "gcp", "aws", or "databricks"
    spark = get_spark_session("MyApp", mode=mode)
    raw = get_data_path("raw", mode=mode)

    # Or explicitly set mode
    spark = get_spark_session("MyApp", mode="aws")
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
        "databricks", "gcp", "aws", or "local"
    """
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return "databricks"
    elif os.environ.get("DATAPROC_CLUSTER"):
        return "gcp"
    elif os.environ.get("AWS_EXECUTION_ENV") or os.environ.get("EMR_CLUSTER_ID"):
        return "aws"
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


# ── AWS configuration ───────────────────────────────────────
def _load_aws_config() -> dict:
    """Load AWS config from aws/.env file if it exists."""
    env_file = PROJECT_ROOT / "aws" / ".env"
    config = {}
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    config[key.strip()] = value.strip()
    return config


def get_s3_bucket() -> str:
    """Get the S3 bucket name from config or environment."""
    bucket = os.environ.get("SPARKLING_S3_BUCKET", "")
    if not bucket:
        config = _load_aws_config()
        bucket = config.get("S3_BUCKET", "")
    if not bucket:
        raise ValueError(
            "S3_BUCKET not set. Copy aws/.env.example to aws/.env and fill in your values, "
            "or set the SPARKLING_S3_BUCKET environment variable."
        )
    return bucket


def get_data_path(layer: str = "raw", mode: str = "local") -> str:
    """
    Get the data path for a given layer, based on execution mode.

    Args:
        layer: Data layer - "raw", "processed", or "analytics"
        mode: "local", "gcp", "aws", or "databricks"

    Returns:
        Local file path, gs:// URI, or s3a:// URI
    """
    if mode in ("aws", "databricks"):
        # S3 is the primary cloud storage for both AWS and Databricks
        bucket = get_s3_bucket()
        return f"s3a://{bucket}/data/{layer}"
    elif mode == "gcp":
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
        mode: "local", "gcp", "aws", or "databricks"
        enable_delta: Whether to enable Delta Lake support

    Returns:
        Configured SparkSession
    """
    if mode == "databricks":
        return _build_databricks_session(app_name)
    elif mode == "gcp":
        return _build_gcp_session(app_name, enable_delta)
    elif mode == "aws":
        return _build_aws_session(app_name, enable_delta)
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


def _build_aws_session(app_name: str, enable_delta: bool) -> SparkSession:
    """
    Build SparkSession for AWS EMR or local with S3 access.

    On EMR, the cluster has Spark + Hadoop-AWS pre-installed.
    For local development, the hadoop-aws JAR is added via packages.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    # S3A configuration — needed for local dev; on EMR these are pre-set
    if not os.environ.get("EMR_CLUSTER_ID"):
        builder = builder \
            .config("spark.master", "local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262" +
                    (",io.delta:delta-core_2.12:2.4.0" if enable_delta else "")) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    else:
        # On EMR, just set Delta if needed
        if enable_delta:
            builder = builder \
                .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")

    if enable_delta:
        builder = builder \
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
║  • aws        → Uses YARN on EMR, data from S3 (s3a://)                     ║
║  • databricks → Uses Databricks serverless, data from S3                    ║
║                                                                              ║
║  STORAGE:                                                                    ║
║  • AWS S3 is the primary cloud storage for all modes                         ║
║  • Configure via aws/.env or SPARKLING_S3_BUCKET env var                     ║
║                                                                              ║
║  LOCAL TIPS:                                                                 ║
║  • Use local[*] to use all CPU cores                                        ║
║  • Use local[2] for lightweight testing                                     ║
║  • Reduce shuffle.partitions to 4-8 for small data                         ║
║  • Monitor at http://localhost:4040 when Spark is running                   ║
║                                                                              ║
║  AWS TIPS:                                                                   ║
║  • Use ./aws/setup_s3.sh to create the S3 bucket                            ║
║  • Use ./aws/sync_data.sh to upload/download data to S3                     ║
║  • Use ./aws/submit_emr_job.sh to submit jobs to EMR                        ║
║  • Configure credentials via aws configure or IAM roles                     ║
║                                                                              ║
║  DATABRICKS TIPS:                                                            ║
║  • SparkSession is pre-created — just use getOrCreate()                     ║
║  • Delta Lake is built-in — no extra config needed                          ║
║  • Data reads from S3 via Unity Catalog External Location                   ║
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
