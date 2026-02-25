# 🔥 Spark-ling: GCP Setup Guide

Run your Spark-ling pipelines on **Google Cloud Dataproc** while continuing to develop locally.

---

## Prerequisites

| Requirement | How to check |
|---|---|
| **GCP Account** with billing enabled | [console.cloud.google.com](https://console.cloud.google.com) |
| **gcloud CLI** installed | `gcloud version` |
| **Authenticated** to GCP | `gcloud auth login` |
| **Python 3.9+** | `python --version` |

If you don't have the gcloud CLI:
```bash
# Install: https://cloud.google.com/sdk/docs/install
curl https://sdk.cloud.google.com | bash
exec -l $SHELL
gcloud init
```

---

## 1. Configure Your Environment

```bash
# Copy the template
cp gcp/.env.example gcp/.env

# Edit with your GCP details
nano gcp/.env
```

Fill in these values in `gcp/.env`:

| Variable | Description | Example |
|---|---|---|
| `GCP_PROJECT_ID` | Your GCP project ID | `my-spark-project-123` |
| `GCP_REGION` | Preferred region | `asia-southeast1` |
| `GCP_ZONE` | Zone within region | `asia-southeast1-a` |

The rest have sensible defaults. Save and close.

---

## 2. Create GCP Infrastructure

```bash
./gcp/setup_gcp.sh
```

This creates:
- ✅ **GCS Bucket** — stores your data and job artifacts
- ✅ **Service Account** — with minimal permissions (Dataproc Worker + Storage Admin)
- ✅ **Dataproc Cluster** — Spark 3.4.x + Delta Lake, Jupyter enabled, auto-stops after 30min idle

---

## 3. Upload Data to GCS

```bash
# Upload raw data (local → GCS)
./gcp/sync_data.sh upload raw

# Upload all data layers
./gcp/sync_data.sh upload
```

---

## 4. Submit a Job to Dataproc

```bash
# Run daily transactions pipeline on GCP
./gcp/submit_job.sh pipelines/daily_transactions.py -- --date 2025-06-15

# Run customer dimension SCD pipeline
./gcp/submit_job.sh pipelines/customer_dim_scd.py
```

Monitor job progress in the [Dataproc Jobs console](https://console.cloud.google.com/dataproc/jobs).

---

## 5. Using GCP Mode in Your Code

The `spark_config.py` now supports a `mode` parameter:

```python
from configs.spark_config import get_spark_session, get_data_path

# Local development (default — unchanged!)
spark = get_spark_session("MyApp")
data = spark.read.csv(get_data_path("raw") + "/transactions.csv")

# GCP Dataproc (set mode="gcp" when submitting to cluster)
spark = get_spark_session("MyApp", mode="gcp")
data = spark.read.csv(get_data_path("raw", mode="gcp") + "/transactions.csv")
```

> **Tip**: For pipelines, you can use `argparse` or environment variables to toggle mode:
> ```python
> import os
> MODE = os.getenv("SPARK_MODE", "local")
> spark = get_spark_session("Pipeline", mode=MODE)
> ```

---

## 6. Download Results

```bash
# Download processed and analytics data from GCS
./gcp/sync_data.sh download processed
./gcp/sync_data.sh download analytics
```

---

## 7. Jupyter on Dataproc (Optional)

The cluster has Jupyter enabled. Access it via:
1. Go to **GCP Console → Dataproc → Clusters**
2. Click your cluster → **Web Interfaces** tab
3. Click **JupyterLab**

Upload notebooks directly or use them alongside local development.

---

## 8. Tear Down When Done

```bash
# Delete the cluster (keeps bucket + data)
./gcp/teardown_gcp.sh

# Delete EVERYTHING (cluster + bucket + service account)
./gcp/teardown_gcp.sh --all
```

> ⚠️ **Cost tip**: The cluster auto-deletes after 30 minutes idle, but always run teardown when you're done for the day.

---

## Cost Estimation

| Resource | Cost | Notes |
|---|---|---|
| **Dataproc cluster** (3× n1-standard-4) | ~$0.30/hr | Auto-deletes after 30min idle |
| **GCS storage** | ~$0.02/GB/month | Minimal for this project (~1GB) |
| **Data transfer** | Free within region | Egress charged if downloading externally |

**Estimated daily cost**: $1–3 for a few hours of active development.

---

## Troubleshooting

| Issue | Solution |
|---|---|
| `gcloud: command not found` | [Install gcloud CLI](https://cloud.google.com/sdk/docs/install) |
| `Permission denied` on scripts | `chmod +x gcp/*.sh` |
| `API not enabled` | Run `gcloud services enable dataproc.googleapis.com storage.googleapis.com` |
| Cluster creation fails | Check quota in GCP Console → IAM → Quotas |
| `GCS_BUCKET not set` | Copy `gcp/.env.example` to `gcp/.env` and fill in values |

---

## File Reference

```
gcp/
├── .env.example      # Template — copy to .env
├── .env              # Your config (gitignored)
├── setup_gcp.sh      # Create all GCP resources
├── teardown_gcp.sh   # Delete resources when done
├── submit_job.sh     # Submit PySpark job to Dataproc
└── sync_data.sh      # Upload/download data to/from GCS
```
