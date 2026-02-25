#!/usr/bin/env bash
# ============================================================
# Spark-ling: GCP Infrastructure Setup
# ============================================================
# Creates GCS bucket, service account, and Dataproc cluster.
#
# Prerequisites:
#   1. gcloud CLI installed and authenticated (gcloud auth login)
#   2. A GCP project with billing enabled
#   3. Copy gcp/.env.example → gcp/.env and fill in your values
#
# Usage:
#   chmod +x gcp/setup_gcp.sh
#   ./gcp/setup_gcp.sh
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# ── Load configuration ──────────────────────────────────────
ENV_FILE="${SCRIPT_DIR}/.env"
if [[ ! -f "$ENV_FILE" ]]; then
    echo "❌ Missing ${ENV_FILE}"
    echo "   Copy the template and fill in your values:"
    echo "   cp gcp/.env.example gcp/.env"
    exit 1
fi
# shellcheck source=/dev/null
source "$ENV_FILE"

echo "============================================================"
echo "🔥 Spark-ling GCP Setup"
echo "============================================================"
echo "  Project:  ${GCP_PROJECT_ID}"
echo "  Region:   ${GCP_REGION}"
echo "  Zone:     ${GCP_ZONE}"
echo "  Bucket:   ${GCS_BUCKET}"
echo "  Cluster:  ${DATAPROC_CLUSTER}"
echo "============================================================"
echo ""

# ── Set active project ──────────────────────────────────────
echo "📋 Setting active project..."
gcloud config set project "${GCP_PROJECT_ID}"

# ── Enable required APIs ────────────────────────────────────
echo "🔌 Enabling required APIs..."
gcloud services enable \
    dataproc.googleapis.com \
    storage.googleapis.com \
    compute.googleapis.com \
    --quiet

# ── Create GCS bucket ───────────────────────────────────────
echo "🪣 Creating GCS bucket: gs://${GCS_BUCKET}..."
if gsutil ls -b "gs://${GCS_BUCKET}" &>/dev/null; then
    echo "   Bucket already exists, skipping."
else
    gsutil mb \
        -p "${GCP_PROJECT_ID}" \
        -l "${GCP_REGION}" \
        -b on \
        "gs://${GCS_BUCKET}"
    echo "   ✅ Bucket created."
fi

# Create bucket directory structure
echo "📁 Creating bucket directory structure..."
for dir in data/raw data/processed data/analytics staging jobs; do
    gsutil cp /dev/null "gs://${GCS_BUCKET}/${dir}/.keep" 2>/dev/null || true
done
echo "   ✅ Directory structure created."

# ── Create service account (optional, for key-based auth) ───
SA_NAME="sparkling-sa"
SA_EMAIL="${SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

echo "👤 Creating service account: ${SA_NAME}..."
if gcloud iam service-accounts describe "${SA_EMAIL}" &>/dev/null; then
    echo "   Service account already exists, skipping."
else
    gcloud iam service-accounts create "${SA_NAME}" \
        --display-name="Spark-ling Dataproc Service Account" \
        --quiet
    echo "   ✅ Service account created."
fi

# Grant permissions
echo "🔐 Granting permissions..."
for role in roles/dataproc.worker roles/storage.objectAdmin; do
    gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="${role}" \
        --quiet \
        --condition=None \
        >/dev/null 2>&1
done
echo "   ✅ Permissions granted."

# ── Create Dataproc cluster ─────────────────────────────────
echo "🖥️  Creating Dataproc cluster: ${DATAPROC_CLUSTER}..."
if gcloud dataproc clusters describe "${DATAPROC_CLUSTER}" \
    --region="${GCP_REGION}" &>/dev/null; then
    echo "   Cluster already exists, skipping."
else
    gcloud dataproc clusters create "${DATAPROC_CLUSTER}" \
        --region="${GCP_REGION}" \
        --zone="${GCP_ZONE}" \
        --image-version="${DATAPROC_IMAGE_VERSION}" \
        --master-machine-type="${DATAPROC_MASTER_MACHINE}" \
        --worker-machine-type="${DATAPROC_WORKER_MACHINE}" \
        --num-workers="${DATAPROC_NUM_WORKERS}" \
        --max-idle="${DATAPROC_MAX_IDLE}" \
        --optional-components=JUPYTER \
        --enable-component-gateway \
        --service-account="${SA_EMAIL}" \
        --bucket="${GCS_BUCKET}" \
        --properties="\
spark:spark.jars.packages=io.delta:delta-core_2.12:2.4.0,\
spark:spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,\
spark:spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog,\
spark:spark.sql.adaptive.enabled=true,\
spark:spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        --quiet
    echo "   ✅ Cluster created."
fi

# ── Summary ─────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "✅ GCP Setup Complete!"
echo "============================================================"
echo ""
echo "  GCS Bucket:   gs://${GCS_BUCKET}"
echo "  Cluster:      ${DATAPROC_CLUSTER} (${GCP_REGION})"
echo "  Jupyter:      Open via GCP Console → Dataproc → Web Interfaces"
echo ""
echo "Next steps:"
echo "  1. Upload data:   ./gcp/sync_data.sh upload"
echo "  2. Submit a job:   ./gcp/submit_job.sh pipelines/daily_transactions.py --date 2025-06-15"
echo "  3. When done:      ./gcp/teardown_gcp.sh"
echo "============================================================"
