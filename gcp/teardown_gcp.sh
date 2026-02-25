#!/usr/bin/env bash
# ============================================================
# Spark-ling: GCP Infrastructure Teardown
# ============================================================
# Deletes Dataproc cluster and optionally the GCS bucket.
# Run this when you're done working to avoid unnecessary charges.
#
# Usage:
#   ./gcp/teardown_gcp.sh              # Delete cluster only
#   ./gcp/teardown_gcp.sh --all        # Delete cluster + bucket + SA
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "❌ Missing ${ENV_FILE}. Nothing to tear down."
    exit 1
fi
# shellcheck source=/dev/null
source "$ENV_FILE"

DELETE_ALL=false
if [[ "${1:-}" == "--all" ]]; then
    DELETE_ALL=true
fi

echo "============================================================"
echo "🧹 Spark-ling GCP Teardown"
echo "============================================================"
echo ""

# ── Delete Dataproc cluster ─────────────────────────────────
echo "🖥️  Deleting Dataproc cluster: ${DATAPROC_CLUSTER}..."
if gcloud dataproc clusters describe "${DATAPROC_CLUSTER}" \
    --region="${GCP_REGION}" &>/dev/null; then
    gcloud dataproc clusters delete "${DATAPROC_CLUSTER}" \
        --region="${GCP_REGION}" \
        --quiet
    echo "   ✅ Cluster deleted."
else
    echo "   Cluster not found, skipping."
fi

if [[ "$DELETE_ALL" == true ]]; then
    # ── Delete GCS bucket ───────────────────────────────────
    echo ""
    echo "🪣 Deleting GCS bucket: gs://${GCS_BUCKET}..."
    if gsutil ls -b "gs://${GCS_BUCKET}" &>/dev/null; then
        echo "   ⚠️  This will delete ALL data in the bucket!"
        read -rp "   Are you sure? (y/N): " confirm
        if [[ "$confirm" =~ ^[Yy]$ ]]; then
            gsutil -m rm -r "gs://${GCS_BUCKET}"
            echo "   ✅ Bucket deleted."
        else
            echo "   Skipped bucket deletion."
        fi
    else
        echo "   Bucket not found, skipping."
    fi

    # ── Delete service account ──────────────────────────────
    SA_EMAIL="sparkling-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
    echo ""
    echo "👤 Deleting service account: ${SA_EMAIL}..."
    if gcloud iam service-accounts describe "${SA_EMAIL}" &>/dev/null; then
        gcloud iam service-accounts delete "${SA_EMAIL}" --quiet
        echo "   ✅ Service account deleted."
    else
        echo "   Service account not found, skipping."
    fi
fi

echo ""
echo "============================================================"
echo "✅ Teardown complete!"
if [[ "$DELETE_ALL" == false ]]; then
    echo "   (Bucket & SA kept. Use --all to remove everything.)"
fi
echo "============================================================"
