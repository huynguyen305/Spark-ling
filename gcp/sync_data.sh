#!/usr/bin/env bash
# ============================================================
# Spark-ling: Sync Data Between Local and GCS
# ============================================================
# Upload local data to GCS or download GCS data to local.
#
# Usage:
#   ./gcp/sync_data.sh upload              # local → GCS (raw data)
#   ./gcp/sync_data.sh download            # GCS → local (all data)
#   ./gcp/sync_data.sh upload processed    # upload processed data
#   ./gcp/sync_data.sh download analytics  # download analytics only
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="${SCRIPT_DIR}/.env"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "❌ Missing ${ENV_FILE}"
    exit 1
fi
# shellcheck source=/dev/null
source "$ENV_FILE"

ACTION="${1:-}"
DATA_LAYER="${2:-all}"
LOCAL_DATA="${PROJECT_ROOT}/data"
GCS_DATA="gs://${GCS_BUCKET}/data"

if [[ -z "$ACTION" ]]; then
    echo "Usage: $0 <upload|download> [raw|processed|analytics|all]"
    exit 1
fi

sync_layer() {
    local layer="$1"
    local src dst

    if [[ "$ACTION" == "upload" ]]; then
        src="${LOCAL_DATA}/${layer}/"
        dst="${GCS_DATA}/${layer}/"
        echo "📤 Uploading ${layer}: ${src} → ${dst}"
        if [[ -d "$src" ]]; then
            gsutil -m rsync -r "$src" "$dst"
            echo "   ✅ ${layer} uploaded."
        else
            echo "   ⚠️  Local ${src} does not exist, skipping."
        fi
    elif [[ "$ACTION" == "download" ]]; then
        src="${GCS_DATA}/${layer}/"
        dst="${LOCAL_DATA}/${layer}/"
        echo "📥 Downloading ${layer}: ${src} → ${dst}"
        mkdir -p "$dst"
        gsutil -m rsync -r "$src" "$dst"
        echo "   ✅ ${layer} downloaded."
    fi
}

echo "============================================================"
echo "🔄 Spark-ling: Data Sync (${ACTION})"
echo "============================================================"
echo ""

if [[ "$DATA_LAYER" == "all" ]]; then
    for layer in raw processed analytics; do
        sync_layer "$layer"
    done
else
    sync_layer "$DATA_LAYER"
fi

echo ""
echo "✅ Sync complete!"
echo "============================================================"
