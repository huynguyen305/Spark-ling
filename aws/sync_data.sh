#!/bin/bash
# ═══════════════════════════════════════════════════════════
# Sync Data: Local ↔ S3
# ═══════════════════════════════════════════════════════════
# Upload local data to S3 or download from S3 to local.
#
# Usage:
#   ./aws/sync_data.sh upload       # Local → S3
#   ./aws/sync_data.sh download     # S3 → Local
#   ./aws/sync_data.sh status       # Show bucket contents
# ═══════════════════════════════════════════════════════════

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Load config
if [ ! -f "$SCRIPT_DIR/.env" ]; then
    echo "❌ aws/.env not found. Run: cp aws/.env.example aws/.env"
    exit 1
fi
source "$SCRIPT_DIR/.env"

BUCKET="${S3_BUCKET:-sparkling-data-${AWS_ACCOUNT_ID}}"
LOCAL_DATA="$PROJECT_ROOT/data"
S3_DATA="s3://$BUCKET/data"

ACTION="${1:-status}"

case "$ACTION" in
    upload)
        echo "⬆️  Uploading local data → S3..."
        echo "   From: $LOCAL_DATA"
        echo "   To:   $S3_DATA"
        echo ""

        for layer in raw processed analytics; do
            if [ -d "$LOCAL_DATA/$layer" ]; then
                echo "   📂 Syncing $layer/..."
                aws s3 sync "$LOCAL_DATA/$layer" "$S3_DATA/$layer/" \
                    --exclude "*.DS_Store" \
                    --exclude "__pycache__/*"
                echo "   ✅ $layer synced"
            else
                echo "   ⚠️  $LOCAL_DATA/$layer not found, skipping"
            fi
        done

        echo ""
        echo "✅ Upload complete!"
        echo "   View: aws s3 ls $S3_DATA/ --recursive --human-readable"
        ;;

    download)
        echo "⬇️  Downloading S3 → local..."
        echo "   From: $S3_DATA"
        echo "   To:   $LOCAL_DATA"
        echo ""

        for layer in raw processed analytics; do
            mkdir -p "$LOCAL_DATA/$layer"
            echo "   📂 Syncing $layer/..."
            aws s3 sync "$S3_DATA/$layer/" "$LOCAL_DATA/$layer/" \
                --exclude "*.DS_Store"
            echo "   ✅ $layer synced"
        done

        echo ""
        echo "✅ Download complete!"
        ;;

    status)
        echo "📊 S3 Bucket Status: s3://$BUCKET"
        echo ""
        echo "── Data folders ─────────────────────────────────"
        aws s3 ls "$S3_DATA/" --recursive --human-readable --summarize 2>/dev/null || \
            echo "   (empty or bucket not found)"
        ;;

    *)
        echo "Usage: $0 {upload|download|status}"
        echo ""
        echo "  upload    Sync local data/ → S3"
        echo "  download  Sync S3 → local data/"
        echo "  status    Show S3 bucket contents"
        exit 1
        ;;
esac
