#!/bin/bash
# ═══════════════════════════════════════════════════════════
# Teardown AWS Resources for Spark-ling
# ═══════════════════════════════════════════════════════════
# Deletes S3 bucket and all contents. USE WITH CAUTION.
#
# Usage:
#   ./aws/teardown.sh              # Interactive confirmation
#   ./aws/teardown.sh --force      # Skip confirmation
# ═══════════════════════════════════════════════════════════

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load config
if [ ! -f "$SCRIPT_DIR/.env" ]; then
    echo "❌ aws/.env not found."
    exit 1
fi
source "$SCRIPT_DIR/.env"

BUCKET="${S3_BUCKET:-sparkling-data-${AWS_ACCOUNT_ID}}"
FORCE="${1:-}"

echo "╔═══════════════════════════════════════════╗"
echo "║   ⚠️  Spark-ling AWS Teardown              ║"
echo "╠═══════════════════════════════════════════╣"
echo "║  This will DELETE:                        ║"
echo "║  • S3 bucket: $BUCKET"
echo "║  • All data inside the bucket             ║"
echo "╚═══════════════════════════════════════════╝"
echo ""

if [ "$FORCE" != "--force" ]; then
    read -p "Are you sure? Type 'DELETE' to confirm: " CONFIRM
    if [ "$CONFIRM" != "DELETE" ]; then
        echo "❌ Cancelled."
        exit 0
    fi
fi

echo "🗑️  Deleting all objects (including versions)..."
aws s3api list-object-versions --bucket "$BUCKET" --output json 2>/dev/null | \
    python3 -c "
import sys, json
data = json.load(sys.stdin)
objects = []
for v in data.get('Versions', []):
    objects.append({'Key': v['Key'], 'VersionId': v['VersionId']})
for d in data.get('DeleteMarkers', []):
    objects.append({'Key': d['Key'], 'VersionId': d['VersionId']})
if objects:
    # Delete in batches of 1000
    for i in range(0, len(objects), 1000):
        batch = objects[i:i+1000]
        print(json.dumps({'Objects': batch, 'Quiet': True}))
" | while read -r batch; do
    echo "$batch" | aws s3api delete-objects --bucket "$BUCKET" --delete file:///dev/stdin > /dev/null 2>&1 || true
done

echo "🗑️  Deleting bucket..."
aws s3 rb "s3://$BUCKET" --force 2>/dev/null || \
    aws s3api delete-bucket --bucket "$BUCKET" 2>/dev/null || true

echo ""
echo "✅ Teardown complete. Bucket s3://$BUCKET deleted."
