#!/bin/bash
# ═══════════════════════════════════════════════════════════
# Setup S3 Bucket for Spark-ling
# ═══════════════════════════════════════════════════════════
# Creates an S3 bucket with versioning & lifecycle policies.
#
# Prerequisites:
#   - AWS CLI installed & configured (aws configure)
#   - cp aws/.env.example aws/.env && fill in your values
#
# Usage:
#   ./aws/setup_s3.sh
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
REGION="${AWS_REGION:-ap-southeast-1}"

echo "╔═══════════════════════════════════════════╗"
echo "║   Spark-ling S3 Setup                     ║"
echo "╠═══════════════════════════════════════════╣"
echo "║  Bucket:  $BUCKET"
echo "║  Region:  $REGION"
echo "╚═══════════════════════════════════════════╝"
echo ""

# ── Create bucket ────────────────────────────────────────
echo "📦 Creating S3 bucket..."
if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
    echo "   ✅ Bucket already exists: s3://$BUCKET"
else
    if [ "$REGION" = "us-east-1" ]; then
        aws s3api create-bucket \
            --bucket "$BUCKET" \
            --region "$REGION"
    else
        aws s3api create-bucket \
            --bucket "$BUCKET" \
            --region "$REGION" \
            --create-bucket-configuration LocationConstraint="$REGION"
    fi
    echo "   ✅ Created: s3://$BUCKET"
fi

# ── Enable versioning ───────────────────────────────────
echo "📋 Enabling versioning..."
aws s3api put-bucket-versioning \
    --bucket "$BUCKET" \
    --versioning-configuration Status=Enabled
echo "   ✅ Versioning enabled"

# ── Create folder structure ─────────────────────────────
echo "📂 Creating data folder structure..."
for folder in data/raw data/processed data/analytics data/quarantine; do
    aws s3api put-object --bucket "$BUCKET" --key "$folder/" > /dev/null
    echo "   ✅ s3://$BUCKET/$folder/"
done

# ── Block public access ─────────────────────────────────
echo "🔒 Blocking public access..."
aws s3api put-public-access-block \
    --bucket "$BUCKET" \
    --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
echo "   ✅ Public access blocked"

# ── Lifecycle policy (optional: delete old versions after 30 days) ──
echo "♻️  Setting lifecycle policy..."
aws s3api put-bucket-lifecycle-configuration \
    --bucket "$BUCKET" \
    --lifecycle-configuration '{
        "Rules": [
            {
                "ID": "ExpireOldVersions",
                "Status": "Enabled",
                "NoncurrentVersionExpiration": {
                    "NoncurrentDays": 30
                },
                "Filter": {
                    "Prefix": ""
                }
            }
        ]
    }'
echo "   ✅ Old versions expire after 30 days"

echo ""
echo "══════════════════════════════════════════════"
echo "✅ S3 setup complete!"
echo ""
echo "   Bucket URI:  s3://$BUCKET"
echo "   Console:     https://s3.console.aws.amazon.com/s3/buckets/$BUCKET"
echo ""
echo "   Next steps:"
echo "   1. Generate data:  python src/data_generator.py"
echo "   2. Upload data:    ./aws/sync_data.sh upload"
echo "   3. Run Spark:      Use mode='aws' in spark_config"
echo "══════════════════════════════════════════════"
