#!/bin/bash
# ===========================================================================
# Build & Deploy: Lambda Layer + Function + Glue Script to S3
# ===========================================================================
#
# LEARNING NOTES:
# ---------------
# 1. LAMBDA LAYERS contain shared dependencies (like psycopg2).
#    Layer structure must be: python/<package_name>/
#
# 2. LAMBDA DEPLOYMENT PACKAGE is a zip containing handler.py.
#
# 3. We use Python's zipfile module instead of the zip command
#    for better portability across environments.
#
# USAGE:
#   chmod +x aws/glue_lambda/build_and_deploy.sh
#   ./aws/glue_lambda/build_and_deploy.sh
# ===========================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
S3_BUCKET="${S3_BUCKET:-sparkling-data-test}"
REGION="${AWS_REGION:-ap-southeast-1}"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Build & Deploy: Glue + Lambda to S3                       ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo "  S3 Bucket: ${S3_BUCKET}"
echo "  Region:    ${REGION}"
echo ""

# ── Step 1: Build psycopg2 Lambda Layer ──────────────────────────────────

echo "📦 Step 1: Building psycopg2 Lambda Layer..."

LAYER_DIR=$(mktemp -d)
mkdir -p "${LAYER_DIR}/python"

# Install psycopg2-binary for Lambda's Python 3.12 on x86_64
pip install psycopg2-binary numpy -t "${LAYER_DIR}/python" --quiet \
    --platform manylinux2014_x86_64 --only-binary=:all: \
    --python-version 3.12 2>/dev/null || \
pip install psycopg2-binary numpy -t "${LAYER_DIR}/python" --quiet 2>/dev/null

# Create layer zip using Python (no zip command needed)
LAYER_ZIP="${SCRIPT_DIR}/psycopg2-layer.zip"
python3 -c "
import zipfile, os, sys
layer_dir = '${LAYER_DIR}'
zip_path = '${LAYER_ZIP}'
with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
    for root, dirs, files in os.walk(os.path.join(layer_dir, 'python')):
        # Skip __pycache__ and dist-info
        dirs[:] = [d for d in dirs if d != '__pycache__' and not d.endswith('.dist-info')]
        for f in files:
            if f.endswith('.pyc'):
                continue
            full = os.path.join(root, f)
            arcname = os.path.relpath(full, layer_dir)
            zf.write(full, arcname)
print(f'   Created: {zip_path}')
size_mb = os.path.getsize(zip_path) / (1024*1024)
print(f'   Size: {size_mb:.1f} MB')
"

# Upload to S3
aws s3 cp "${LAYER_ZIP}" "s3://${S3_BUCKET}/layers/psycopg2-layer.zip" --region "${REGION}" --quiet
echo "   ✅ Uploaded to s3://${S3_BUCKET}/layers/psycopg2-layer.zip"

rm -rf "${LAYER_DIR}" "${LAYER_ZIP}"

# ── Step 2: Package Lambda Function ─────────────────────────────────────

echo ""
echo "📦 Step 2: Packaging Lambda function..."

LAMBDA_DIR="${SCRIPT_DIR}/lambda_daily_generator"
LAMBDA_ZIP="${SCRIPT_DIR}/daily-generator.zip"

python3 -c "
import zipfile
with zipfile.ZipFile('${LAMBDA_ZIP}', 'w', zipfile.ZIP_DEFLATED) as zf:
    zf.write('${LAMBDA_DIR}/handler.py', 'handler.py')
import os
size_kb = os.path.getsize('${LAMBDA_ZIP}') / 1024
print(f'   Lambda package: {size_kb:.1f} KB')
"

aws s3 cp "${LAMBDA_ZIP}" "s3://${S3_BUCKET}/lambda/daily-generator.zip" --region "${REGION}" --quiet
echo "   ✅ Uploaded to s3://${S3_BUCKET}/lambda/daily-generator.zip"

rm -f "${LAMBDA_ZIP}"

# ── Step 3: Upload Glue Script ──────────────────────────────────────────

echo ""
echo "📦 Step 3: Uploading Glue seed script..."

aws s3 cp "${SCRIPT_DIR}/rds_seed_glue.py" "s3://${S3_BUCKET}/scripts/rds_seed_glue.py" --region "${REGION}" --quiet
echo "   ✅ Uploaded to s3://${S3_BUCKET}/scripts/rds_seed_glue.py"

# ── Summary ─────────────────────────────────────────────────────────────

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "✅ All artifacts uploaded to S3!"
echo ""
echo "S3 contents:"
echo "  s3://${S3_BUCKET}/layers/psycopg2-layer.zip    (Lambda Layer)"
echo "  s3://${S3_BUCKET}/lambda/daily-generator.zip    (Lambda Code)"
echo "  s3://${S3_BUCKET}/scripts/rds_seed_glue.py      (Glue Script)"
echo ""
echo "Next: Deploy CloudFormation stack:"
echo "  aws cloudformation deploy \\"
echo "    --template-file aws/glue_lambda/glue_lambda_setup.yaml \\"
echo "    --stack-name sparkling-glue-lambda \\"
echo "    --capabilities CAPABILITY_NAMED_IAM \\"
echo "    --parameter-overrides \\"
echo "      RDSHost=<YOUR_RDS_ENDPOINT> \\"
echo "      RDSPassword='SparkLing2026!' \\"
echo "    --region ${REGION}"
echo "════════════════════════════════════════════════════════════════"
