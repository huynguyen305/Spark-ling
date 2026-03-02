#!/bin/bash
# ═══════════════════════════════════════════════════════════
# Submit Spark Job to AWS EMR
# ═══════════════════════════════════════════════════════════
# Submits a PySpark job to an existing EMR cluster.
#
# Prerequisites:
#   - Running EMR cluster (see setup instructions in docs/AWS_SETUP.md)
#   - AWS CLI configured with appropriate permissions
#
# Usage:
#   ./aws/submit_emr_job.sh pipelines/daily_transactions.py --date 2025-01-15
#   ./aws/submit_emr_job.sh src/data_generator.py
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
CLUSTER_ID="${EMR_CLUSTER_ID:-}"

# Parse arguments
SCRIPT_PATH="${1:?Usage: $0 <script.py> [args...]}"
shift
SCRIPT_ARGS="$@"
SCRIPT_NAME="$(basename "$SCRIPT_PATH")"

if [ -z "$CLUSTER_ID" ]; then
    echo "❌ EMR_CLUSTER_ID not set in aws/.env"
    echo "   Create a cluster first or set the ID of an existing cluster."
    exit 1
fi

echo "╔═══════════════════════════════════════════╗"
echo "║   Submit Spark Job to EMR                 ║"
echo "╠═══════════════════════════════════════════╣"
echo "║  Cluster:   $CLUSTER_ID"
echo "║  Script:    $SCRIPT_NAME"
echo "║  Args:      $SCRIPT_ARGS"
echo "╚═══════════════════════════════════════════╝"
echo ""

# ── Upload script + src to S3 ──────────────────────────
echo "📤 Uploading project files to S3..."
aws s3 sync "$PROJECT_ROOT/src/" "s3://$BUCKET/code/src/" --exclude "__pycache__/*"
aws s3 sync "$PROJECT_ROOT/configs/" "s3://$BUCKET/code/configs/" --exclude "__pycache__/*"
aws s3 sync "$PROJECT_ROOT/pipelines/" "s3://$BUCKET/code/pipelines/" --exclude "__pycache__/*"
aws s3 cp "$PROJECT_ROOT/$SCRIPT_PATH" "s3://$BUCKET/code/$SCRIPT_PATH"
echo "   ✅ Files uploaded to s3://$BUCKET/code/"

# ── Submit step to EMR ──────────────────────────────────
echo "🚀 Submitting job..."
STEP_ID=$(aws emr add-steps \
    --cluster-id "$CLUSTER_ID" \
    --region "$REGION" \
    --steps "Type=Spark,Name=$SCRIPT_NAME,ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,s3://$BUCKET/code/$SCRIPT_PATH,$SCRIPT_ARGS]" \
    --query 'StepIds[0]' \
    --output text)

echo "   ✅ Step submitted: $STEP_ID"
echo ""

# ── Monitor step ────────────────────────────────────────
echo "⏳ Monitoring step status..."
while true; do
    STATUS=$(aws emr describe-step \
        --cluster-id "$CLUSTER_ID" \
        --step-id "$STEP_ID" \
        --region "$REGION" \
        --query 'Step.Status.State' \
        --output text)

    echo "   Status: $STATUS"

    case "$STATUS" in
        COMPLETED)
            echo ""
            echo "✅ Job completed successfully!"
            echo "   View logs: aws emr ssh --cluster-id $CLUSTER_ID --command 'cat /mnt/var/log/spark/apps/$STEP_ID'"
            break
            ;;
        FAILED|CANCELLED)
            echo ""
            echo "❌ Job $STATUS"
            echo "   View logs:"
            echo "   aws emr describe-step --cluster-id $CLUSTER_ID --step-id $STEP_ID --region $REGION"
            exit 1
            ;;
        *)
            sleep 10
            ;;
    esac
done
