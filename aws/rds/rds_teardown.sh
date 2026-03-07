#!/bin/bash
################################################################################
# RDS PostgreSQL Teardown Script
# ================================
# Destroys the PostgreSQL RDS CloudFormation stack to STOP BILLING.
#
# LEARNING NOTES:
# ---------------
# 1. COST: db.t3.micro = FREE TIER eligible (750 hrs/month first year).
#    After free tier: ~$0.02/hr (~$0.48/day). Still much cheaper than Oracle.
#
# 2. IMPORTANT: This is IRREVERSIBLE. All data in the RDS instance will be
#    PERMANENTLY DELETED because:
#    - BackupRetentionPeriod = 0 (no automated snapshots)
#    - DeletionPolicy = Delete (resource deleted with stack)
#    - No final snapshot is taken
#
# 3. If you want to save data before teardown:
#    pg_dump -h <endpoint> -U admin sparkdb > backup.sql
#
# USAGE:
#    chmod +x aws/rds/rds_teardown.sh
#    ./aws/rds/rds_teardown.sh
################################################################################

set -e

STACK_NAME="sparkling-rds-postgres"
REGION="${AWS_REGION:-ap-southeast-1}"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  RDS PostgreSQL Teardown                                    ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "  Stack: ${STACK_NAME}"
echo "  Region: ${REGION}"
echo ""

# Check if stack exists
STACK_STATUS=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].StackStatus' \
    --output text 2>/dev/null || echo "NOT_FOUND")

if [ "${STACK_STATUS}" = "NOT_FOUND" ]; then
    echo "  ℹ️  Stack '${STACK_NAME}' not found. Nothing to tear down."
    exit 0
fi

echo "  Current status: ${STACK_STATUS}"
echo ""
echo "  ⚠️  WARNING: This will PERMANENTLY DELETE:"
echo "    - PostgreSQL RDS instance (sparkling-postgres-db)"
echo "    - All data in the database"
echo "    - Security group and subnet group"
echo ""

# Confirmation
read -p "  Type 'DELETE' to confirm: " CONFIRM
if [ "${CONFIRM}" != "DELETE" ]; then
    echo "  ❌ Aborted. Stack not deleted."
    exit 1
fi

echo ""
echo "  🗑️  Deleting stack..."
aws cloudformation delete-stack \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}"

echo "  ⏳ Waiting for deletion to complete..."
aws cloudformation wait stack-delete-complete \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}"

echo ""
echo "  ✅ Stack '${STACK_NAME}' deleted successfully!"

# Also clean up Glue/Lambda stack if it exists
GLUE_STACK="sparkling-glue-lambda"
GLUE_STATUS=$(aws cloudformation describe-stacks \
    --stack-name "${GLUE_STACK}" \
    --region "${REGION}" \
    --query 'Stacks[0].StackStatus' \
    --output text 2>/dev/null || echo "NOT_FOUND")

if [ "${GLUE_STATUS}" != "NOT_FOUND" ]; then
    echo ""
    echo "  🗑️  Also deleting Glue/Lambda stack '${GLUE_STACK}'..."
    aws cloudformation delete-stack \
        --stack-name "${GLUE_STACK}" \
        --region "${REGION}"
    aws cloudformation wait stack-delete-complete \
        --stack-name "${GLUE_STACK}" \
        --region "${REGION}"
    echo "  ✅ Glue/Lambda stack deleted!"
fi
echo ""
echo "  💰 COST SAVINGS:"
echo "     - RDS billing stopped immediately"
echo "     - Storage costs stopped"
echo ""
echo "  📝 To recreate later:"
echo "     aws cloudformation deploy \\"
echo "       --template-file aws/rds/rds_setup.yaml \\"
echo "       --stack-name ${STACK_NAME} \\"
echo "       --parameter-overrides MasterPassword='YourPass!' \\"
echo "         AllowedCIDR=\$(curl -s ifconfig.me)/32 \\"
echo "       --region ${REGION}"
