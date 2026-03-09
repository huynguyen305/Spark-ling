#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════
# Spark-ling — Deploy Budget Controls
# ═══════════════════════════════════════════════════════════
# Deploys budget alerts + auto-stop Lambda to protect $200 credit.
#
# Usage:
#   ./aws/deploy_budget_controls.sh YOUR_EMAIL@example.com
#
# Optional env vars:
#   MONTHLY_LIMIT   — monthly budget (default: 40)
#   TOTAL_CREDIT    — total credit amount (default: 200)
#   RESOURCE_REGION — region with your resources (default: ap-southeast-1)
# ═══════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STACK_NAME="sparkling-budget-controls"
DEPLOY_REGION="us-east-1"   # Required: budget SNS must be in us-east-1

# ── Validate input ──────────────────────────────────────
EMAIL="${1:?Usage: $0 YOUR_EMAIL@example.com}"
MONTHLY_LIMIT="${MONTHLY_LIMIT:-40}"
TOTAL_CREDIT="${TOTAL_CREDIT:-200}"
RESOURCE_REGION="${RESOURCE_REGION:-ap-southeast-1}"

echo "╔════════════════════════════════════════════╗"
echo "║  Spark-ling Budget Controls Deployment     ║"
echo "╠════════════════════════════════════════════╣"
echo "║  Email:           ${EMAIL}"
echo "║  Monthly limit:   \$${MONTHLY_LIMIT}"
echo "║  Total credit:    \$${TOTAL_CREDIT}"
echo "║  Resource region: ${RESOURCE_REGION}"
echo "║  Deploy region:   ${DEPLOY_REGION}"
echo "║  Stack name:      ${STACK_NAME}"
echo "╚════════════════════════════════════════════╝"
echo ""

# ── Check existing stack ────────────────────────────────
EXISTING=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$DEPLOY_REGION" \
  --query 'Stacks[0].StackStatus' \
  --output text 2>/dev/null || echo "NONE")

if [[ "$EXISTING" == "NONE" ]]; then
  echo "→ Creating new stack..."
  CF_ACTION="create-stack"
  CF_WAIT="stack-create-complete"
else
  echo "→ Updating existing stack (current status: ${EXISTING})..."
  CF_ACTION="update-stack"
  CF_WAIT="stack-update-complete"
fi

# ── Deploy CloudFormation ───────────────────────────────
aws cloudformation "$CF_ACTION" \
  --stack-name "$STACK_NAME" \
  --template-body "file://${SCRIPT_DIR}/budget-controls.yaml" \
  --capabilities CAPABILITY_NAMED_IAM \
  --region "$DEPLOY_REGION" \
  --parameters \
    "ParameterKey=NotificationEmail,ParameterValue=${EMAIL}" \
    "ParameterKey=MonthlyLimit,ParameterValue=${MONTHLY_LIMIT}" \
    "ParameterKey=TotalCreditLimit,ParameterValue=${TOTAL_CREDIT}" \
    "ParameterKey=ResourceRegion,ParameterValue=${RESOURCE_REGION}"

echo "→ Waiting for stack to complete..."
aws cloudformation wait "$CF_WAIT" \
  --stack-name "$STACK_NAME" \
  --region "$DEPLOY_REGION"

# ── Show outputs ────────────────────────────────────────
echo ""
echo "✓ Stack deployed successfully!"
echo ""
aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$DEPLOY_REGION" \
  --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' \
  --output table

echo ""
echo "╔════════════════════════════════════════════════════════╗"
echo "║  IMPORTANT — Action Required:                         ║"
echo "║                                                       ║"
echo "║  1. Check your email (${EMAIL})"
echo "║     and CONFIRM the SNS subscription.                 ║"
echo "║     Without confirmation, you won't get alerts!       ║"
echo "║                                                       ║"
echo "║  2. Consider deleting the old budget:                 ║"
echo "║     aws budgets delete-budget \\                       ║"
echo "║       --account-id $(aws sts get-caller-identity --query Account --output text) \\    ║"
echo "║       --budget-name Monthly-Cost-Budget-50USD          ║"
echo "║                                                       ║"
echo "║  Budget Alert Thresholds:                             ║"
echo "║  Monthly (\$${MONTHLY_LIMIT}):                              ║"
echo "║    50% → email | 80% → email | 95% → email+STOP      ║"
echo "║    Forecasted >100% → email                           ║"
echo "║                                                       ║"
echo "║  Total Credit (\$${TOTAL_CREDIT}):                          ║"
echo "║    25% → email | 50% → email | 75% → email           ║"
echo "║    90% → email+STOP | Forecasted >80% → email        ║"
echo "║                                                       ║"
echo "║  STOP = Lambda auto-stops all EC2 + RDS instances     ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""
echo "To manually trigger emergency stop (test):"
echo "  aws lambda invoke --function-name sparkling-budget-stop-resources \\"
echo "    --region ${DEPLOY_REGION} --payload '{}' /dev/stdout"
