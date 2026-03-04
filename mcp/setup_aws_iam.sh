#!/bin/bash
# ═══════════════════════════════════════════════════════════
# Setup IAM Role & Policies for Spark-ling MCP Server
# ═══════════════════════════════════════════════════════════
# Creates least-privilege IAM resources for an EC2-hosted
# MCP server that uses the Athena backend.
#
# What it creates:
#   1. IAM Policy — S3 read, Athena query, Glue catalog access
#   2. IAM Role — EC2 trust relationship
#   3. Instance Profile — attach to EC2 instances
#
# Prerequisites:
#   - AWS CLI configured with admin/IAM permissions
#   - aws/.env file with S3_BUCKET and AWS_REGION set
#
# Usage:
#   ./mcp/setup_aws_iam.sh              # Create role
#   ./mcp/setup_aws_iam.sh status       # Check if role exists
#   ./mcp/setup_aws_iam.sh teardown     # Delete role and policy
# ═══════════════════════════════════════════════════════════

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Load AWS config
AWS_ENV="$PROJECT_ROOT/aws/.env"
if [ -f "$AWS_ENV" ]; then
    source "$AWS_ENV"
fi

REGION="${AWS_REGION:-ap-southeast-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "")
S3_BUCKET="${S3_BUCKET:-sparkling-data-test}"

ROLE_NAME="sparkling-mcp-server-role"
POLICY_NAME="sparkling-mcp-data-access"
INSTANCE_PROFILE_NAME="sparkling-mcp-profile"

ACTION="${1:-create}"

# ── Colors ───────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

info()  { echo -e "${GREEN}✅ $1${NC}"; }
warn()  { echo -e "${YELLOW}⚠️  $1${NC}"; }
error() { echo -e "${RED}❌ $1${NC}"; }

# ── Prerequisite checks ─────────────────────────────────
check_prereqs() {
    if ! command -v aws &> /dev/null; then
        error "AWS CLI not found. Install: https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html"
        exit 1
    fi

    if [ -z "$ACCOUNT_ID" ]; then
        error "Cannot determine AWS account ID. Run 'aws configure' first."
        exit 1
    fi

    echo "╔═══════════════════════════════════════════╗"
    echo "║   Spark-ling MCP IAM Setup                ║"
    echo "╠═══════════════════════════════════════════╣"
    echo "║  Account:  $ACCOUNT_ID"
    echo "║  Region:   $REGION"
    echo "║  S3:       $S3_BUCKET"
    echo "║  Role:     $ROLE_NAME"
    echo "╚═══════════════════════════════════════════╝"
    echo ""
}

# ── Create trust policy ──────────────────────────────────
create_trust_policy() {
    cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "ec2.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF
}

# ── Create data access policy ────────────────────────────
create_data_policy() {
    cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3ReadData",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::${S3_BUCKET}",
                "arn:aws:s3:::${S3_BUCKET}/*"
            ]
        },
        {
            "Sid": "S3WriteAthenaResults",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::${S3_BUCKET}/athena-results/*"
            ]
        },
        {
            "Sid": "AthenaQueryAccess",
            "Effect": "Allow",
            "Action": [
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:StopQueryExecution",
                "athena:ListWorkGroups",
                "athena:GetWorkGroup"
            ],
            "Resource": [
                "arn:aws:athena:${REGION}:${ACCOUNT_ID}:workgroup/primary",
                "arn:aws:athena:${REGION}:${ACCOUNT_ID}:workgroup/sparkling-mcp"
            ]
        },
        {
            "Sid": "GlueCatalogAccess",
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:CreateDatabase",
                "glue:GetTable",
                "glue:GetTables",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:GetPartitions"
            ],
            "Resource": [
                "arn:aws:glue:${REGION}:${ACCOUNT_ID}:catalog",
                "arn:aws:glue:${REGION}:${ACCOUNT_ID}:database/sparkling",
                "arn:aws:glue:${REGION}:${ACCOUNT_ID}:table/sparkling/*"
            ]
        },
        {
            "Sid": "CloudWatchLogs",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:${REGION}:${ACCOUNT_ID}:*"
        }
    ]
}
EOF
}

# ── Create ─────────────────────────────────────────────
do_create() {
    check_prereqs

    # Check if role already exists
    if aws iam get-role --role-name "$ROLE_NAME" &> /dev/null; then
        info "Role '$ROLE_NAME' already exists"
        echo "   Use '$0 teardown' first if you want to recreate it."
        return 0
    fi

    # 1. Create IAM policy
    echo "📋 Creating IAM policy..."
    POLICY_ARN=$(aws iam create-policy \
        --policy-name "$POLICY_NAME" \
        --policy-document "$(create_data_policy)" \
        --description "Data access policy for Spark-ling MCP server (S3, Athena, Glue)" \
        --query 'Policy.Arn' \
        --output text 2>/dev/null || echo "")

    if [ -z "$POLICY_ARN" ]; then
        # Policy may already exist
        POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"
        warn "Policy may already exist: $POLICY_ARN"
    else
        info "Created policy: $POLICY_ARN"
    fi

    # 2. Create IAM role
    echo "🔐 Creating IAM role..."
    aws iam create-role \
        --role-name "$ROLE_NAME" \
        --assume-role-policy-document "$(create_trust_policy)" \
        --description "IAM role for Spark-ling MCP server on EC2" \
        --tags Key=Project,Value=Spark-ling \
        > /dev/null

    info "Created role: $ROLE_NAME"

    # 3. Attach policy to role
    echo "🔗 Attaching policy to role..."
    aws iam attach-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-arn "$POLICY_ARN"

    # Also attach SSM for remote management
    aws iam attach-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-arn "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"

    info "Attached policies to role"

    # 4. Create instance profile
    echo "📦 Creating instance profile..."
    aws iam create-instance-profile \
        --instance-profile-name "$INSTANCE_PROFILE_NAME" \
        > /dev/null 2>&1 || warn "Instance profile may already exist"

    aws iam add-role-to-instance-profile \
        --instance-profile-name "$INSTANCE_PROFILE_NAME" \
        --role-name "$ROLE_NAME" \
        > /dev/null 2>&1 || warn "Role may already be attached to profile"

    info "Created instance profile: $INSTANCE_PROFILE_NAME"

    echo ""
    echo "══════════════════════════════════════════════"
    info "IAM setup complete!"
    echo ""
    echo "   Role ARN: arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
    echo "   Profile:  $INSTANCE_PROFILE_NAME"
    echo ""
    echo "   Use this profile when launching EC2 instances:"
    echo "   --iam-instance-profile Name=$INSTANCE_PROFILE_NAME"
    echo ""
    echo "   Or use CloudFormation:"
    echo "   aws cloudformation deploy \\"
    echo "     --template-file mcp/cloudformation.yaml \\"
    echo "     --stack-name sparkling-mcp \\"
    echo "     --capabilities CAPABILITY_NAMED_IAM \\"
    echo "     --parameter-overrides S3Bucket=$S3_BUCKET KeyPairName=YOUR_KEY"
    echo "══════════════════════════════════════════════"
}

# ── Status ─────────────────────────────────────────────
do_status() {
    check_prereqs

    echo "Checking IAM resources..."
    echo ""

    # Role
    if aws iam get-role --role-name "$ROLE_NAME" &> /dev/null; then
        info "Role '$ROLE_NAME' exists"
        ROLE_ARN=$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text)
        echo "   ARN: $ROLE_ARN"

        # List attached policies
        echo "   Attached policies:"
        aws iam list-attached-role-policies --role-name "$ROLE_NAME" \
            --query 'AttachedPolicies[].PolicyName' --output table
    else
        warn "Role '$ROLE_NAME' does not exist"
    fi

    echo ""

    # Instance profile
    if aws iam get-instance-profile --instance-profile-name "$INSTANCE_PROFILE_NAME" &> /dev/null; then
        info "Instance profile '$INSTANCE_PROFILE_NAME' exists"
    else
        warn "Instance profile '$INSTANCE_PROFILE_NAME' does not exist"
    fi
}

# ── Teardown ───────────────────────────────────────────
do_teardown() {
    check_prereqs

    echo "🗑️  Tearing down IAM resources..."

    # Remove role from instance profile
    aws iam remove-role-from-instance-profile \
        --instance-profile-name "$INSTANCE_PROFILE_NAME" \
        --role-name "$ROLE_NAME" 2>/dev/null || true

    # Delete instance profile
    aws iam delete-instance-profile \
        --instance-profile-name "$INSTANCE_PROFILE_NAME" 2>/dev/null || true
    info "Deleted instance profile"

    # Detach policies
    POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"
    aws iam detach-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-arn "$POLICY_ARN" 2>/dev/null || true
    aws iam detach-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-arn "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore" 2>/dev/null || true
    info "Detached policies"

    # Delete role
    aws iam delete-role --role-name "$ROLE_NAME" 2>/dev/null || true
    info "Deleted role: $ROLE_NAME"

    # Delete policy
    aws iam delete-policy --policy-arn "$POLICY_ARN" 2>/dev/null || true
    info "Deleted policy: $POLICY_NAME"

    echo ""
    info "IAM teardown complete"
}

# ── Main ───────────────────────────────────────────────
case "$ACTION" in
    create)
        do_create
        ;;
    status)
        do_status
        ;;
    teardown)
        do_teardown
        ;;
    *)
        echo "Usage: $0 {create|status|teardown}"
        exit 1
        ;;
esac
