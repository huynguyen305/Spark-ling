#!/bin/bash
# ═══════════════════════════════════════════════════════════
# Deploy MCP Server to AWS EC2
# ═══════════════════════════════════════════════════════════
# Launches a small EC2 instance and deploys the MCP server.
#
# Prerequisites:
#   - AWS CLI configured with appropriate permissions
#   - Key pair created in the target region
#   - Fill in aws/.env with your values
#
# Usage:
#   ./mcp/deploy_ec2.sh              # Deploy or update
#   ./mcp/deploy_ec2.sh status       # Check instance status
#   ./mcp/deploy_ec2.sh teardown     # Terminate instance
# ═══════════════════════════════════════════════════════════

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Load AWS config
AWS_ENV="$PROJECT_ROOT/aws/.env"
MCP_ENV="$SCRIPT_DIR/.env"

if [ ! -f "$AWS_ENV" ]; then
    echo "❌ aws/.env not found. Run: cp aws/.env.example aws/.env"
    exit 1
fi
source "$AWS_ENV"

REGION="${AWS_REGION:-ap-southeast-1}"
INSTANCE_TYPE="${MCP_EC2_INSTANCE_TYPE:-t3.small}"
KEY_NAME="${EMR_KEY_PAIR:-}"
TAG_NAME="sparkling-mcp-server"

# Security group name
SG_NAME="sparkling-mcp-sg"

ACTION="${1:-deploy}"

# ── Helper: find existing instance ───────────────────────
find_instance() {
    aws ec2 describe-instances \
        --region "$REGION" \
        --filters "Name=tag:Name,Values=$TAG_NAME" "Name=instance-state-name,Values=running,pending" \
        --query 'Reservations[0].Instances[0].InstanceId' \
        --output text 2>/dev/null || echo "None"
}

get_instance_ip() {
    aws ec2 describe-instances \
        --region "$REGION" \
        --instance-ids "$1" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' \
        --output text 2>/dev/null || echo ""
}

case "$ACTION" in
    deploy)
        echo "╔═══════════════════════════════════════════╗"
        echo "║   Deploy MCP Server to EC2                ║"
        echo "╠═══════════════════════════════════════════╣"
        echo "║  Instance: $INSTANCE_TYPE"
        echo "║  Region:   $REGION"
        echo "╚═══════════════════════════════════════════╝"
        echo ""

        # Check if already running
        EXISTING=$(find_instance)
        if [ "$EXISTING" != "None" ] && [ -n "$EXISTING" ]; then
            IP=$(get_instance_ip "$EXISTING")
            echo "✅ MCP server already running!"
            echo "   Instance: $EXISTING"
            echo "   IP: $IP"
            echo ""
            echo "   To update code, run:"
            echo "   scp -r mcp/ ec2-user@$IP:~/sparkling-mcp/"
            echo "   ssh ec2-user@$IP 'sudo systemctl restart sparkling-mcp'"
            exit 0
        fi

        if [ -z "$KEY_NAME" ]; then
            echo "❌ EMR_KEY_PAIR not set in aws/.env"
            echo "   Create a key pair: aws ec2 create-key-pair --key-name sparkling-key --region $REGION"
            exit 1
        fi

        # ── Create security group ──────────────────────────
        echo "🔒 Setting up security group..."
        SG_ID=$(aws ec2 describe-security-groups \
            --region "$REGION" \
            --filters "Name=group-name,Values=$SG_NAME" \
            --query 'SecurityGroups[0].GroupId' \
            --output text 2>/dev/null || echo "None")

        if [ "$SG_ID" = "None" ] || [ -z "$SG_ID" ]; then
            SG_ID=$(aws ec2 create-security-group \
                --group-name "$SG_NAME" \
                --description "Security group for Spark-ling MCP server" \
                --region "$REGION" \
                --query 'GroupId' \
                --output text)

            # Allow SSH
            aws ec2 authorize-security-group-ingress \
                --group-id "$SG_ID" \
                --protocol tcp --port 22 \
                --cidr 0.0.0.0/0 \
                --region "$REGION" > /dev/null

            # Allow MCP server port (for SSE transport)
            aws ec2 authorize-security-group-ingress \
                --group-id "$SG_ID" \
                --protocol tcp --port 8080 \
                --cidr 0.0.0.0/0 \
                --region "$REGION" > /dev/null

            echo "   ✅ Created security group: $SG_ID"
        else
            echo "   ✅ Using existing security group: $SG_ID"
        fi

        # ── Get latest Amazon Linux 2023 AMI ────────────────
        echo "🔍 Finding AMI..."
        AMI_ID=$(aws ec2 describe-images \
            --region "$REGION" \
            --owners amazon \
            --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" \
            --query 'sort_by(Images, &CreationDate)[-1].ImageId' \
            --output text)
        echo "   ✅ AMI: $AMI_ID"

        # ── Create user-data script ─────────────────────────
        USER_DATA=$(cat <<'USERDATA'
#!/bin/bash
set -e

# Install Python 3.11+ and pip
dnf install -y python3.11 python3.11-pip git

# Create app directory
mkdir -p /opt/sparkling-mcp
cd /opt/sparkling-mcp

# Install MCP dependencies
cat > requirements.txt << 'EOF'
mcp[cli]>=1.0.0
databricks-sql-connector>=3.0.0
databricks-sdk>=0.20.0
boto3>=1.28.0
pyspark>=3.4.1
pyarrow>=12.0.0
python-dotenv>=1.0.0
EOF

python3.11 -m pip install -r requirements.txt

# Create systemd service
cat > /etc/systemd/system/sparkling-mcp.service << 'EOF'
[Unit]
Description=Spark-ling MCP Server
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/opt/sparkling-mcp
ExecStart=/usr/bin/python3.11 -m mcp.server
Restart=always
RestartSec=5
Environment=MCP_TRANSPORT=sse
Environment=MCP_PORT=8080
EnvironmentFile=-/opt/sparkling-mcp/mcp/.env

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable sparkling-mcp

echo "✅ MCP server setup complete. Upload code to /opt/sparkling-mcp then start the service."
USERDATA
)

        # ── Launch instance ─────────────────────────────────
        echo "🚀 Launching EC2 instance..."
        INSTANCE_ID=$(aws ec2 run-instances \
            --region "$REGION" \
            --image-id "$AMI_ID" \
            --instance-type "$INSTANCE_TYPE" \
            --key-name "$KEY_NAME" \
            --security-group-ids "$SG_ID" \
            --user-data "$USER_DATA" \
            --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$TAG_NAME}]" \
            --iam-instance-profile Name=sparkling-ec2-role 2>/dev/null || \
        aws ec2 run-instances \
            --region "$REGION" \
            --image-id "$AMI_ID" \
            --instance-type "$INSTANCE_TYPE" \
            --key-name "$KEY_NAME" \
            --security-group-ids "$SG_ID" \
            --user-data "$USER_DATA" \
            --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$TAG_NAME}]" \
            --query 'Instances[0].InstanceId' \
            --output text)

        echo "   ✅ Instance launched: $INSTANCE_ID"

        # Wait for running
        echo "⏳ Waiting for instance to start..."
        aws ec2 wait instance-running --instance-ids "$INSTANCE_ID" --region "$REGION"

        IP=$(get_instance_ip "$INSTANCE_ID")
        echo "   ✅ Instance running at: $IP"

        echo ""
        echo "══════════════════════════════════════════════"
        echo "✅ EC2 instance ready!"
        echo ""
        echo "   Next steps:"
        echo "   1. Wait ~2 min for user-data setup to complete"
        echo ""
        echo "   2. Upload project code:"
        echo "      scp -i ~/.ssh/$KEY_NAME.pem -r $PROJECT_ROOT/ ec2-user@$IP:/opt/sparkling-mcp/"
        echo ""
        echo "   3. Upload MCP config:"
        echo "      scp -i ~/.ssh/$KEY_NAME.pem $MCP_ENV ec2-user@$IP:/opt/sparkling-mcp/mcp/.env"
        echo ""
        echo "   4. Start the server:"
        echo "      ssh -i ~/.ssh/$KEY_NAME.pem ec2-user@$IP 'sudo systemctl start sparkling-mcp'"
        echo ""
        echo "   5. Connect from your IDE (SSE transport):"
        echo "      MCP server URL: http://$IP:8080/sse"
        echo "══════════════════════════════════════════════"
        ;;

    status)
        INSTANCE_ID=$(find_instance)
        if [ "$INSTANCE_ID" = "None" ] || [ -z "$INSTANCE_ID" ]; then
            echo "❌ No running MCP server instance found."
            exit 1
        fi

        IP=$(get_instance_ip "$INSTANCE_ID")
        echo "✅ MCP Server Status"
        echo "   Instance: $INSTANCE_ID"
        echo "   IP: $IP"
        echo "   URL: http://$IP:8080/sse"
        echo ""
        echo "   SSH:  ssh -i ~/.ssh/$KEY_NAME.pem ec2-user@$IP"
        echo "   Logs: ssh ec2-user@$IP 'journalctl -u sparkling-mcp -f'"
        ;;

    teardown)
        INSTANCE_ID=$(find_instance)
        if [ "$INSTANCE_ID" = "None" ] || [ -z "$INSTANCE_ID" ]; then
            echo "No running MCP server instance found."
            exit 0
        fi

        echo "🗑️  Terminating MCP server instance: $INSTANCE_ID"
        aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" --region "$REGION" > /dev/null
        echo "✅ Instance terminated."
        ;;

    *)
        echo "Usage: $0 {deploy|status|teardown}"
        exit 1
        ;;
esac
