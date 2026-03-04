#!/bin/bash
# ═══════════════════════════════════════════════════════════
# Deploy MCP Server to AWS EC2
# ═══════════════════════════════════════════════════════════
# Launches an EC2 instance and deploys the MCP server with
# optional Docker support and IAM role configuration.
#
# Prerequisites:
#   - AWS CLI configured with appropriate permissions
#   - Key pair created in the target region
#   - Fill in aws/.env with your values
#   - (Optional) Run ./mcp/setup_aws_iam.sh to create IAM role
#
# Usage:
#   ./mcp/deploy_ec2.sh              # Deploy (Python, direct)
#   ./mcp/deploy_ec2.sh deploy docker # Deploy with Docker
#   ./mcp/deploy_ec2.sh status       # Check instance status + health
#   ./mcp/deploy_ec2.sh logs         # Tail server logs via SSH
#   ./mcp/deploy_ec2.sh update       # Upload latest code + restart
#   ./mcp/deploy_ec2.sh teardown     # Terminate instance
#
# CloudFormation alternative (recommended for production):
#   aws cloudformation deploy \
#     --template-file mcp/cloudformation.yaml \
#     --stack-name sparkling-mcp \
#     --capabilities CAPABILITY_NAMED_IAM \
#     --parameter-overrides S3Bucket=sparkling-data-test KeyPairName=your-key
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
MCP_PORT="${MCP_PORT:-8080}"
MCP_BACKEND="${MCP_BACKEND:-athena}"
INSTANCE_PROFILE_NAME="sparkling-mcp-profile"

# Security group name
SG_NAME="sparkling-mcp-sg"

ACTION="${1:-deploy}"
DEPLOY_MODE="${2:-python}"  # python or docker

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
        echo "║  Mode:     $DEPLOY_MODE"
        echo "║  Backend:  $MCP_BACKEND"
        echo "║  Port:     $MCP_PORT"
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
            echo "   To update, run:"
            echo "   $0 update"
            exit 0
        fi

        if [ -z "$KEY_NAME" ]; then
            echo "❌ EMR_KEY_PAIR not set in aws/.env"
            echo "   Create a key pair: aws ec2 create-key-pair --key-name sparkling-key --region $REGION"
            exit 1
        fi

        # ── Check IAM role ─────────────────────────────────
        echo "🔐 Checking IAM role..."
        HAS_IAM_PROFILE=false
        if aws iam get-instance-profile --instance-profile-name "$INSTANCE_PROFILE_NAME" &> /dev/null; then
            echo "   ✅ IAM instance profile found: $INSTANCE_PROFILE_NAME"
            HAS_IAM_PROFILE=true
        else
            echo "   ⚠️  IAM profile '$INSTANCE_PROFILE_NAME' not found."
            echo "      Run './mcp/setup_aws_iam.sh' to create it."
            echo "      Continuing without IAM role (you'll need to configure AWS credentials manually)."
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
                --protocol tcp --port "$MCP_PORT" \
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
        if [ "$DEPLOY_MODE" = "docker" ]; then
            USER_DATA=$(cat <<USERDATA
#!/bin/bash
set -e

# Install Docker
dnf update -y -q
dnf install -y docker git -q
systemctl enable docker
systemctl start docker
usermod -aG docker ec2-user

# Clone project
cd /opt
git clone https://github.com/huynguyen305/Spark-ling.git sparkling-mcp
cd sparkling-mcp

# Write environment config
cat > /opt/sparkling-mcp/mcp/.env << 'ENVEOF'
MCP_BACKEND=${MCP_BACKEND}
MCP_TRANSPORT=sse
MCP_PORT=${MCP_PORT}
MCP_HOST=0.0.0.0
S3_BUCKET=${S3_BUCKET}
AWS_REGION=${REGION}
ATHENA_DATABASE=sparkling
ATHENA_WORKGROUP=primary
ENVEOF

# Build and run Docker container
docker build -t sparkling-mcp -f mcp/Dockerfile .
docker run -d \
    --name sparkling-mcp \
    --restart always \
    -p ${MCP_PORT}:${MCP_PORT} \
    --env-file /opt/sparkling-mcp/mcp/.env \
    sparkling-mcp

# Create update helper script
cat > /opt/sparkling-mcp/update.sh << 'UPDEOF'
#!/bin/bash
cd /opt/sparkling-mcp
git pull
docker build -t sparkling-mcp -f mcp/Dockerfile .
docker stop sparkling-mcp && docker rm sparkling-mcp
docker run -d --name sparkling-mcp --restart always \
    -p ${MCP_PORT}:${MCP_PORT} --env-file /opt/sparkling-mcp/mcp/.env sparkling-mcp
echo "✅ MCP server updated and restarted"
UPDEOF
chmod +x /opt/sparkling-mcp/update.sh

echo "✅ Docker deployment complete"
USERDATA
)
        else
            USER_DATA=$(cat <<USERDATA
#!/bin/bash
set -e

# Install Python 3.11+ and pip
dnf install -y python3.11 python3.11-pip git -q

# Clone project
cd /opt
git clone https://github.com/huynguyen305/Spark-ling.git sparkling-mcp
cd sparkling-mcp

# Install MCP dependencies
python3.11 -m pip install -r mcp/requirements.txt -q

# Write environment config
cat > /opt/sparkling-mcp/mcp/.env << 'ENVEOF'
MCP_BACKEND=${MCP_BACKEND}
MCP_TRANSPORT=sse
MCP_PORT=${MCP_PORT}
MCP_HOST=0.0.0.0
S3_BUCKET=${S3_BUCKET}
AWS_REGION=${REGION}
ATHENA_DATABASE=sparkling
ATHENA_WORKGROUP=primary
ENVEOF

# Create systemd service
cat > /etc/systemd/system/sparkling-mcp.service << 'SVCEOF'
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
EnvironmentFile=/opt/sparkling-mcp/mcp/.env

[Install]
WantedBy=multi-user.target
SVCEOF

chown -R ec2-user:ec2-user /opt/sparkling-mcp
systemctl daemon-reload
systemctl enable sparkling-mcp
systemctl start sparkling-mcp

echo "✅ MCP server setup and started"
USERDATA
)
        fi

        # ── Launch instance ─────────────────────────────────
        echo "🚀 Launching EC2 instance ($DEPLOY_MODE mode)..."

        LAUNCH_ARGS=(
            --region "$REGION"
            --image-id "$AMI_ID"
            --instance-type "$INSTANCE_TYPE"
            --key-name "$KEY_NAME"
            --security-group-ids "$SG_ID"
            --user-data "$USER_DATA"
            --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$TAG_NAME},{Key=Project,Value=Spark-ling},{Key=DeployMode,Value=$DEPLOY_MODE}]"
            --query 'Instances[0].InstanceId'
            --output text
        )

        # Attach IAM profile if available
        if [ "$HAS_IAM_PROFILE" = true ]; then
            LAUNCH_ARGS+=(--iam-instance-profile "Name=$INSTANCE_PROFILE_NAME")
        fi

        INSTANCE_ID=$(aws ec2 run-instances "${LAUNCH_ARGS[@]}")
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
        echo "   MCP server URL: http://$IP:$MCP_PORT/sse"
        echo ""
        echo "   SSH:  ssh -i ~/.ssh/$KEY_NAME.pem ec2-user@$IP"
        echo "   Logs: ssh -i ~/.ssh/$KEY_NAME.pem ec2-user@$IP 'journalctl -u sparkling-mcp -f'"
        echo ""
        echo "   VS Code MCP config (.vscode/mcp.json):"
        echo '   {'
        echo '     "servers": {'
        echo '       "sparkling-data-explorer": {'
        echo '         "type": "sse",'
        echo "         \"url\": \"http://$IP:$MCP_PORT/sse\""
        echo '       }'
        echo '     }'
        echo '   }'
        echo ""
        echo "   ⏱️  Wait ~3 min for setup to complete, then test:"
        echo "      curl http://$IP:$MCP_PORT/sse"
        echo "══════════════════════════════════════════════"
        ;;

    status)
        INSTANCE_ID=$(find_instance)
        if [ "$INSTANCE_ID" = "None" ] || [ -z "$INSTANCE_ID" ]; then
            echo "❌ No running MCP server instance found."
            exit 1
        fi

        IP=$(get_instance_ip "$INSTANCE_ID")
        echo "╔═══════════════════════════════════════════╗"
        echo "║   MCP Server Status                       ║"
        echo "╠═══════════════════════════════════════════╣"
        echo "║  Instance: $INSTANCE_ID"
        echo "║  IP:       $IP"
        echo "║  Port:     $MCP_PORT"
        echo "╚═══════════════════════════════════════════╝"
        echo ""
        echo "   URL: http://$IP:$MCP_PORT/sse"
        echo ""

        # Health check
        echo "🏥 Health check..."
        if curl -sf --max-time 5 "http://$IP:$MCP_PORT/sse" > /dev/null 2>&1; then
            echo "   ✅ MCP server is responding"
        else
            echo "   ⚠️  MCP server not responding (may still be starting)"
        fi

        echo ""
        echo "   SSH:  ssh -i ~/.ssh/$KEY_NAME.pem ec2-user@$IP"
        echo "   Logs: $0 logs"
        ;;

    logs)
        INSTANCE_ID=$(find_instance)
        if [ "$INSTANCE_ID" = "None" ] || [ -z "$INSTANCE_ID" ]; then
            echo "❌ No running MCP server instance found."
            exit 1
        fi

        IP=$(get_instance_ip "$INSTANCE_ID")
        echo "📋 Tailing MCP server logs on $IP..."
        ssh -i "$HOME/.ssh/$KEY_NAME.pem" -o StrictHostKeyChecking=no \
            "ec2-user@$IP" 'journalctl -u sparkling-mcp -f --no-pager -n 50 2>/dev/null || docker logs -f sparkling-mcp --tail 50'
        ;;

    update)
        INSTANCE_ID=$(find_instance)
        if [ "$INSTANCE_ID" = "None" ] || [ -z "$INSTANCE_ID" ]; then
            echo "❌ No running MCP server instance found."
            exit 1
        fi

        IP=$(get_instance_ip "$INSTANCE_ID")
        echo "🔄 Updating MCP server on $IP..."

        # Upload latest code
        echo "   Uploading project code..."
        scp -i "$HOME/.ssh/$KEY_NAME.pem" -o StrictHostKeyChecking=no \
            -r "$PROJECT_ROOT/mcp/" "$PROJECT_ROOT/src/" "$PROJECT_ROOT/configs/" \
            "ec2-user@$IP:/opt/sparkling-mcp/"

        # Restart service
        echo "   Restarting service..."
        ssh -i "$HOME/.ssh/$KEY_NAME.pem" -o StrictHostKeyChecking=no \
            "ec2-user@$IP" 'sudo systemctl restart sparkling-mcp 2>/dev/null || (cd /opt/sparkling-mcp && docker build -t sparkling-mcp -f mcp/Dockerfile . && docker restart sparkling-mcp)'

        echo "   ✅ MCP server updated and restarted"
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
        echo "Usage: $0 {deploy [docker]|status|logs|update|teardown}"
        exit 1
        ;;
esac
