# 🔌 MCP Guide: AI-Assisted Data Exploration

Use the **Model Context Protocol (MCP)** to let AI assistants (Claude, Gemini, etc.) query your Spark-ling banking data directly from your IDE.

---

## What is MCP?

MCP allows AI coding assistants to access external data through a standardized protocol. The Spark-ling MCP server exposes your banking data as callable tools.

```mermaid
sequenceDiagram
    participant You as You (IDE)
    participant AI as AI Assistant
    participant MCP as MCP Server
    participant Data as Athena / Databricks / S3

    You->>AI: "What's the customer segment distribution?"
    AI->>MCP: call list_tables()
    MCP->>Data: query
    Data-->>MCP: results
    MCP-->>AI: table list
    AI->>MCP: call query_sql("SELECT segment, COUNT(*) ...")
    MCP->>Data: execute SQL
    Data-->>MCP: results
    MCP-->>AI: formatted data
    AI-->>You: "Here's the breakdown: Mass 50%, Affluent 15%..."
```

---

## Quick Setup (Local — stdio transport)

### Step 1 — Install MCP dependencies

```bash
# Activate the project venv first
source ~/sparking_repo/Spark-ling/.venv/bin/activate

# Install MCP server dependencies
pip install -r mcp/requirements.txt
```

### Step 2 — Configure the MCP backend

```bash
cp mcp/.env.example mcp/.env
```

Edit `mcp/.env`:

```env
# Choose backend: databricks | athena | s3 | auto
MCP_BACKEND=athena

# AWS settings (Athena & S3 backends)
S3_BUCKET=sparkling-data-test
AWS_REGION=ap-southeast-1
ATHENA_DATABASE=sparkling
ATHENA_WORKGROUP=primary

# Databricks settings (optional)
# DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
# DATABRICKS_TOKEN=dapi-xxxxxxxxxxxxxxxx
# DATABRICKS_WAREHOUSE_ID=xxxxxxxxxxxxxxxx
```

| `MCP_BACKEND` | When to use | Requires |
|---------------|-------------|----------|
| `athena` | **Recommended** — serverless SQL on S3 via Glue Catalog | AWS credentials + S3 data |
| `databricks` | Rich SQL queries via SQL warehouse | Running warehouse + token |
| `s3` | Cost-effective, reads Parquet/CSV via local PySpark | AWS credentials + S3 data |
| `auto` | Tries Databricks → Athena → S3 in order | Any credentials set |

### Step 3 — Test the server locally

```bash
# With .venv activated, from project root:
python mcp/server.py
# Should print: Starting sparkling-data-explorer MCP server (transport=stdio, backend=athena)...
# Press Ctrl+C to stop
```

### Step 4 — Connect to VS Code

Create `.vscode/mcp.json` in the project root:

```json
{
  "servers": {
    "sparkling-data": {
      "command": "/home/huynguyenle/sparking_repo/Spark-ling/.venv/bin/python",
      "args": ["mcp/server.py"],
      "cwd": "/home/huynguyenle/sparking_repo/Spark-ling",
      "env": {
        "MCP_BACKEND": "athena"
      }
    }
  }
}
```

> **Important**: Use `"args": ["mcp/server.py"]` (script mode), not `["-m", "mcp.server"]` (module mode). The `mcp/` project directory shadows the pip `mcp` package, so module mode causes a circular import.
>
> Use the full `.venv/bin/python` path — not just `python` — so the MCP server uses the correct venv with all dependencies installed.

### Step 5 — Connect to Gemini / Antigravity

Add to `.gemini/settings.json` or workspace MCP config:

```json
{
  "mcpServers": {
    "sparkling-data": {
      "command": "/home/huynguyenle/sparking_repo/Spark-ling/.venv/bin/python",
      "args": ["mcp/server.py"],
      "cwd": "/home/huynguyenle/sparking_repo/Spark-ling"
    }
  }
}
```

### Step 6 — Connect to Cursor

In Cursor → Settings → MCP:

```json
{
  "mcpServers": {
    "sparkling-data": {
      "command": "/home/huynguyenle/sparking_repo/Spark-ling/.venv/bin/python",
      "args": ["mcp/server.py"],
      "cwd": "/home/huynguyenle/sparking_repo/Spark-ling"
    }
  }
}
```

---

## Available Tools

| Tool | Description | Example Prompt |
|------|-------------|----------------|
| `list_tables` | List all datasets | *"What tables are available?"* |
| `describe_table` | Get column names & types | *"Show me the schema of transactions"* |
| `sample_data` | Preview first N rows | *"Show me 5 sample customers"* |
| `query_sql` | Run read-only SQL | *"How many transactions per month?"* |
| `get_data_profile` | Stats: nulls, distinct counts | *"Profile the accounts table"* |
| `server_status` | Check backend, transport & health | *"Is the MCP server healthy?"* |

### Example Conversations

> **You**: *"What does the customer data look like?"*
> **AI** → `describe_table("customers")` → shows schema → `sample_data("customers", 5)` → shows rows

> **You**: *"Which branches have the most high-value transactions?"*
> **AI** → `query_sql("SELECT b.branch_name, COUNT(*) FROM branches b JOIN accounts a ...")`

> **You**: *"Are there data quality issues in transactions?"*
> **AI** → `get_data_profile("transactions")` → analyzes nulls and distributions

---

## MCP Server Architecture

```
mcp/
├── server.py              # FastMCP entry point; handles tool routing
├── config.py              # Reads mcp/.env, selects backend & transport
├── auth.py                # API key authentication for remote deployments
├── athena_backend.py      # Serverless SQL via AWS Athena + Glue Catalog
├── databricks_backend.py  # Queries via Databricks SQL warehouse (JDBC)
├── s3_backend.py          # Reads Parquet from S3 via local PySpark
├── Dockerfile             # Container image for Docker/ECS deployment
├── cloudformation.yaml    # One-click CloudFormation deployment template
├── setup_aws_iam.sh       # Least-privilege IAM role/policy setup
├── deploy_ec2.sh          # Deploy to EC2 (Python or Docker mode)
├── .env.example           # Config template → copy to .env
└── requirements.txt       # mcp[cli], boto3, databricks-sql-connector, etc.
```

**Transport modes:**
- `stdio` — local IDE integration (default, no auth needed)
- `sse` — remote EC2/ECS deployment (Server-Sent Events)
- `streamable-http` — modern MCP HTTP transport (recommended for new deployments)

---

## AWS Deployment Options

Three ways to deploy the MCP server on AWS, from simplest to most automated:

| Method | Best For | Effort |
|--------|----------|--------|
| [CloudFormation](#option-a--cloudformation-one-click) | Production — one-click deploy with IAM, monitoring | Low |
| [deploy_ec2.sh](#option-b--deploy-script) | Development — quick EC2 with Docker or Python mode | Medium |
| [Docker on ECS](#option-c--docker-on-ecs) | Scale — container orchestration, Fargate | Advanced |

---

### Prerequisites (all methods)

```bash
# 1) AWS CLI configured
aws sts get-caller-identity

# 2) S3 bucket with banking data
aws s3 ls s3://sparkling-data-test/processed/

# 3) (Optional) Set up least-privilege IAM role
chmod +x mcp/setup_aws_iam.sh
./mcp/setup_aws_iam.sh create
```

---

### Option A — CloudFormation (one-click)

Deploy a fully configured EC2 instance with IAM, security group, and systemd service:

```bash
# Generate an API key for authentication
API_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
echo "Save this key: $API_KEY"

# Deploy the stack
aws cloudformation create-stack \
  --stack-name sparkling-mcp \
  --template-body file://mcp/cloudformation.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameters \
    ParameterKey=S3Bucket,ParameterValue=sparkling-data-test \
    ParameterKey=McpApiKey,ParameterValue="$API_KEY" \
    ParameterKey=McpBackend,ParameterValue=athena

# Wait for completion (~3 minutes)
aws cloudformation wait stack-create-complete --stack-name sparkling-mcp

# Get the endpoint URL
aws cloudformation describe-stacks --stack-name sparkling-mcp \
  --query 'Stacks[0].Outputs[?OutputKey==`McpEndpoint`].OutputValue' --output text
```

The CloudFormation template creates:
- EC2 instance (t3.small) with Amazon Linux 2023 + 1 GB swap
- IAM role with least-privilege S3/Athena/Glue/CloudWatch access
- Security group (SSH + MCP port)
- SSM Parameter Store for the API key
- systemd service that starts automatically
- SSM Session Manager access (no SSH key needed)

> **Note**: The `KeyPairName` parameter is optional. If omitted, use SSM Session Manager to connect:
> ```bash
> aws ssm start-session --target <INSTANCE_ID>
> ```

**Tear down:**
```bash
aws cloudformation delete-stack --stack-name sparkling-mcp
```

---

### Option B — Deploy Script

Quick deployment using the interactive deploy script:

```bash
# Docker mode (recommended) — builds and runs container on EC2
./mcp/deploy_ec2.sh deploy docker

# Python mode — clones repo and runs via systemd
./mcp/deploy_ec2.sh deploy python
```

Management commands:
```bash
./mcp/deploy_ec2.sh status      # instance health + MCP endpoint check
./mcp/deploy_ec2.sh logs         # view live server logs (SSH)
./mcp/deploy_ec2.sh update       # push code changes and restart
./mcp/deploy_ec2.sh teardown     # terminate instance + cleanup
```

---

### Option C — Docker on ECS

Build the Docker image locally and push to ECR:

```bash
# Build from project root
docker build -f mcp/Dockerfile -t sparkling-mcp .

# Test locally
docker run -p 8080:8080 \
  -e MCP_TRANSPORT=sse \
  -e MCP_BACKEND=athena \
  -e S3_BUCKET=sparkling-data-test \
  -e AWS_REGION=ap-southeast-1 \
  sparkling-mcp

# Push to ECR (then deploy via ECS/Fargate)
aws ecr get-login-password | docker login --username AWS --password-stdin <ACCOUNT>.dkr.ecr.ap-southeast-1.amazonaws.com
docker tag sparkling-mcp:latest <ACCOUNT>.dkr.ecr.ap-southeast-1.amazonaws.com/sparkling-mcp:latest
docker push <ACCOUNT>.dkr.ecr.ap-southeast-1.amazonaws.com/sparkling-mcp:latest
```

---

### Connect IDE to Remote MCP Server

After deploying, connect your IDE using SSE transport:

**VS Code** (`.vscode/mcp.json`):
```json
{
  "servers": {
    "sparkling-data-remote": {
      "type": "sse",
      "url": "http://<EC2-IP>:8080/sse",
      "headers": {
        "Authorization": "Bearer <your-api-key>"
      }
    }
  }
}
```

**Cursor / Gemini:**
```json
{
  "mcpServers": {
    "sparkling-data": {
      "url": "http://<EC2-IP>:8080/sse"
    }
  }
}
```

---

## Authentication

Remote deployments (SSE/streamable-http) use API key authentication.
Local stdio transport skips authentication automatically.

**Generate a key:**
```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

**Configure in `.env`:**
```env
MCP_API_KEY=your-generated-key-here
```

**Send with requests** — include as a Bearer token:
```
Authorization: Bearer your-generated-key-here
```

---

## IAM Setup

The `setup_aws_iam.sh` script creates least-privilege AWS permissions:

```bash
# Create IAM role and policies
./mcp/setup_aws_iam.sh create

# Check current status
./mcp/setup_aws_iam.sh status

# Clean up everything
./mcp/setup_aws_iam.sh teardown
```

Policies created:
- **S3**: Read access to your data bucket, write to athena-results prefix
- **Athena**: Query execution on configured workgroups
- **Glue**: Catalog read/write for the `sparkling` database
- **CloudWatch**: Log group creation and log writing
- **SSM**: Read access to API key parameter

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `No module named 'mcp'` | Activate venv: `source .venv/bin/activate` then `pip install -r mcp/requirements.txt` |
| `circular import ... mcp.server` | Run as script (`python mcp/server.py`), not module (`python -m mcp.server`). The `mcp/` directory shadows the pip package. |
| `FastMCP.run() got an unexpected keyword argument 'host'` | In mcp SDK >=1.20, pass `host`/`port` to `FastMCP()` constructor, not `run()` |
| OOM during EC2 pip install | Use `mcp/requirements-ec2.txt` (lightweight — no PySpark/Databricks). The full `requirements.txt` installs PySpark which can OOM on t3.small. |
| MCP server won't start | Check `mcp/.env` exists and has valid credentials |
| `"DATABRICKS_HOST not set"` | Fill in `mcp/.env` or set `MCP_BACKEND=athena` to skip Databricks |
| Databricks connection refused | Verify token and warehouse ID; ensure warehouse is not suspended |
| S3 access denied | Run `aws sts get-caller-identity`; check `aws configure` credentials |
| Athena query timeout | Check `ATHENA_WORKGROUP` setting; ensure data exists in S3 prefix |
| Athena table not found | Run `server_status` tool to verify backend connectivity and table count |
| Slow queries on S3 backend | Normal — S3 backend runs PySpark locally; use Athena or Databricks for speed |
| IDE can't find MCP server | Use the full `.venv/bin/python` path in your IDE MCP config, not just `python` |
| EC2 service not starting | Connect via SSM and check: `journalctl -u sparkling-mcp -n 50` |
| `401 Unauthorized` on remote | Ensure `Authorization: Bearer <key>` header matches `MCP_API_KEY` in `.env` |
| Docker container exits | Run `docker logs sparkling-mcp` to check startup errors |
| CloudFormation rollback | Check Events tab in AWS Console for specific failure reason |
| Can't reach MCP from IDE | Verify security group allows your IP on port 8080; check local firewall/VPN doesn't block outbound 8080 |
