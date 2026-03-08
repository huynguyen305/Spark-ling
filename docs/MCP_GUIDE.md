# 🔌 MCP Guide: AI-Assisted Data Exploration

Use the **Model Context Protocol (MCP)** to let AI assistants (Claude, Gemini, etc.) query your Spark-ling banking data directly from your IDE.

---

## Two MCP server options

| Server | Compute | Best for |
|--------|---------|----------|
| [`mcp_databricks_connect/`](#option-a--mcp-with-databricks-connect-recommended) | Databricks serverless (gRPC) | **Unity Catalog tables** after RDS migration; full Spark DataFrame API |
| [`mcp/`](#option-b--mcp-with-athena--s3--sql-warehouse) | Athena / S3 / SQL warehouse | S3 Parquet data; cost-effective exploration |

> **Recommendation**: Use `mcp_databricks_connect/` when your data is in Unity Catalog
> (post-migration). Use `mcp/` for Athena or S3-backed data exploration.

---

## What is MCP?

MCP allows AI coding assistants to access external data through a standardized protocol. The Spark-ling MCP server exposes your banking data as callable tools.

```mermaid
sequenceDiagram
    participant You as You (IDE)
    participant AI as AI Assistant
    participant MCP as MCP Server
    participant Data as Databricks / Athena / S3

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

## Option A — MCP with Databricks Connect (Recommended)

Backed by **Databricks Connect (gRPC)** — gives AI assistants full Spark computing
power on remote serverless compute.  Supports SQL *and* the full DataFrame API.

### Setup

```bash
# 1) Install dependencies
source ~/sparking_repo/Spark-ling/.venv/bin/activate
pip install -r mcp_databricks_connect/requirements.txt

# 2) Configure credentials
cp mcp_databricks_connect/.env.example mcp_databricks_connect/.env
# Edit .env — set DATABRICKS_HOST and DATABRICKS_TOKEN
```

`mcp_databricks_connect/.env`:

```env
DATABRICKS_HOST=https://dbc-cdbdfd07-5797.cloud.databricks.com
DATABRICKS_TOKEN=dapi...your-personal-access-token...

# Unity Catalog defaults (tables migrated from RDS)
DATABRICKS_CATALOG=sparkling
DATABRICKS_SCHEMA=banking

MCP_TRANSPORT=stdio
MCP_PORT=8081
MCP_SERVER_NAME=sparkling-spark-engine
```

### Test locally

```bash
python mcp_databricks_connect/server.py
# Expected: "Starting sparkling-spark-engine MCP server (transport=stdio)..."
```

### Connect to VS Code (`.vscode/mcp.json`)

```json
{
  "servers": {
    "sparkling-spark-engine": {
      "command": "/home/huynguyenle/sparking_repo/Spark-ling/.venv/bin/python",
      "args": ["mcp_databricks_connect/server.py"],
      "cwd": "/home/huynguyenle/sparking_repo/Spark-ling"
    }
  }
}
```

### Connect to Cursor / Gemini

```json
{
  "mcpServers": {
    "sparkling-spark-engine": {
      "command": "/home/huynguyenle/sparking_repo/Spark-ling/.venv/bin/python",
      "args": ["mcp_databricks_connect/server.py"],
      "cwd": "/home/huynguyenle/sparking_repo/Spark-ling"
    }
  }
}
```

### Extra tools (Databricks Connect MCP)

In addition to the standard `list_tables`, `describe_table`, `sample_data`, `query_sql` tools,
the Databricks Connect MCP exposes:

| Tool | Description |
|------|-------------|
| `dataframe_operation` | filter / groupby_agg / orderby / select / corr — full DataFrame API |
| `read_s3_path` | Ad-hoc Parquet / CSV / JSON / Delta reads from S3 |
| `explain_sql` | Show Spark execution plan (for query tuning) |
| `cache_table` / `uncache_table` | Cache hot tables for faster repeated queries |
| `list_catalogs` / `list_schemas` | Explore Unity Catalog hierarchy |
| `spark_session_info` | Show active Spark session details |

### Architecture

```mermaid
graph LR
    IDE["VS Code / Cursor / Gemini"] -->|"stdio"| MCP["mcp_databricks_connect/server.py\n(FastMCP + Databricks Connect)"]
    MCP -->|"gRPC (Databricks Connect)"| SPARK["Databricks Serverless\nSpark 4.1.0"]
    SPARK -->|"Unity Catalog"| UC["sparkling.banking.*\n(rpt_* reporting tables)"]
    SPARK -->|"s3a://"| S3["S3 Bucket\nsparkling-data-test"]
```

---

## Option B — MCP with Athena / S3 / SQL warehouse

The original MCP server (`mcp/`) supports multiple backends via configuration.

### Quick Setup (Local — stdio transport)

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

### Standard tools (both MCP servers)

| Tool | Description | Example Prompt |
|------|-------------|----------------|
| `list_tables` | List all datasets | *"What tables are available?"* |
| `describe_table` | Get column names & types | *"Show me the schema of transactions"* |
| `sample_data` | Preview first N rows | *"Show me 5 sample customers"* |
| `query_sql` | Run read-only SQL | *"How many transactions per month?"* |
| `get_data_profile` | Stats: nulls, distinct counts | *"Profile the accounts table"* |
| `server_status` | Check backend, transport & health | *"Is the MCP server healthy?"* |

### Additional tools (mcp_databricks_connect only)

| Tool | Description |
|------|-------------|
| `dataframe_operation` | PySpark operations: filter, groupby_agg, orderby, select, corr |
| `read_s3_path` | Ad-hoc Parquet/CSV/JSON/Delta reads from S3 |
| `explain_sql` | Show Spark execution plan (simple / extended / cost / formatted) |
| `cache_table` / `uncache_table` | Cache hot tables for repeated queries |
| `list_catalogs` / `list_schemas` | Explore Unity Catalog hierarchy |
| `spark_session_info` | Active session details |

### Example Conversations

> **You**: *"What does the customer data look like?"*
> **AI** → `describe_table("customers")` → shows schema → `sample_data("customers", 5)` → shows rows

> **You**: *"Which branches have the most high-value transactions?"*
> **AI** → `query_sql("SELECT b.branch_name, COUNT(*) FROM branches b JOIN accounts a ...")`

> **You**: *"Are there data quality issues in transactions?"*
> **AI** → `get_data_profile("transactions")` → analyzes nulls and distributions

> **You** (Databricks Connect MCP): *"Show me the monthly trend in digital channel share"*
> **AI** → `query_sql("SELECT report_month, SUM(pct_of_total_txns) FROM rpt_channel_analysis WHERE digital_flag=true GROUP BY 1 ORDER BY 1")`

---

## MCP Server Architecture

```
mcp/                           (Option B: multi-backend)
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

mcp_databricks_connect/        (Option A: Databricks Connect — recommended)
├── server.py              # FastMCP entry point; Databricks Connect backend
├── config.py              # Reads .env, cluster/serverless config
├── spark_connect_backend.py  # All Spark operations via DatabricksSession
├── .env.example           # Config template → copy to .env
└── requirements.txt       # mcp[cli], databricks-connect
```

**Transport modes (both servers):**
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
