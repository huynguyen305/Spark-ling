# 🔥 Spark-ling: Banking Analytics Practice Project

**Target**: Techcombank Onboarding Preparation  
**Deadline**: March 9, 2026  
**Author**: Huy Nguyen Le

---

## 🎯 Project Overview

A hands-on Spark project simulating **banking data analytics** scenarios. Build a complete data pipeline that:
1. Ingests transaction & customer data
2. Performs data quality checks
3. Builds analytical aggregations
4. Implements SCD Type 2 for customer dimensions
5. Generates business insights reports

### 🏦 Aligned with Techcombank JD

| JD Requirement | Module Coverage |
|----------------|-----------------|
| **Data Architecture** — Build infrastructure, evaluate architectures | Module 10: Medallion Architecture, Data Products |
| **Data Integration** — Multi-source ETL, reusable ML assets | Module 10: Feature Store, Multi-source Integration |
| **Real-time Analytics** — Critical decision making | Module 11: Streaming, Transaction Monitoring |
| **Optimized Pipelines** — ETL for analysis | Modules 04-05: Quality Framework, SCD Type 2 |
| **Performance** — Handle concurrent workloads | Module 09: Fair Scheduler, Dynamic Allocation |
| **Cloud Migration** — AWS integration | Module 08: Delta Lake, DBX→AWS patterns |

---

## 🚀 Quick Start

There are **two modes** to run notebooks:

| Mode | Engine | Data location | Best for |
|------|--------|---------------|----------|
| **Local** | PySpark on your machine | `data/raw/*.csv` | Offline, small data |
| **Databricks Connect** ⭐ | Databricks serverless | `s3://sparkling-data-test` | Fast, production-like |

**Databricks Connect is recommended** — runs on cloud compute, no Java required, reads Parquet from S3.

---

## 🛠️ Setup: Databricks Connect (Recommended)

### Prerequisites
- Python 3.12 (Ubuntu 24.04 default)
- AWS CLI configured (`aws configure`)
- Databricks workspace access

### Step 1 — Install system package

```bash
sudo apt install -y python3.12-venv
```

### Step 2 — Create virtual environment

```bash
cd ~/sparking_repo/Spark-ling
python3 -m venv .venv
```

### Step 3 — Activate and install packages

```bash
source .venv/bin/activate
# Your prompt changes to (.venv) — all pip commands now go to the venv

pip install "databricks-connect==17.3.*" ipykernel
```

> **Note**: The version `17.3` must match your Databricks Runtime version.  
> Check: Databricks UI → Compute → Serverless runtime label.

### Step 4 — Configure Databricks credentials

```bash
cat > ~/.databrickscfg << 'EOF'
[DEFAULT]
host  = https://dbc-a460ab68-eabd.cloud.databricks.com
token = <your-personal-access-token>
EOF
```

Generate a token: Databricks UI → avatar (top-right) → **Settings** → **Developer** → **Access tokens** → **Generate new token**.

### Step 5 — Register the Jupyter kernel

```bash
# With .venv activated:
python -m ipykernel install --user \
    --name sparkling-databricks \
    --display-name "Spark-ling (Databricks)"
```

### Step 6 — Verify connection

```bash
python -c "
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.serverless().getOrCreate()
print(f'✅ Connected! Spark {spark.version}')
spark.stop()
"
```

### Step 7 — Open a notebook in VS Code

1. Open `notebooks/01_spark_basics.ipynb`
2. Click **Select Kernel** (top-right)
3. Click **Python Environments...**
4. Select `.venv — Python 3.12 — .../Spark-ling/.venv/bin/python`

> If `.venv` not listed: `Ctrl+Shift+P` → **Python: Select Interpreter** → enter the full path above.

### Step 8 — First notebook cells

```python
# Connect to Databricks serverless
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.serverless().getOrCreate()
print(f"✅ Spark {spark.version}")
```

```python
# Read from S3 (data was generated via scripts/generate_to_s3.py)
S3_RAW = "s3a://sparkling-data-test/data/raw"
customers_df    = spark.read.parquet(f"{S3_RAW}/customers")
accounts_df     = spark.read.parquet(f"{S3_RAW}/accounts")
transactions_df = spark.read.parquet(f"{S3_RAW}/transactions")
print(f"Customers: {customers_df.count():,} | Transactions: {transactions_df.count():,}")
```

---

## 🛠️ Setup: Local Mode (Alternative)

Requires Java 8 or 11 and more RAM, works offline.

```bash
# Install Java
sudo apt install -y openjdk-11-jdk

# Activate venv (or use system Python)
source .venv/bin/activate

# Install local Spark dependencies
pip install pyspark==3.5.* ipykernel numpy faker

# Generate synthetic data locally
python src/data_generator.py

# Launch Jupyter
jupyter notebook notebooks/
```

```python
# In notebook — local mode
from configs.spark_config import get_spark_session, get_data_path
spark = get_spark_session("MyApp", mode="local")
df = spark.read.csv("data/raw/customers.csv", header=True, inferSchema=True)
```

---

## ☁️ AWS S3 Setup

### Step 1 — Configure AWS credentials

```bash
aws configure
# Access Key ID:     AKIARH3LLC57XYMHY57N
# Secret Access Key: <from .secrets>
# Region:            ap-southeast-1
# Output format:     json
```

### Step 2 — Create environment file

```bash
cp aws/.env.example aws/.env
# Edit aws/.env — set AWS_REGION, AWS_ACCOUNT_ID, S3_BUCKET
```

### Step 3 — Create S3 bucket

```bash
./aws/setup_s3.sh
# Creates: s3://sparkling-data-test with versioning + lifecycle policy
```

### Step 4 — Generate data on Databricks → writes directly to S3

```python
# In a Databricks notebook:
%run /Repos/<your-username>/Spark-ling/scripts/generate_to_s3
```

This creates Parquet files at:
- `s3://sparkling-data-test/data/raw/branches/` — 100 rows
- `s3://sparkling-data-test/data/raw/customers/` — 10,000 rows
- `s3://sparkling-data-test/data/raw/accounts/` — ~15,000 rows
- `s3://sparkling-data-test/data/raw/transactions/` — 5,000,000 rows

### Step 5 — Connect Databricks to S3 (Unity Catalog)

1. In Databricks: **Catalog** → **External Data** → **Credentials** → **+ Add**
2. Choose **AWS IAM Role**, enter ARN: `arn:aws:iam::085587597183:role/sparkling-databricks-role`
3. Copy the **External ID** shown (e.g. `532d2e01-...`)
4. In AWS IAM → edit the role's trust policy replacing `ExternalId` with the copied value
5. Back in Databricks: **Catalog** → **External Locations** → **+ Create**
   - Name: `sparkling-data-test`
   - URL: `s3://sparkling-data-test/`
   - Credential: the one you just created
6. Click **Test connection** → all S3 checks should pass ✅

See [docs/AWS_SETUP.md](docs/AWS_SETUP.md) for full guide.

---

## 📁 Project Structure

```
Spark-ling/
├── README.md                     # You are here
├── requirements.txt              # Python dependencies
│
├── .venv/                        # Virtual environment (gitignored)
│   └── bin/python, pip, ...      # Use: source .venv/bin/activate
│
├── data/                         # Local data (gitignored)
│   ├── raw/                      # CSV data (local mode)
│   ├── processed/                # Cleaned outputs
│   └── analytics/                # Final analytics
│
├── notebooks/                    # Learning modules (start here!)
│   ├── 01_spark_basics.ipynb     # → Start here
│   ├── 02_dataframe_api.ipynb
│   ├── 03_window_functions.ipynb
│   ├── 04_data_quality.ipynb
│   ├── 05_scd_type2.ipynb
│   ├── 06_performance.ipynb
│   ├── 07_star_schema.ipynb
│   ├── 08_delta_lake.ipynb
│   ├── 09_concurrency.ipynb
│   ├── 10_medallion_arch.ipynb
│   └── 11_streaming.ipynb
│
├── scripts/
│   └── generate_to_s3.py         # Generate data on Databricks → S3
│
├── src/                          # Reusable Python modules
│   ├── data_generator.py         # Local CSV data generation
│   ├── transformations.py
│   ├── quality_checks.py
│   └── scd_handler.py
│
├── pipelines/                    # Production-style pipelines
│   ├── daily_transactions.py
│   └── customer_dim_scd.py
│
├── configs/
│   └── spark_config.py           # Multi-mode: local/aws/databricks
│
├── aws/                          # ☁️ AWS infrastructure scripts
│   ├── setup_s3.sh               # Create S3 bucket
│   ├── sync_data.sh              # Upload/download data ↔ S3
│   ├── submit_emr_job.sh         # Run Spark on EMR
│   ├── teardown.sh               # Delete S3 bucket + contents
│   └── .env.example              # Config template → copy to .env
│
├── mcp/                          # 🔌 MCP server for AI data exploration
│   ├── server.py                 # FastMCP entry point
│   ├── config.py                 # Loads .env config
│   ├── databricks_backend.py     # Queries via Databricks SQL warehouse
│   ├── s3_backend.py             # Reads from S3 via local PySpark
│   ├── deploy_ec2.sh             # Deploy MCP server to EC2
│   ├── .env.example              # MCP config template → copy to .env
│   └── requirements.txt          # MCP-specific deps
│
└── docs/
    ├── DATABRICKS_CONNECT.md     # ⭐ Local IDE → Databricks remote engine
    ├── AWS_SETUP.md              # S3 bucket, IAM, External Location
    ├── MCP_GUIDE.md              # MCP server setup + AI tools
    ├── GCP_SETUP.md              # GCP/Dataproc (legacy)
    └── INTEGRATION_GUIDE.md      # End-to-end architecture overview
```

---

## 📚 Learning Path

### 📅 Suggested Timeline

| Week | Focus | Hours | Modules |
|------|-------|-------|---------|
| **Jan 23-29** | Setup & Quick Review | 2-3h | Install, generate data, skim 01-02 |
| **Feb 3-9** | Window Functions + Quality | 4h | 03, 04 |
| **Feb 10-16** | SCD + Data Modeling ⭐ | 6h | 05, 07 |
| **Feb 17-23** | Performance + Concurrency ⭐ | 6h | 06, 09 |
| **Feb 24-Mar 2** | Delta Lake + AWS ⭐ | 6h | 08 |
| **Mar 3-8** | **Data Arch + Streaming** ⭐ | 8h | **10, 11** + run pipelines |

**Total: ~32-37 hours over 6 weeks**

### Module Overview

| Module | Level | Focus |
|--------|-------|-------|
| 01-02 | Review | Basics (skim if comfortable) |
| 03 | Core | Window functions — running totals, rankings |
| 04 | Core | Data quality framework |
| 05 | Core | SCD Type 2 implementation |
| 06 | Core | Broadcast joins, caching, Spark UI |
| **07** | ⭐ Senior | Star Schema, Business KPIs, Data Vault |
| **08** | ⭐ Senior | Delta Lake, Databricks → AWS migration |
| **09** | ⭐ Senior | Concurrency optimization |
| **10** | ⭐ JD | Medallion Architecture, ML Feature Assets |
| **11** | ⭐ JD | Streaming & Real-time Transaction Monitoring |

> **Senior DE Path**: Start at Module 3, prioritize 07-11.

---

## 🏦 Data Model

| Table | Rows | Key Fields |
|-------|------|------------|
| `branches` | 100 | branch_id, region, city |
| `customers` | 10,000 | customer_id, segment, kyc_status |
| `accounts` | ~15,000 | account_id, customer_id, balance |
| `transactions` | 5,000,000 | txn_id, account_id, amount, channel |

---

## 🔌 MCP: AI-Assisted Data Exploration

```bash
# Install MCP dependencies (in venv)
pip install -r mcp/requirements.txt

# Configure
cp mcp/.env.example mcp/.env
# Edit mcp/.env → set MCP_BACKEND, DATABRICKS_TOKEN, etc.
```

Add to VS Code `.vscode/mcp.json`:
```json
{
  "servers": {
    "sparkling-data": {
      "command": "/path/to/Spark-ling/.venv/bin/python",
      "args": ["-m", "mcp.server"],
      "cwd": "/path/to/Spark-ling"
    }
  }
}
```

Then ask your AI: *"What's the customer segment distribution?"* or *"Show me the top 10 accounts by balance."*

See [docs/MCP_GUIDE.md](docs/MCP_GUIDE.md) for full guide.

---

## ✅ Success Criteria (by March 9)

### Core Spark Skills
- [ ] Explain Spark architecture (Driver, Executors, Partitions)
- [ ] Write complex DataFrame transformations fluently
- [ ] Implement window functions for running totals, rankings
- [ ] Build a data quality validation framework
- [ ] Implement SCD Type 2 for dimension tables
- [ ] Optimize Spark jobs using broadcast joins, caching
- [ ] Debug using Spark UI (stages, tasks, shuffle)

### Senior DE Skills (Data Modeling & Business Focus)
- [ ] Design Star Schema (Fact & Dimension tables) in Spark
- [ ] Build business KPI metrics (CLV, MAU, Channel Mix)
- [ ] Implement aggregate fact tables (periodic snapshots)
- [ ] Understand Data Vault concepts (Hub, Link, Satellite)
- [ ] Write production-grade pipelines with quality gates
- [ ] Implement incremental load patterns

### Cloud & Infrastructure
- [ ] Use Delta Lake (MERGE, TIME TRAVEL, OPTIMIZE)
- [ ] Understand Databricks to AWS EMR/Glue migration patterns
- [ ] Configure Fair Scheduler for concurrent queries
- [ ] Implement Dynamic Resource Allocation
- [ ] Run notebooks via Databricks Connect (local → cloud)
- [ ] Query S3 data from Databricks external locations

---

## 📝 Notes

- All data is synthetic — no real customer information
- `.venv` and `aws/.env`, `mcp/.env`, `.secrets` are gitignored
- Spark UI available at `http://localhost:4040` in local mode; use Databricks job runs UI in remote mode
- Use `spark.stop()` to cleanly shut down local Spark sessions

---

**Good luck with your Techcombank onboarding! 🚀**
