# RDS PostgreSQL → Databricks Migration: 2-Day Learning Guide

> **Goal**: Build an end-to-end migration pipeline from PostgreSQL RDS to Databricks,
> simulating a real banking data migration workflow.
>
> **Why PostgreSQL?** Many enterprises (including banks) migrate FROM Oracle TO PostgreSQL
> to save on licensing costs — so this is a very realistic scenario! The pipeline concepts
> (JDBC extraction, CDC, Medallion architecture) are identical regardless of source database.

## Prerequisites

Before starting, ensure you have:

| Requirement | Status | Command to Verify |
|---|---|---|
| AWS CLI configured | Required | `aws sts get-caller-identity` |
| Python 3.9+ | Required | `python3 --version` |
| pip packages | Required | `pip install psycopg2-binary pyspark delta-spark numpy` |
| AWS Region set | ap-southeast-1 | `echo $AWS_REGION` |
| S3 bucket exists | sparkling-data-test | `aws s3 ls s3://sparkling-data-test/` |
| Databricks workspace | Optional (Day 2) | Check your Databricks URL |

> [!IMPORTANT]
> **Cost Warning**: PostgreSQL RDS `db.t3.medium` costs **~$0.04/hr (~$0.96/day)**.
> Always run `aws/rds/rds_teardown.sh` when done to stop billing!

---

## Architecture

```mermaid
graph TD
    subgraph "Data Extraction & Lakeflow Pipelines"
        A["PostgreSQL RDS<br/>(Source Database)"] -->|"JDBC Auto-Refresh<br/>(rds_tables.py DLT)"| C["Unity Catalog<br/>Bronze MVs (Raw)"]
        A -->|"Legacy JDBC Extract"| B["S3 Landing Zone"]
        B -->|"Delta Write"| C2["Legacy Bronze<br/>(S3 Delta)"]
        C -->|"Cleanse & Validate"| D["Unity Catalog<br/>Silver Tables"]
        D -->|"Aggregate"| E["Unity Catalog<br/>Gold Tables"]
    end

    subgraph "Daily Operations"
        F["Daily Simulator<br/>(rds_daily_generator.py)"] -->|"New txns + updates"| A
    end

    subgraph "Databricks Storage"
        C -.->|"Managed by Unity Catalog"| S3["AWS S3 Bucket<br/>(sparkling-data-test)"]
        D -.-> S3
        E -.-> S3
        E --> H["Databricks SQL Dashboard"]
    end
```

---

## Day 1: Initial Migration (4-6 hours)

### Step 1.1: Deploy PostgreSQL RDS (~15 min)

**What you'll learn**: AWS CloudFormation, RDS PostgreSQL provisioning, VPC security groups.

```bash
# Get your current IP for security group
MY_IP=$(curl -s ifconfig.me)
echo "Your IP: ${MY_IP}"

# Deploy the CloudFormation stack
aws cloudformation deploy \
  --template-file aws/rds/rds_setup.yaml \
  --stack-name sparkling-rds-postgres \
  --parameter-overrides \
    MasterPassword='SparkLing2026!' \
    AllowedCIDR="${MY_IP}/32" \
  --region ap-southeast-1

# Get the RDS endpoint (wait 5-10 min for creation)
aws cloudformation describe-stacks \
  --stack-name sparkling-rds-postgres \
  --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' \
  --output table \
  --region ap-southeast-1
```

> [!NOTE]
> **Why PostgreSQL RDS?** PostgreSQL is open-source, free-tier eligible, and widely used
> in banking migrations. The JDBC extraction, CDC, and pipeline patterns are identical
> to Oracle — only the driver and SQL syntax differ slightly.

**Save the endpoint** — update `aws/.env`:

```bash
# Add these lines to aws/.env
RDS_HOST=sparkling-postgres-db.xxxxxxxxx.ap-southeast-1.rds.amazonaws.com
RDS_PORT=5432
RDS_DATABASE=sparkdb
RDS_USERNAME=sparkadmin
RDS_PASSWORD=SparkLing2026!
```

---

### Step 1.2: Create Schema & Seed Data (~20 min)

**What you'll learn**: PostgreSQL DDL, dimension/fact modeling, SCD Type 2 design, batch inserts.

```bash
# Install PostgreSQL driver
pip install psycopg2-binary

# Create schema and seed data
python aws/rds/rds_seed_data.py
```

**Key tables created**:

| Table | Type | Rows | Purpose |
|---|---|---|---|
| `dim_date` | Dimension | ~2,557 | Calendar (2020-2026) |
| `dim_branch` | Dimension | 100 | Bank branches |
| `dim_account_type` | Dimension | 6 | Account categories |
| `dim_customer` | Dimension (SCD2) | 10,000 | Customer master with history |
| `fact_transaction` | Fact | 500,000 | Banking transactions |
| `fact_daily_balance` | Fact (Snapshot) | 150,000 | End-of-day balances |
| `cdc_watermark` | System | 5 | CDC tracking |

> [!TIP]
> **Study the schema**: Open `aws/rds/rds_schema.sql` and read the comments.
> Pay attention to: `last_modified` columns (CDC), `GENERATED ALWAYS AS IDENTITY`
> columns (surrogate keys), `is_current` flag (SCD Type 2), and the `cdc_watermark` table.
> Comments compare PostgreSQL syntax with Oracle equivalents.

**Validate the seeding**:

```bash
python aws/rds/rds_seed_data.py --validate-only
```

---

### Step 1.3: Full Load Migration (~30 min)

**What you'll learn**: Spark JDBC, parallel extraction, S3 landing zone, Delta Lake Bronze tables.

```bash
# Install Spark dependencies
pip install pyspark delta-spark

# Run the full load pipeline
python pipelines/migration/full_load_pipeline.py
```

**What happens under the hood**:

1. **JDBC Extraction**: Spark connects to PostgreSQL via `spark.read.jdbc()` with parallel partitions
2. **S3 Landing**: Raw data lands as Parquet in `s3://sparkling-data-test/migration/landing/`
3. **Bronze Delta**: Data is converted to Delta format with ingestion metadata

> [!NOTE]
> **Why not JDBC directly to Databricks?**
> Direct JDBC is slow for large tables. The S3 intermediate pattern is
> standard in production: extract → land → load. AWS DMS automates this,
> but understanding the manual process is valuable.

**Validate the migration**:

```bash
python pipelines/migration/full_load_pipeline.py --validate-only
```

---

### Step 1.4: Bronze → Silver Transformation (~30 min)

**What you'll learn**: Data quality validation, deduplication, null handling, SCD Type 2, column standardization.

```bash
python pipelines/migration/bronze_to_silver.py
```

**Transformations applied**:

- Column names: PostgreSQL already uses `snake_case` (unlike Oracle's `UPPER_CASE`)
- Null handling: Fill defaults or quarantine
- Deduplication: Remove duplicate `txn_id` values
- Enrichment: `amount_category`, `age_group`, `suspicious_flag`
- Quality flags: `dq_has_null_name`, `dq_valid_amount`

> [!TIP]
> **Study the code**: Open `pipelines/migration/bronze_to_silver.py` and trace
> each transformation. The Silver layer is where you enforce the "single source of truth".

---

### Step 1.5: Silver → Gold Aggregation (~20 min)

**What you'll learn**: Star schema joins, business aggregations, Customer 360, risk alerting.

```bash
python pipelines/migration/silver_to_gold.py
```

**Gold tables built**:

| Gold Table | Business Purpose | Key Metrics |
|---|---|---|
| `daily_branch_summary` | Branch performance | Revenue, volume, suspicious count |
| `customer_360` | Complete customer profile | CLV, tenure, channel preference |
| `monthly_balance_trends` | Balance trend analysis | Avg balance, volatility, net flow |
| `risk_alerts` | Compliance/AML | Suspicious transactions by risk level |

---

### Step 1.6: Run the Full Pipeline via Orchestrator (~5 min)

**What you'll learn**: Pipeline DAG, dependency resolution, retry logic, orchestration patterns.

```bash
# Dry run to see execution plan
python pipelines/migration/pipeline_orchestrator.py --mode dry-run

# Full initial migration (all steps above in sequence)
python pipelines/migration/pipeline_orchestrator.py --mode initial
```

---

## Day 2: Daily Operations & Monitoring (4-6 hours)

### Step 2.1: Simulate Daily Activity (~15 min)

**What you'll learn**: Operational data patterns, SCD Type 2 updates, transaction generation.

```bash
# Simulate one day of banking activity
python aws/rds/rds_daily_generator.py --simulate-date 2026-03-07

# Simulate a full week
python aws/rds/rds_daily_generator.py \
  --start-date 2026-03-01 \
  --end-date 2026-03-07
```

**What this generates**:

- ~5,000 new transactions per weekday (~1,000 on weekends)
- ~200 customer attribute changes (SCD Type 2)
- End-of-day balance snapshots for 5,000 accounts

> [!NOTE]
> **Scaling Performance (Learning Curve)**:
> When scaling from 5k to 2M+ rows (3M rows weekday / 1.5M weekend):
>
> 1. **Memory**: A Python list of 3M dicts can exceed 1.5GB RAM. Increase Lambda `MemorySize` to 3008MB+.
> 2. **Timeout**: Bulk inserts of 3M rows take time. Increase Lambda `Timeout` to 900s (15 min).
> 3. **Database IOPS**: Massive writes on `db.t3.micro` may cause throttling. In production, use `gp3` storage with high provisioned IOPS.
> 4. **Batching**: Always use `execute_values` (psycopg2) or Spark-based loaders. Row-by-row inserts will fail scaling tests.

---

### Step 2.2: Run Incremental CDC Pipeline (~20 min)

**What you'll learn**: CDC watermark pattern, Delta MERGE, incremental processing.

```bash
# Check current watermarks
python pipelines/migration/incremental_load_pipeline.py --show-watermarks

# Run the incremental pipeline
python pipelines/migration/incremental_load_pipeline.py

# Check updated watermarks
python pipelines/migration/incremental_load_pipeline.py --show-watermarks
```

> [!IMPORTANT]
> **CDC is the hardest part of migration**: Understanding how changed data
> flows from source to destination is the #1 skill for data engineering.
> Study `incremental_load_pipeline.py` thoroughly.

---

### Step 2.3: Run Daily Orchestration (~10 min)

```bash
python pipelines/migration/pipeline_orchestrator.py --mode daily
```

This runs the full daily cycle: CDC → Bronze → Silver → Gold.

---

### Step 2.4: Set Up Monitoring Dashboard (~30 min)

**What you'll learn**: Databricks SQL dashboards, data freshness monitoring, operational metrics.

Open `pipelines/migration/monitor_dashboard.sql` and create each query in your Databricks workspace:

1. Go to **Databricks SQL → Queries → Create Query**
2. Paste each query from the SQL file
3. Create a **Dashboard** and add each query as a widget

---

### Step 2.5: Validation & Consistency Checks (~20 min)

```bash
# Cross-layer validation
python pipelines/migration/pipeline_orchestrator.py --mode validate
```

**Manual checks to perform**:

1. Compare RDS `fact_transaction` count with Bronze total
2. Verify Silver has fewer rows than Bronze (quarantined records)
3. Check Gold `customer_360` matches Silver `dim_customer` current records
4. Confirm CDC watermarks advanced after incremental run

---

### Step 2.6: Teardown (~5 min)

> [!CAUTION]
> **Stop billing immediately after learning!**

```bash
# Destroy PostgreSQL RDS (saves ~$0.96/day)
chmod +x aws/rds/rds_teardown.sh
./aws/rds/rds_teardown.sh

# (Optional) Clean S3 migration data
aws s3 rm s3://sparkling-data-test/migration/ --recursive
```

---

## Key Concepts Reference

### Medallion Architecture (Bronze/Silver/Gold)

| Layer | Format | Purpose | Update Pattern |
|---|---|---|---|
| **Bronze** | Delta (raw) | Exact copy of source | Append/MERGE |
| **Silver** | Delta (clean) | Validated, deduplicated | Overwrite/MERGE |
| **Gold** | Delta (aggregated) | Business metrics | Overwrite |

### CDC Strategies Comparison

| Strategy | Complexity | Latency | Captures Deletes? | Tool |
|---|---|---|---|---|
| Timestamp watermark | Low | Minutes-Hours | ❌ | This pipeline |
| PostgreSQL logical repl | Medium | Seconds | ✅ | pgoutput / wal2json |
| AWS DMS | Low | Seconds | ✅ | AWS managed |
| Debezium | Medium | Seconds | ✅ | Open source |

### File Map

```
aws/rds/
├── rds_setup.yaml          # CloudFormation for PostgreSQL RDS
├── rds_teardown.sh         # Cleanup script
├── rds_schema.sql          # PostgreSQL DDL (6 tables + CDC)
├── rds_seed_data.py        # Initial data seeder (psycopg2)
└── rds_daily_generator.py  # Daily activity simulator

pipelines/migration/
├── full_load_pipeline.py          # Initial full extraction
├── incremental_load_pipeline.py   # Daily CDC pipeline
├── bronze_to_silver.py            # Data quality & transformation
├── silver_to_gold.py              # Business aggregations
├── pipeline_orchestrator.py       # DAG execution engine
└── monitor_dashboard.sql          # Databricks SQL queries
```

---

## Connecting to Databricks

Your existing Databricks workspace and MCP integration can be used to:

1. **Query Bronze/Silver/Gold tables** via the Databricks SQL warehouse
2. **Run transformations** via Databricks notebooks
3. **Use MCP Databricks** from Antigravity to query tables in the `sparkling.banking` schema

To register external S3 Delta tables in Databricks:

```sql
-- Run once in Databricks SQL
CREATE SCHEMA IF NOT EXISTS sparkling.banking;

CREATE TABLE IF NOT EXISTS sparkling.banking.bronze_customers
USING DELTA LOCATION 's3://sparkling-data-test/migration/bronze/dim_customer';

CREATE TABLE IF NOT EXISTS sparkling.banking.bronze_transactions
USING DELTA LOCATION 's3://sparkling-data-test/migration/bronze/fact_transaction';

-- Repeat for silver and gold tables...
```

---

## Troubleshooting

| Issue | Cause | Solution |
|---|---|---|
| `connection refused` on port 5432 | Security group blocks your IP | Update `AllowedCIDR` in CloudFormation |
| `ClassNotFoundException: org.postgresql.Driver` | Missing JDBC JAR | Spark auto-downloads via `spark.jars.packages` |
| S3 access denied | Missing AWS credentials | Set `AWS_ACCESS_KEY_ID` or use IAM role |
| Delta table not found | Full load not run yet | Run `full_load_pipeline.py` first |
| Watermark not advancing | No new data in RDS | Run `rds_daily_generator.py` |
