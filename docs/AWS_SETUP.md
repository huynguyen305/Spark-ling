# ☁️ AWS S3 Setup Guide

Set up AWS S3 as the primary cloud storage for Spark-ling, and connect Databricks to it via Unity Catalog External Location.

---

## Architecture

```mermaid
graph TB
    subgraph LOCAL["💻 Local Machine (WSL)"]
        CODE["VS Code\n+ Databricks Connect"]
        CLI["AWS CLI"]
        SCRIPTS["aws/ scripts"]
    end

    subgraph AWS["☁️ Amazon Web Services"]
        S3["S3 Bucket\nsparkling-data-test\nap-southeast-1"]
        IAM["IAM Role\nsparkling-databricks-role"]
    end

    subgraph DBX["☁️ Databricks (dbc-cdbdfd07-5797)"]
        SERVERLESS["Serverless Compute"]
        UC["Unity Catalog"]
        STORAGECRED["Storage Credential\n(IAM Role ARN)"]
        EXTLOC["External Location\ns3://sparkling-data-test/"]
    end

    CODE -->|"gRPC via Databricks Connect"| SERVERLESS
    SCRIPTS -->|"aws s3 sync"| S3
    CLI -->|"aws configure"| AWS
    IAM -->|"grants access"| S3
    UC --> STORAGECRED
    STORAGECRED -->|"AssumeRole"| IAM
    UC --> EXTLOC
    EXTLOC -->|"points to"| S3
    SERVERLESS -->|"s3a://"| S3
```

---

## Your Current Configuration

| Setting | Value |
|---------|-------|
| **S3 Bucket** | `sparkling-data-test` |
| **AWS Region** | `ap-southeast-1` (Singapore) |
| **AWS Account ID** | `085587597183` |
| **IAM Role** | `arn:aws:iam::085587597183:role/sparkling-databricks-role` |
| **Databricks host** | `https://dbc-cdbdfd07-5797.cloud.databricks.com` |

---

## Step 1 — Configure AWS CLI

```bash
# Install AWS CLI (if not already)
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip
unzip awscliv2.zip && sudo ./aws/install
rm -rf awscliv2.zip aws/   # cleanup

# Configure credentials
aws configure
# → AWS Access Key ID:     AKIARH3LLC57XYMHY57N
# → AWS Secret Access Key: <from .secrets file>
# → Default region:        ap-southeast-1
# → Output format:         json

# Verify
aws sts get-caller-identity
# Should show your account ID: 085587597183
```

---

## Step 2 — Create Environment File

```bash
# Copy the template
cp aws/.env.example aws/.env

# The file is pre-filled with your values:
# AWS_REGION=ap-southeast-1
# AWS_ACCOUNT_ID=085587597183
# S3_BUCKET=sparkling-data-test
# DATABRICKS_HOST=https://dbc-cdbdfd07-5797.cloud.databricks.com

# aws/.env is gitignored — safe to store real values there
```

---

## Step 3 — Create S3 Bucket

```bash
./aws/setup_s3.sh
```

This script:
1. Creates `s3://sparkling-data-test` in `ap-southeast-1`
2. Enables versioning
3. Creates folder structure: `data/raw/`, `data/processed/`, `data/analytics/`, `data/quarantine/`
4. Blocks public access
5. Sets lifecycle policy (old versions expire after 30 days)

Expected output:
```
✅ Bucket already exists: s3://sparkling-data-test
✅ Versioning enabled
✅ s3://sparkling-data-test/data/raw/
✅ S3 setup complete!
```

---

## Step 4 — Generate Data on Databricks → S3

**Option A: Via Databricks notebook** (recommended — uses serverless compute)

```python
# In a Databricks notebook in your Repos:
%run /Repos/<your-username>/Spark-ling/scripts/generate_to_s3
```

Or upload and run as a Databricks job:
1. Databricks → **Jobs** → **Create Job**
2. Task type: **Python script**
3. Source: **Workspace** → upload `scripts/generate_to_s3.py`
4. Cluster: **Serverless**
5. Click **Run now**

Data written (as Parquet):
```
s3://sparkling-data-test/data/raw/
├── branches/       part-*.parquet   (100 rows)
├── customers/      part-*.parquet   (10,000 rows)
├── accounts/       part-*.parquet   (~15,000 rows)
└── transactions/   part-*.parquet   (5,000,000 rows, 16 partitions)
```

**Option B: Generate locally → sync to S3**

```bash
# Generate CSV data locally
python src/data_generator.py         # → data/raw/*.csv

# Upload to S3
./aws/sync_data.sh upload            # syncs data/raw/ to s3://sparkling-data-test/data/raw/

# Verify upload
./aws/sync_data.sh status
```

---

## Step 5 — Connect Databricks to S3 (Unity Catalog)

This is a one-time setup to allow Databricks serverless to read/write your S3 bucket.

### 5a. Create IAM Role in AWS

1. Go to [AWS IAM Console → Roles → Create Role](https://console.aws.amazon.com/iam/home?region=ap-southeast-1#/roles/create)
2. **Trusted entity type**: Custom trust policy — paste:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"AWS": "arn:aws:iam::414351767093:root"},
    "Action": "sts:AssumeRole",
    "Condition": {
      "StringEquals": {"sts:ExternalId": "PLACEHOLDER"}
    }
  }]
}
```

3. **Attach permissions** — create inline policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
      "s3:ListBucket", "s3:GetBucketLocation"
    ],
    "Resource": [
      "arn:aws:s3:::sparkling-data-test",
      "arn:aws:s3:::sparkling-data-test/*"
    ]
  }]
}
```

4. **Role name**: `sparkling-databricks-role`
5. Save → your ARN: `arn:aws:iam::085587597183:role/sparkling-databricks-role`

### 5b. Create Storage Credential in Databricks

1. Databricks → **Catalog** → **External Data** → **Credentials** → **+ Add**
2. **Credential type**: AWS IAM Role
3. Paste your role ARN: `arn:aws:iam::085587597183:role/sparkling-databricks-role`
4. Click **Create** — a credential page shows the **External ID** (e.g. `532d2e01-363a-4e...`)
5. **Copy the full External ID**

### 5c. Update IAM Role Trust Policy

1. AWS Console → IAM → Roles → `sparkling-databricks-role` → **Trust relationships** → **Edit trust policy**
2. Replace `PLACEHOLDER` with the exact External ID copied in step 5b:

```json
"sts:ExternalId": "532d2e01-363a-4e..."
```

3. Save the policy

### 5d. Validate the Storage Credential

Back in Databricks → select the credential → click **Validate Configuration**

Expected results:
```
✅ Read
✅ List
✅ Write
✅ Delete
✅ Path Exists
✅ Assume Role
✅ Self Assume Role
✅ External ID Condition
⚠️ File Events Read  (optional — only needed for Auto Loader)
```

The File Events warning is safe to ignore for standard S3 reads/writes.

### 5e. Create External Location

1. Databricks → **Catalog** → **External Locations** → **+ Create manually**
2. **Name**: `sparkling-data-test`
3. **URL**: `s3://sparkling-data-test/`
4. **Credential**: select the credential from step 5b
5. Click **Test connection** → should show green ✅ for Read, List, Write, Delete

---

## Step 6 — Use in Code

```python
# Via Databricks Connect (local → remote)
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.serverless().getOrCreate()

branches_df     = spark.read.parquet("s3a://sparkling-data-test/data/raw/branches")
customers_df    = spark.read.parquet("s3a://sparkling-data-test/data/raw/customers")
accounts_df     = spark.read.parquet("s3a://sparkling-data-test/data/raw/accounts")
transactions_df = spark.read.parquet("s3a://sparkling-data-test/data/raw/transactions")
```

```python
# Via spark_config (any mode)
from configs.spark_config import get_spark_session, get_data_path

spark = get_spark_session("MyApp", mode="databricks")
raw   = get_data_path("raw", mode="databricks")   # → s3a://sparkling-data-test/data/raw
df    = spark.read.parquet(raw + "/customers")
```

---

## Sync Data Commands Reference

```bash
# Upload local data/ to S3
./aws/sync_data.sh upload

# Download S3 data to local data/
./aws/sync_data.sh download

# Show what's in S3
./aws/sync_data.sh status

# Run a Spark job on EMR
./aws/submit_emr_job.sh pipelines/daily_transactions.py
```

---

## IAM Policy (Minimum Required)

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ],
    "Resource": [
      "arn:aws:s3:::sparkling-data-test",
      "arn:aws:s3:::sparkling-data-test/*"
    ]
  }]
}
```

For Auto Loader (File Events), also add:
```json
{
  "Effect": "Allow",
  "Action": ["s3:GetBucketNotification", "s3:PutBucketNotification",
             "sqs:CreateQueue", "sqs:DeleteQueue", "sqs:SendMessage",
             "sqs:ReceiveMessage", "sqs:DeleteMessage",
             "sqs:GetQueueAttributes", "sqs:SetQueueAttributes"],
  "Resource": [
    "arn:aws:s3:::sparkling-data-test",
    "arn:aws:sqs:ap-southeast-1:085587597183:*"
  ]
}
```

---

## Teardown

```bash
# Delete the S3 bucket and all data (prompts for confirmation)
./aws/teardown.sh

# Skip confirmation prompt
./aws/teardown.sh --force
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `AccessDenied` on S3 | Run `aws sts get-caller-identity`; check `aws configure` credentials |
| `NoSuchBucket` | Run `./aws/setup_s3.sh` first |
| `S3_BUCKET not set` | Check `aws/.env` exists and has `S3_BUCKET=sparkling-data-test` |
| Databricks `PERMISSION_DENIED` | Verify IAM trust policy has the correct External ID from Unity Catalog |
| `File Events Read` failed | Ignored for basic use; add SQS/SNS permissions to IAM role if needed |
| Slow reads from S3 locally | Data is Parquet — use `spark.read.parquet()` not CSV for best performance |
| `hadoop-aws` not found | Ensure `databricks-connect` or `pyspark` 3.4+ is installed in `.venv` |
