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

| JD Requirement                                                       | Module Coverage                                    |
| -------------------------------------------------------------------- | -------------------------------------------------- |
| **Data Architecture** - Build infrastructure, evaluate architectures | Module 10: Medallion Architecture, Data Products   |
| **Data Integration** - Multi-source ETL, reusable ML assets          | Module 10: Feature Store, Multi-source Integration |
| **Real-time Analytics** - Critical decision making                   | Module 11: Streaming, Transaction Monitoring       |
| **Optimized Pipelines** - ETL for analysis                           | Modules 04-05: Quality Framework, SCD Type 2       |
| **Performance** - Handle concurrent workloads                        | Module 09: Fair Scheduler, Dynamic Allocation      |
| **Cloud Migration** - AWS integration                                | Module 08: Delta Lake, DBX→AWS patterns            |

---

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Java 8 or 11 (required for Spark)
- 8GB+ RAM recommended

### Installation

```bash
# Navigate to project directory
cd Spark-ling

# Install dependencies
pip install -r requirements.txt

# Generate synthetic banking data
python src/data_generator.py

# Launch Jupyter to start learning
jupyter notebook notebooks/
```

### Verify Installation

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkTest") \
    .master("local[*]") \
    .getOrCreate()

print(f"Spark Version: {spark.version}")
spark.stop()
```

---

## 📁 Project Structure

```
Spark-ling/
├── README.md                    # You are here
├── requirements.txt             # Python dependencies
│
├── data/
│   ├── raw/                     # Synthetic banking data
│   ├── processed/               # Cleaned data output
│   └── analytics/               # Final analytics output
│
├── notebooks/                   # Learning modules (start here!)
│   ├── 01_spark_basics.ipynb ... 11_streaming_realtime_analytics.ipynb
│
├── src/                         # Reusable code modules
│   ├── data_generator.py
│   ├── transformations.py
│   ├── quality_checks.py
│   └── scd_handler.py
│
├── pipelines/                   # Production-style pipelines
│   ├── daily_transactions.py
│   └── customer_dim_scd.py
│
├── configs/
│   └── spark_config.py          # Multi-mode: local/gcp/aws/databricks
│
├── aws/                         # ☁️ AWS infrastructure scripts
│   ├── setup_s3.sh, sync_data.sh, submit_emr_job.sh, teardown.sh
│   └── .env.example
│
├── gcp/                         # GCP infrastructure scripts
│   ├── setup_gcp.sh, sync_data.sh, submit_job.sh, teardown_gcp.sh
│   └── .env.example
│
├── mcp/                         # 🔌 MCP server for AI data exploration
│   ├── server.py, config.py
│   ├── databricks_backend.py, s3_backend.py
│   ├── .env.example
│   └── requirements.txt
│
└── docs/
    ├── AWS_SETUP.md, GCP_SETUP.md
    ├── INTEGRATION_GUIDE.md
    └── MCP_GUIDE.md
```

---

## 📚 Learning Path

### 📅 Suggested Timeline (4-8 hrs/week from February)

| Week             | Focus                       | Hours | Modules                            |
| ---------------- | --------------------------- | ----- | ---------------------------------- |
| **Jan 23-29**    | Setup & Quick Review        | 2-3h  | Install, generate data, skim 01-02 |
| **Feb 3-9**      | Window Functions + Quality  | 4h    | 03, 04                             |
| **Feb 10-16**    | SCD + Data Modeling ⭐       | 6h    | 05, 07                             |
| **Feb 17-23**    | Performance + Concurrency ⭐ | 6h    | 06, 09                             |
| **Feb 24-Mar 2** | Delta Lake + AWS ⭐          | 6h    | 08                                 |
| **Mar 3-8**      | **Data Arch + Streaming** ⭐ | 8h    | **10, 11** + run pipelines         |

**Total: ~32-37 hours over 6 weeks**

### Module Overview (11 Modules)

| Module | Level    | Focus                                           |
| ------ | -------- | ----------------------------------------------- |
| 01-02  | Review   | Basics (skim if comfortable)                    |
| 03     | Core     | Window functions - running totals, rankings     |
| 04     | Core     | Data quality framework                          |
| 05     | Core     | SCD Type 2 implementation                       |
| 06     | Core     | Broadcast joins, caching, Spark UI              |
| **07** | ⭐ Senior | Star Schema, Business KPIs, Data Vault          |
| **08** | ⭐ Senior | Delta Lake, Databricks → AWS migration          |
| **09** | ⭐ Senior | Concurrency optimization (no Synapse queueing!) |
| **10** | ⭐ JD     | Medallion Architecture, ML Feature Assets       |
| **11** | ⭐ JD     | Streaming & Real-time Transaction Monitoring    |

> **Senior DE Path**: Start at Module 3, prioritize 07-11 for Techcombank JD alignment.

---

## 🏦 Data Model

### Entities
- **Customers** (10,000): Personal info, segment, KYC status
- **Accounts** (15,000): Balance, type, status
- **Transactions** (5,000,000): 1 year of data - large enough for real performance challenges! 🔥
- **Branches** (100): Regional distribution

### Sample Queries After Setup

```python
# Top 10 customers by transaction volume
spark.sql("""
    SELECT c.name, COUNT(t.txn_id) as txn_count, SUM(t.amount) as total_amount
    FROM customers c
    JOIN accounts a ON c.customer_id = a.customer_id
    JOIN transactions t ON a.account_id = t.account_id
    GROUP BY c.name
    ORDER BY total_amount DESC
    LIMIT 10
""").show()
```

---

## ☁️ AWS S3 Integration

```bash
cp aws/.env.example aws/.env   # fill in your values
./aws/setup_s3.sh              # create S3 bucket
python src/data_generator.py   # generate data
./aws/sync_data.sh upload      # upload to S3
```

```python
from configs.spark_config import get_spark_session, get_data_path
spark = get_spark_session("MyApp", mode="aws")
raw = get_data_path("raw", mode="aws")  # s3a://bucket/data/raw
```

See [docs/AWS_SETUP.md](docs/AWS_SETUP.md) for full guide.

---

## 🔌 MCP: AI-Assisted Data Exploration

```bash
pip install -r mcp/requirements.txt
cp mcp/.env.example mcp/.env   # fill in credentials
```

Add to IDE MCP config:
```json
{
  "mcpServers": {
    "sparkling-data": {
      "command": "python",
      "args": ["-m", "mcp.server"],
      "cwd": "/path/to/Spark-ling"
    }
  }
}
```

Then ask your AI: *"What's the customer segment distribution?"*

See [docs/MCP_GUIDE.md](docs/MCP_GUIDE.md) for full guide.

---

## ✅ Success Criteria

By March 9, you should be able to:

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

### Migration & Concurrency Skills (AWS Focus)
- [ ] Use Delta Lake (MERGE, TIME TRAVEL, OPTIMIZE)
- [ ] Understand Databricks to AWS EMR/Glue migration patterns
- [ ] Configure Fair Scheduler for concurrent queries
- [ ] Implement Dynamic Resource Allocation

---

## 🔗 Related Notes

- [[10-00-Spark-Fundamentals-Index|Spark Fundamentals Course]]
- [[11-00-Spark-AWS-Master-Index|Advanced Spark on AWS]]
- [[10-Spark-Complete-MindMap|Spark Mind Map]]

---

## 📝 Notes

- All data is synthetic - no real customer information
- Spark UI available at `http://localhost:4040` when SparkSession is active
- Use `spark.stop()` to cleanly shutdown Spark sessions

---

**Good luck with your Techcombank onboarding! 🚀**
