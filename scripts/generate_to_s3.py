"""
Databricks Data Generator → S3
================================
Generates synthetic banking data on Databricks serverless compute
and writes Parquet directly to S3.

Usage (in Databricks notebook):
    %run ./scripts/generate_to_s3

Or from terminal:
    databricks jobs submit --json '{"run_name":"gen-data","tasks":[...]}'
"""

# ── Config ─────────────────────────────────────────────────
S3_BUCKET = "sparkling-data-test"
S3_RAW_PATH = f"s3a://{S3_BUCKET}/data/raw"
OUTPUT_FORMAT = "parquet"      # parquet is optimal for Spark reads

NUM_CUSTOMERS = 10_000
NUM_BRANCHES = 100
NUM_TRANSACTIONS = 5_000_000

# ── Get Spark session (pre-created on Databricks) ──────────
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
import numpy as np

spark = SparkSession.builder.getOrCreate()

print("╔═══════════════════════════════════════════╗")
print("║  🏦 Generate Banking Data → S3            ║")
print(f"║  Target: {S3_RAW_PATH}")
print("╚═══════════════════════════════════════════╝")

# Set seeds
random.seed(42)
np.random.seed(42)

# ── Lookup data ─────────────────────────────────────────────
CUSTOMER_SEGMENTS = ["Mass", "Mass Affluent", "Affluent", "HNW", "UHNW"]
SEGMENT_WEIGHTS = [0.5, 0.25, 0.15, 0.07, 0.03]
ACCOUNT_TYPES = ["Checking", "Savings", "Term Deposit", "Investment", "Credit"]
ACCOUNT_STATUSES = ["Active", "Dormant", "Closed", "Frozen"]
TXN_TYPES = ["Deposit", "Withdrawal", "Transfer In", "Transfer Out", "Payment", "Fee", "Interest"]
TXN_CHANNELS = ["Branch", "ATM", "Mobile App", "Internet Banking", "POS", "API"]
MERCHANT_CATEGORIES = ["Retail", "F&B", "Travel", "Utilities", "Entertainment", "Healthcare", "Education", "Others"]
REGIONS = ["Hanoi", "Ho Chi Minh", "Da Nang", "Hai Phong", "Can Tho", "Binh Duong", "Dong Nai", "Quang Ninh"]
KYC_STATUSES = ["Verified", "Pending", "Expired", "Rejected"]


# ═══════════════════════════════════════════════════════════
# 1. BRANCHES
# ═══════════════════════════════════════════════════════════
print("\n📊 Generating branches...")
branches_data = []
for i in range(1, NUM_BRANCHES + 1):
    region = random.choice(REGIONS)
    branches_data.append(Row(
        branch_id=f"BR{i:06d}",
        branch_name=f"{region} Branch {i:03d}",
        region=region,
        city=region,
        address=f"{random.randint(1, 999)} Street {random.randint(1, 50)}, {region}",
        opened_date=f"{random.randint(2010, 2023)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
    ))

df_branches = spark.createDataFrame(branches_data)
df_branches.write.mode("overwrite").parquet(f"{S3_RAW_PATH}/branches")
print(f"   ✅ {df_branches.count()} branches → {S3_RAW_PATH}/branches")


# ═══════════════════════════════════════════════════════════
# 2. CUSTOMERS
# ═══════════════════════════════════════════════════════════
print("\n👥 Generating customers...")
customers_data = []
for i in range(1, NUM_CUSTOMERS + 1):
    segment = random.choices(CUSTOMER_SEGMENTS, weights=SEGMENT_WEIGHTS)[0]
    customers_data.append(Row(
        customer_id=f"CUST{i:06d}",
        name=f"Customer {i:05d}",
        email=f"customer{i}@example.com",
        phone=f"09{random.randint(10000000, 99999999)}",
        segment=segment,
        registration_date=f"{random.randint(2015, 2024)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        kyc_status=random.choices(KYC_STATUSES, weights=[0.85, 0.08, 0.05, 0.02])[0],
        date_of_birth=f"{random.randint(1960, 2000)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        gender=random.choice(["M", "F"]),
        nationality="Vietnamese"
    ))

df_customers = spark.createDataFrame(customers_data)
df_customers.write.mode("overwrite").parquet(f"{S3_RAW_PATH}/customers")
print(f"   ✅ {df_customers.count()} customers → {S3_RAW_PATH}/customers")


# ═══════════════════════════════════════════════════════════
# 3. ACCOUNTS
# ═══════════════════════════════════════════════════════════
print("\n💳 Generating accounts...")

# Build customer → segment mapping for balance ranges
customer_segments = {c.customer_id: c.segment for c in customers_data}
branch_ids = [b.branch_id for b in branches_data]

BALANCE_RANGES = {
    "UHNW":          (5_000_000_000, 50_000_000_000),
    "HNW":           (500_000_000,   5_000_000_000),
    "Affluent":      (100_000_000,   500_000_000),
    "Mass Affluent": (20_000_000,    100_000_000),
    "Mass":          (100_000,       20_000_000),
}

accounts_data = []
acct_num = 1
for cust_id, segment in customer_segments.items():
    num_accounts = random.choices([1, 2, 3], weights=[0.5, 0.35, 0.15])[0]
    lo, hi = BALANCE_RANGES[segment]

    for _ in range(num_accounts):
        balance = round(random.uniform(lo, hi), 2)
        accounts_data.append(Row(
            account_id=f"ACCT{acct_num:06d}",
            customer_id=cust_id,
            branch_id=random.choice(branch_ids),
            account_type=random.choice(ACCOUNT_TYPES),
            balance=balance,
            currency="VND",
            status=random.choices(ACCOUNT_STATUSES, weights=[0.85, 0.08, 0.05, 0.02])[0],
            opened_date=f"{random.randint(2018, 2024)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            last_activity_date=f"{random.randint(2024, 2025)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
        ))
        acct_num += 1

df_accounts = spark.createDataFrame(accounts_data)
df_accounts.write.mode("overwrite").parquet(f"{S3_RAW_PATH}/accounts")
print(f"   ✅ {df_accounts.count()} accounts → {S3_RAW_PATH}/accounts")


# ═══════════════════════════════════════════════════════════
# 4. TRANSACTIONS  (vectorized with NumPy, parallelized via Spark)
# ═══════════════════════════════════════════════════════════
print(f"\n💰 Generating {NUM_TRANSACTIONS:,} transactions...")

# Pre-compute account data
account_ids = np.array([a.account_id for a in accounts_data])
account_balances = np.array([a.balance for a in accounts_data])
account_weights = account_balances / account_balances.sum()

# Generate all random indices at once
print("   Generating random values...")
acct_indices = np.random.choice(len(accounts_data), size=NUM_TRANSACTIONS, p=account_weights)
txn_type_idx = np.random.randint(0, len(TXN_TYPES), size=NUM_TRANSACTIONS)
channel_idx = np.random.randint(0, len(TXN_CHANNELS), size=NUM_TRANSACTIONS)
merch_idx = np.random.randint(0, len(MERCHANT_CATEGORIES), size=NUM_TRANSACTIONS)
status_idx = np.random.choice(4, size=NUM_TRANSACTIONS, p=[0.92, 0.04, 0.02, 0.02])
ref_nums = np.random.randint(100000000, 999999999, size=NUM_TRANSACTIONS)
base_amounts = np.random.uniform(50000, 100000000, size=NUM_TRANSACTIONS)
fee_amounts = np.random.uniform(10000, 100000, size=NUM_TRANSACTIONS)
interest_mult = np.random.uniform(0.001, 0.01, size=NUM_TRANSACTIONS)

from datetime import datetime, timedelta
start_ts = datetime(2025, 1, 1).timestamp()
end_ts = datetime(2025, 12, 31, 23, 59, 59).timestamp()
random_ts = np.random.uniform(start_ts, end_ts, size=NUM_TRANSACTIONS)

STATUSES = ["Completed", "Pending", "Failed", "Reversed"]

print("   Building transaction records...")
BATCH = 500_000
all_rows = []

for batch_start in range(0, NUM_TRANSACTIONS, BATCH):
    batch_end = min(batch_start + BATCH, NUM_TRANSACTIONS)
    batch_rows = []

    for i in range(batch_start, batch_end):
        idx = acct_indices[i]
        txn_type = TXN_TYPES[txn_type_idx[i]]
        balance = account_balances[idx]

        if txn_type == "Fee":
            amount = round(float(fee_amounts[i]), 2)
        elif txn_type == "Interest":
            amount = round(float(balance * interest_mult[i]), 2)
        else:
            max_amt = min(balance * 0.1, 100000000)
            amount = round(float(min(base_amounts[i], max(50001, max_amt))), 2)

        dt = datetime.fromtimestamp(float(random_ts[i]))

        batch_rows.append(Row(
            txn_id=f"TXN{i+1:07d}",
            account_id=str(account_ids[idx]),
            txn_datetime=dt.strftime("%Y-%m-%d %H:%M:%S"),
            txn_type=txn_type,
            amount=amount,
            currency="VND",
            channel=TXN_CHANNELS[channel_idx[i]],
            merchant_category=MERCHANT_CATEGORIES[merch_idx[i]] if txn_type == "Payment" else None,
            status=STATUSES[status_idx[i]],
            reference=f"REF{ref_nums[i]}",
            description=f"{txn_type} transaction"
        ))

    all_rows.extend(batch_rows)
    print(f"   Processed {batch_end:,}/{NUM_TRANSACTIONS:,}")

# Create DataFrame and write — Spark handles parallelized Parquet writing
df_txn = spark.createDataFrame(all_rows)
df_txn = df_txn.repartition(16)  # Good parallelism for 5M rows
df_txn.write.mode("overwrite").parquet(f"{S3_RAW_PATH}/transactions")
print(f"   ✅ {NUM_TRANSACTIONS:,} transactions → {S3_RAW_PATH}/transactions")


# ═══════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════
print("\n" + "=" * 55)
print("✅ Data generation complete!")
print(f"📁 Data written to: {S3_RAW_PATH}")
print("=" * 55)

# Verify by listing files
print("\n📋 Verification:")
for table in ["branches", "customers", "accounts", "transactions"]:
    path = f"{S3_RAW_PATH}/{table}"
    df_check = spark.read.parquet(path)
    print(f"   {table:15s}: {df_check.count():>12,} rows, {len(df_check.columns)} cols")

print("\n🎉 Ready for analytics! Use mode='databricks' in spark_config.")
