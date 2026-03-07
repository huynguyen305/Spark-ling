"""
AWS Glue Python Shell Job: RDS PostgreSQL Data Seeder
=====================================================
Runs as a Glue Python Shell job to seed the PostgreSQL RDS instance
with realistic Vietnamese banking data (~3.38M rows).

LEARNING NOTES:
---------------
1. GLUE PYTHON SHELL vs GLUE ETL:
   - Python Shell: Pure Python, 1 DPU max, $0.44/hr per DPU
   - Glue ETL: PySpark, 2-100 DPUs, $0.44/hr per DPU
   - Use Python Shell for simple DB operations (like this seeder)
   - Use Glue ETL for Spark-based transformations

2. GLUE JOB ARGUMENTS:
   Accessed via awsglue.utils.getResolvedOptions(sys.argv, ['arg1'])
   All arguments must start with -- in the CloudFormation definition.

3. ADDITIONAL PYTHON MODULES:
   Set --additional-python-modules in DefaultArguments.
   Glue will pip install these at job start. We use:
   - psycopg2-binary: PostgreSQL driver
   - numpy: For realistic data distributions

4. LOGGING:
   print() statements go to CloudWatch Logs automatically.
   Log group: /aws-glue/python-jobs/output

USAGE:
   # Upload to S3 first:
   aws s3 cp aws/glue_lambda/rds_seed_glue.py s3://sparkling-data-test/scripts/rds_seed_glue.py

   # Run via AWS CLI:
   aws glue start-job-run --job-name sparkling-rds-seed --region ap-southeast-1

   # Monitor:
   aws glue get-job-run --job-name sparkling-rds-seed --run-id <run-id>
"""

import sys
import random
import string
from datetime import datetime, date, timedelta
from typing import Dict, List

import numpy as np
import psycopg2
from psycopg2.extras import execute_values

# ── Glue Job Arguments ──────────────────────────────────────────────────────
# LEARNING: getResolvedOptions parses Glue job arguments from sys.argv.
# Arguments are passed via CloudFormation DefaultArguments or --arguments
# in the start-job-run API call.
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, [
    "RDS_HOST", "RDS_PORT", "RDS_DATABASE", "RDS_USERNAME", "RDS_PASSWORD"
])

RDS_CONFIG = {
    "host": args["RDS_HOST"],
    "port": int(args["RDS_PORT"]),
    "database": args["RDS_DATABASE"],
    "username": args["RDS_USERNAME"],
    "password": args["RDS_PASSWORD"],
}


# ── Constants ────────────────────────────────────────────────────────────────

VIETNAMESE_CITIES = {
    "North":   ["Ha Noi", "Hai Phong", "Quang Ninh", "Nam Dinh", "Bac Ninh",
                "Thai Nguyen", "Vinh Phuc", "Hai Duong"],
    "Central": ["Da Nang", "Hue", "Nha Trang", "Quy Nhon", "Vinh",
                "Thanh Hoa", "Ha Tinh", "Quang Nam"],
    "South":   ["Ho Chi Minh", "Can Tho", "Bien Hoa", "Vung Tau",
                "Long An", "Binh Duong", "Da Lat", "Phu Quoc"],
}

SEGMENTS = ["Mass", "Affluent", "VIP", "Premium", "Corporate"]
SEGMENT_WEIGHTS = [0.50, 0.25, 0.10, 0.10, 0.05]

ACCOUNT_TYPES = [
    ("SAV", "Savings Account", "Deposit", 3.5, 500000),
    ("CHK", "Checking Account", "Deposit", 0.1, 0),
    ("FD",  "Fixed Deposit", "Deposit", 6.5, 10000000),
    ("CRD", "Credit Card", "Credit", 18.0, 0),
    ("LN",  "Personal Loan", "Credit", 12.0, 0),
    ("INV", "Investment Account", "Investment", 0.0, 50000000),
]

BRANCH_TYPES = ["Head Office", "Branch", "Branch", "Branch", "Sub-branch"]

TXN_TYPES = ["Deposit", "Withdrawal", "Transfer", "Payment", "Fee"]
TXN_CHANNELS = ["ATM", "Branch", "Mobile", "Internet", "POS"]
TXN_STATUSES = ["Completed", "Completed", "Completed", "Completed", "Failed"]

FIRST_NAMES = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Phan", "Vu", "Dang",
               "Bui", "Do", "Ngo", "Duong", "Ly", "Mai", "Truong"]
LAST_NAMES = ["Anh", "Binh", "Chi", "Dung", "Em", "Giang", "Hai", "Hung",
              "Khanh", "Lan", "Minh", "Nam", "Phuong", "Quang", "Son",
              "Thanh", "Tuan", "Uyen", "Van", "Xuan", "Yen"]


# ── Schema SQL ───────────────────────────────────────────────────────────────
# Inline schema so the Glue job is self-contained (no file dependencies).

SCHEMA_SQL = """
-- PostgreSQL schema for Spark-ling banking simulation
-- Dimensions + Facts + CDC tracking

CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,
    full_date       DATE NOT NULL,
    day_of_week     SMALLINT,
    day_name        VARCHAR(10),
    day_of_month    SMALLINT,
    day_of_year     SMALLINT,
    week_of_year    SMALLINT,
    month_number    SMALLINT,
    month_name      VARCHAR(10),
    quarter         SMALLINT,
    year            SMALLINT,
    is_weekend      BOOLEAN DEFAULT FALSE,
    is_holiday      BOOLEAN DEFAULT FALSE,
    fiscal_quarter  SMALLINT,
    fiscal_year     SMALLINT,
    last_modified   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_branch (
    branch_id       VARCHAR(10) PRIMARY KEY,
    branch_name     VARCHAR(100),
    city            VARCHAR(50),
    region          VARCHAR(20),
    branch_type     VARCHAR(20),
    opening_date    DATE,
    is_active       BOOLEAN DEFAULT TRUE,
    last_modified   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_account_type (
    account_type_code   VARCHAR(5) PRIMARY KEY,
    account_type_name   VARCHAR(50),
    category            VARCHAR(20),
    interest_rate       NUMERIC(5,2),
    min_balance         NUMERIC(15,2),
    last_modified       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key        INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id         VARCHAR(20) NOT NULL,
    full_name           VARCHAR(100),
    date_of_birth       DATE,
    email               VARCHAR(100),
    phone               VARCHAR(20),
    address             VARCHAR(200),
    city                VARCHAR(50),
    region              VARCHAR(20),
    segment             VARCHAR(20),
    registration_date   DATE,
    kyc_status          VARCHAR(20),
    risk_score          SMALLINT,
    effective_date      DATE NOT NULL,
    expiry_date         DATE NOT NULL DEFAULT '9999-12-31',
    is_current          BOOLEAN DEFAULT TRUE,
    last_modified       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_transaction (
    txn_key             INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    txn_id              VARCHAR(30) NOT NULL,
    customer_id         VARCHAR(20),
    branch_id           VARCHAR(10),
    account_type_code   VARCHAR(5),
    txn_date_key        INTEGER,
    txn_datetime        TIMESTAMP,
    txn_type            VARCHAR(20),
    amount              NUMERIC(18,2),
    currency            VARCHAR(3) DEFAULT 'VND',
    channel             VARCHAR(20),
    status              VARCHAR(20),
    description         VARCHAR(200),
    last_modified       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_daily_balance (
    balance_key         INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id         VARCHAR(20),
    account_type_code   VARCHAR(5),
    date_key            INTEGER,
    opening_balance     NUMERIC(18,2),
    closing_balance     NUMERIC(18,2),
    total_credits       NUMERIC(18,2),
    total_debits        NUMERIC(18,2),
    txn_count           INTEGER,
    last_modified       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cdc_watermark (
    table_name          VARCHAR(50) PRIMARY KEY,
    last_extracted      TIMESTAMP NOT NULL DEFAULT '2020-01-01 00:00:00',
    row_count           INTEGER DEFAULT 0,
    last_modified       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO cdc_watermark (table_name) VALUES
    ('dim_customer'), ('dim_branch'), ('fact_transaction'),
    ('fact_daily_balance'), ('dim_date')
ON CONFLICT DO NOTHING;
"""


# ── Data Generators ──────────────────────────────────────────────────────────

def generate_dates(start_year=2020, end_year=2026) -> List[Dict]:
    dates = []
    current = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    while current <= end:
        dates.append({
            "date_key": int(current.strftime("%Y%m%d")),
            "full_date": current,
            "day_of_week": current.isoweekday(),
            "day_name": current.strftime("%A"),
            "day_of_month": current.day,
            "day_of_year": current.timetuple().tm_yday,
            "week_of_year": current.isocalendar()[1],
            "month_number": current.month,
            "month_name": current.strftime("%B"),
            "quarter": (current.month - 1) // 3 + 1,
            "year": current.year,
            "is_weekend": current.isoweekday() >= 6,
            "is_holiday": False,
            "fiscal_quarter": (current.month - 1) // 3 + 1,
            "fiscal_year": current.year,
        })
        current += timedelta(days=1)
    return dates


def generate_branches(count=100) -> List[Dict]:
    branches = []
    for i in range(count):
        region = random.choice(list(VIETNAMESE_CITIES.keys()))
        city = random.choice(VIETNAMESE_CITIES[region])
        branch_type = random.choice(BRANCH_TYPES)
        branches.append({
            "branch_id": f"BR{i+1:03d}",
            "branch_name": f"{city} {branch_type} {i+1:03d}",
            "city": city,
            "region": region,
            "branch_type": branch_type,
            "opening_date": date(2010 + random.randint(0, 13),
                                 random.randint(1, 12), random.randint(1, 28)),
            "is_active": random.random() > 0.05,
        })
    return branches


def generate_customers(count=10000) -> List[Dict]:
    customers = []
    for i in range(count):
        region = random.choice(list(VIETNAMESE_CITIES.keys()))
        city = random.choice(VIETNAMESE_CITIES[region])
        segment = random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS)[0]
        reg_date = date(2020, 1, 1) + timedelta(days=random.randint(0, 1800))
        customers.append({
            "customer_id": f"CUST{i+1:06d}",
            "full_name": f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)} "
                         f"{random.choice(LAST_NAMES)}",
            "date_of_birth": date(1960 + random.randint(0, 45),
                                  random.randint(1, 12), random.randint(1, 28)),
            "email": f"cust{i+1:06d}@example.com",
            "phone": f"+84{random.randint(900000000, 999999999)}",
            "address": f"{random.randint(1, 999)} {random.choice(['Le Loi', 'Tran Hung Dao', 'Nguyen Hue', 'Hai Ba Trung', 'Dong Khoi'])}",
            "city": city,
            "region": region,
            "segment": segment,
            "registration_date": reg_date,
            "kyc_status": random.choice(["Verified", "Verified", "Verified", "Pending"]),
            "risk_score": int(max(0, min(100, np.random.normal(50, 15)))),
            "effective_date": reg_date,
            "expiry_date": date(9999, 12, 31),
            "is_current": True,
        })
    return customers


def generate_transactions(customers, start_date=date(2024, 1, 1),
                          end_date=date(2025, 12, 31)) -> List[Dict]:
    transactions = []
    current = start_date
    txn_counter = 0
    while current <= end_date:
        daily_count = random.randint(500, 900) if current.isoweekday() <= 5 else random.randint(100, 300)
        date_key = int(current.strftime("%Y%m%d"))
        for _ in range(daily_count):
            txn_counter += 1
            customer = random.choice(customers)
            segment = customer["segment"]
            if segment == "Corporate":
                amount = round(random.uniform(100000000, 5000000000), 2)
            elif segment in ("VIP", "Premium"):
                amount = round(random.uniform(10000000, 500000000), 2)
            elif segment == "Affluent":
                amount = round(random.uniform(1000000, 100000000), 2)
            else:
                amount = round(random.uniform(50000, 50000000), 2)
            hour = random.choices(range(24),
                weights=[1,1,1,1,1,2,5,8,10,10,9,8,8,9,10,10,8,7,5,3,2,1,1,1])[0]
            transactions.append({
                "txn_id": f"TXN{current.strftime('%Y%m%d')}{txn_counter:07d}",
                "customer_id": customer["customer_id"],
                "branch_id": f"BR{random.randint(1, 100):03d}",
                "account_type_code": random.choice(["SAV", "CHK", "FD", "CRD"]),
                "txn_date_key": date_key,
                "txn_datetime": datetime(current.year, current.month, current.day,
                                         hour, random.randint(0, 59), random.randint(0, 59)),
                "txn_type": random.choice(TXN_TYPES),
                "amount": amount,
                "currency": "VND",
                "channel": random.choice(TXN_CHANNELS),
                "status": random.choice(TXN_STATUSES),
                "description": f"Transaction {txn_counter}",
            })
        current += timedelta(days=1)
    return transactions


def generate_daily_balances(customers, start_date=date(2024, 1, 1),
                            end_date=date(2025, 12, 31)) -> List[Dict]:
    balances = []
    sampled = random.sample(customers, min(5000, len(customers)))
    current = start_date
    customer_balances = {
        c["customer_id"]: round(random.uniform(1000000, 500000000), 2)
        for c in sampled
    }
    while current <= end_date:
        if current.isoweekday() >= 6 and random.random() < 0.7:
            current += timedelta(days=1)
            continue
        date_key = int(current.strftime("%Y%m%d"))
        for cust in sampled:
            cid = cust["customer_id"]
            opening = customer_balances[cid]
            credits = round(random.uniform(0, opening * 0.1), 2)
            debits = round(random.uniform(0, opening * 0.08), 2)
            closing = round(opening + credits - debits, 2)
            balances.append({
                "customer_id": cid,
                "account_type_code": "SAV",
                "date_key": date_key,
                "opening_balance": opening,
                "closing_balance": closing,
                "total_credits": credits,
                "total_debits": debits,
                "txn_count": random.randint(0, 15),
            })
            customer_balances[cid] = closing
        current += timedelta(days=1)
    return balances


# ── Batch Insert ─────────────────────────────────────────────────────────────

def batch_insert(conn, table: str, data: List[Dict], batch_size: int = 5000):
    """Bulk insert using psycopg2.extras.execute_values (fast!)."""
    if not data:
        return 0
    columns = list(data[0].keys())
    col_str = ", ".join(columns)
    template = "(" + ", ".join([f"%({c})s" for c in columns]) + ")"
    cursor = conn.cursor()
    total = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        try:
            execute_values(
                cursor,
                f"INSERT INTO {table} ({col_str}) VALUES %s ON CONFLICT DO NOTHING",
                batch, template=template, page_size=batch_size,
            )
            conn.commit()
            total += len(batch)
            if (i // batch_size) % 20 == 0:
                print(f"   {table}: {total:,}/{len(data):,} rows")
        except Exception as e:
            conn.rollback()
            print(f"   ❌ {table} batch error: {e}")
    print(f"   ✅ {table}: {total:,} rows inserted")
    return total


# ── Main Job Logic ───────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("GLUE JOB: sparkling-rds-seed")
    print(f"Host: {RDS_CONFIG['host']}")
    print(f"Database: {RDS_CONFIG['database']}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 60)

    # Connect
    conn = psycopg2.connect(
        host=RDS_CONFIG["host"],
        port=RDS_CONFIG["port"],
        dbname=RDS_CONFIG["database"],
        user=RDS_CONFIG["username"],
        password=RDS_CONFIG["password"],
    )
    print("✅ Connected to PostgreSQL")

    try:
        # Create schema
        print("\n📋 Creating schema...")
        cursor = conn.cursor()
        cursor.execute(SCHEMA_SQL)
        conn.commit()
        print("   ✅ Schema created")

        # Seed dimensions
        print("\n📅 Seeding dim_date...")
        batch_insert(conn, "dim_date", generate_dates())

        print("\n🏦 Seeding dim_branch...")
        batch_insert(conn, "dim_branch", generate_branches(100))

        print("\n💳 Seeding dim_account_type...")
        cursor = conn.cursor()
        for at in ACCOUNT_TYPES:
            cursor.execute(
                "INSERT INTO dim_account_type "
                "(account_type_code, account_type_name, category, interest_rate, min_balance) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", at,
            )
        conn.commit()
        print("   ✅ dim_account_type: 6 rows")

        print("\n👤 Seeding dim_customer...")
        customers = generate_customers(10000)
        batch_insert(conn, "dim_customer", customers)

        print("\n💰 Seeding fact_transaction (this takes ~2-3 min)...")
        transactions = generate_transactions(customers)
        batch_insert(conn, "fact_transaction", transactions)

        print("\n📊 Seeding fact_daily_balance (this takes ~5-8 min)...")
        balances = generate_daily_balances(customers)
        batch_insert(conn, "fact_daily_balance", balances)

        # Validate
        print("\n" + "=" * 60)
        print("📊 Final validation:")
        tables = ["dim_date", "dim_branch", "dim_account_type", "dim_customer",
                  "fact_transaction", "fact_daily_balance", "cdc_watermark"]
        cursor = conn.cursor()
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"   {'✅' if count > 0 else '⚠️ '} {table}: {count:,} rows")
        print("=" * 60)
        print("\n✅ GLUE SEED JOB COMPLETE!")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
