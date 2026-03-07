"""
RDS PostgreSQL Data Seeder — Initial Population
=================================================
Seeds the PostgreSQL RDS instance with realistic Vietnamese banking data
for the migration simulation.

LEARNING NOTES:
---------------
1. CONNECTION LIBRARY:
   Oracle: oracledb (or cx_Oracle)
   PostgreSQL: psycopg2 (most popular) or asyncpg (async)

   psycopg2 is the de-facto standard PostgreSQL adapter for Python.
   Install with: pip install psycopg2-binary

2. BATCH INSERT STRATEGY:
   Oracle: cursor.executemany(sql, data)
   PostgreSQL: psycopg2.extras.execute_values(cursor, sql, data)

   execute_values() is 10-100x faster than executemany() for PostgreSQL
   because it sends multiple rows in a single INSERT statement:
   INSERT INTO t (a,b) VALUES (1,'x'),(2,'y'),(3,'z')

3. PARAMETERIZED QUERIES:
   Oracle: :param_name  (named parameters)
   PostgreSQL: %s  (positional parameters) or %(name)s (named)

4. DATA VOLUME:
   ~10,000 customers, 500,000+ transactions, 150,000 balance snapshots
   This takes ~60-120 seconds on db.t3.micro.

USAGE:
    python aws/rds/rds_seed_data.py
    python aws/rds/rds_seed_data.py --validate-only
"""

import argparse
import os
import sys
import random
import string
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List

import numpy as np

# Add project root to path
# Path: aws/rds/rds_seed_data.py → .parent = aws/rds/ → .parent = aws/ → .parent = Spark-ling/
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# ── Constants (reused from data_generator.py where possible) ────────────────

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


# ── Configuration ────────────────────────────────────────────────────────────

def load_config() -> dict:
    """Load RDS configuration from environment or .env file."""
    env_file = PROJECT_ROOT / "aws" / ".env"
    config = {}
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    config[key.strip()] = value.strip()

    return {
        "host": os.environ.get("RDS_HOST", config.get("RDS_HOST", "")),
        "port": int(os.environ.get("RDS_PORT", config.get("RDS_PORT", "5432"))),
        "database": os.environ.get("RDS_DATABASE", config.get("RDS_DATABASE", "sparkdb")),
        "username": os.environ.get("RDS_USERNAME", config.get("RDS_USERNAME", "sparkadmin")),
        "password": os.environ.get("RDS_PASSWORD", config.get("RDS_PASSWORD", "")),
    }


def get_connection(config: dict):
    """
    Create a PostgreSQL connection using psycopg2.

    LEARNING: psycopg2 connection vs Oracle:
      Oracle:      oracledb.connect(user=..., password=..., dsn="host:port/dbname")
      PostgreSQL:  psycopg2.connect(host=..., port=..., dbname=..., user=..., password=...)
    """
    import psycopg2
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["database"],
        user=config["username"],
        password=config["password"],
    )


# ── Data Generators ─────────────────────────────────────────────────────────

def generate_dates(start_year=2020, end_year=2026) -> List[Dict]:
    """Generate date dimension records."""
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
    """Generate bank branch records across Vietnam."""
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
    """Generate customer dimension records with SCD Type 2 initial state."""
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


def generate_transactions(customers: List[Dict],
                           start_date=date(2024, 1, 1),
                           end_date=date(2025, 12, 31)) -> List[Dict]:
    """Generate transaction fact records."""
    transactions = []
    current = start_date
    txn_counter = 0

    while current <= end_date:
        # Weekday = more transactions
        daily_count = random.randint(500, 900) if current.isoweekday() <= 5 else random.randint(100, 300)
        date_key = int(current.strftime("%Y%m%d"))

        for _ in range(daily_count):
            txn_counter += 1
            customer = random.choice(customers)

            # Amount varies by segment
            segment = customer["segment"]
            if segment == "Corporate":
                amount = round(random.uniform(100000000, 5000000000), 2)
            elif segment in ("VIP", "Premium"):
                amount = round(random.uniform(10000000, 500000000), 2)
            elif segment == "Affluent":
                amount = round(random.uniform(1000000, 100000000), 2)
            else:
                amount = round(random.uniform(50000, 50000000), 2)

            hour = random.choices(
                range(24),
                weights=[1,1,1,1,1,2,5,8,10,10,9,8,8,9,10,10,8,7,5,3,2,1,1,1]
            )[0]

            transactions.append({
                "txn_id": f"TXN{current.strftime('%Y%m%d')}{txn_counter:07d}",
                "customer_id": customer["customer_id"],
                "branch_id": f"BR{random.randint(1, 100):03d}",
                "account_type_code": random.choice(["SAV", "CHK", "FD", "CRD"]),
                "txn_date_key": date_key,
                "txn_datetime": datetime(current.year, current.month, current.day,
                                         hour, random.randint(0, 59),
                                         random.randint(0, 59)),
                "txn_type": random.choice(TXN_TYPES),
                "amount": amount,
                "currency": "VND",
                "channel": random.choice(TXN_CHANNELS),
                "status": random.choice(TXN_STATUSES),
                "description": f"Transaction {txn_counter}",
            })

        current += timedelta(days=1)

    return transactions


def generate_daily_balances(customers: List[Dict],
                              start_date=date(2024, 1, 1),
                              end_date=date(2025, 12, 31)) -> List[Dict]:
    """Generate daily balance snapshot records."""
    balances = []
    # Sample ~5000 customers for balance tracking
    sampled = random.sample(customers, min(5000, len(customers)))
    current = start_date

    # Initialize balances
    customer_balances = {
        c["customer_id"]: round(random.uniform(1000000, 500000000), 2)
        for c in sampled
    }

    while current <= end_date:
        # Skip some weekends for realism
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
            txn_count = random.randint(0, 15)

            balances.append({
                "customer_id": cid,
                "account_type_code": "SAV",
                "date_key": date_key,
                "opening_balance": opening,
                "closing_balance": closing,
                "total_credits": credits,
                "total_debits": debits,
                "txn_count": txn_count,
            })

            customer_balances[cid] = closing

        current += timedelta(days=1)

    return balances


# ── Database Operations ─────────────────────────────────────────────────────

def create_schema(conn):
    """Execute the schema SQL file to create tables."""
    schema_path = Path(__file__).parent / "rds_schema.sql"
    if schema_path.exists():
        with open(schema_path) as f:
            sql = f.read()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        print("   ✅ Schema created/verified")
    else:
        print("   ⚠️  Schema file not found, assuming tables exist")


def batch_insert(conn, table: str, data: List[Dict], batch_size: int = 5000):
    """
    Batch insert using psycopg2.extras.execute_values (fast!).

    LEARNING: execute_values() vs executemany():
    - executemany: sends N individual INSERT statements
    - execute_values: sends 1 INSERT with N value tuples
    - execute_values is 10-100x faster for bulk loading

    PostgreSQL syntax: INSERT INTO t (a,b) VALUES %s
    The %s is replaced by execute_values with: (v1,v2),(v3,v4),...
    """
    from psycopg2.extras import execute_values

    if not data:
        return

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
                batch,
                template=template,
                page_size=batch_size,
            )
            conn.commit()
            total += len(batch)
            print(f"      Inserted batch {i//batch_size + 1}: "
                  f"{total:,}/{len(data):,} rows")
        except Exception as e:
            conn.rollback()
            print(f"      ❌ Batch error: {e}")
            # Try row-by-row for remaining
            for row in batch:
                try:
                    cursor.execute(
                        f"INSERT INTO {table} ({col_str}) VALUES "
                        f"({', '.join(['%(' + c + ')s' for c in columns])}) "
                        f"ON CONFLICT DO NOTHING",
                        row,
                    )
                    conn.commit()
                    total += 1
                except Exception as row_e:
                    conn.rollback()

    return total


def validate_tables(conn) -> dict:
    """Check row counts for all tables."""
    cursor = conn.cursor()
    tables = ["dim_date", "dim_branch", "dim_account_type", "dim_customer",
              "fact_transaction", "fact_daily_balance", "cdc_watermark"]

    counts = {}
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            counts[table] = count
            print(f"   {'✅' if count > 0 else '⚠️ '} {table}: {count:,} rows")
        except Exception as e:
            counts[table] = 0
            print(f"   ❌ {table}: {e}")
            conn.rollback()

    return counts


# ── Main ────────────────────────────────────────────────────────────────────

def run_seed(validate_only: bool = False):
    """Execute the full data seeding process."""
    config = load_config()

    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  RDS PostgreSQL Data Seeder                                ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print(f"  Host: {config['host']}")
    print(f"  Database: {config['database']}")
    print(f"  Timestamp: {datetime.now().isoformat()}")

    if not config["host"]:
        print("\n❌ RDS_HOST not set. Add it to aws/.env or environment.")
        print("   Run: aws cloudformation describe-stacks "
              "--stack-name sparkling-rds-postgres "
              "--query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' "
              "--output table")
        return

    conn = get_connection(config)
    print("   ✅ Connected to PostgreSQL")

    try:
        if validate_only:
            print("\n📊 Table validation:")
            validate_tables(conn)
            return

        # Step 1: Create schema
        print("\n📋 Step 1: Creating schema...")
        create_schema(conn)

        # Step 2: Seed dimension tables
        print("\n📅 Step 2: Seeding DIM_DATE...")
        dates = generate_dates()
        batch_insert(conn, "dim_date", dates)
        print(f"   ✅ {len(dates):,} date records")

        print("\n🏦 Step 3: Seeding DIM_BRANCH...")
        branches = generate_branches(100)
        batch_insert(conn, "dim_branch", branches)
        print(f"   ✅ {len(branches):,} branch records")

        print("\n💳 Step 4: Seeding DIM_ACCOUNT_TYPE...")
        cursor = conn.cursor()
        for at in ACCOUNT_TYPES:
            cursor.execute(
                "INSERT INTO dim_account_type "
                "(account_type_code, account_type_name, category, interest_rate, min_balance) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                at,
            )
        conn.commit()
        print(f"   ✅ {len(ACCOUNT_TYPES)} account type records")

        print("\n👤 Step 5: Seeding DIM_CUSTOMER...")
        customers = generate_customers(10000)
        batch_insert(conn, "dim_customer", customers)
        print(f"   ✅ {len(customers):,} customer records")

        print("\n💰 Step 6: Seeding FACT_TRANSACTION (this takes ~60s)...")
        transactions = generate_transactions(customers)
        batch_insert(conn, "fact_transaction", transactions)
        print(f"   ✅ {len(transactions):,} transaction records")

        print("\n📊 Step 7: Seeding FACT_DAILY_BALANCE...")
        balances = generate_daily_balances(customers)
        batch_insert(conn, "fact_daily_balance", balances)
        print(f"   ✅ {len(balances):,} balance records")

        # Validate
        print("\n" + "=" * 60)
        print("📊 Final validation:")
        validate_tables(conn)
        print("=" * 60)
        print("\n✅ Data seeding complete!")

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Seed RDS PostgreSQL with banking data",
        epilog="""
LEARNING — PostgreSQL vs Oracle:
  Connection: psycopg2.connect() vs oracledb.connect()
  Batch Insert: execute_values() vs executemany()
  Parameters: %%s (positional) vs :name (named)
  Auto-increment: SERIAL / IDENTITY vs SEQUENCE
        """
    )
    parser.add_argument("--validate-only", action="store_true",
                        help="Only check table counts, don't seed")

    args = parser.parse_args()
    run_seed(validate_only=args.validate_only)


if __name__ == "__main__":
    main()
