"""
AWS Lambda Handler: Daily Banking Activity Simulator
====================================================
Simulates one day of banking activity in the PostgreSQL RDS instance.
Triggered by EventBridge on a cron schedule (weekdays at 8AM ICT).

LEARNING NOTES:
---------------
1. LAMBDA HANDLER SIGNATURE:
   handler(event, context) — always these two parameters.
   - event: JSON payload from the trigger (EventBridge, API Gateway, etc.)
   - context: Lambda runtime info (function name, memory, time remaining)

2. LAMBDA ENVIRONMENT VARIABLES:
   Access via os.environ['KEY']. Set in CloudFormation or console.
   For production: use Secrets Manager with the Lambda extension layer.

3. LAMBDA TIMEOUT:
   Max 15 minutes (900 seconds). Our job takes ~20-30 seconds.
   If you hit timeout, increase MemorySize (more memory = more CPU).

4. LAMBDA COLD STARTS:
   First invocation after idle period takes longer (~1-3s extra).
   psycopg2 import + DB connection are the main cold start costs.
   Keep Lambda warm with provisioned concurrency (production only).

6. SCALING TO MILLIONS (2M+ ROWS):
   - Memory Management: 2M dicts in a list can consume >1.5GB RAM. Ensure 
     MemorySize is high (e.g., 3072MB).
   - Batch Size: execute_values is efficient, but the database's 
     max_connections and max_allowed_packet must handle the load.
   - Generator vs List: For 3M+ rows, consider using a generator to 
     yield records and insert in chunks to keep memory usage flat.
"""

import json
import os
import random
from datetime import datetime, date, timedelta
from typing import Dict, List

import psycopg2
from psycopg2.extras import execute_values


# ── Configuration ────────────────────────────────────────────────────────────

def get_config() -> dict:
    """
    Read RDS config from Lambda environment variables.

    LEARNING: Lambda environment variables are set in CloudFormation
    or the Lambda console. They're injected into the container at startup.
    For secrets, production systems use Secrets Manager + Lambda extension.
    """
    return {
        "host": os.environ["RDS_HOST"],
        "port": int(os.environ.get("RDS_PORT", "5432")),
        "database": os.environ.get("RDS_DATABASE", "sparkdb"),
        "username": os.environ.get("RDS_USERNAME", "sparkadmin"),
        "password": os.environ["RDS_PASSWORD"],
    }


def get_connection(config: dict):
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["database"],
        user=config["username"],
        password=config["password"],
    )


# ── Constants ────────────────────────────────────────────────────────────────

VIETNAMESE_CITIES = {
    "North":   ["Ha Noi", "Hai Phong", "Quang Ninh", "Nam Dinh"],
    "Central": ["Da Nang", "Hue", "Nha Trang", "Quy Nhon"],
    "South":   ["Ho Chi Minh", "Can Tho", "Bien Hoa", "Vung Tau"],
}

SEGMENTS = ["Mass", "Affluent", "VIP", "Premium", "Corporate"]
TXN_TYPES = ["Deposit", "Withdrawal", "Transfer", "Payment", "Fee"]
TXN_CHANNELS = ["ATM", "Branch", "Mobile", "Internet", "POS"]
TXN_STATUSES = ["Completed", "Completed", "Completed", "Completed", "Failed"]
FIRST_NAMES = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Phan", "Vu", "Dang"]
LAST_NAMES = ["Anh", "Binh", "Chi", "Dung", "Giang", "Hai", "Hung", "Khanh",
              "Lan", "Minh", "Nam", "Phuong", "Quang", "Son", "Thanh"]


# ── Data Generators ─────────────────────────────────────────────────────────

def generate_daily_transactions(conn, sim_date: date) -> List[Dict]:
    """Generate ~5,000 transactions for a given day."""
    cursor = conn.cursor()
    cursor.execute("SELECT customer_id, segment FROM dim_customer WHERE is_current = TRUE")
    customers = [{"customer_id": r[0], "segment": r[1]} for r in cursor.fetchall()]

    if not customers:
        return []

    is_weekday = sim_date.isoweekday() <= 5
    daily_count = random.randint(2000000, 3000000) if is_weekday else random.randint(800000, 1500000)
    date_key = int(sim_date.strftime("%Y%m%d"))

    # Get the next txn counter
    cursor.execute("SELECT COALESCE(MAX(txn_key), 0) FROM fact_transaction")
    base_counter = cursor.fetchone()[0]

    transactions = []
    for i in range(daily_count):
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
            "txn_id": f"TXN{sim_date.strftime('%Y%m%d')}{base_counter + i + 1:07d}",
            "customer_id": customer["customer_id"],
            "branch_id": f"BR{random.randint(1, 100):03d}",
            "account_type_code": random.choice(["SAV", "CHK", "FD", "CRD"]),
            "txn_date_key": date_key,
            "txn_datetime": datetime(sim_date.year, sim_date.month, sim_date.day,
                                     hour, random.randint(0, 59), random.randint(0, 59)),
            "txn_type": random.choice(TXN_TYPES),
            "amount": amount,
            "currency": "VND",
            "channel": random.choice(TXN_CHANNELS),
            "status": random.choice(TXN_STATUSES),
            "description": f"Lambda-generated txn {sim_date}",
        })

    return transactions


def update_customers(conn, sim_date: date, count: int = 200) -> int:
    """
    Simulate ~200 customer attribute changes (SCD Type 2 updates).

    LEARNING: SCD Type 2 means we:
    1. Expire the current record (is_current=FALSE, expiry_date=today)
    2. Insert a new record with updated values (is_current=TRUE)
    This preserves the full history of changes.
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT customer_id, full_name, city, region, segment, risk_score "
        "FROM dim_customer WHERE is_current = TRUE "
        "ORDER BY RANDOM() LIMIT %s", (count,)
    )
    customers = cursor.fetchall()

    updated = 0
    for cust in customers:
        cid, name, city, region, segment, risk = cust

        # Random changes
        change_type = random.choice(["address", "segment", "kyc", "risk"])
        new_values = {
            "customer_id": cid,
            "full_name": name,
            "city": city,
            "region": region,
            "segment": segment,
            "risk_score": risk,
        }

        if change_type == "address":
            new_region = random.choice(list(VIETNAMESE_CITIES.keys()))
            new_values["city"] = random.choice(VIETNAMESE_CITIES[new_region])
            new_values["region"] = new_region
        elif change_type == "segment":
            new_values["segment"] = random.choice(SEGMENTS)
        elif change_type == "risk":
            new_values["risk_score"] = int(max(0, min(100, risk + random.randint(-10, 10))))

        # SCD Type 2: expire old, insert new
        try:
            cursor.execute(
                "UPDATE dim_customer SET is_current = FALSE, "
                "expiry_date = %s, last_modified = CURRENT_TIMESTAMP "
                "WHERE customer_id = %s AND is_current = TRUE",
                (sim_date, cid)
            )
            cursor.execute(
                "INSERT INTO dim_customer "
                "(customer_id, full_name, date_of_birth, email, phone, address, "
                "city, region, segment, registration_date, kyc_status, risk_score, "
                "effective_date, expiry_date, is_current) "
                "SELECT customer_id, %s, date_of_birth, email, phone, address, "
                "%s, %s, %s, registration_date, kyc_status, %s, "
                "%s, '9999-12-31', TRUE "
                "FROM dim_customer WHERE customer_id = %s AND expiry_date = %s",
                (new_values["full_name"], new_values["city"], new_values["region"],
                 new_values["segment"], new_values["risk_score"],
                 sim_date, cid, sim_date)
            )
            updated += 1
        except Exception as e:
            conn.rollback()
            print(f"Error updating {cid}: {e}")
            continue

    conn.commit()
    return updated


def batch_insert(conn, table: str, data: List[Dict], batch_size: int = 5000) -> int:
    """Bulk insert using psycopg2 execute_values."""
    if not data:
        return 0
    columns = list(data[0].keys())
    col_str = ", ".join(columns)
    template = "(" + ", ".join([f"%({c})s" for c in columns]) + ")"
    cursor = conn.cursor()
    total = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        execute_values(
            cursor,
            f"INSERT INTO {table} ({col_str}) VALUES %s ON CONFLICT DO NOTHING",
            batch, template=template, page_size=batch_size,
        )
        conn.commit()
        total += len(batch)
    return total


def update_cdc_watermarks(conn, sim_date: date):
    """Update CDC watermark table so the incremental pipeline picks up new data."""
    cursor = conn.cursor()
    tables = ["dim_customer", "fact_transaction", "fact_daily_balance"]
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        cursor.execute(
            "UPDATE cdc_watermark SET last_watermark = %s, last_row_count = %s, "
            "last_run_status = 'COMPLETED', updated_at = CURRENT_TIMESTAMP "
            "WHERE table_name = %s",
            (datetime.combine(sim_date, datetime.min.time()), count, table)
        )
    conn.commit()


# ── Lambda Handler ───────────────────────────────────────────────────────────

def lambda_handler(event, context):
    """
    Main Lambda entry point.

    LEARNING: The handler is the function Lambda calls when triggered.
    - event: dict with trigger payload (EventBridge sends scheduled event info)
    - context: LambdaContext with function metadata, time remaining, etc.
    - Return value becomes the Lambda response (visible in test console)
    """
    print("=" * 60)
    print("LAMBDA: sparkling-daily-generator")
    print(f"Event: {json.dumps(event)}")
    print("=" * 60)

    # Parse simulation date from event or default to today
    sim_date_str = event.get("simulate_date")
    if sim_date_str:
        sim_date = date.fromisoformat(sim_date_str)
    else:
        sim_date = date.today()

    print(f"📅 Simulating date: {sim_date}")

    config = get_config()
    conn = get_connection(config)

    try:
        # 1. Generate transactions
        print("\n💰 Generating transactions...")
        transactions = generate_daily_transactions(conn, sim_date)
        txn_count = batch_insert(conn, "fact_transaction", transactions)
        print(f"   ✅ {txn_count:,} transactions inserted")

        # 2. Update customers (SCD Type 2)
        print("\n👤 Updating customer attributes...")
        cust_updates = update_customers(conn, sim_date, count=200)
        print(f"   ✅ {cust_updates} customer updates (SCD Type 2)")

        # 3. Update CDC watermarks
        print("\n📌 Updating CDC watermarks...")
        update_cdc_watermarks(conn, sim_date)
        print("   ✅ Watermarks updated")

        # Summary
        result = {
            "status": "success",
            "simulate_date": str(sim_date),
            "transactions_inserted": txn_count,
            "customer_updates": cust_updates,
            "timestamp": datetime.now().isoformat(),
        }
        print(f"\n✅ Daily simulation complete: {json.dumps(result)}")
        return result

    except Exception as e:
        error_result = {
            "status": "error",
            "simulate_date": str(sim_date),
            "error": str(e),
            "timestamp": datetime.now().isoformat(),
        }
        print(f"❌ Error: {e}")
        return error_result

    finally:
        conn.close()
