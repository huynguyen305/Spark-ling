"""
RDS PostgreSQL Daily Activity Simulator
=========================================
Simulates one day of banking operations by generating new transactions,
customer attribute changes (SCD Type 2), and balance snapshots.

LEARNING NOTES:
---------------
1. This script runs AFTER the initial seed (rds_seed_data.py) to simulate
   daily operations. The incremental pipeline then picks up these changes.

2. SCD TYPE 2 UPDATE PROCESS:
   When a customer attribute changes (e.g., address update):
   a) Set the OLD row: is_current=FALSE, expiry_date=today
   b) INSERT a NEW row: is_current=TRUE, effective_date=today, expiry_date=9999-12-31
   This preserves the complete history of changes.

3. CDC DETECTION:
   All changed rows get last_modified = CURRENT_TIMESTAMP.
   The incremental pipeline uses WHERE last_modified > :watermark
   to find these changes.

USAGE:
    # Simulate one day:
    python aws/rds/rds_daily_generator.py --simulate-date 2026-03-07

    # Simulate a date range:
    python aws/rds/rds_daily_generator.py --start-date 2026-03-01 --end-date 2026-03-07

    # Dry run (preview without database changes):
    python aws/rds/rds_daily_generator.py --simulate-date 2026-03-07 --dry-run
"""

import argparse
import os
import random
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np

# Path: aws/rds/rds_daily_generator.py → .parent = aws/rds/ → .parent = aws/ → .parent = Spark-ling/
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Reuse constants from seed script
from rds_seed_data import (
    load_config, get_connection, VIETNAMESE_CITIES,
    TXN_TYPES, TXN_CHANNELS, TXN_STATUSES, SEGMENTS
)


# ── Daily Transaction Generator ─────────────────────────────────────────────

def generate_daily_transactions(conn, simulate_date: date) -> List[Dict]:
    """
    Generate realistic daily transactions.

    LEARNING: Transaction volume patterns:
    - Weekdays: 3,000–7,000 transactions
    - Weekends: 500–2,000 transactions
    - Peak hours: 9am–11am and 2pm–4pm
    - Low hours: midnight–6am
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT a.account_id, a.customer_id, a.branch_id, a.account_type_code, c.segment "
        "FROM dim_account a JOIN dim_customer c ON a.customer_id = c.customer_id "
        "WHERE c.is_current = TRUE"
    )
    accounts = cursor.fetchall()

    if not accounts:
        print("   ⚠️  No accounts found")
        return []

    is_weekday = simulate_date.isoweekday() <= 5
    daily_count = random.randint(3000, 7000) if is_weekday else random.randint(500, 2000)
    date_key = int(simulate_date.strftime("%Y%m%d"))

    transactions = []
    for i in range(daily_count):
        acc_id, cust_id, branch_id, acc_type, segment = random.choice(accounts)

        # Amount varies by segment
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
            "txn_id": f"TXN{simulate_date.strftime('%Y%m%d')}{i+1:07d}",
            "customer_id": cust_id,
            "account_id": acc_id,
            "branch_id": branch_id,
            "account_type_code": acc_type,
            "txn_date_key": date_key,
            "txn_datetime": datetime(simulate_date.year, simulate_date.month,
                                     simulate_date.day, hour,
                                     random.randint(0, 59),
                                     random.randint(0, 59)),
            "txn_type": random.choice(TXN_TYPES),
            "amount": amount,
            "currency": "VND",
            "channel": random.choice(TXN_CHANNELS),
            "status": random.choice(TXN_STATUSES),
            "description": f"Daily txn {simulate_date} #{i+1}",
        })

    return transactions


# ── SCD Type 2 Customer Updates ──────────────────────────────────────────────

def simulate_customer_updates(conn, simulate_date: date,
                                dry_run: bool = False) -> int:
    """
    Simulate customer attribute changes, triggering SCD Type 2 history.

    LEARNING: ~2% of customers change attributes daily:
    - Address changes (relocation)
    - Segment upgrades/downgrades
    - KYC status updates
    - Risk score adjustments
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT customer_key, customer_id, full_name, date_of_birth, email, "
        "phone, address, city, region, segment, registration_date, "
        "kyc_status, risk_score "
        "FROM dim_customer WHERE is_current = TRUE ORDER BY RANDOM() LIMIT 200"
    )
    candidates = cursor.fetchall()

    col_names = ["customer_key", "customer_id", "full_name", "date_of_birth",
                 "email", "phone", "address", "city", "region", "segment",
                 "registration_date", "kyc_status", "risk_score"]

    update_count = 0
    for row in candidates:
        cust = dict(zip(col_names, row))
        # Decide what to change
        change_type = random.choice(["address", "segment", "kyc", "risk"])

        new_values = dict(cust)  # Copy

        if change_type == "address":
            region = random.choice(list(VIETNAMESE_CITIES.keys()))
            city = random.choice(VIETNAMESE_CITIES[region])
            new_values["city"] = city
            new_values["region"] = region
            new_values["address"] = f"{random.randint(1, 999)} New Street"
        elif change_type == "segment":
            new_values["segment"] = random.choice(SEGMENTS)
        elif change_type == "kyc":
            new_values["kyc_status"] = random.choice(["Verified", "Pending", "Expired"])
        elif change_type == "risk":
            new_values["risk_score"] = int(np.clip(cust["risk_score"] + random.randint(-10, 10), 0, 100))

        if dry_run:
            update_count += 1
            continue

        # SCD Type 2: Expire old row, insert new row
        # Step 1: Expire current record
        cursor.execute(
            "UPDATE dim_customer SET is_current = FALSE, "
            "expiry_date = %s, last_modified = CURRENT_TIMESTAMP "
            "WHERE customer_key = %s",
            (simulate_date, cust["customer_key"]),
        )

        # Step 2: Insert new current record
        cursor.execute(
            "INSERT INTO dim_customer "
            "(customer_id, full_name, date_of_birth, email, phone, address, "
            "city, region, segment, registration_date, kyc_status, risk_score, "
            "effective_date, expiry_date, is_current) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (new_values["customer_id"], new_values["full_name"],
             new_values["date_of_birth"], new_values["email"],
             new_values["phone"], new_values["address"],
             new_values["city"], new_values["region"],
             new_values["segment"], new_values["registration_date"],
             new_values["kyc_status"], new_values["risk_score"],
             simulate_date, date(9999, 12, 31), True),
        )
        update_count += 1

    if not dry_run:
        conn.commit()

    return update_count


# ── Balance Snapshot ─────────────────────────────────────────────────────────

def generate_balance_snapshot(conn, simulate_date: date,
                                dry_run: bool = False) -> int:
    """Generate end-of-day balance snapshots."""
    cursor = conn.cursor()
    date_key = int(simulate_date.strftime("%Y%m%d"))

    # Get a sample of customers
    cursor.execute(
        "SELECT DISTINCT customer_id FROM dim_customer "
        "WHERE is_current = TRUE ORDER BY RANDOM() LIMIT 5000"
    )
    customer_ids = [row[0] for row in cursor.fetchall()]

    if dry_run:
        return len(customer_ids)

    from psycopg2.extras import execute_values
    balances = []
    for cid in customer_ids:
        opening = round(random.uniform(1000000, 500000000), 2)
        credits = round(random.uniform(0, opening * 0.1), 2)
        debits = round(random.uniform(0, opening * 0.08), 2)
        closing = round(opening + credits - debits, 2)

        balances.append((
            cid, "SAV", date_key, opening, closing,
            credits, debits, random.randint(0, 15),
        ))

    execute_values(
        cursor,
        "INSERT INTO fact_daily_balance "
        "(customer_id, account_type_code, date_key, opening_balance, "
        "closing_balance, total_credits, total_debits, txn_count) "
        "VALUES %s",
        balances,
    )
    conn.commit()
    return len(balances)


# ── Watermark Update ────────────────────────────────────────────────────────

def update_watermarks(conn, simulate_date: date):
    """Update CDC watermarks after successful simulation."""
    cursor = conn.cursor()
    tables = ["dim_customer", "fact_transaction", "fact_daily_balance"]

    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]

        cursor.execute(
            "UPDATE cdc_watermark SET last_watermark = CURRENT_TIMESTAMP, "
            "last_row_count = %s, last_run_status = 'SIMULATED', "
            "updated_at = CURRENT_TIMESTAMP "
            "WHERE table_name = %s",
            (count, table),
        )

    conn.commit()
    print("   ✅ CDC watermarks updated")


# ── Main ────────────────────────────────────────────────────────────────────

def simulate_day(conn, simulate_date: date, dry_run: bool = False):
    """Simulate a full day of banking operations."""
    print(f"\n{'─'*50}")
    print(f"  📅 Simulating: {simulate_date} "
          f"({'Weekday' if simulate_date.isoweekday() <= 5 else 'Weekend'})")
    print(f"{'─'*50}")

    # Step 1: Generate transactions
    print("   💰 Generating transactions...")
    txns = generate_daily_transactions(conn, simulate_date)
    if not dry_run and txns:
        from psycopg2.extras import execute_values
        cursor = conn.cursor()
        columns = list(txns[0].keys())
        template = "(" + ", ".join([f"%({c})s" for c in columns]) + ")"
        col_str = ", ".join(columns)

        execute_values(
            cursor,
            f"INSERT INTO fact_transaction ({col_str}) VALUES %s "
            f"ON CONFLICT (txn_id) DO NOTHING",
            txns,
            template=template,
        )
        conn.commit()
    print(f"   ✅ {len(txns):,} transactions")

    # Step 2: Customer updates (SCD Type 2)
    print("   👤 Simulating customer updates...")
    updates = simulate_customer_updates(conn, simulate_date, dry_run)
    print(f"   ✅ {updates} customer updates (SCD Type 2)")

    # Step 3: Balance snapshots
    print("   📊 Generating balance snapshots...")
    balances = generate_balance_snapshot(conn, simulate_date, dry_run)
    print(f"   ✅ {balances:,} balance snapshots")

    # Step 4: Update watermarks
    if not dry_run:
        update_watermarks(conn, simulate_date)

    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n   {prefix}Day simulation complete: "
          f"{len(txns):,} txns, {updates} updates, {balances:,} balances")


def main():
    parser = argparse.ArgumentParser(
        description="Simulate daily banking activity in PostgreSQL RDS",
        epilog="""
LEARNING — DAILY SIMULATION FLOW:
  1. Generate new transactions (varies by weekday/weekend)
  2. Simulate customer attribute changes (SCD Type 2)
  3. Generate end-of-day balance snapshots
  4. Update CDC watermarks
  5. Incremental pipeline picks up changes via WHERE last_modified > watermark
        """
    )
    parser.add_argument("--simulate-date", type=str,
                        help="Date to simulate (YYYY-MM-DD). Default: today")
    parser.add_argument("--start-date", type=str,
                        help="Start of date range (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str,
                        help="End of date range (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without database changes")

    args = parser.parse_args()

    config = load_config()
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  Daily Activity Simulator (PostgreSQL)                     ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print(f"  Host: {config['host']}")
    print(f"  Database: {config['database']}")

    if not config["host"]:
        print("\n❌ RDS_HOST not set. Add it to aws/.env")
        return

    conn = get_connection(config)

    try:
        if args.start_date and args.end_date:
            current = date.fromisoformat(args.start_date)
            end = date.fromisoformat(args.end_date)
            while current <= end:
                simulate_day(conn, current, args.dry_run)
                current += timedelta(days=1)
        else:
            sim_date = date.fromisoformat(args.simulate_date) if args.simulate_date else date.today()
            simulate_day(conn, sim_date, args.dry_run)

        print("\n✅ Simulation complete!")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
