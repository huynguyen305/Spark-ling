"""
Synthetic Banking Data Generator
================================
Generates realistic banking data for Spark practice:
- Customers (10,000)
- Accounts (15,000)
- Transactions (500,000+)
- Branches (100)

Usage:
    python src/data_generator.py
"""

import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any
import csv
import numpy as np

# Set seeds for reproducibility
np.random.seed(42)

# Try to import Faker, fallback to simple generation if not available
try:
    from faker import Faker
    fake = Faker('vi_VN')  # Vietnamese locale
    Faker.seed(42)
    HAS_FAKER = True
except ImportError:
    HAS_FAKER = False
    print("⚠️ Faker not installed. Using simple data generation.")

random.seed(42)

# Project paths — __file__ works locally; on Databricks, cwd is the repo root
try:
    PROJECT_ROOT = Path(__file__).parent.parent
except NameError:
    PROJECT_ROOT = Path.cwd()
DATA_RAW = PROJECT_ROOT / "data" / "raw"

# Constants
CUSTOMER_SEGMENTS = ["Mass", "Mass Affluent", "Affluent", "HNW", "UHNW"]
ACCOUNT_TYPES = ["Checking", "Savings", "Term Deposit", "Investment", "Credit"]
ACCOUNT_STATUSES = ["Active", "Dormant", "Closed", "Frozen"]
TXN_TYPES = ["Deposit", "Withdrawal", "Transfer In", "Transfer Out", "Payment", "Fee", "Interest"]
TXN_CHANNELS = ["Branch", "ATM", "Mobile App", "Internet Banking", "POS", "API"]
MERCHANT_CATEGORIES = ["Retail", "F&B", "Travel", "Utilities", "Entertainment", "Healthcare", "Education", "Others"]
REGIONS = ["Hanoi", "Ho Chi Minh", "Da Nang", "Hai Phong", "Can Tho", "Binh Duong", "Dong Nai", "Quang Ninh"]
KYC_STATUSES = ["Verified", "Pending", "Expired", "Rejected"]


def generate_id(prefix: str, num: int) -> str:
    """Generate ID like CUST000001, ACCT000001, etc."""
    return f"{prefix}{num:06d}"


def random_date(start_year: int = 2020, end_year: int = 2025) -> str:
    """Generate random date string."""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")


def random_datetime(start_date: str = "2025-01-01", end_date: str = "2025-12-31") -> str:
    """Generate random datetime string."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    dt = start + timedelta(seconds=random_seconds)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def generate_branches(n: int = 100) -> List[Dict[str, Any]]:
    """Generate branch data."""
    branches = []
    for i in range(1, n + 1):
        region = random.choice(REGIONS)
        branches.append({
            "branch_id": generate_id("BR", i),
            "branch_name": f"{region} Branch {i:03d}",
            "region": region,
            "city": region,  # Simplified
            "address": f"{random.randint(1, 999)} Street {random.randint(1, 50)}, {region}",
            "opened_date": random_date(2010, 2023)
        })
    return branches


def generate_customers(n: int = 10000) -> List[Dict[str, Any]]:
    """Generate customer data."""
    customers = []
    for i in range(1, n + 1):
        if HAS_FAKER:
            name = fake.name()
            email = fake.email()
            phone = fake.phone_number()
        else:
            name = f"Customer {i:05d}"
            email = f"customer{i}@example.com"
            phone = f"09{random.randint(10000000, 99999999)}"
        
        # Higher segments are less common
        segment_weights = [0.5, 0.25, 0.15, 0.07, 0.03]
        segment = random.choices(CUSTOMER_SEGMENTS, weights=segment_weights)[0]
        
        customers.append({
            "customer_id": generate_id("CUST", i),
            "name": name,
            "email": email,
            "phone": phone,
            "segment": segment,
            "registration_date": random_date(2015, 2024),
            "kyc_status": random.choices(KYC_STATUSES, weights=[0.85, 0.08, 0.05, 0.02])[0],
            "date_of_birth": random_date(1960, 2000),
            "gender": random.choice(["M", "F"]),
            "nationality": "Vietnamese"
        })
    return customers


def generate_accounts(customers: List[Dict], branches: List[Dict], avg_accounts_per_customer: float = 1.5) -> List[Dict[str, Any]]:
    """Generate account data."""
    accounts = []
    account_num = 1
    
    for customer in customers:
        # Each customer has 1-3 accounts
        num_accounts = random.choices([1, 2, 3], weights=[0.5, 0.35, 0.15])[0]
        
        for _ in range(num_accounts):
            branch = random.choice(branches)
            account_type = random.choice(ACCOUNT_TYPES)
            
            # Balance based on segment
            segment = customer["segment"]
            if segment == "UHNW":
                balance = round(random.uniform(5000000000, 50000000000), 2)  # 5-50 billion VND
            elif segment == "HNW":
                balance = round(random.uniform(500000000, 5000000000), 2)  # 500M-5B VND
            elif segment == "Affluent":
                balance = round(random.uniform(100000000, 500000000), 2)  # 100-500M VND
            elif segment == "Mass Affluent":
                balance = round(random.uniform(20000000, 100000000), 2)  # 20-100M VND
            else:
                balance = round(random.uniform(100000, 20000000), 2)  # 100K-20M VND
            
            accounts.append({
                "account_id": generate_id("ACCT", account_num),
                "customer_id": customer["customer_id"],
                "branch_id": branch["branch_id"],
                "account_type": account_type,
                "balance": balance,
                "currency": "VND",
                "status": random.choices(ACCOUNT_STATUSES, weights=[0.85, 0.08, 0.05, 0.02])[0],
                "opened_date": random_date(2018, 2024),
                "last_activity_date": random_date(2024, 2025)
            })
            account_num += 1
    
    return accounts


def generate_transactions(accounts: List[Dict], n: int = 500000) -> List[Dict[str, Any]]:
    """
    Generate transaction data using NumPy vectorization.
    
    This approach pre-generates all random values as NumPy arrays,
    which is 10-50x faster than Python loops or multiprocessing.
    """
    print(f"  Using NumPy vectorized generation for {n:,} transactions...")
    
    # Extract account data as numpy arrays for fast access
    account_ids = np.array([acc["account_id"] for acc in accounts])
    account_balances = np.array([acc["balance"] for acc in accounts])
    
    # Weight accounts by balance (higher balance = more transactions)
    account_weights = account_balances / account_balances.sum()
    
    # Pre-generate ALL random values at once using NumPy (FAST!)
    print("  Generating random indices...")
    account_indices = np.random.choice(len(accounts), size=n, p=account_weights)
    txn_type_indices = np.random.randint(0, len(TXN_TYPES), size=n)
    channel_indices = np.random.randint(0, len(TXN_CHANNELS), size=n)
    merchant_indices = np.random.randint(0, len(MERCHANT_CATEGORIES), size=n)
    status_indices = np.random.choice(4, size=n, p=[0.92, 0.04, 0.02, 0.02])
    reference_nums = np.random.randint(100000000, 999999999, size=n)
    
    # Pre-generate amounts
    print("  Generating amounts...")
    base_amounts = np.random.uniform(50000, 100000000, size=n)
    fee_amounts = np.random.uniform(10000, 100000, size=n)
    interest_multipliers = np.random.uniform(0.001, 0.01, size=n)
    
    # Pre-generate timestamps (as seconds since start of year)
    print("  Generating timestamps...")
    start_ts = datetime(2025, 1, 1).timestamp()
    end_ts = datetime(2025, 12, 31, 23, 59, 59).timestamp()
    random_timestamps = np.random.uniform(start_ts, end_ts, size=n)
    
    # Status lookup
    statuses = ["Completed", "Pending", "Failed", "Reversed"]
    
    # Build transactions in batches
    print("  Building transaction records...")
    transactions = []
    batch_size = 100000
    
    for batch_start in range(0, n, batch_size):
        batch_end = min(batch_start + batch_size, n)
        
        for i in range(batch_start, batch_end):
            acc_idx = account_indices[i]
            txn_type = TXN_TYPES[txn_type_indices[i]]
            balance = account_balances[acc_idx]
            
            # Calculate amount based on type
            if txn_type == "Fee":
                amount = round(fee_amounts[i], 2)
            elif txn_type == "Interest":
                amount = round(balance * interest_multipliers[i], 2)
            else:
                max_amount = min(balance * 0.1, 100000000)
                amount = round(min(base_amounts[i], max(50001, max_amount)), 2)
            
            # Convert timestamp to datetime string
            dt = datetime.fromtimestamp(random_timestamps[i])
            txn_datetime = dt.strftime("%Y-%m-%d %H:%M:%S")
            
            transactions.append({
                "txn_id": f"TXN{i+1:06d}",
                "account_id": account_ids[acc_idx],
                "txn_datetime": txn_datetime,
                "txn_type": txn_type,
                "amount": amount,
                "currency": "VND",
                "channel": TXN_CHANNELS[channel_indices[i]],
                "merchant_category": MERCHANT_CATEGORIES[merchant_indices[i]] if txn_type == "Payment" else None,
                "status": statuses[status_indices[i]],
                "reference": f"REF{reference_nums[i]}",
                "description": f"{txn_type} transaction"
            })
        
        print(f"  Processed {batch_end:,}/{n:,} transactions...")
    
    print(f"  ✅ Total generated: {len(transactions):,} transactions")
    return transactions


def save_to_csv(data: List[Dict], filename: str) -> None:
    """Save data to CSV file. If data is too large, split into multiple files."""
    if not data:
        return
    batch_size = 100000
    if len(data) > batch_size:
        num_parts = (len(data) + batch_size - 1) // batch_size
        for part in range(num_parts):
            part_data = data[part*batch_size : (part+1)*batch_size]
            part_filename = f"{filename.replace('.csv','')}_part{part+1}.csv"
            filepath = DATA_RAW / part_filename
            filepath.parent.mkdir(parents=True, exist_ok=True)
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=part_data[0].keys())
                writer.writeheader()
                writer.writerows(part_data)
            print(f"✅ Saved {len(part_data):,} records to {filepath}")
    else:
        filepath = DATA_RAW / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print(f"✅ Saved {len(data):,} records to {filepath}")


def main():
    """Generate all synthetic banking data."""
    print("=" * 60)
    print("🏦 Spark-ling: Synthetic Banking Data Generator")
    print("=" * 60)
    
    # Create data directories
    (PROJECT_ROOT / "data" / "raw").mkdir(parents=True, exist_ok=True)
    (PROJECT_ROOT / "data" / "processed").mkdir(parents=True, exist_ok=True)
    (PROJECT_ROOT / "data" / "analytics").mkdir(parents=True, exist_ok=True)
    
    print("\n📊 Generating branches...")
    branches = generate_branches(100)
    save_to_csv(branches, "branches.csv")
    
    print("\n👥 Generating customers...")
    customers = generate_customers(10000)
    save_to_csv(customers, "customers.csv")
    
    print("\n💳 Generating accounts...")
    accounts = generate_accounts(customers, branches)
    save_to_csv(accounts, "accounts.csv")
    
    print("\n💰 Generating transactions (5M records - this will take a few minutes)...")
    transactions = generate_transactions(accounts, 5_000_000)
    save_to_csv(transactions, "transactions.csv")
    
    print("\n" + "=" * 60)
    print("✅ Data generation complete!")
    print(f"📁 Data saved to: {DATA_RAW}")
    print("=" * 60)
    
    # Summary
    print("\n📊 Data Summary:")
    print(f"  • Branches:     {len(branches):,}")
    print(f"  • Customers:    {len(customers):,}")
    print(f"  • Accounts:     {len(accounts):,}")
    print(f"  • Transactions: {len(transactions):,}")


if __name__ == "__main__":
    main()
