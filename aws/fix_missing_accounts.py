import os
import random
from pyspark.sql import Row

try:
    from databricks.connect import DatabricksSession
    print("Using Databricks Connect Serverless...")
    spark = DatabricksSession.builder.serverless().getOrCreate()
except ImportError:
    from pyspark.sql import SparkSession
    print("Using Local SparkSession...")
    spark = SparkSession.builder \
        .appName("FixMissingAccounts") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

S3_BUCKET = "sparkling-data-test"
S3_BRONZE = f"s3a://{S3_BUCKET}/migration/bronze"
S3_RAW_PATH = f"s3a://{S3_BUCKET}/data/raw/accounts"

print("Reading dim_customer from Bronze...")
try:
    customers = spark.read.format("delta").load(f"{S3_BRONZE}/dim_customer")
    customer_data = customers.select("customer_id", "segment").collect()
except Exception as e:
    print(f"Cannot read dim_customer: {e}")
    print("Mocking customer data...")
    customer_data = [Row(customer_id=f"CUST{i:06d}", segment="Mass") for i in range(1, 10001)]

print("Reading dim_branch from Bronze...")
try:
    branches = spark.read.format("delta").load(f"{S3_BRONZE}/dim_branch")
    branch_ids = [row.branch_id for row in branches.select("branch_id").collect()]
except Exception as e:
    print(f"Cannot read dim_branch: {e}")
    branch_ids = [f"BR{i:03d}" for i in range(1, 101)]

ACCOUNT_TYPES = ["Checking", "Savings", "Term Deposit", "Investment", "Credit"]
ACCOUNT_STATUSES = ["Active", "Dormant", "Closed", "Frozen"]

BALANCE_RANGES = {
    "Corporate":     (10_000_000_000, 100_000_000_000),
    "VIP":           (5_000_000_000,  50_000_000_000),
    "Premium":       (500_000_000,    5_000_000_000),
    "Affluent":      (100_000_000,    500_000_000),
    "Mass Affluent": (20_000_000,     100_000_000),
    "Mass":          (100_000,        20_000_000),
}

print("Generating accounts...")
accounts_data = []
acct_num = 1
for cust in customer_data:
    segment = cust.segment
    num_accounts = random.choices([1, 2, 3], weights=[0.5, 0.35, 0.15])[0]
    lo, hi = BALANCE_RANGES.get(segment, (100_000, 20_000_000))

    for _ in range(num_accounts):
        balance = round(random.uniform(lo, hi), 2)
        accounts_data.append(Row(
            account_id=f"ACCT{acct_num:06d}",
            customer_id=cust.customer_id,
            branch_id=random.choice(branch_ids) if branch_ids else "BR001",
            account_type=random.choice(ACCOUNT_TYPES),
            balance=balance,
            currency="VND",
            status=random.choices(ACCOUNT_STATUSES, weights=[0.85, 0.08, 0.05, 0.02])[0],
            opened_date=f"{random.randint(2018, 2024)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            last_activity_date=f"{random.randint(2024, 2025)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
        ))
        acct_num += 1

df_accounts = spark.createDataFrame(accounts_data)
# Repartition before write to optimize small files issue if Databricks Serverless allows
df_accounts = df_accounts.repartition(4)

print(f"Writing {df_accounts.count()} accounts to S3 Parquet at {S3_RAW_PATH}...")
df_accounts.write.mode("overwrite").parquet(S3_RAW_PATH)
df_accounts.write.mode("overwrite").format("delta").save(f"{S3_BRONZE}/dim_account")

print("Done! 🎉")
spark.stop()
