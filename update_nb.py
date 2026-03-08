import json
from pathlib import Path

path = '/home/huyng/sparkling/Spark-ling/notebooks/02_banking_transformations.ipynb'
with open(path, 'r') as f:
    nb = json.load(f)

for i, cell in enumerate(nb['cells']):
    if cell['cell_type'] == 'code':
        source = "".join(cell['source'])
        if "customers_df" in source and "accounts_df =" in source:
            nb['cells'][i]['source'] = [
                "# ── Load data (Delta format from Bronze layer) ──\n",
                "S3_BRONZE = \"s3a://sparkling-data-test/migration/bronze\"\n",
                "DATA_BRONZE = Path(\"../data/bronze\")\n",
                "\n",
                "dim_account_type_df = spark.read.format(\"delta\").load(f\"{S3_BRONZE}/dim_account_type\") if MODE == \"databricks\" else spark.read.format(\"delta\").load(str(DATA_BRONZE / \"dim_account_type\"))\n",
                "dim_branch_df = spark.read.format(\"delta\").load(f\"{S3_BRONZE}/dim_branch\") if MODE == \"databricks\" else spark.read.format(\"delta\").load(str(DATA_BRONZE / \"dim_branch\"))\n",
                "dim_customer_df = spark.read.format(\"delta\").load(f\"{S3_BRONZE}/dim_customer\") if MODE == \"databricks\" else spark.read.format(\"delta\").load(str(DATA_BRONZE / \"dim_customer\"))\n",
                "dim_date_df = spark.read.format(\"delta\").load(f\"{S3_BRONZE}/dim_date\") if MODE == \"databricks\" else spark.read.format(\"delta\").load(str(DATA_BRONZE / \"dim_date\"))\n",
                "fact_daily_balance_df = spark.read.format(\"delta\").load(f\"{S3_BRONZE}/fact_daily_balance\") if MODE == \"databricks\" else spark.read.format(\"delta\").load(str(DATA_BRONZE / \"fact_daily_balance\"))\n",
                "fact_transaction_df = spark.read.format(\"delta\").load(f\"{S3_BRONZE}/fact_transaction\") if MODE == \"databricks\" else spark.read.format(\"delta\").load(str(DATA_BRONZE / \"fact_transaction\"))\n",
                "\n",
                "# Map new tables to existing notebook variables for compatibility\n",
                "customers_df = dim_customer_df\n",
                "accounts_df = fact_daily_balance_df\n",
                "transactions_df = fact_transaction_df\n",
                "branches_df = dim_branch_df\n",
                "\n",
                "print(f\"Loaded: {customers_df.count()} customers, {accounts_df.count()} accounts, {transactions_df.count()} transactions\")\n"
            ]
            break

with open(path, 'w') as f:
    json.dump(nb, f, indent=1)
print("Notebook cell updated.")
