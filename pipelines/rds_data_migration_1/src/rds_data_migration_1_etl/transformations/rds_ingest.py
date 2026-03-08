import argparse
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--rds-host", required=True)
    parser.add_argument("--rds-port", required=True)
    parser.add_argument("--rds-database", required=True)
    parser.add_argument("--rds-username", required=True)
    parser.add_argument("--rds-password", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    
    catalog = args.catalog
    schema = args.schema
    
    jdbc_url = f"jdbc:postgresql://{args.rds_host}:{args.rds_port}/{args.rds_database}"
    jdbc_properties = {
        "user": args.rds_username,
        "password": args.rds_password,
        "driver": "org.postgresql.Driver"
    }

    tables = [
        "dim_date",
        "dim_branch",
        "dim_account_type",
        "dim_customer",
        "dim_account",
        "fact_transaction",
        "fact_daily_balance"
    ]
    
    # Ensure Unity Catalog schema exists
    print(f"Ensuring schema {catalog}.{schema} exists...")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    
    # Sequentially read tables from JDBC and write exactly once to UC Delta Tables
    for table_name in tables:
        target_table_name = f"{catalog}.{schema}.{table_name}"
        print(f"Reading {table_name} from PostgreSQL...")
        
        df = spark.read.jdbc(
            url=jdbc_url, 
            table=table_name, 
            properties=jdbc_properties
        )
        
        print(f"Writing data to Unity Catalog at {target_table_name}...")
        df.write.format("delta").mode("overwrite").saveAsTable(target_table_name)
        
    print("All tables successfully ingested into the Lakehouse!")

if __name__ == "__main__":
    main()
