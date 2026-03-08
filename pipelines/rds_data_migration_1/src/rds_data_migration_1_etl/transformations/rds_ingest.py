from pyspark.sql import SparkSession
import os

def main():
    spark = SparkSession.builder.getOrCreate()
    
    # Retrieve Unity Catalog target
    catalog = os.environ.get("TARGET_CATALOG", "sparkling")
    schema = os.environ.get("TARGET_SCHEMA", "default")
    
    # RDS connection details
    rds_host = os.environ.get("RDS_HOST")
    rds_port = os.environ.get("RDS_PORT")
    rds_database = os.environ.get("RDS_DATABASE")
    rds_username = os.environ.get("RDS_USERNAME")
    rds_password = os.environ.get("RDS_PASSWORD")

    jdbc_url = f"jdbc:postgresql://{rds_host}:{rds_port}/{rds_database}"
    jdbc_properties = {
        "user": rds_username,
        "password": rds_password,
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
