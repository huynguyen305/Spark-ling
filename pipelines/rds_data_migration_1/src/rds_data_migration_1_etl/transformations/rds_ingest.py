import argparse
from pyspark.sql import SparkSession

def _get_dbutils(spark):
    from pyspark.dbutils import DBUtils
    return DBUtils(spark)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--secret-scope", default="sparkling",
                        help="Databricks secret scope holding rds_host, rds_username, rds_password")
    parser.add_argument("--rds-port", default="5432")
    parser.add_argument("--rds-database", default="sparkdb")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    dbutils = _get_dbutils(spark)

    catalog = args.catalog
    schema = args.schema

    rds_host     = dbutils.secrets.get(scope=args.secret_scope, key="rds_host")
    rds_username = dbutils.secrets.get(scope=args.secret_scope, key="rds_username")
    rds_password = dbutils.secrets.get(scope=args.secret_scope, key="rds_password")

    jdbc_url = f"jdbc:postgresql://{rds_host}:{args.rds_port}/{args.rds_database}"
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
