# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Export Gold KPI tables to S3 as Parquet.
# We export these tables so the Streamlit dashboard can query them via DuckDB.
# This decouples the presentation layer from Databricks Serverless, meaning dashboard
# viewers don't incur persistent or per-query compute costs on the warehouse.

dbutils.widgets.text("s3_bucket", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

s3_bucket = dbutils.widgets.get("s3_bucket")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

if not s3_bucket or not catalog or not schema:
    raise ValueError("Widgets 's3_bucket', 'catalog', and 'schema' must all be set to non-empty values.")

kpi_tables = [
    "kpi_repo_health",
    "kpi_time_series",
    "kpi_user_contributions",
    "kpi_pr_complexity"
]

for table in kpi_tables:
    table_path = f"{catalog}.{schema}.{table}"
    print(f"Exporting {table_path} to S3...")
    
    # Read the Delta table from Unity Catalog
    df = spark.table(table_path)
    
    # Export to S3 in Parquet format
    s3_path = f"s3://{s3_bucket}/exports/kpis/{table}"
    df.write.format("parquet").mode("overwrite").save(s3_path)
    
    print(f"Successfully exported {table} to {s3_path}") 