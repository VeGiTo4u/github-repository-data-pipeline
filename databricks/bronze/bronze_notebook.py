# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze Load — All Resource Types
# MAGIC
# MAGIC Thin entry point executed by Databricks (triggered from Airflow).
# MAGIC Imports `run_all_bronze_loads` from the repo via Databricks Repos.

# COMMAND ----------

from databricks.bronze.loader import run_all_bronze_loads

# COMMAND ----------

# Widget parameters — passed from Airflow's DatabricksSubmitRunOperator
dbutils.widgets.text("s3_bucket", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("ingestion_date", "")

s3_bucket = dbutils.widgets.get("s3_bucket")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
ingestion_date = dbutils.widgets.get("ingestion_date")

print(f"[bronze_notebook] bucket={s3_bucket}, catalog={catalog}, schema={schema}, date={ingestion_date}")

# COMMAND ----------

run_all_bronze_loads(
    bucket=s3_bucket,
    catalog=catalog,
    schema=schema,
    ingestion_date=ingestion_date,
    spark=spark,
)