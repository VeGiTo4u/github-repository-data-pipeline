"""DuckDB connection layer.
We use DuckDB + httpfs to query Parquet files directly from S3 because it provides 
sub-second OLAP performance without needing a running database warehouse or cluster."""

import duckdb
import streamlit as st
import pandas as pd


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """Returns a singleton DuckDB connection.
    Cached via Streamlit to avoid the overhead of re-initializing the in-memory database 
    and re-loading the httpfs extension on every user interaction."""
    con = duckdb.connect(database=":memory:")
    con.install_extension("httpfs")
    con.load_extension("httpfs")

    if "aws" not in st.secrets:
        st.error("Missing `[aws]` section in Streamlit secrets. Please configure AWS credentials in Streamlit Cloud settings.")
        return con

    aws = st.secrets["aws"]
    region = str(aws.get("region", "us-east-1")).strip()
    access_key = str(aws.get("access_key_id", "")).strip()
    secret_key = str(aws.get("secret_access_key", "")).strip()

    # Try modern DuckDB 1.0+ Secret Manager first, fall back to legacy SET commands
    try:
        con.execute(f"""
            CREATE OR REPLACE SECRET s3_creds (
                TYPE S3,
                KEY_ID '{access_key}',
                SECRET '{secret_key}',
                REGION '{region}'
            );
        """)
    except Exception:
        con.execute(f"""
            SET s3_region = '{region}';
            SET s3_access_key_id = '{access_key}';
            SET s3_secret_access_key = '{secret_key}';
        """)
    return con


def query(sql: str) -> pd.DataFrame | None:
    """Executes SQL and returns a pandas DataFrame.
    Returns None if the query fails (e.g. S3 connectivity/credentials or missing data)
    to allow the UI to handle it gracefully."""
    try:
        con = get_connection()
        return con.execute(sql).fetchdf()
    except Exception as e:
        st.error(f"S3/DuckDB Query Error: {e}")
        return None


def s3_path(table_name: str) -> str:
    """Builds the S3 Parquet path for a KPI table.
    We append '/*.parquet' so DuckDB automatically discovers all partition files, 
    making the query resilient to how Databricks distributes the output."""
    if "aws" not in st.secrets:
        return f"s3://unknown-bucket/exports/kpis/{table_name}/*.parquet"
    bucket = str(st.secrets["aws"].get("s3_bucket", "")).strip()
    return f"s3://{bucket}/exports/kpis/{table_name}/*.parquet"

