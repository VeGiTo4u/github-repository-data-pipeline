"""DuckDB connection layer — reads Parquet from S3 via httpfs."""

import duckdb
import streamlit as st


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """Return a singleton DuckDB connection configured for S3 access."""
    con = duckdb.connect(database=":memory:")
    con.install_extension("httpfs")
    con.load_extension("httpfs")

    aws = st.secrets["aws"]
    con.execute(f"""
        SET s3_region = '{aws["region"]}';
        SET s3_access_key_id = '{aws["access_key_id"]}';
        SET s3_secret_access_key = '{aws["secret_access_key"]}';
    """)
    return con


def query(sql: str) -> "pd.DataFrame":
    """Execute SQL against DuckDB and return a pandas DataFrame."""
    con = get_connection()
    return con.execute(sql).fetchdf()


def s3_path(table_name: str) -> str:
    """Build the S3 Parquet path for a KPI table."""
    bucket = st.secrets["aws"]["s3_bucket"]
    return f"s3://{bucket}/exports/kpis/{table_name}/*.parquet"
