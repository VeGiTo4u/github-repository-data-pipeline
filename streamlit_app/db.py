"""DuckDB connection layer.
We use DuckDB + httpfs to query Parquet files directly from S3 because it provides 
sub-second OLAP performance without needing a running database warehouse or cluster."""

import duckdb
import streamlit as st


@st.cache_resource
@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """Returns a singleton DuckDB connection.
    Cached via Streamlit to avoid the overhead of re-initializing the in-memory database 
    and re-loading the httpfs extension on every user interaction."""
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
    """Executes SQL and returns a pandas DataFrame.
    We standardize on pandas because Streamlit and Plotly render DataFrames natively, 
    decoupling our UI layer from the specific database engine."""
    con = get_connection()
    return con.execute(sql).fetchdf()


def s3_path(table_name: str) -> str:
    """Builds the S3 Parquet path for a KPI table.
    We append '/*.parquet' so DuckDB automatically discovers all partition files, 
    making the query resilient to how Databricks distributes the output."""
    bucket = st.secrets["aws"]["s3_bucket"]
    return f"s3://{bucket}/exports/kpis/{table_name}/*.parquet"
