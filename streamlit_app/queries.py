"""SQL queries for each KPI dataset."""

from db import query, s3_path
import pandas as pd


def get_repo_health() -> pd.DataFrame:
    """Repo-level health metrics."""
    return query(f"""
        SELECT *
        FROM read_parquet('{s3_path("kpi_repo_health")}')
        ORDER BY repo_name
    """)


def get_time_series(repo_filter: str | None = None) -> pd.DataFrame:
    """Monthly time-series metrics, optionally filtered by repo."""
    where = f"WHERE repo_name = '{repo_filter}'" if repo_filter else ""
    return query(f"""
        SELECT *
        FROM read_parquet('{s3_path("kpi_time_series")}')
        {where}
        ORDER BY month_year
    """)


def get_user_contributions(repo_filter: str | None = None) -> pd.DataFrame:
    """Per-user contribution metrics."""
    where = f"WHERE repo_name = '{repo_filter}'" if repo_filter else ""
    return query(f"""
        SELECT *
        FROM read_parquet('{s3_path("kpi_user_contributions")}')
        {where}
        ORDER BY (total_prs_merged + total_issues_opened) DESC
    """)


def get_pr_complexity(repo_filter: str | None = None) -> pd.DataFrame:
    """PR complexity and size analysis."""
    where = f"WHERE repo_name = '{repo_filter}'" if repo_filter else ""
    return query(f"""
        SELECT *
        FROM read_parquet('{s3_path("kpi_pr_complexity")}')
        {where}
        ORDER BY total_lines_changed DESC
    """)


def get_repo_list() -> list[str]:
    """Get distinct repo names for sidebar filter."""
    df = query(f"""
        SELECT DISTINCT repo_name
        FROM read_parquet('{s3_path("kpi_repo_health")}')
        ORDER BY repo_name
    """)
    return df["repo_name"].tolist()
