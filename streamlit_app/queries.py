"""SQL queries for each KPI dataset."""

from db import query, s3_path
import pandas as pd


def get_repo_health() -> pd.DataFrame:
    """Fetches base metrics to power the KPI cards and overview charts.
    This provides the high-level health snapshot (total vs open counts) that users 
    need first before drilling down into specific trends."""
    return query(f"""
        SELECT *
        FROM read_parquet('{s3_path("kpi_repo_health")}')
        ORDER BY repo_name
    """)


def get_time_series(repo_filter: str | None = None) -> pd.DataFrame:
    """Fetches monthly velocity metrics.
    Visualizing activity over time helps maintainers spot trends in community 
    engagement or potential bottlenecks in review cycles."""
    where = f"WHERE repo_name = '{repo_filter}'" if repo_filter else ""
    return query(f"""
        SELECT *
        FROM read_parquet('{s3_path("kpi_time_series")}')
        {where}
        ORDER BY month_year
    """)


def get_user_contributions(repo_filter: str | None = None) -> pd.DataFrame:
    """Fetches per-user metrics for the leaderboard.
    Identifying top contributors highlights community health and helps maintainers 
    spot bus-factor risks (where a project relies too heavily on one person)."""
    where = f"WHERE repo_name = '{repo_filter}'" if repo_filter else ""
    return query(f"""
        SELECT *
        FROM read_parquet('{s3_path("kpi_user_contributions")}')
        {where}
        ORDER BY (total_prs_merged + total_issues_opened) DESC
    """)


def get_pr_complexity(repo_filter: str | None = None) -> pd.DataFrame:
    """Fetches PR size and merge time metrics.
    We use this to correlate PR size with review delays, providing actionable 
    evidence if a project needs to enforce smaller, more manageable PR policies."""
    where = f"WHERE repo_name = '{repo_filter}'" if repo_filter else ""
    return query(f"""
        SELECT *
        FROM read_parquet('{s3_path("kpi_pr_complexity")}')
        {where}
        ORDER BY total_lines_changed DESC
    """)


def get_repo_list() -> list[str]:
    """Fetches distinct repo names dynamically.
    Instead of hardcoding the list in the UI, we query it from the data so the sidebar 
    dropdown automatically updates if new repositories are added to the ingestion pipeline."""
    df = query(f"""
        SELECT DISTINCT repo_name
        FROM read_parquet('{s3_path("kpi_repo_health")}')
        ORDER BY repo_name
    """)
    return df["repo_name"].tolist()
