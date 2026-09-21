"""GitHub Repository Analytics Dashboard.
We decouple the presentation layer from the data warehouse by exporting Gold tables to S3 Parquet 
and querying them with DuckDB. This architecture provides sub-second interactive latency for the 
dashboard without incurring the high, persistent compute costs of a live warehouse connection."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

st.set_page_config(
    page_title="GitHub Repository Analytics",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    /* ── Global Reset ────────────────────────────────────────────────── */
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
        max-width: 1400px;
    }

    /* ── Header ──────────────────────────────────────────────────────── */
    .dashboard-header {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%);
        border: 1px solid rgba(99, 102, 241, 0.2);
        border-radius: 16px;
        padding: 2rem 2.5rem;
        margin-bottom: 1.5rem;
        position: relative;
        overflow: hidden;
    }
    .dashboard-header::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        background: linear-gradient(90deg, #6366f1, #8b5cf6, #a78bfa, #6366f1);
        background-size: 200% 100%;
        animation: shimmer 3s ease-in-out infinite;
    }
    @keyframes shimmer {
        0%, 100% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
    }
    .dashboard-header h1 {
        margin: 0;
        font-size: 1.75rem;
        font-weight: 800;
        color: #f1f5f9;
        letter-spacing: -0.02em;
    }
    .dashboard-header p {
        margin: 0.4rem 0 0 0;
        font-size: 0.875rem;
        color: #94a3b8;
        font-weight: 400;
    }
    .header-badges {
        display: flex;
        gap: 8px;
        margin-top: 0.75rem;
    }
    .header-badge {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        padding: 4px 10px;
        border-radius: 6px;
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .badge-duckdb {
        background: rgba(255, 213, 0, 0.12);
        color: #fbbf24;
        border: 1px solid rgba(255, 213, 0, 0.2);
    }
    .badge-airflow {
        background: rgba(0, 122, 204, 0.12);
        color: #60a5fa;
        border: 1px solid rgba(0, 122, 204, 0.2);
    }
    .badge-dbt {
        background: rgba(255, 105, 60, 0.12);
        color: #fb923c;
        border: 1px solid rgba(255, 105, 60, 0.2);
    }

    /* ── Metric Cards ────────────────────────────────────────────────── */
    div[data-testid="stMetric"] {
        background: #1e293b;
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 1.25rem 1.5rem;
        transition: border-color 0.2s ease, transform 0.2s ease;
    }
    div[data-testid="stMetric"]:hover {
        border-color: #6366f1;
        transform: translateY(-1px);
    }
    div[data-testid="stMetric"] label {
        color: #64748b !important;
        font-weight: 600;
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #f1f5f9 !important;
        font-weight: 700;
        font-size: 1.75rem;
    }

    /* ── Tabs ─────────────────────────────────────────────────────────── */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        background: #1e293b;
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 5px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 10px 24px;
        font-weight: 500;
        font-size: 0.85rem;
        color: #94a3b8;
        background: transparent;
        border: none;
    }
    .stTabs [data-baseweb="tab"]:hover {
        color: #e2e8f0;
        background: rgba(99, 102, 241, 0.08);
    }
    .stTabs [aria-selected="true"] {
        background: #6366f1 !important;
        color: white !important;
        font-weight: 600;
    }
    /* Remove the default bottom border highlight */
    .stTabs [data-baseweb="tab-highlight"] {
        display: none;
    }
    .stTabs [data-baseweb="tab-border"] {
        display: none;
    }

    /* ── Section Headers ──────────────────────────────────────────────── */
    .section-header {
        font-size: 1rem;
        font-weight: 700;
        color: #e2e8f0;
        margin: 1.5rem 0 0.75rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid #334155;
        letter-spacing: -0.01em;
    }

    /* ── Chart Container ──────────────────────────────────────────────── */
    .chart-container {
        background: #1e293b;
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 1rem;
        margin-bottom: 1rem;
    }

    /* ── Sidebar ──────────────────────────────────────────────────────── */
    section[data-testid="stSidebar"] {
        background: #0f172a;
        border-right: 1px solid #1e293b;
    }
    section[data-testid="stSidebar"] .stMarkdown h3 {
        color: #a78bfa;
        font-size: 0.8rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.1em;
    }
    section[data-testid="stSidebar"] .stSelectbox label {
        color: #94a3b8;
        font-weight: 500;
    }

    /* ── DataFrames ───────────────────────────────────────────────────── */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
    }

    /* ── Divider ──────────────────────────────────────────────────────── */
    hr {
        border-color: #1e293b !important;
        margin: 1rem 0 !important;
    }

    /* ── Footer ──────────────────────────────────────────────────────── */
    .dashboard-footer {
        text-align: center;
        padding: 1.5rem 0 0.5rem 0;
        color: #475569;
        font-size: 0.75rem;
        font-weight: 500;
    }
    .dashboard-footer a {
        color: #6366f1;
        text-decoration: none;
    }
</style>
""", unsafe_allow_html=True)

# ── Import queries ───────────────────────────────────────────────────────────
from queries import (
    get_repo_health,
    get_time_series,
    get_user_contributions,
    get_pr_complexity,
    get_repo_list,
)

# ── Plotly Theme ─────────────────────────────────────────────────────────────
PLOTLY_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, sans-serif", size=12, color="#94a3b8"),
    title_font=dict(size=14, color="#e2e8f0", family="Inter, sans-serif"),
    margin=dict(l=50, r=20, t=50, b=60),
    xaxis=dict(
        gridcolor="rgba(51,65,85,0.5)",
        zerolinecolor="rgba(51,65,85,0.5)",
        tickfont=dict(size=10),
    ),
    yaxis=dict(
        gridcolor="rgba(51,65,85,0.5)",
        zerolinecolor="rgba(51,65,85,0.5)",
        tickfont=dict(size=10),
    ),
)

# Curated palette — indigo / violet / teal / amber / rose
COLORS = ["#6366f1", "#8b5cf6", "#14b8a6", "#f59e0b", "#f43f5e", "#06b6d4", "#a78bfa"]


# ── Fetch Repo List ───────────────────────────────────────────────────────────
repos = get_repo_list()
repo_count = len(repos)

# ── Header ───────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="dashboard-header">
    <h1>GitHub Repository Analytics</h1>
    <p>Real-time insights from {repo_count} open-source repositories · Data refreshed daily</p>
    <div class="header-badges">
        <span class="header-badge badge-duckdb">DuckDB</span>
        <span class="header-badge badge-airflow">Airflow</span>
        <span class="header-badge badge-dbt">dbt</span>
    </div>
</div>
""", unsafe_allow_html=True)


# ── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Filters")
    selected_repo = st.selectbox(
        "Repository",
        options=["All Repositories"] + repos,
        index=0,
    )
    repo_filter = None if selected_repo == "All Repositories" else selected_repo

    st.divider()
    st.markdown("### About")
    st.caption(
        "Queries pre-aggregated KPI Parquet files "
        "exported from Databricks Unity Catalog to S3. "
        "DuckDB provides sub-second query latency."
    )


# ── Helper: short repo name ──────────────────────────────────────────────────
def short_name(full_name: str) -> str:
    """apache/spark → spark"""
    return full_name.split("/")[-1] if "/" in str(full_name) else str(full_name)


# ── Tabs ─────────────────────────────────────────────────────────────────────
tab_overview, tab_trends, tab_prs, tab_community = st.tabs([
    "Repo Health",
    "Trends",
    "PR Complexity",
    "Community",
])

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 1: REPO HEALTH
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_overview:
    df_health = get_repo_health()
    if repo_filter:
        df_health = df_health[df_health["repo_name"] == repo_filter]

    if df_health.empty:
        st.info("No data available for this filter.")
    else:
        total_issues = int(df_health["total_issues"].sum())
        open_issues = int(df_health["open_issues"].sum())
        total_prs = int(df_health["total_prs"].sum())
        open_prs = int(df_health["open_prs"].sum())
        avg_close = df_health["median_time_to_close_hours"].mean() # Mean of medians across repos
        avg_merge = df_health["median_time_to_merge_hours"].mean()
        total_contribs = int(df_health["approximate_total_contributors"].sum())

        # KPI row
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Issues", f"{total_issues:,}")
        c2.metric("Open Issues", f"{open_issues:,}")
        c3.metric("Total PRs", f"{total_prs:,}")
        c4.metric("Open PRs", f"{open_prs:,}")

        c5, c6, c7, c8 = st.columns(4)
        c5.metric("Avg Repo Median Close", f"{avg_close:,.0f} hrs" if pd.notna(avg_close) else "—")
        c6.metric("Avg Repo Median Merge", f"{avg_merge:,.0f} hrs" if pd.notna(avg_merge) else "—")
        c7.metric("Contributors", f"{total_contribs:,}")
        c8.metric("Repositories", f"{len(df_health):,}")

        st.markdown("")

        # Prepare short names for charts
        df_health = df_health.copy()
        df_health["short_name"] = df_health["repo_name"].apply(short_name)

        col_left, col_right = st.columns(2)

        with col_left:
            fig_issues = go.Figure()
            fig_issues.add_trace(go.Bar(
                name="Closed",
                x=df_health["short_name"],
                y=df_health["total_issues"] - df_health["open_issues"],
                marker_color="#6366f1",
                marker_line_width=0,
            ))
            fig_issues.add_trace(go.Bar(
                name="Open",
                x=df_health["short_name"],
                y=df_health["open_issues"],
                marker_color="#f43f5e",
                marker_line_width=0,
            ))
            fig_issues.update_layout(
                **PLOTLY_LAYOUT,
                title="Issues · Open vs Closed",
                barmode="stack",
                xaxis_title="",
                yaxis_title="",
            )
            fig_issues.update_layout(legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center", font=dict(size=11, color="#94a3b8"), bgcolor="rgba(0,0,0,0)", borderwidth=0))
            st.plotly_chart(fig_issues, use_container_width=True)

        with col_right:
            fig_prs = go.Figure()
            fig_prs.add_trace(go.Bar(
                name="Merged / Closed",
                x=df_health["short_name"],
                y=df_health["total_prs"] - df_health["open_prs"],
                marker_color="#8b5cf6",
                marker_line_width=0,
            ))
            fig_prs.add_trace(go.Bar(
                name="Open",
                x=df_health["short_name"],
                y=df_health["open_prs"],
                marker_color="#f59e0b",
                marker_line_width=0,
            ))
            fig_prs.update_layout(
                **PLOTLY_LAYOUT,
                title="Pull Requests · Open vs Closed",
                barmode="stack",
                xaxis_title="",
                yaxis_title="",
            )
            fig_prs.update_layout(legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center", font=dict(size=11, color="#94a3b8"), bgcolor="rgba(0,0,0,0)", borderwidth=0))
            st.plotly_chart(fig_prs, use_container_width=True)

        # Contributors bar chart
        fig_contrib = px.bar(
            df_health.sort_values("approximate_total_contributors", ascending=True),
            x="approximate_total_contributors",
            y="short_name",
            orientation="h",
            color_discrete_sequence=["#14b8a6"],
            title="Contributors per Repository",
        )
        fig_contrib.update_layout(
            **PLOTLY_LAYOUT,
            xaxis_title="",
            yaxis_title="",
            height=350,
        )
        st.plotly_chart(fig_contrib, use_container_width=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 2: TRENDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_trends:
    df_ts = get_time_series(repo_filter)

    if df_ts.empty:
        st.info("No time-series data available.")
    else:
        df_ts_active = df_ts[
            (df_ts["issues_opened"] > 0) |
            (df_ts["issues_closed"] > 0) |
            (df_ts["prs_opened"] > 0) |
            (df_ts["prs_merged"] > 0)
        ].copy()

        if df_ts_active.empty:
            st.info("No active months found in the data.")
        else:
            if not repo_filter:
                df_agg = df_ts_active.groupby("month_year", as_index=False).agg({
                    "issues_opened": "sum",
                    "issues_closed": "sum",
                    "prs_opened": "sum",
                    "prs_merged": "sum",
                    "median_time_to_close_hours": "mean",
                    "median_time_to_merge_hours": "mean",
                })
            else:
                df_agg = df_ts_active

            # Headline metrics
            t1, t2, t3, t4 = st.columns(4)
            t1.metric("Issues Opened", f"{int(df_agg['issues_opened'].sum()):,}")
            t2.metric("Issues Closed", f"{int(df_agg['issues_closed'].sum()):,}")
            t3.metric("PRs Opened", f"{int(df_agg['prs_opened'].sum()):,}")
            t4.metric("PRs Merged", f"{int(df_agg['prs_merged'].sum()):,}")

            st.markdown("")

            col_l, col_r = st.columns(2)
            with col_l:
                fig_i = go.Figure()
                fig_i.add_trace(go.Scatter(
                    x=df_agg["month_year"], y=df_agg["issues_opened"],
                    name="Opened", mode="lines+markers",
                    line=dict(color="#6366f1", width=2.5),
                    marker=dict(size=5),
                    fill="tozeroy",
                    fillcolor="rgba(99,102,241,0.08)",
                ))
                fig_i.add_trace(go.Scatter(
                    x=df_agg["month_year"], y=df_agg["issues_closed"],
                    name="Closed", mode="lines+markers",
                    line=dict(color="#14b8a6", width=2.5),
                    marker=dict(size=5),
                    fill="tozeroy",
                    fillcolor="rgba(20,184,166,0.08)",
                ))
                fig_i.update_layout(
                    **PLOTLY_LAYOUT,
                    title="Issues Over Time",
                    xaxis_title="",
                    yaxis_title="",
                    hovermode="x unified",
                )
                fig_i.update_layout(legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center", font=dict(size=11, color="#94a3b8"), bgcolor="rgba(0,0,0,0)", borderwidth=0))
                st.plotly_chart(fig_i, use_container_width=True)

            with col_r:
                fig_p = go.Figure()
                fig_p.add_trace(go.Scatter(
                    x=df_agg["month_year"], y=df_agg["prs_opened"],
                    name="Opened", mode="lines+markers",
                    line=dict(color="#8b5cf6", width=2.5),
                    marker=dict(size=5),
                    fill="tozeroy",
                    fillcolor="rgba(139,92,246,0.08)",
                ))
                fig_p.add_trace(go.Scatter(
                    x=df_agg["month_year"], y=df_agg["prs_merged"],
                    name="Merged", mode="lines+markers",
                    line=dict(color="#f59e0b", width=2.5),
                    marker=dict(size=5),
                    fill="tozeroy",
                    fillcolor="rgba(245,158,11,0.08)",
                ))
                fig_p.update_layout(
                    **PLOTLY_LAYOUT,
                    title="Pull Requests Over Time",
                    xaxis_title="",
                    yaxis_title="",
                    hovermode="x unified",
                )
                fig_p.update_layout(legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center", font=dict(size=11, color="#94a3b8"), bgcolor="rgba(0,0,0,0)", borderwidth=0))
                st.plotly_chart(fig_p, use_container_width=True)

            # Per-repo breakdown
            if not repo_filter and len(df_ts_active["repo_name"].unique()) > 1:
                df_ts_active["short_name"] = df_ts_active["repo_name"].apply(short_name)
                fig_repo = px.area(
                    df_ts_active,
                    x="month_year",
                    y="issues_opened",
                    color="short_name",
                    title="Issues Opened by Repository",
                    color_discrete_sequence=COLORS,
                )
                fig_repo.update_layout(
                    **PLOTLY_LAYOUT,
                    xaxis_title="",
                    yaxis_title="",
                    legend_title_text="",
                    height=400,
                )
                fig_repo.update_layout(legend=dict(orientation="h", y=-0.15, x=0.5, xanchor="center", font=dict(size=11, color="#94a3b8"), bgcolor="rgba(0,0,0,0)", borderwidth=0))
                st.plotly_chart(fig_repo, use_container_width=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 3: PR COMPLEXITY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_prs:
    df_pr = get_pr_complexity(repo_filter)

    if df_pr.empty:
        st.info("No PR data available.")
    else:
        merged_prs = df_pr[df_pr["merged_date_key"].notna()]
        avg_merge_hrs = merged_prs["time_to_merge_hours"].median()

        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric("Total PRs", f"{len(df_pr):,}")
        mc2.metric("Merged", f"{len(merged_prs):,}")
        mc3.metric("Median Merge Time", f"{avg_merge_hrs:,.0f} hrs" if pd.notna(avg_merge_hrs) else "—")
        mc4.metric("Avg Lines Changed", f"{df_pr['total_lines_changed'].mean():,.0f}")

        st.markdown("")

        col_l, col_r = st.columns(2)

        with col_l:
            size_order = ["Small", "Medium", "Large", "XL", "Unknown"]
            size_colors = {"Small": "#14b8a6", "Medium": "#6366f1", "Large": "#f59e0b", "XL": "#f43f5e", "Unknown": "#475569"}
            size_counts = df_pr["pr_size_bucket"].value_counts().reindex(size_order, fill_value=0)

            fig_size = go.Figure(data=[go.Pie(
                labels=size_counts.index,
                values=size_counts.values,
                hole=0.55,
                marker=dict(colors=[size_colors.get(s, "#475569") for s in size_counts.index]),
                textinfo="percent+label",
                textposition="outside",
                textfont=dict(size=11, color="#e2e8f0"),
                pull=[0.02] * len(size_counts),
            )])
            fig_size.update_layout(
                **PLOTLY_LAYOUT,
                title="PR Size Distribution",
                showlegend=False,
                height=420,
            )
            st.plotly_chart(fig_size, use_container_width=True)

        with col_r:
            scatter_data = merged_prs[merged_prs["time_to_merge_hours"].notna()].copy()
            if not scatter_data.empty:
                # Cap outliers for better viz
                p99 = scatter_data["time_to_merge_hours"].quantile(0.99)
                scatter_capped = scatter_data[scatter_data["time_to_merge_hours"] <= p99]

                fig_scatter = px.scatter(
                    scatter_capped,
                    x="total_lines_changed",
                    y="time_to_merge_hours",
                    color="pr_size_bucket",
                    title="Lines Changed vs Merge Time",
                    color_discrete_map=size_colors,
                    opacity=0.6,
                    hover_data=["changed_files", "commits_count"],
                )
                fig_scatter.update_layout(
                    **PLOTLY_LAYOUT,
                    xaxis_title="Lines Changed",
                    yaxis_title="Hours to Merge",
                    legend_title_text="",
                    height=420,
                )
                fig_scatter.update_layout(legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center", font=dict(size=11, color="#94a3b8"), bgcolor="rgba(0,0,0,0)", borderwidth=0))
                fig_scatter.update_traces(marker=dict(size=5, line=dict(width=0)))
                st.plotly_chart(fig_scatter, use_container_width=True)
            else:
                st.info("Not enough merged PR data for scatter plot.")

        # Histogram of files changed
        fig_files = px.histogram(
            df_pr[df_pr["changed_files"] <= df_pr["changed_files"].quantile(0.95)],
            x="changed_files",
            nbins=40,
            title="Files Changed per PR (Distribution)",
            color_discrete_sequence=["#6366f1"],
        )
        fig_files.update_layout(
            **PLOTLY_LAYOUT,
            xaxis_title="Files Changed",
            yaxis_title="Count",
            bargap=0.05,
            height=350,
        )
        st.plotly_chart(fig_files, use_container_width=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 4: COMMUNITY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_community:
    df_users = get_user_contributions(repo_filter)

    if df_users.empty:
        st.info("No contribution data available.")
    else:
        uc1, uc2, uc3, uc4 = st.columns(4)
        uc1.metric("Unique Contributors", f"{df_users['user_id'].nunique():,}")
        uc2.metric("PRs Merged", f"{int(df_users['total_prs_merged'].sum()):,}")
        uc3.metric("Issues Opened", f"{int(df_users['total_issues_opened'].sum()):,}")
        uc4.metric("Total Comments", f"{int(df_users['total_comments'].sum()):,}")

        st.markdown("")

        # Leaderboard
        st.markdown('<div class="section-header">Top Contributors</div>', unsafe_allow_html=True)
        df_top = df_users.head(10).copy()
        df_top["short_repo"] = df_top["repo_name"].apply(short_name)
        display_name = "github_username" if "github_username" in df_top.columns else "user_id"

        display_df = df_top[[
            display_name, "short_repo",
            "total_prs_merged", "total_issues_opened",
            "total_comments", "total_additions", "total_deletions",
        ]].rename(columns={
            display_name: "User",
            "short_repo": "Repository",
            "total_prs_merged": "PRs Merged",
            "total_issues_opened": "Issues",
            "total_comments": "Comments",
            "total_additions": "Lines Added",
            "total_deletions": "Lines Deleted",
        })
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        st.markdown("")

        col_l, col_r = st.columns(2)

        with col_l:
            top_pr = df_users.nlargest(10, "total_prs_merged").copy()
            top_pr["label"] = top_pr.apply(
                lambda r: f"{r.get('github_username', r['user_id'])} · {short_name(r['repo_name'])}",
                axis=1
            )
            fig_top_pr = px.bar(
                top_pr,
                x="total_prs_merged",
                y="label",
                orientation="h",
                color_discrete_sequence=["#8b5cf6"],
                title="Top 10 · PRs Merged",
            )
            fig_top_pr.update_layout(
                **PLOTLY_LAYOUT,
                xaxis_title="",
                yaxis_title="",
                height=400,
            )
            fig_top_pr.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_top_pr, use_container_width=True)

        with col_r:
            top_adds = df_users.nlargest(10, "total_additions").copy()
            top_adds["label"] = top_adds.apply(
                lambda r: f"{r.get('github_username', r['user_id'])} · {short_name(r['repo_name'])}",
                axis=1
            )
            fig_top_adds = px.bar(
                top_adds,
                x="total_additions",
                y="label",
                orientation="h",
                color_discrete_sequence=["#14b8a6"],
                title="Top 10 · Lines Added",
            )
            fig_top_adds.update_layout(
                **PLOTLY_LAYOUT,
                xaxis_title="",
                yaxis_title="",
                height=400,
            )
            fig_top_adds.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_top_adds, use_container_width=True)


# ── Footer ───────────────────────────────────────────────────────────────────
st.markdown("""
<div class="dashboard-footer">
    Built with Streamlit · DuckDB · Databricks · dbt · Airflow
</div>
""", unsafe_allow_html=True)
