<p align="center">
  <h1 align="center">GitHub Repository Data Pipeline</h1>
  <p align="center">
    An end-to-end data engineering pipeline that ingests, transforms, and visualizes analytics from major open-source GitHub repositories.
  </p>
</p>

<p align="center">
  <a href="https://repository-data-pipeline.streamlit.app/"><img src="https://static.streamlit.io/badges/streamlit_badge_black_white.svg" alt="Open in Streamlit"></a>
</p>

<p align="center">
  <a href="#architecture">Architecture</a> •
  <a href="#tech-stack">Tech Stack</a> •
  <a href="#data-model">Data Model</a> •
  <a href="#project-structure">Project Structure</a> •
  <a href="#getting-started">Getting Started</a> •
  <a href="#pipeline-deep-dive">Pipeline Deep Dive</a> •
  <a href="#dashboard">Dashboard</a> •
  <a href="#key-design-decisions">Key Design Decisions</a> •
  <a href="#license">License</a>
</p>

---

## Overview

This project implements a production-grade data pipeline that extracts repository metadata, issues, pull requests, releases, and language statistics from the GitHub API (REST + GraphQL), lands them in S3, transforms them through a Medallion architecture (Bronze → Silver → Gold), and serves a Streamlit analytics dashboard via DuckDB.

**Repositories Tracked:**

| Repository | Tier | Est. Issues | Est. PRs |
|---|---|---|---|
| `apache/spark` | Large | ~45,000 | ~42,000 |
| `kubernetes/kubernetes` | XLarge | ~60,000 | ~100,000 |
| `microsoft/vscode` | XLarge | ~170,000 | ~25,000 |
| `rust-lang/rust` | Large | ~60,000 | ~70,000 |
| `facebook/react` | Medium | ~14,000 | ~16,000 |
| `langchain-ai/langchain` | Medium | ~8,000 | ~15,000 |
| `duckdb/duckdb` | Medium | ~22,000 | ~14,000 |
| `dbt-labs/dbt-core` | Small | ~7,000 | ~6,000 |

---

## Architecture

<p align="center">
  <img src="docs/images/architecture.jpg" alt="Pipeline Architecture" width="850">
</p>

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Orchestration** | Apache Airflow 2.10 (Docker) | DAG scheduling, Dynamic Task Mapping, retry logic |
| **Ingestion** | Python (requests, boto3) | GitHub REST API + GraphQL extraction |
| **Storage** | Amazon S3 | Raw (NDJSON) + Bronze/Silver/Gold (Delta) + Exports (Parquet) |
| **Processing** | Databricks (Serverless, Unity Catalog) | Bronze Delta table loading via PySpark |
| **Transformation** | dbt (dbt-databricks adapter) | Staging → Intermediate → Silver → Gold modeling |
| **Presentation** | Streamlit + Plotly | Interactive analytics dashboard |
| **Query Engine** | DuckDB (httpfs) | Sub-second OLAP queries on S3 Parquet exports |
| **Infrastructure** | Docker Compose, PostgreSQL | Local Airflow deployment with metadata DB |

---

## Data Model

The pipeline implements a **Kimball-style star schema** in the Gold layer, fed by a multi-hop **Medallion architecture** through dbt.

### Gold Layer (Star Schema)

```
                            ┌─────────────────-┐
                            │  dim_date        │
                            │  (date_key PK)   │
                            └────────┬─────────┘
                                     │
┌───────────────────┐    ┌───────────┴──────────┐    ┌──────────────────┐
│  dim_repositories │    │    fact_issues       │    │    dim_users     │
│  (repository_sk)  │◀───│  (issue_sk PK)       │───▶│  (user_sk PK)    │
│  Type 2 SCD       │    │  Accumulating Snap.  │    │  Type 1          │
└───────────────────┘    └──────────────────────┘    └──────────────────┘
         ▲                                                    ▲
         │               ┌──────────────────────┐             │
         └───────────────│  fact_pull_requests  │─────────────┘
                         │  (pull_request_sk PK)│
                         │  Accumulating Snap.  │
                         └──────────────────────┘

                         ┌──────────────────────┐
                         │   fact_releases      │
                         │  (release_sk PK)     │
                         │  Transactional Fact  │
                         └──────────────────────┘
```

### KPI Tables (Dashboard-Ready)

| Table | Description | Key Metrics |
|---|---|---|
| `kpi_repo_health` | Per-repo health snapshot | Total/open issues & PRs, median close/merge times, contributor count |
| `kpi_time_series` | Monthly velocity metrics | Issues opened/closed, PRs opened/merged, median response times |
| `kpi_user_contributions` | Per-user contribution leaderboard | PRs merged, issues opened, comments, lines added/deleted |
| `kpi_pr_complexity` | PR size and review analysis | Lines changed, files changed, commits, size bucket, merge time |

### dbt Model Lineage

<p align="center">
  <img src="docs/images/dbt_lineage.jpg" alt="dbt Model Lineage DAG" width="700">
</p>

---

## Project Structure

```
github-repository-data-pipeline/
│
├── airflow/                          # Orchestration layer
│   ├── Dockerfile                    # Airflow image with dbt + Databricks providers
│   ├── docker-compose.yaml           # LocalExecutor setup (Airflow + PostgreSQL)
│   └── dags/
│       └── github_repository_analytics.py   # Main DAG with Dynamic Task Mapping
│
├── ingestion/                        # Data extraction & loading
│   ├── extractor.py                  # Per-repo extraction (REST + GraphQL)
│   ├── github_client.py              # HTTP client with rate-limit backpressure
│   ├── s3_writer.py                  # S3 upload (single file + chunked streaming)
│   ├── normalizer.py                 # Lineage envelope wrapper for audit trail
│   ├── repo_config.py                # Tier-based throttle + repo registry
│   ├── logger.py                     # Structured JSON-lines logger
│   └── requirements.txt              # requests, boto3, python-dotenv
│
├── databricks/                       # Spark-based Bronze layer
│   ├── bronze/
│   │   ├── bronze_notebook.py        # Thin Databricks notebook entry point
│   │   ├── loader.py                 # Generic Delta loader (registry-driven)
│   │   └── bronze_config.py          # Metadata registry for all resource types
│   └── export/
│       └── export_kpis_notebook.py   # Gold → S3 Parquet export for dashboard
│
├── dbt/                              # Transformation layer (Silver + Gold)
│   ├── dbt_project.yml               # Project config with materialization rules
│   ├── profiles.yml                  # Databricks connection profile
│   ├── models/
│   │   ├── staging/                  # Views: JSON flattening, dedup, typecasting
│   │   ├── intermediate/            # DQ validation, watermark incrementality
│   │   └── marts/
│   │       ├── silver/              # SCD2 current-state + quarantine visibility
│   │       └── gold/                # Star schema (dims, facts, KPIs)
│   ├── snapshots/                    # SCD Type 2 (issues, PRs, repositories)
│   ├── macros/
│   │   ├── evaluate_dq_rules.sql     # Reusable DQ quarantine macro
│   │   ├── generate_schema_name.sql  # Custom schema routing
│   │   └── ...
│   └── tests/                        # Singular tests for snapshot integrity
│
├── streamlit_app/                    # Presentation layer
│   ├── app.py                        # Dashboard (4 tabs, Plotly charts)
│   ├── db.py                         # DuckDB connection (httpfs → S3 Parquet)
│   ├── queries.py                    # SQL queries for each KPI dataset
│   └── requirements.txt              # streamlit, duckdb, plotly, pandas
│
├── docs/                             # Technical documentation
│   ├── Architecture.md               # 17 Architecture Decision Records (ADRs)
│   ├── DATA MODEL.md                 # Gold star schema + layer documentation
│   └── CLAUDE.md                     # AI-assistant context file
│
├── .env                              # Environment variables (git-ignored)
├── .gitignore
├── LICENSE                           # MIT License
└── README.md                         # ← You are here
```

---

## Getting Started

### Prerequisites

- **Docker** & **Docker Compose** (for Airflow)
- **Python 3.11+**
- **GitHub Personal Access Token** (PAT) with `public_repo` scope
- **AWS Account** with an S3 bucket and IAM credentials
- **Databricks Workspace** with Unity Catalog enabled
- **Streamlit** (for the dashboard)

### 1. Clone the Repository

```bash
git clone https://github.com/VeGiTo4u/github-repository-data-pipeline.git
cd github-repository-data-pipeline
```

### 2. Configure Environment Variables

Create a `.env` file at the project root:

```env
# GitHub
GITHUB_TOKEN=github_pat_XXXXXXXXXXXXXXXXXXXX
AIRFLOW_VAR_GITHUB_TOKEN=github_pat_XXXXXXXXXXXXXXXXXXXX

# AWS
AWS_ACCESS_KEY_ID=AKIAXXXXXXXXXXXX
AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name

# Databricks
DATABRICKS_HOST=dbc-XXXXXXXX-XXXX.cloud.databricks.com
DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
DATABRICKS_CATALOG=github_analytics
DATABRICKS_SCHEMA=bronze
DATABRICKS_SQL_WAREHOUSE_HTTP_PATH=/sql/1.0/warehouses/XXXXXXXXXXXXXXXX

# Airflow Connections (auto-created on startup)
AIRFLOW_CONN_AWS_DEFAULT=aws://AKIAXXXXXXXXXXXX:XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX@
AIRFLOW_CONN_DATABRICKS_DEFAULT=databricks://token:dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX@dbc-XXXXXXXX-XXXX.cloud.databricks.com

# Airflow Variables
AIRFLOW_VAR_S3_BUCKET_NAME=your-bucket-name
AIRFLOW_VAR_DATABRICKS_CATALOG=github_analytics
AIRFLOW_VAR_DATABRICKS_SCHEMA=bronze
AIRFLOW_VAR_DATABRICKS_NOTEBOOK_PATH=/Workspace/your-repo/databricks/bronze/bronze_notebook
```

### 3. Launch Airflow

```bash
cd airflow
docker compose up -d
```

Airflow UI will be available at `http://localhost:8080` (credentials: `admin` / `admin`).

### 4. Configure Streamlit Secrets

Create `streamlit_app/.streamlit/secrets.toml`:

```toml
[aws]
region = "us-east-1"
access_key_id = "AKIAXXXXXXXXXXXX"
secret_access_key = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
s3_bucket = "your-bucket-name"
```

### 5. Run the Dashboard

```bash
cd streamlit_app
pip install -r requirements.txt
streamlit run app.py
```

---

## Pipeline Deep Dive

### Ingestion Layer

The ingestion module extracts data from the GitHub API with several production-hardened patterns:

- **Dynamic Task Mapping**: Each repository is processed by an independent Airflow task instance, enabling parallel extraction and retry isolation at the task-instance level. Downstream Bronze processing intentionally requires the complete mapped extraction stage to succeed, forming an all-or-nothing daily batch design.
- **Generator-Based Streaming**: `GitHubClient.get()` yields records one page at a time via Python generators, keeping memory footprint at exactly 1 API page (~100 records) regardless of total volume. This prevents OOM on repos with 170K+ issues.
- **Chunked S3 Uploads**: Large paginated resources (issues, PRs, releases) are streamed directly to S3 in 5,000-record part files via `stream_records_to_s3()`, avoiding the need to hold the full dataset in memory.
- **Cooperative Throttling**: A tier-based system (`repo_config.py`) assigns API budget thresholds per repository size. Large repos trigger a 1-second cooperative slowdown before exhausting the shared rate limit, preventing starvation of smaller repos.
- **Per-Repo Watermarks**: Each repository maintains its own `since` timestamp via an individual Airflow Variable, enabling independent incremental extraction with no read-modify-write races.
- **GraphQL Batch Queries**: PR code-level details (additions, deletions, changed files) are extracted via batched GraphQL queries (15 PRs per request) with automatic fallback to individual queries when a batch contains a poisoned/deleted PR.
- **Lineage Envelope**: Every extracted record is wrapped with audit metadata (run ID, timestamp, API status, SHA-256 checksum, S3 URI) at the extraction boundary for full traceability.

### Bronze Layer (Databricks)

- **Registry-Driven Loading**: A single generic loader iterates a metadata registry (`BRONZE_REGISTRY`) to load all 6 resource types. Adding a new endpoint requires only a config entry — zero code changes.
- **Idempotent Writes**: Uses Delta `replaceWhere` on the `_ingestion_date` partition to ensure re-runs overwrite rather than append.
- **Serverless Compatible**: All PySpark code avoids RDD APIs and uses DataFrame-native operations for compatibility with Databricks Serverless Compute.

### Silver Layer (dbt)

- **Staging Views**: Pure SQL views that flatten nested JSON, typecast, and deduplicate. No business logic — just structural cleanup.
- **Intermediate Validation**: A reusable Jinja macro (`evaluate_dq_rules`) evaluates data quality rules and tags records with `is_quarantined` and `dq_failed_rules`. Quarantined records are excluded from SCD2 snapshots but remain visible in Silver marts.
- **SCD2 Snapshots**: `dbt snapshot` tracks historical changes for repositories, issues, and PRs using a timestamp-based strategy.
- **Current-State Views**: Silver marts provide `_current` views that UNION clean snapshot records with quarantined intermediate records for full auditability.

### Gold Layer (dbt)

- **Kimball Star Schema**: Dimension tables (`dim_repositories`, `dim_users`, `dim_date`) + accumulating snapshot fact tables (`fact_issues`, `fact_pull_requests`) + transactional facts (`fact_releases`).
- **Point-in-Time Joins**: Facts join against `dim_repositories` using the event timestamp within the dimension's SCD2 validity window.
- **KPI Aggregations**: Pre-computed KPI tables power the dashboard without requiring complex BI-layer SQL.

### Presentation Layer

- **DuckDB over S3**: Gold KPI tables are exported to S3 as Parquet files. The Streamlit dashboard queries them via DuckDB's `httpfs` extension, providing sub-second OLAP performance without a running warehouse.
- **Zero Warehouse Cost**: Dashboard viewers never trigger Databricks compute — all queries are served by an in-memory DuckDB engine reading S3 Parquet files.

---

## Dashboard

> **🔗 Live Demo: [repository-data-pipeline.streamlit.app](https://repository-data-pipeline.streamlit.app/)**

The Streamlit dashboard provides four interactive views:

| Tab | Description | Visualizations |
|---|---|---|
| **Repo Health** | High-level health metrics across all repositories | Stacked bar charts (issues/PRs open vs closed), contributor comparison |
| **Trends** | Monthly activity over time | Area charts for issues opened/closed, PRs opened/merged; per-repo breakdown |
| **PR Complexity** | Pull request size and review analysis | Donut chart (size distribution), scatter plot (lines vs merge time), histogram |
| **Community** | Contributor leaderboard and engagement | Top 10 tables, horizontal bar charts for PRs merged and lines added |

**Features:**
- Repository-level filtering via sidebar dropdown
- Responsive dark theme with glassmorphism design
- Curated color palette (indigo, violet, teal, amber, rose)
- Dynamic repo list populated from the data (auto-updates when repos are added)

---

## Data Quality

The pipeline implements a multi-layered data quality strategy:

### Intermediate Layer (Quarantine)
The `evaluate_dq_rules` macro applies declarative DQ rules per entity:

```sql
{{ evaluate_dq_rules([
    {'name': 'issue_id_not_null',           'expr': 'issue_id IS NOT NULL'},
    {'name': 'state_valid',                 'expr': "state IN ('open', 'closed')"},
    {'name': 'created_before_updated',      'expr': 'created_at <= updated_at'},
    {'name': 'closed_at_after_created',     'expr': 'closed_at IS NULL OR closed_at >= created_at'},
]) }}
```

Failing records are flagged (`is_quarantined = true`) with the specific failed rules captured in `dq_failed_rules` (array of strings). These records are:
- **Excluded** from SCD2 snapshots (preventing bad data from entering history)
- **Included** in Silver `_current` marts (preserving visibility for debugging)
- **Excluded** from Gold (clean presentation layer guarantee)

### dbt Tests
- **Schema Tests**: `unique`, `not_null`, `relationships` on all primary/foreign keys
- **Singular Tests**: Custom assertions like `assert_one_current_row_per_issue` to validate snapshot integrity
- **dbt-fusion Compatible**: All test configurations use strict `arguments:` YAML syntax

---

## Key Design Decisions

This project contains 17 documented Architecture Decision Records (ADRs) in [`docs/Architecture.md`](docs/Architecture.md). Key highlights:

| # | Decision | Rationale |
|---|---|---|
| 1 | Airflow as orchestrator only (no business logic) | Enables local testing without Airflow; prevents vendor lock-in |
| 2 | S3 as canonical data lake (no Databricks Volumes) | Compute-engine agnostic; no storage duplication |
| 4 | Dynamic Task Mapping with `max_active_tasks=3` | Parallel repos without exhausting the 5K req/hr API budget |
| 5 | Tier-based cooperative API throttling | Prevents large repos from starving smaller ones |
| 6 | Per-repo scalar watermarks | Atomic writes; no read-modify-write race conditions |
| 7 | Least-privilege IAM (no `s3:DeleteObject` for ingestion) | Protects data lake from accidental deletions |
| 9 | Metadata-driven Bronze registry | Zero-code extensibility for new resource types |
| 11 | DQ quarantine before SCD2 snapshots | Bad data never enters historical state |
| 14 | Accumulating snapshot facts + point-in-time dim joins | Simplifies time-to-close/merge BI queries |
| 16 | GraphQL batch queries with individual fallback | 15× fewer API calls; resilient to poisoned PRs |

---

## Known Limitations

- **Hard Deletes**: GitHub entities that are hard-deleted after initial extraction are not captured by the incremental `since` parameter. These will appear as permanently open/active in downstream models.
- **Single PAT**: The pipeline uses a single GitHub PAT (5,000 req/hr). For significantly higher throughput, round-robin multiple PATs would be needed.
- **No CDC**: The pipeline relies on polling the API daily. Real-time event processing via GitHub Webhooks is not implemented.

---

## Future Enhancements

- Astronomer Cosmos integration for per-model Airflow task visibility
- GitHub Webhooks for near-real-time ingestion
- Multi-PAT round-robin for higher API throughput
- Great Expectations integration for richer DQ reporting

---

## License

This project is licensed under the MIT License — see [LICENSE](LICENSE) for details.

**Copyright © 2026 Krrish Sethiya**
