# GitHub Data Model Documentation

This document provides an in-depth analysis of the data model for the GitHub Repository Data Analysis pipeline. It covers the overall architecture, layer-by-layer design decisions, table definitions, granularities, and the specific business questions this model is designed to answer.

## 1. Overall Architecture & Design Decisions

The pipeline uses a multi-hop (Medallion) architecture implemented via dbt:

*   **Bronze / Raw**: Raw JSON data extracted from the GitHub API and stored in S3, loaded natively via Databricks Unity Catalog external locations.
*   **Staging**: Lightweight SQL views that flatten nested JSON, typecast columns, and deduplicate records.
*   **Intermediate**: A validation layer that applies Data Quality (DQ) rules. Records failing DQ checks are flagged as `is_quarantined = true` and do not enter the historical SCD2 snapshots, but are kept for visibility. High-water-mark incremental logic is handled here.
*   **Silver**: Clean, current-state tables and SCD2 snapshots of historical states for entities like repositories, issues, and pull requests.
*   **Gold**: A Kimball-style star schema optimized for BI and analytics. Uses Type 1 and Type 2 dimensions and accumulating snapshot fact tables to simplify querying.

### Key Design Decisions
*   **Accumulating Snapshot Fact Tables**: Instead of tracking every event (opened, merged, closed) as separate rows, `fact_issues` and `fact_pull_requests` maintain a single row per entity that updates as it progresses through its lifecycle. This makes aggregations like "time to merge" drastically simpler.
*   **Current-State Joins**: Fact tables join to `dim_repositories_current` (the current-state slice of the SCD2 dimension) via a simple equi-join on `repository_id`. The full SCD2 history is retained in `dim_repositories` for analytical queries that need it. This approach keeps fact-dimension joins simple while still preserving historical repository metadata. These joins associate facts with the latest observed repository state — not a complete reconstruction of GitHub's historical metadata.
*   **Data Quality Quarantining**: Bad data is flagged and quarantined in Silver, guaranteeing that the Gold presentation layer only serves trustworthy, clean data.
*   **End-to-End Lineage Tracking**: Every layer retains ingestion timestamps (`_bronze_ingest_ts`, `_gold_update_ts`) and the unique `_extraction_run_id`. This allows precise auditing of when a specific data point was ingested from the API and transformed into Gold.
*   **GraphQL Code-Level Enrichment**: While core issues and pull requests come from the REST API, `fact_pull_requests` is heavily enriched with code-level details (additions, deletions, changed files) extracted via batched GraphQL queries.
*   **Hard Deletes Limitation (Gotcha)**: Because the raw extraction relies on the `since` parameter for efficiency, entities that are hard-deleted on GitHub are not captured by the delta. Downstream SCD2 snapshots will represent these entities as permanently open/active.

---

## 2. Gold Layer (Presentation / BI)

The Gold layer is designed for ease of use in Business Intelligence tools.

### `dim_repositories` (Type 2 SCD Dimension)
*   **Description**: Tracks the historical metadata of GitHub repositories (e.g., stars, forks, descriptions). Preserves full history of observed changes.
*   **Grain**: One row per repository per historical state (determined by snapshot).
*   **Key Columns**:
    *   `repository_sk`: Surrogate key (`dbt_scd_id`).
    *   `repo_id`: Natural key from GitHub.
    *   `valid_from`, `valid_to`: Defines the active period of this specific state.
    *   `is_current`: Boolean indicating the current active record.

### `dim_repositories_current` (Current-State Repository Dimension)
*   **Description**: The current active state of each repository (`dbt_valid_to IS NULL`). Fact tables join to this table on `repository_id` for stable identity resolution.
*   **Grain**: One row per unique repository.
*   **Key Columns**:
    *   `repository_sk`: Surrogate key (MD5 of `repo_id`).
    *   `repo_id`: Natural key from GitHub.
    *   `repo_name`, `owner_login`, `primary_language`, `license_name`.

### `dim_users` (Type 1 Dimension)
*   **Description**: Details about GitHub users who author issues, PRs, or releases. Overwritten with the latest state (no history tracking).
*   **Grain**: One row per unique user.
*   **Key Columns**:
    *   `user_sk`: Surrogate key (Primary).
    *   `user_id`: Natural key from GitHub.

### `dim_date` (Static Dimension)
*   **Description**: Standard date dimension for temporal slicing and dicing.
*   **Grain**: One row per calendar date.

### `fact_issues` (Accumulating Snapshot Fact)
*   **Description**: Fact table tracking the lifecycle of GitHub issues.
*   **Grain**: One row per issue.
*   **Questions Answered**:
    *   What is the average time to close an issue?
    *   How many issues were opened vs. closed in a given month?
    *   Which repositories have the highest issue engagement (comments)?
*   **Key Columns**:
    *   `issue_sk`: Surrogate key (Primary).
    *   `repository_sk`: Foreign key to the current repository state (`dim_repositories_current`).
    *   `author_user_sk`: Foreign key to the user who opened the issue.
    *   `created_date_key`, `closed_date_key`: Foreign keys to `dim_date`.
    *   `state`: Current state of the issue (open, closed).
    *   `time_to_close_hours`: Calculated measure of resolution speed.
    *   `comment_count`: Measure of engagement.

### `fact_pull_requests` (Accumulating Snapshot Fact)
*   **Description**: Fact table tracking the lifecycle and complexity of pull requests.
*   **Grain**: One row per pull request.
*   **Questions Answered**:
    *   What is the average time to merge a PR?
    *   Are larger PRs (more additions/deletions) taking disproportionately longer to merge?
    *   How many files are typically changed in a merged PR?
*   **Key Columns**:
    *   `pull_request_sk`: Surrogate key (Primary).
    *   `repository_sk`: Foreign key to `dim_repositories`.
    *   `author_user_sk`: Foreign key to `dim_users`.
    *   `created_date_key`, `merged_date_key`, `closed_date_key`: Foreign keys to `dim_date`.
    *   `state`: Current state of the PR.
    *   `time_to_merge_hours`: Calculated measure of merge speed.
    *   `time_to_close_hours`: Calculated measure of close speed.
    *   `additions`, `deletions`, `changed_files`, `commits_count`: Measures of PR size/complexity.
    *   `comment_count`: Measure of review engagement.

### `fact_releases` (Transactional Fact)
*   **Description**: Fact table tracking repository releases.
*   **Grain**: One row per release event.
*   **Questions Answered**:
    *   How frequently are releases published?
    *   Who is publishing the most releases?
*   **Key Columns**:
    *   `release_sk`: Surrogate key (Primary).
    *   `repository_sk`: Foreign key to the current repository state (`dim_repositories_current`).
    *   `author_user_sk`: Foreign key to the user publishing the release.

---

## 3. Silver Layer (Clean & Curated)

The Silver layer provides current-state representations and historical snapshots. It is useful for data science and ad-hoc analytics that require raw forms but with quality guarantees.

*   **`repositories_current`**: Current-state view of repositories (clean snapshot records where `dbt_valid_to IS NULL` UNION ALL quarantined intermediate records).
*   **`issues_current`**: Current-state view of issues, explicitly filtering out PRs (`is_pull_request = false`, clean snapshot records UNION ALL quarantined intermediate records).
*   **`pull_requests_current`**: Current-state view of pull requests (clean snapshot records UNION ALL quarantined intermediate records).
*   **`releases`**: Curated releases table (clean incremental records UNION ALL quarantined intermediate records).
*   **`languages`**: Current unpivoted programming languages per repository, showing the breakdown of code bytes by language.

**Quality Columns** (Present on all Silver models):
*   `is_quarantined` (boolean): Flags records that failed one or more Data Quality rules.
*   `dq_failed_rules` (array): A list of the specific rules that failed, used for pipeline monitoring and alerting.

---

## 4. Intermediate & Staging Layers

### Intermediate (Validation)
This layer acts as a barrier protecting Silver/Gold from bad data.
*   Applies data quality (DQ) tests.
*   Executes high-water-mark logic to identify only the net-new or updated records since the last run.
*   Resolves identifiers (e.g., mapping `repository_url` to `repository_id`).

### Staging (Base)
*   Pulls data from the Bronze tables (raw JSON).
*   Flattens heavily nested arrays/structs into relational rows and columns.
*   Performs structural deduplication to handle API pagination overlaps.
*   Standardizes types (e.g., string to timestamp).
