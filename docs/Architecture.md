# Architecture Decision Records (ADR)

This document tracks the core design decisions for the GitHub Repository Data Analysis pipeline, including the rationale, trade-offs, and alternative approaches considered during development.

---

## 1. Orchestration vs. Execution Boundary

**Decision:** Airflow is strictly used for orchestration. All tasks are thin `PythonOperator` wrappers that call standalone, testable Python functions in the `ingestion/` and `databricks/` modules.

**Reason:** 
- Enables local testing of extraction and writing logic without running an Airflow environment.
- Prevents business logic from being tightly coupled to DAG definitions.
- Makes the codebase modular, versionable, and independently verifiable.

**Alternatives Discussed:** 
- Embedding transformation logic directly inside Airflow operators. This was rejected because it violates the "orchestrator is not an execution engine" best practice and makes unit testing significantly harder.

---

## 2. Storage and Compute Decoupling (S3 & Databricks)

**Decision:** Databricks reads `raw/` data directly from S3 via Unity Catalog External Locations. No data is duplicated into Databricks-managed Volumes.

**Reason:** 
- Keeps S3 as the canonical, decoupled data lake.
- Allows compute engines other than Databricks to read the raw data in the future without lock-in.
- Simplifies the architecture by removing intermediate "landing zone" Volume copy steps.

**Alternatives Discussed:** 
- Initially considered a bridge step where Airflow pushes to a Databricks Volume, and Bronze reads from the Volume. Rejected because Unity Catalog external locations make this redundant and add unnecessary storage duplication and I/O overhead.

---

## 3. S3 Folder Architecture

**Decision:** S3 `raw/` is organized by resource type and date, with the repository slug as the filename.
`raw/{resource_type}/ingestion_date={YYYY-MM-DD}/{repo_slug}.json`

**Reason:** 
- **Organize by query pattern:** Downstream Bronze tables query by resource type (e.g., `SELECT * FROM bronze.issues`). This layout allows a single prefix scan.
- Easily scalable to `N` repositories without changing the folder structure.

**Alternatives Discussed:**
- Grouping by repository first: `raw/{repo_slug}/{resource_type}/...`
- Rejected because reading all issues across 8 repositories would require 8 separate prefix scans (globs), harming read performance and complicating Databricks load logic.

---

## 4. Multi-Repo Scaling & Concurrency (Dynamic Task Mapping)

**Decision:** Use Airflow's Dynamic Task Mapping (`expand`) with `max_active_tasks=3` to process repositories in parallel, using a single GitHub PAT (5,000 req/hr). The `GitHubClient.get()` method uses a generator pattern (`yield`) to stream records one at a time — memory footprint is exactly 1 API page (100 records) regardless of total result size. The extractor streams these directly into S3 multipart uploads via `boto3`, completely bypassing the local file system.

**Reason:** 
- A serial loop would take too long for large backfills. Unbounded parallel tasks would instantly exhaust the GitHub API rate limit.
- Capping concurrency at 3 provides a sweet spot (avg ~1,650 req/hr per task) that allows the pipeline to make parallel progress while leaving enough headroom for cooperative throttling to manage the budget safely.
- The generator pattern prevents OOM kills: a repo like `microsoft/vscode` (170k issues) no longer loads gigabytes of JSON into RAM before writing to disk.

**Alternatives Discussed:** 
- **Serial loop:** Rejected because extracting 8 high-volume repositories (e.g., Kubernetes, VSCode) sequentially is too slow and doesn't utilize available bandwidth.
- **Returning full list from `get()`:** The original implementation accumulated all paginated records into a `list[dict]` before returning. Rejected because 3 concurrent tasks extracting large repos would OOM the Airflow worker container.
- **Round-robin multiple PATs:** Discussed as a future scaling option to get 10,000+ req/hr, but deemed unnecessary for Phase 1 if rate-limits are managed cooperatively.

---

## 5. Cooperative API Throttling

**Decision:** The `GitHubClient` implements a tier-based cooperative throttle. When the remaining rate limit drops below a threshold (determined by the repo's tier in `repo_config.py`), the client sleeps for 1 second between page requests. At `<= 5` requests remaining, it performs a hard sleep until the reset window.

**Reason:** 
- Prevents a single massive repository (e.g., `microsoft/vscode` with 170k issues) from starving sibling tasks of API budget during parallel execution.
- Shared-nothing architecture: each task inspects its own response headers and backs off independently without needing a distributed lock or Redis counter.

**Alternatives Discussed:**
- **Hard stops only:** Let the client run at full speed until hitting 0 budget, then sleep. Rejected because it leads to "stop-and-go" starvation where one large repo exhausts the budget while smaller, faster repos are blocked unnecessarily.

---

## 6. Granular Per-Repo Watermarks

**Decision:** State management uses per-repo scalar Airflow Variables (`watermark_{repo_slug}`, e.g. `watermark_apache_spark`) to track the `since` timestamp independently for each repository. Each Variable is a simple string value — no JSON parsing, no shared blob.

**Reason:**
- **Atomicity:** Airflow `Variable.set()` on a scalar key is atomic at the database level. With 3 parallel tasks, there is no read-modify-write race — each task writes only its own Variable.
- **Failure isolation:** If `kubernetes/kubernetes` fails halfway through a 4-hour backfill due to rate limits or network errors, other successfully completed repositories (e.g., `duckdb/duckdb`) still get their watermarks advanced.
- On the next run, the failed repository resumes its backfill, while successful ones only fetch the fast incremental delta.

**Alternatives Discussed:**
- **Shared JSON dictionary Variable (`extraction_watermarks`):** Used in the initial implementation. Rejected because parallel tasks performing read-modify-write on a single JSON blob create a race condition — Task B's write silently overwrites Task A's watermark update, causing repos to "forget" their progress and trigger redundant full backfills.
- **Single scalar timestamp:** Used in the initial 1-repo proof of concept. Rejected for multi-repo scaling because a failure in *any* repository would prevent the global watermark from advancing, causing successful repos to redundantly re-fetch data on the next run.

---

## 7. Least-Privilege IAM Policy

**Decision:** The Airflow ingestion IAM user has `s3:PutObject`, `s3:GetObject`, and explicitly **requires `s3:DeleteObject`** on `raw/*`.

**Reason:** 
- Ingestion must purge existing partial parts for a given `(resource, date, repo)` prefix before writing new parts during an Airflow retry. Without `DeleteObject`, retries would silently accumulate partial parts, causing duplicate data in downstream Bronze loads since Databricks reads the entire prefix.

**Alternatives Discussed:**
- **No `s3:DeleteObject` (Append-Only):** Enforces stricter least-privilege, but breaks idempotency on retries since Airflow cannot clean up its own failed uploads. Rejected because data correctness is paramount.

---

## 8. Databricks Serverless Compute Compatibility

**Decision:** All PySpark code avoids RDD operations and uses only DataFrame-native APIs. Specifically, `raw_df.isEmpty()` instead of `raw_df.rdd.isEmpty()`, and `col("_metadata.file_path")` instead of `input_file_name()`.

**Reason:**
- Databricks Serverless runs on Spark Connect, which completely disables the RDD API (`PySparkNotImplementedError: rdd is not implemented`).
- Unity Catalog blocks `input_file_name()` as a security measure to prevent leaking underlying cloud storage paths. The `_metadata.file_path` column is UC's sanctioned replacement.
- These constraints were discovered during production deployment and are non-negotiable on Serverless + UC.

**Alternatives Discussed:**
- Using classic (non-serverless) clusters to avoid these restrictions. Rejected because Serverless eliminates cluster management overhead and cold-start delays, which outweighs the minor API adjustments.

---

## 9. Metadata-Driven Bronze Registry

**Decision:** All Bronze resource types are defined in a single `BRONZE_REGISTRY` dictionary (`bronze_config.py`). Adding a new resource type requires one config entry and zero code changes.

**Reason:**
- Eliminates per-resource boilerplate (no `repositories.py`, `issues.py`, `pull_requests.py` — one `loader.py` handles them all).
- The registry maps each resource type to its raw S3 prefix, bronze S3 prefix, and Unity Catalog table name. The generic loader iterates the registry and processes each entry identically.
- Makes the pipeline trivially extensible — adding `commits` or `stargazers` later is a 5-line config addition.

**Alternatives Discussed:**
- One Python file per resource type with hardcoded paths. Rejected because it creates duplicated logic and increases the surface area for bugs when patterns change (e.g., adding a new audit column requires editing N files instead of one).

---

## 10. Airflow → Databricks Serverless Orchestration

**Decision:** Airflow triggers Databricks notebooks via `DatabricksSubmitRunOperator` using the Jobs API 2.1 `tasks` array format (multi-task structure), not the legacy single `notebook_task` format.

**Reason:**
- Databricks Serverless compute requires the `tasks` array structure in the `runs/submit` API. The legacy single-task format returns `INVALID_PARAMETER_VALUE: One of job_cluster_key, new_cluster, or existing_cluster_id must be specified`.
- The `tasks` array also future-proofs the pipeline for adding parallel Silver/Gold tasks within the same Databricks job submission.

**Alternatives Discussed:**
- Using `DatabricksRunNowOperator` with a pre-created job. Rejected for Phase 1 because `SubmitRunOperator` is simpler (no job setup in Databricks UI) and the notebook path + parameters are version-controlled in the DAG.

---

## 11. Silver Layer Validation & Data Quality Framework

**Decision:** The Silver layer introduces a dedicated `intermediate` validation layer between Staging and Snapshots/Marts. A centralized Jinja macro (`evaluate_dq_rules`) executes data quality checks, generating an `ARRAY<STRING>` of failed rules (`dq_failed_rules`) and a boolean flag (`is_quarantined`).

**Reason:**
- Prevents bad data from entering the SCD2 history (`silver_snapshots`), as snapshots filter out `is_quarantined = true`.
- Allows downstream analysts to see the quarantined records in the `silver_*_current` marts via a `UNION ALL` pattern, rather than dropping them silently.
- Centralizing DQ rules in a macro standardizes the approach and makes extending rules trivial.

**Alternatives Discussed:**
- **Silently dropping bad records:** Rejected because it removes visibility into ingestion or source API issues.
- **Putting DQ checks in Staging:** Rejected because staging should remain a pure, lightweight flattening/typing view layer over Bronze.
- **Using dbt tests to drop records:** dbt tests run *after* the tables are built and can alert, but they do not automatically quarantine rows mid-pipeline.

---

## 12. Staging Layer Simplicity

**Decision:** The `staging` layer consists solely of lightweight SQL `VIEW`s that flatten nested JSON, typecast columns, and deduplicate records. High-water-mark incremental logic was moved from staging to the `intermediate` layer.

**Reason:**
- Keeps the raw-to-clean plumbing completely separate from business logic (watermarks, DQ).
- Simplifies debugging: a staging view always shows the exact cleaned representation of the Bronze table.

**Alternatives Discussed:**
- **Incremental materialization in Staging:** Used initially but shifted to intermediate to ensure that any record evaluated for incrementality is also immediately subjected to DQ rules in the same physical table build.

---

## 13. Silver Layer Orchestration via dbt

**Decision:** The Silver layer models are orchestrated directly within the Airflow DAG using a `BashOperator` running `dbt build`, forming a complete end-to-end dependency chain: `extract_repo >> run_bronze_load >> dbt_build`.

**Reason:**
- Unifies the entire data pipeline (Extract -> Land -> Transform) into a single orchestration graph, providing a single pane of glass for monitoring and troubleshooting.
- Ensures the dbt transformation layer only runs when all upstream ingestion and Bronze loading tasks have succeeded, preventing partial or staggered data from bleeding into downstream reports.
- Keeps simplicity for Phase 1 by executing the dbt CLI inside the Airflow worker container (using local environment variables mapping to Databricks), deferring more complex remote execution (like dbt Cloud or Cosmos) until explicitly needed.

**Alternatives Discussed:**
- **Astronomer Cosmos:** Considered for mapping individual dbt models to Airflow tasks. Deferred to later phases to prioritize simplicity and keep the initial DAG lightweight with a single `dbt build` wrapper.
- **Databricks Workflows for dbt:** Considered running dbt as a Databricks Job task. Rejected because it fragments orchestration (Airflow for ingestion, Databricks for transformation), making failure tracking across the pipeline harder.

---

## 14. Gold Layer (Presentation) Architecture

**Decision:** The Gold layer uses a Kimball-style star schema. Dimension tables (`dim_*`) are materialized as full-refresh `table`s (Type 1 overwrites). Fact tables (`fact_*`) are materialized as `incremental` with a `unique_key` to enable Delta `MERGE` (Accumulating Snapshot pattern).

**Reason:**
- **Accumulating Snapshots:** GitHub events (Issues, PRs) have multiple lifecycle stages (opened, merged, closed). Instead of a transactional fact table appending every state change, an accumulating snapshot incrementally updates a single row per entity as it progresses, making downstream BI queries significantly simpler and faster.
- **Data Quality Barrier:** Gold models read strictly from the Silver SCD2 snapshots (`where dbt_valid_to IS NULL`). Quarantined records in the Silver intermediate layer are entirely hidden from Gold, guaranteeing presentation-layer integrity.
- **Delta Efficiency:** The `incremental` materialization maps natively to Databricks Delta `MERGE`, allowing fast upserts of mutating GitHub entities without rewriting the entire dataset.

**Alternatives Discussed:**
- **Transactional Fact Tables:** Rejected because tracking every GitHub event state change as an immutable ledger makes BI aggregations (e.g., "time to merge") extremely complex to write.
- **SCD2 Dimensions in Gold:** We implemented Type 2 SCD tracking for `dim_repositories` based on a `valid_from` and `valid_to` snapshot strategy. Fact tables (`fact_issues` and `fact_pull_requests`) perform **Point-in-Time Joins** against the `dim_repositories` table. This guarantees that an issue or PR is linked to the repository's exact metadata state (e.g., its star count or name) precisely at the time the event occurred (`created_at` or `updated_at` within the validity window).

## 15. S3 Tempfile Extraction Integrity
**Decision:** All local temporary files used for accumulating records before S3 upload are wrapped in `with` context managers and explicitly specify `encoding="utf-8"`. The repository watermark is advanced **before** the final S3 upload loop.
**Reason:**
- Prevents file descriptor leaks if the disk fills up or the API crashes mid-extraction.
- Enforcing UTF-8 ensures emojis or non-standard characters in GitHub issues/PRs don't crash the pipeline on servers with a different default locale.
- Saving the watermark *before* S3 upload ensures that if an AWS S3 API failure (e.g. 503) crashes the task, the next Airflow retry will not redundantly hammer the GitHub API to re-download the data. It will simply retry the upload (or, in the current design, skip safely since `since` is already advanced).

## 16. GraphQL Batch Query Fallback
**Decision:** The `pr_details` extraction batches 15 pull requests into a single GraphQL query using aliases. If the batch query fails (e.g., due to a single "poisoned" or deleted PR causing a resolution error), the extractor falls back to querying the 15 PRs individually in a loop.
**Reason:**
- Batching drastically improves performance and reduces rate-limit consumption for code-level details.
- The fallback mechanism ensures that one corrupted PR doesn't cause the pipeline to drop the other 14 valid PRs in the chunk.

## 17. dbt-fusion Compatibility
**Decision:** All dbt YAML configurations (like generic tests) use the strict `arguments:` syntax for passing properties (e.g., `to` and `field` for `relationships` tests) instead of top-level keys.
**Reason:**
- Ensures compatibility with `dbt-fusion` (dbt 2.0+) strict YAML validation mode.
