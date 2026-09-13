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

**Decision:** Use Airflow's Dynamic Task Mapping (`expand`) with `max_active_tasks=3` to process repositories in parallel, using a single GitHub PAT (5,000 req/hr).

**Reason:** 
- A serial loop would take too long for large backfills. Unbounded parallel tasks would instantly exhaust the GitHub API rate limit.
- Capping concurrency at 3 provides a sweet spot (avg ~1,650 req/hr per task) that allows the pipeline to make parallel progress while leaving enough headroom for cooperative throttling to manage the budget safely.

**Alternatives Discussed:** 
- **Serial loop:** Rejected because extracting 8 high-volume repositories (e.g., Kubernetes, VSCode) sequentially is too slow and doesn't utilize available bandwidth.
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

**Decision:** State management uses a JSON dictionary Airflow Variable (`extraction_watermarks`) to track the `since` timestamp independently for each repository slug.

**Reason:**
- **Failure isolation:** If `kubernetes/kubernetes` fails halfway through a 4-hour backfill due to rate limits or network errors, other successfully completed repositories (e.g., `duckdb/duckdb`) still get their watermarks advanced.
- On the next run, the failed repository resumes its backfill, while successful ones only fetch the fast incremental delta.

**Alternatives Discussed:**
- **Single scalar timestamp:** Used in the initial 1-repo proof of concept. Rejected for multi-repo scaling because a failure in *any* repository would prevent the global watermark from advancing, causing successful repos to redundantly re-fetch data on the next run.

---

## 7. Least-Privilege IAM Policy

**Decision:** The Airflow ingestion IAM user has `s3:PutObject` and `s3:GetObject` on `raw/*`, but explicitly **lacks `s3:DeleteObject`**.

**Reason:** 
- Ingestion only ever appends new files or overwrites existing ones (idempotency). It should never delete data.
- Protects the data lake from accidental recursive deletions via Airflow bugs. Housekeeping (`VACUUM`/`OPTIMIZE`) is the responsibility of Databricks credentials on Bronze/Silver, not Airflow ingestion.

**Alternatives Discussed:**
- **Full S3 Admin access:** Easier to setup and allows automated cleanup. Rejected because it violates security best practices and increases the blast radius of a compromised or buggy ingestion script.
