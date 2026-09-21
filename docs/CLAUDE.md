# CLAUDE.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

## 5. Project-Specific Context (GitHub Repository Data Analysis)

**Architecture Principles:**
- **Separation of Concerns:** Airflow handles orchestration only (thin `PythonOperator`s). Business logic lives in testable Python modules (`ingestion/`, `databricks/`).
- **Storage Decoupling:** S3 is the canonical raw data lake. Databricks reads directly from S3 via External Locations (no internal volume duplication).
- **Idempotency:** Re-runs for a given date replace the data at the S3 location (via partition overwrite or `replaceWhere` in Databricks) rather than duplicating rows.
- **Data Organization:** "Organize by how you query, not by how you ingest." The S3 `raw/` prefix is partitioned by resource type and date (`raw/{resource_type}/ingestion_date=.../{repo}.json`), making downstream scanning highly efficient.
- **Rate Limit Budgeting:** GitHub API rate limits (5,000 req/hr) are a hard constraint. The pipeline uses cooperative throttling in the `GitHubClient`, Airflow Dynamic Task Mapping concurrency limits (`max_active_tasks=3`), and per-repo watermark tracking to survive API exhaustion and resume cleanly.
- **Streaming Extraction:** `GitHubClient.get()` is a generator that yields records one at a time (100 records per API page). The extractor streams these directly into multipart S3 uploads via `boto3`, so memory footprint is exactly 1 page regardless of repo size and no local disk tempfiles are needed.
- **Idempotent Watermarks:** `since` parameters (watermarks) only advance *after* raw data successfully lands in the Databricks Bronze layer, avoiding data loss if the ingest/load job crashes midway.
- **Watermark Overlap:** API timestamp queries use a 10-minute overlap window to gracefully capture delayed replica syncs from GitHub's eventual consistency. Each parallel task reads/writes only its own Variable, eliminating read-modify-write race conditions.
- **Security:** Credentials must never be hardcoded or logged. Use Airflow Connections/Variables in production, and a `.env` file strictly for local development testing. IAM policies are explicitly least-privilege but must include `s3:DeleteObject` to allow Airflow to purge failed parts on retry for idempotency.

**Bronze Layer (Medallion Architecture):**
- **Metadata-Driven:** All 5 resource types (repositories, issues, pull_requests, releases, languages) are defined in `BRONZE_REGISTRY` (`databricks/bronze/bronze_config.py`). Adding a new resource = one config entry, zero code changes.
- **No Business Logic in Bronze:** Bronze is a faithful landing zone. No DQ checks, no transformations, no filtering. Data Quality walls belong at Silver and Gold only.
- **Audit Lineage Columns:** Every Bronze row carries `_bronze_ingest_ts`, `_source_file`, `_raw_s3_uri`, `_extraction_run_id`, `_ingestion_date`, `_payload_checksum`.
- **External Tables:** Bronze tables are registered as Unity Catalog external tables with explicit `LOCATION` clauses pointing at S3 `bronze/` prefixes. Never use managed tables.

**Silver Layer (Medallion Architecture):**
- **Pipeline Flow:** Staging (views) → Intermediate (validation/incrementality) → Snapshots (SCD2 history) → Silver Marts (presentation).
- **Data Quality Framework:** A centralized macro (`evaluate_dq_rules`) applies rule checks in the `intermediate` layer. It generates `dq_failed_rules` (array) and `is_quarantined` (boolean).
- **Quarantine Handling:** Bad records (`is_quarantined = true`) are strictly blocked from entering the SCD2 `silver_snapshots`. However, the final `silver_*_current` marts perform a `UNION ALL` of clean snapshot rows and quarantined intermediate rows so analysts can see the bad data without corrupting the historical tracking.
- **Staging Simplicity:** Staging models are purely lightweight `VIEW`s (JSON flattening, typecasting). All incremental high-water-mark logic lives in the `intermediate` layer.
- **External Tables:** Silver marts are also registered as Unity Catalog external tables via a dbt `post-hook` macro (`create_silver_external_table.sql`).

**Gold Layer (Medallion Architecture):**
- **Star Schema:** Dimensions (`dim_`) use Type 1 full-refresh `table` materializations. Facts (`fact_`) use `incremental` materialization with `unique_key` to support Delta `MERGE` (Accumulating Snapshot pattern).
- **Data Quality Barrier:** Gold models MUST select only from clean Silver snapshots (`where dbt_valid_to is null`). They must never read from the `silver_*_current` views, which contain quarantined records.
- **Custom Schema Generation:** Gold tables are forced into the `gold` schema using a custom `generate_schema_name.sql` macro to override dbt's default behavior of appending `_gold` to the target schema.

**Databricks Serverless Constraints (MUST follow):**
- **No RDDs:** Serverless runs Spark Connect which bans all `.rdd` operations. Use DataFrame-native APIs only (e.g., `df.isEmpty()` not `df.rdd.isEmpty()`).
- **No `input_file_name()`:** Unity Catalog blocks this function. Use `col("_metadata.file_path")` instead.
- **Jobs API 2.1 `tasks` array:** Serverless requires multi-task format in `runs/submit`. Never use the legacy single `notebook_task` at root level.
- **Databricks Repos for code delivery:** Scripts are pulled via Databricks Repos (user manually syncs), not uploaded via DBFS or Volumes.

**Airflow-Databricks Integration:**
- **Connection:** `databricks_default` with `conn_type=databricks`, `login=token`, `password=<PAT>`. Set via Airflow CLI or UI, not `.env` (environment variable overrides are fragile with Docker).
- **Variables:** `databricks_notebook_path`, `databricks_catalog`, `databricks_schema` stored in Airflow's internal database via CLI (`airflow variables set`).
- **Operator:** `DatabricksSubmitRunOperator` with `tasks` array structure for Serverless compatibility.

**Airflow-dbt Integration (Silver/Gold layers):**
- **Dependency:** `dbt-databricks` and `databricks-sdk` are installed directly into the custom Airflow image (`airflow/Dockerfile`).
- **Execution:** dbt runs natively inside the Airflow worker container via a `BashOperator` wrapping `dbt build`. This keeps orchestration simple (Phase 1) rather than mapping individual models (Cosmos) or delegating to Databricks Jobs.
- **Environment Context:** Environment variables (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `S3_BUCKET_NAME`) are securely mapped from the root `.env` via Docker Compose's `env_file`, preventing Compose-parsing evaluation bugs and ensuring the `BashOperator` resolves them transparently.

**Hard Deletes Limitation:**
- **Gotcha:** Incremental extraction uses the `since` API parameter for issues and PRs. If a record is hard-deleted on GitHub, the `since` delta will never see it.
- **Consequence:** Your Silver SCD2 (`silver_snapshots`) will show that issue/PR as active/open forever. Acknowledge this limitation in downstream dashboards, or schedule periodic full-refresh reconciliations.

**S3 Streaming & Watermark Integrity:**
- Records are streamed directly to S3 via `boto3` — there are no local temporary files. Ensure S3 multi-part uploads are properly handled on failure to prevent incomplete parts from lingering.
- The extraction boundary timestamp is captured *before* extraction begins and committed as the watermark only *after* Bronze loading succeeds. This ensures the watermark represents the actual source data boundary, not the time of a later task.
- Never advance watermarks before downstream processing (Bronze) has confirmed success. A failed Bronze load must not result in skipped data on the next run.

**dbt-fusion Compatibility:**
- When defining YAML generic tests (e.g. `relationships`), always nest parameters (like `to`, `field`) under an `arguments:` dictionary to support `dbt-fusion`'s strict parsing mode (resolves `dbt1159`).
