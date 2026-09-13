# Phase 1 — Setup, Credentials & Ingestion Pipeline

## GitHub Repository Analytics Platform

---

## 0. Scope of This Phase

This phase covers everything needed to go from zero to:

```
GitHub API → Python (Airflow-orchestrated) → S3 raw zone → Databricks Bronze table
```

with full audit/lineage metadata at every hop, idempotent re-runs, and structured logging.

This is a broader Phase 1 than the original master context draft (which stopped at "one repository, raw JSON, local"). You explicitly asked to fold in Airflow setup, credentials, and the S3 → Bronze hop, so that's what's covered here. dbt, Silver, Gold, DuckDB, and Streamlit remain out of scope for this document.

---

## 1. Explicit Assumptions

State these back to me if any are wrong — everything below is built on top of them.

1. **Databricks already has a Unity Catalog External Location configured against your S3 bucket** (storage credential + external location, read/write). This removes the Volume-bridge workaround from the earlier draft entirely — Bronze reads `s3://` directly. One thing I'm assuming and flagging: the external location's registered path covers the `raw/` prefix (or the whole bucket) that ingestion writes to. If it's scoped to a narrower or different prefix, Section 5.1's folder layout needs to match it exactly, or the read will fail on a permissions/path mismatch rather than a missing-file error — worth a 30-second check in Databricks (Catalog → External Locations) before the first run.
2. **AWS account is ready** — you have permissions to create S3 buckets and IAM users/policies.
3. **Repo list for Phase 1** — start with **one repository** (`apache/spark`) to prove the full path end-to-end, then widen to the 3–5 repo list from the master context once Bronze is verified. If you'd rather start with all 3–5 immediately, say so — the code doesn't change, just the `REPO_LIST` variable.
4. **Airflow runs via `docker-compose`** using the official Apache Airflow image, **LocalExecutor** (single machine, no Celery/Redis needed at this scale — simpler to run and to explain in an interview).
5. **Airflow does orchestration only.** No PySpark, no pandas transformation logic inside operators. Every task is a thin `PythonOperator` that imports and calls a function from `ingestion/` or triggers a Databricks job — the actual logic lives in versioned, independently-testable Python modules, per your instruction.
6. **Naming convention = snake_case** everywhere (Python, SQL, S3 keys, Delta columns, Airflow task/dag ids).
7. **GitHub auth** — a single Personal Access Token is enough for Phase 1 (no GitHub App needed at this scale).
8. **S3 stays the canonical raw data lake, and storage/compute stay decoupled** — Databricks reads `s3://` directly through the external location rather than owning a copy of the data. This is the cleaner version of the decoupling you asked for in the original architecture discussion: no data physically duplicated into Databricks-managed storage at all, so replaying Bronze from scratch is just re-pointing a read at the same S3 prefix.

---

## 2. Accounts & Credentials Checklist

| # | Credential | Where it's used | How to get it |
|---|-----------|------------------|----------------|
| 1 | GitHub Personal Access Token (fine-grained, **no special scopes needed** for public repo metadata — it only exists to raise your rate limit) | `github_client.py` | GitHub → Settings → Developer settings → Fine-grained tokens → Generate new token → no repository access needed beyond "Public Repositories (read-only)" |
| 2 | AWS Access Key ID + Secret Access Key (for an **IAM user**, not your root account) | Airflow's S3 connection, boto3 in `s3_writer.py` | IAM → Users → create `github-analytics-ingestion` user → attach the least-privilege policy in Section 5 → generate access key |
| 3 | S3 bucket name + region | `.env`, Airflow Variables | You create this (Section 5) |
| 4 | Databricks workspace URL | Airflow's Databricks connection, REST/SDK calls | Workspace you already have set up |
| 5 | Databricks Personal Access Token | Same as above | Databricks workspace → user icon → Settings → Developer → Access tokens → Generate |
| 6 | Unity Catalog catalog / schema names, and the External Location name your S3 access already runs through | Bronze notebook | Catalog/schema you create (Section 6); external location you confirm already exists |

**Rate limit note:** unauthenticated GitHub API calls are capped at 60/hour; an authenticated token gets you 5,000/hour. For 3–5 repos with issues/PRs/releases/languages, you want the token from day one.

---

## 3. Secrets Management — Where Each Credential Lives

**Never hardcode credentials in DAG files or committed code.** Two layers:

### 3.1 Local development — `.env` (git-ignored)

```bash
# .env — copy from .env.example, never commit this file
GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx

AWS_ACCESS_KEY_ID=AKIAXXXXXXXXXXXXXXXX
AWS_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
AWS_REGION=ap-south-1
S3_BUCKET_NAME=github-repository-analytics

DATABRICKS_HOST=https://your-workspace-instance.cloud.databricks.com
DATABRICKS_TOKEN=dapixxxxxxxxxxxxxxxxxxxxxxxxxxxx
DATABRICKS_CATALOG=github_analytics
DATABRICKS_SCHEMA=bronze
```

`.env.example` should list the same keys with empty/placeholder values so the repo documents what's required without leaking anything.

### 3.2 Airflow — Connections & Variables (not `.env`)

Airflow containers don't read your host `.env` automatically — set these explicitly once the stack is up, via the Airflow UI (Admin → Connections / Variables) or `airflow connections add` / `airflow variables set` CLI:

**Connections:**

| conn_id | type | fields |
|---|---|---|
| `aws_default` | Amazon Web Services | Access Key ID, Secret Access Key, Region |
| `databricks_default` | Databricks | Host = workspace URL, Token = PAT |

**Variables:**

| key | example value |
|---|---|
| `repo_list` | `["apache/spark"]` (JSON list) |
| `s3_bucket_name` | `github-repository-analytics` |
| `databricks_catalog` | `github_analytics` |
| `databricks_schema` | `bronze` |

This separation matters: your Python modules (`extractor.py`, `s3_writer.py`) should accept these as **function arguments**, not read `os.environ` directly — Airflow tasks pass them in from Connections/Variables, while local testing passes them in from `.env`. Same function, two callers. That's the DRY boundary.

---

## 4. Repository Skeleton for Phase 1

Only the pieces relevant to this phase (full structure lives in the master context):

```text
github-repository-analytics/
├── .env.example
├── .gitignore
├── requirements.txt
│
├── ingestion/
│   ├── __init__.py
│   ├── config.py           # loads env vars / accepts explicit args
│   ├── logger.py           # shared structured logging setup
│   ├── github_client.py    # auth, pagination, retries, rate-limit handling
│   ├── extractor.py        # orchestrates fetch per repo/resource
│   ├── normalizer.py       # attaches lineage metadata, schema shape
│   └── s3_writer.py        # writes partitioned raw JSON to S3
│
├── databricks/
│   └── bronze/
│       └── repositories.py # PySpark: S3 (external location) → Bronze Delta table
│
├── airflow/
│   ├── docker-compose.yaml
│   └── dags/
│       └── github_repository_analytics.py
│
└── tests/
    ├── test_github_client.py
    ├── test_normalizer.py
    └── fixtures/
```

---

## 5. AWS S3 Setup

### 5.1 Bucket & folder structure

Bucket already provisioned: `reddit-community-data-store`, with top-level `raw/`, `bronze/`, `silver/`, `gold/` prefixes already in place — a flatter layout than the master context's original sketch (that one nested bronze/silver under a `processed/` prefix; yours doesn't, and that's fine — Bronze and Silver will be registered as external Delta tables pointing at `s3://reddit-community-data-store/bronze/...` and `.../silver/...` directly).

One thing worth a sentence, not a blocker: the bucket name reads like it's from a different project (Reddit vs. GitHub). If it's a general-purpose data bucket you're reusing on purpose, no issue — just flagging in case it was meant to be renamed and slipped through.

```text
s3://reddit-community-data-store/
  raw/
    repositories/
      ingestion_date=YYYY-MM-DD/
        apache_spark.json
    issues/
      ingestion_date=YYYY-MM-DD/
    pull_requests/
      ingestion_date=YYYY-MM-DD/
    releases/
      ingestion_date=YYYY-MM-DD/
    languages/
      ingestion_date=YYYY-MM-DD/
  bronze/
    repositories/
    issues/
    pull_requests/
    releases/
    languages/
  silver/
    (Phase 5+)
  gold/
    (Phase 6+)
```

`ingestion_date=` partitioning stays under `raw/`, per the master context. `bronze/` gets its own per-table sub-prefixes since each Bronze table will be an external Delta table backed by its own S3 location.

### 5.2 IAM policy (least privilege) — for the ingestion user, separate from Databricks' access

This is a **different credential** from the Databricks storage credential behind the external location (which you've confirmed has full read/list/write/delete access — that's normal and needed, since Delta operations like `OPTIMIZE`/`VACUUM` require delete permissions). The ingestion-side IAM user only ever writes to `raw/`, so keep it narrow regardless of what Databricks' credential is allowed to do — least privilege per credential, not just per bucket.

Attach this to the `github-analytics-ingestion` IAM user, scoped to the `raw/` prefix only:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowListRawPrefix",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::reddit-community-data-store",
      "Condition": { "StringLike": { "s3:prefix": ["raw/*"] } }
    },
    {
      "Sid": "AllowObjectReadWriteUnderRaw",
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject"],
      "Resource": "arn:aws:s3:::reddit-community-data-store/raw/*"
    }
  ]
}
```

No `s3:DeleteObject`, and no access outside `raw/` — ingestion should never be able to touch `bronze/`, `silver/`, or `gold/`. If a re-run needs to replace a partition, that's an explicit, logged overwrite of the same key, not a delete-then-write.

---

## 6. Databricks Setup

### 6.1 What's already in place

The External Location test connection came back clean — Read, List, Write, Delete, Path Exists, File Events Read, Assume Role, Self Assume Role, and External ID Condition all passed, with full bucket access on `reddit-community-data-store`. That's everything a Bronze job needs and then some (delete is there for `OPTIMIZE`/`VACUUM` housekeeping later, not something Bronze writes will use directly). Pipeline is now simply:

```
S3 raw/  (written by Airflow)  →  Bronze notebook reads s3:// directly  →  Bronze Delta table, written to S3 bronze/
```

No Volume, no bridge step, no landing copy.

### 6.2 One-time setup (via a notebook or SQL editor in the workspace)

```sql
CREATE CATALOG IF NOT EXISTS github_analytics;
CREATE SCHEMA IF NOT EXISTS github_analytics.bronze;
```

### 6.3 External tables, not managed tables

Since `bronze/` already exists as its own S3 prefix and the goal is to keep storage and compute decoupled, register Bronze tables as **external** Delta tables pointing at that prefix explicitly, rather than letting `saveAsTable` default to Databricks-managed storage under the metastore. Concretely: write with `.save(s3_path)` and register the table with a `LOCATION` clause (shown in Section 9.1) instead of a plain `saveAsTable`. That way the Delta files live at `s3://reddit-community-data-store/bronze/repositories/` regardless of which compute wrote them — any engine, not just this Databricks workspace, could read them later. This is the same decoupling principle from the original architecture discussion, now applied concretely to Bronze.

---

## 7. Airflow Setup (Docker)

### 7.1 docker-compose

Use the official Apache Airflow `docker-compose.yaml` (pull the current stable one from Airflow's official docs — don't hand-roll it, the maintained version handles init containers, volumes, and healthchecks correctly). Key points to configure:

- `AIRFLOW__CORE__EXECUTOR: LocalExecutor`
- Mount your `dags/`, `logs/`, and `plugins/` folders as volumes
- Mount the `ingestion/` and `databricks/` packages into the container (or install them as an editable package via a custom `requirements.txt` baked into the image) so `PythonOperator` tasks can `import` them directly
- Do **not** bake `.env` secrets into the image — set Connections/Variables after the stack is up (Section 3.2)

### 7.2 DAG skeleton — orchestration only

```python
# airflow/dags/github_repository_analytics.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

from ingestion.extractor import extract_repository_metadata
from ingestion.s3_writer import write_raw_to_s3
from databricks.bronze.repositories import run_bronze_load

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="github_repository_analytics",
    schedule_interval="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    default_args=default_args,
    tags=["github", "analytics", "phase1"],
) as dag:

    def _extract_and_load(**context):
        repo_list = Variable.get("repo_list", deserialize_json=True)
        run_id = context["run_id"]
        for repo in repo_list:
            raw_payload, lineage = extract_repository_metadata(
                repo_full_name=repo,
                extraction_run_id=run_id,
            )
            write_raw_to_s3(raw_payload, lineage, bucket=Variable.get("s3_bucket_name"))

    extract_repositories = PythonOperator(
        task_id="extract_repositories",
        python_callable=_extract_and_load,
    )

    load_bronze = PythonOperator(
        task_id="run_databricks_bronze",
        python_callable=run_bronze_load,
    )

    extract_repositories >> load_bronze
```

Every task is a one- or two-line call into a tested module. The DAG file itself contains **zero business logic** — that's the "Airflow only orchestrates" boundary you asked for.

---

## 8. Ingestion Design (GitHub → S3)

### 8.1 `github_client.py` responsibilities

- Bearer token auth via the `Authorization: Bearer <token>` header
- Pagination (GitHub uses `Link` headers — follow `rel="next"` until absent)
- Rate-limit handling — inspect `X-RateLimit-Remaining`; if near zero, sleep until `X-RateLimit-Reset`
- Retry with exponential backoff on 5xx and network errors (not on 4xx — those are real errors, fail fast and log)
- Raise a clear, typed exception on repeated failure so Airflow retries/alerts correctly

### 8.2 Lineage metadata — attach this to every raw record at ingestion time

This is the audit trail your master context's data-quality section wants, made concrete:

| Field | Purpose |
|---|---|
| `extraction_run_id` | Airflow `run_id` (or a uuid when run locally) — ties every downstream row back to one pipeline execution |
| `ingestion_timestamp` | UTC timestamp when the record was fetched |
| `ingestion_date` | Partition key, derived from `ingestion_timestamp` |
| `source_endpoint` | Exact GitHub API URL called |
| `http_status` | Response status code |
| `api_rate_limit_remaining` | Value of `X-RateLimit-Remaining` at fetch time |
| `record_count` | Number of records in this payload (for volume checks downstream) |
| `payload_checksum` | SHA-256 of the raw JSON body — lets you detect if a "re-fetch" actually changed anything |

`normalizer.py` wraps the raw GitHub response in an envelope carrying these fields rather than mutating the payload itself — Bronze should still see the untouched API response plus this metadata alongside it.

### 8.3 `s3_writer.py`

Writes the envelope (raw payload + lineage metadata) as one JSON file per repo per resource per run, at the partition path from Section 5.1. **Idempotency rule:** if the same `(resource, repo, ingestion_date)` key is written twice in one day (e.g. DAG retry), overwrite the same S3 key rather than creating a second file — same key in, same key out, always.

---

## 9. S3 → Bronze Design

### 9.1 Bronze table (`databricks/bronze/repositories.py`)

No bridge step needed — the external location makes `s3://` a normal readable path from a Databricks notebook or job. Following the medallion contract — Bronze never transforms, only lands with metadata:

```python
from pyspark.sql.functions import current_timestamp, col

def run_bronze_load(s3_raw_path: str, s3_bronze_path: str, bronze_table: str, ingestion_date: str, spark) -> None:
    """
    s3_raw_path:    e.g. s3://reddit-community-data-store/raw/repositories/ingestion_date=2026-09-13/
    s3_bronze_path: e.g. s3://reddit-community-data-store/bronze/repositories
    bronze_table:   e.g. github_analytics.bronze.repositories
    """
    raw_df = spark.read.json(s3_raw_path)

    bronze_df = (
        raw_df
        .withColumn("_bronze_ingest_ts", current_timestamp())
        .withColumn("_raw_s3_uri", col("lineage.source_s3_uri"))
        .withColumn("_extraction_run_id", col("lineage.extraction_run_id"))
        .withColumn("_ingestion_date", col("lineage.ingestion_date"))
        .withColumn("_payload_checksum", col("lineage.payload_checksum"))
    )

    # Idempotent re-run guard: replace this partition's files at the S3 location, not a metastore-managed table
    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_ingestion_date = '{ingestion_date}'")
        .option("mergeSchema", "true")
        .partitionBy("_ingestion_date")
        .save(s3_bronze_path)
    )

    # Register/refresh the external table pointer (no-op after the first run, cheap either way)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {bronze_table}
        USING DELTA
        LOCATION '{s3_bronze_path}'
    """)
```

**Idempotency, made concrete here:** `replaceWhere` on the `_ingestion_date` partition means re-running the DAG for the same date replaces exactly those files at the S3 location — no duplicate rows, no separate `DELETE` statement needed, and it works whether or not the table's been registered yet. This is a partition-overwrite pattern rather than a row-level `MERGE`; that's intentional for Bronze (MERGE-level dedup logic belongs at Silver, per the medallion pattern) and it's the same idempotency guarantee either way: run it twice, get the same result.

### 9.2 Data quality gate at this boundary (minimal, Phase 1 scope)

- `repository_id IS NOT NULL`, `repository_name IS NOT NULL` — structural, ERROR severity
- Row count in Bronze for the partition == `record_count` summed from the lineage metadata — a completeness check, logged as a pipeline metric, not yet a hard fail (that graduates to a proper quarantine pattern once Silver exists)

---

## 10. Logging

One shared `logger.py` (structured JSON lines: `timestamp`, `level`, `module`, `run_id`, `message`, `extra` dict) imported everywhere instead of `print`. Airflow captures each task's stdout into its own task log automatically, so structured lines there are enough for Phase 1 — no separate log aggregation needed yet.

---

## 11. Definition of Done — Phase 1

- [ ] `.env.example` committed, `.env` git-ignored, real credentials never committed
- [ ] Ingestion IAM user created with the least-privilege, `raw/`-scoped policy in Section 5.2
- [ ] `raw/<resource>/ingestion_date=.../` structure confirmed under the existing `reddit-community-data-store` bucket
- [ ] Unity Catalog catalog/schema created in Databricks (external location already confirmed working)
- [ ] Airflow stack up via `docker-compose`, Connections and Variables set (not hardcoded)
- [ ] `github_client.py` fetches one repo's metadata, handles pagination + rate limits + retries — covered by `tests/test_github_client.py`
- [ ] Raw JSON lands in `raw/` with full lineage envelope, keyed and partitioned as specified
- [ ] Bronze Delta table exists in `github_analytics.bronze.repositories`, backed by `s3://reddit-community-data-store/bronze/repositories`, partitioned by `_ingestion_date`, carrying all lineage columns
- [ ] Running the DAG twice for the same date does not duplicate Bronze rows (verify the `replaceWhere` overwrite actually replaces, not appends)
- [ ] End-to-end run is traceable: given one Bronze row, you can name the exact S3 object, Airflow run, and timestamp it came from

---

## 12. Explicitly Open Items (confirm or correct)

1. Starting with `apache/spark` alone for the first successful run — say the word if you'd rather start with all 3–5 repos at once.
2. Airflow `docker-compose` — I'm pointing you at pulling the current official one from Airflow's docs rather than pinning a version number here, since it changes over time and I'd rather you grab the maintained file than a stale copy.
3. Bronze uses partition-level `replaceWhere` overwrite rather than a row-level `MERGE` — a pragmatic Phase 1 choice, with MERGE-based dedup graduating in at Silver per your master context. Flag it if you'd rather do MERGE from Bronze onward.
4. The bucket name `reddit-community-data-store` doesn't match the project domain — using it as given since it's already wired up with working permissions, but flagging in case it was meant to be project-specific.
