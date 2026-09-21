# Final In-Depth Audit — github-repository-data-pipeline(3)

I reviewed the latest uploaded repository again rather than relying on the
previous audit.

Scope reviewed:

- Airflow DAG
- Airflow Docker configuration
- Airflow requirements
- Python ingestion package
- GitHub REST/GraphQL client
- S3 writer
- Normalizer/lineage
- Repository configuration
- Databricks Bronze loader/notebook
- Databricks export notebook
- All dbt staging models
- All dbt intermediate models
- All dbt Silver models
- All dbt Gold dimensions/facts/KPIs
- dbt snapshots
- dbt macros
- dbt tests
- Streamlit application
- Streamlit SQL queries
- README
- Architecture documentation
- Data-model documentation
- CLAUDE/project-context documentation
- AUDIT.md
- Docker/requirements/configuration files

I also ran:

- Python compilation across the Python source files: PASS
- YAML parsing across YAML files: PASS
- Repository-wide searches for stale architecture markers and unresolved
  comments
- Static inspection of the updated watermark, S3, Airflow, dbt and data-model
  paths

There are 74 non-Git project files in this version.

# Current Verdict

## Portfolio rating: 8.5/10

For a 4th-year Data Engineering student:

## Strong project.

The architecture is now considerably more coherent than the first version.

Several of the previous issues are genuinely fixed:

- Airflow requirements path
- Watermark capture timing
- Watermark advancement after Bronze
- S3 deletion failure handling
- GitHub username mapping
- Median KPI naming
- Artificial 1900 SCD date
- dbt clean removal
- Dynamic-mapping documentation has improved
- Extractor no longer uses the old temporary-file implementation

However, I found **two important data-model/correctness issues that I would fix
before final submission**, plus several smaller engineering/documentation issues.

The most important one is actually a new consequence of removing the
1900-date workaround.

---

# 1. HIGH PRIORITY — Repository SCD2 Can Cause `repository_sk` NULLs in Facts

## Severity

P1 — Data-model correctness / potentially failing dbt tests

This is now the most important issue I found.

You correctly removed the artificial:

    1900-01-01

from `dim_repositories`.

That was the correct direction.

However, the current fact models perform a point-in-time join:

    fact created_at
        >= repository.valid_from
        AND
    fact created_at
        < repository.valid_to

For example:

    fact_pull_requests.sql

and:

    fact_issues.sql

use:

    left join dim_repositories r
      on p.repository_id = r.repo_id
      and p.created_at >= r.valid_from
      and p.created_at < coalesce(r.valid_to, '9999-12-31')

The problem is that the repository snapshot's first:

    dbt_valid_from

is approximately:

    when the pipeline first observed the repository

It is NOT:

    when the repository was created

Example:

    Repository created:          2015
    First pipeline observation: 2026
    Issue created:               2020

The point-in-time join asks:

    2020 >= 2026 ?

False.

Therefore:

    repository_sk = NULL

for that historical issue.

This is especially important because your Gold schema currently has:

    repository_sk
        not_null
        relationships -> dim_repositories.repository_sk

So you have a potential contradiction:

    historical GitHub facts
            ↓
    repository created_at < first SCD observation
            ↓
    no matching SCD row
            ↓
    repository_sk = NULL
            ↓
    dbt not_null test fails

This is not merely theoretical. Your tracked repositories contain historical
issues and PRs that predate the first pipeline observation.

## Why the old 1900 workaround existed

The old:

    1900-01-01

was technically semantically questionable because it pretended the observed
repository state existed historically.

But it also guaranteed a dimension match.

Removing it fixed one semantic problem but exposed the dimensional-FK problem.

## Recommended solution

Do NOT put 1900 back.

Instead choose one of these designs.

### Option A — Recommended for this project

Separate:

    repository identity

from:

    repository historical state

Use `repo_id` as the stable business relationship and associate facts with the
current repository dimension row.

Keep the SCD2 history separately for historical repository metadata analysis.

Conceptually:

    fact_issue
        repo_id
           ↓
    current repository dimension

while:

    dim_repository_history
        ↓
    historical metadata analysis

This is the simplest and most defensible student architecture.

### Option B — Add a current dimension model

Create:

    dim_repositories_current

containing exactly one row per:

    repo_id

Then facts use:

    repository_sk

from the current dimension.

The existing:

    dim_repositories

can remain the SCD2 history table.

This is architecturally clean but adds another model.

### Option C — Fallback join

Use:

    point-in-time repository row
        OR
    current repository row if no historical observation exists

This is simpler but requires very explicit documentation because it creates
mixed temporal semantics.

## What I recommend

For this project:

    dim_repositories
        = SCD2 history

    dim_repositories_current
        = one current row per repository

    fact_issues.repository_sk
        -> dim_repositories_current

    fact_pull_requests.repository_sk
        -> dim_repositories_current

This avoids inventing historical data while keeping the SCD2 demonstration.

If you don't want another model, use `repo_id` as the primary repository
relationship in facts and retain the SCD2 table for repository history.

## Status

NOT FIXED.

This should be addressed before final submission.


============================================================
# 2. HIGH PRIORITY — fact_releases STILL HAS NO repository_sk
============================================================

## Severity

P1 — Data-model completeness

This issue remains unchanged.

`fact_releases.sql` currently contains:

    release_sk
    author_user_sk
    release_id
    author_user_id
    published_date_key
    tag_name
    ...

but does not contain:

    repository_sk
    repo_id

The comment still says:

    skip repo_id, it is not present in releases intermediate model.

This is particularly unnecessary because:

`stg_github_releases.sql` already contains:

    _repo_full_name

and therefore the repository identity is available.

The release API itself is repository-specific:

    /repos/{owner}/{repo}/releases

## Fix

Carry:

    _repo_full_name

through:

    staging
        ↓
    intermediate
        ↓
    Silver
        ↓
    Gold

Then resolve it against the repository dimension.

Recommended final structure:

    fact_releases

        release_sk
        repository_sk
        author_user_sk
        release_id
        author_user_id
        published_date_key
        tag_name
        is_prerelease
        ...

Add:

    relationships:
      to: ref('dim_repositories_current')
      field: repository_sk

or the appropriate repository dimension depending on the solution selected
for Issue #1.

## Status

NOT FIXED.


============================================================
# 3. IMPORTANT — SCD2 Documentation Is Still Too Strong
============================================================

## Severity

P1 — Semantic correctness

Your `gold.yml` currently describes the SCD2 dimension as allowing facts to:

    join against the exact state of the repository at the time the event
    occurred.

That is not strictly true.

Your SCD2 tracks:

    observed repository state

not:

    complete historical GitHub state.

Example:

    GitHub actual state:
        2020 → 500 stars
        2021 → 800 stars

    Pipeline starts:
        2026

The pipeline cannot reconstruct the 2020 or 2021 state unless the source
provides historical snapshots.

Therefore:

    dbt_valid_from

means:

    "when this state was observed by the pipeline"

not:

    "when this state actually became true on GitHub"

## Fix

Change the documentation to something like:

    "The repository dimension tracks repository metadata states observed by
    successive pipeline snapshots. Point-in-time joins use the latest
    available observed state covering the event timestamp. This does not
    represent a complete reconstruction of GitHub's historical metadata."

Do not claim:

    exact repository state at event time

unless the source actually provides that history.

## Status

Documentation still needs correction.


============================================================
# 4. GOOD — WATERMARK TIMING IS NOW CORRECT
============================================================

This was one of the major previous issues.

You now capture:

    extraction_boundary = datetime.now(timezone.utc)

BEFORE extraction begins.

Then the extraction task returns:

    {
        "repo": repo,
        "extraction_boundary": extraction_boundary
    }

After Bronze:

    advance_watermarks

reads the XCom values and commits:

    extraction_boundary

This is the correct direction.

The previous bug:

    watermark = datetime.now()
    at the later watermark task

has been fixed.

## Current flow

    previous watermark
            ↓
    10-minute overlap
            ↓
    capture extraction boundary
            ↓
    GitHub extraction
            ↓
    S3
            ↓
    Bronze
            ↓
    commit extraction boundary
            ↓
    dbt
            ↓
    export

This is much better.

## One nuance

The extraction boundary is not passed into GitHub API calls as an upper
`updated_at` limit.

That is not necessarily a bug.

Because the watermark is set to the earlier boundary, any records returned
after the boundary will be fetched again during the next run.

That creates some duplicate/reprocessing work, but it does not create a gap.

Given your downstream deduplication, this is acceptable.

## Status

FIXED.


============================================================
# 5. GOOD — WATERMARK NOW ADVANCES AFTER BRONZE
============================================================

The current DAG is:

    extract_repos
        ↓
    run_bronze
        ↓
    advance_watermarks
        ↓
    dbt_build
        ↓
    export_kpis

This fixes the original dangerous situation:

    extraction succeeds
        ↓
    watermark advances
        ↓
    Bronze fails

The current behavior is:

    extraction
        ↓
    Bronze
        ↓
    watermark

This is correct.

If dbt later fails, Bronze already contains the data and the next execution
can rebuild dbt without requiring the source to be re-extracted.

That is a reasonable design.

## Status

FIXED.


============================================================
# 6. DYNAMIC TASK MAPPING — DOCUMENTATION IS NOW CORRECT
============================================================

This was improved.

README now correctly explains that:

- repository extraction runs as independent mapped tasks
- task-level retry isolation exists
- downstream Bronze requires the complete mapped extraction stage
- the daily pipeline is intentionally all-or-nothing

That is technically accurate.

Current semantics:

    repo A ─┐
    repo B ─┤
    repo C ─┤
    repo D ─┤
            ↓
        Bronze
            ↓
        watermark
            ↓
          dbt

If repo C fails:

    repo C = failed
    Bronze = blocked
    watermark = not advanced
    dbt = blocked

This is acceptable.

## One remaining stale comment

`extractor.py` still says:

    "failures in one repository ... don't crash or block the ingestion
    of the others."

That is misleading.

The mapped extraction tasks can execute/retry independently, but the overall
pipeline is intentionally all-or-nothing before Bronze.

Change the wording to:

    "Each repository is isolated as an independent Airflow task instance,
    allowing independent retries and parallel execution. The downstream
    Bronze stage intentionally waits for the complete mapped extraction
    stage to succeed."

## Status

Architecture/documentation: FIXED

One stale extractor comment remains.


============================================================
# 7. S3 IDEMPOTENCY — NOW ACCEPTABLE
============================================================

The previous contradiction has been fixed.

The writer now:

    deletes existing parts
        ↓
    fails the task if deletion fails
        ↓
    writes new parts

This is much better than:

    deletion fails
        ↓
    log warning
        ↓
    continue

The IAM/documentation now also acknowledges:

    s3:DeleteObject

This is internally consistent.

## Remaining architectural limitation

The current approach is:

    delete old data
        ↓
    upload new data

If deletion succeeds and upload fails halfway, the old data has already been
removed.

For a production-grade lake architecture, immutable run-based raw storage is
safer:

    raw/.../run_id=<run_id>/

But this is no longer a blocker for this portfolio project.

## Status

FIXED for current project scope.

Future hardening:

    immutable raw runs + publish/commit semantics.


============================================================
# 8. AIRFLOW DOCKER REQUIREMENTS — FIXED
============================================================

You now have:

    airflow/requirements.txt

and:

    COPY airflow/requirements.txt /requirements.txt

This resolves the previous broken:

    COPY requirements.txt

problem.

The Docker build context can now access the required file.

## Minor cleanup

The dependencies are now correctly centralized in:

    airflow/requirements.txt

The Dockerfile only needs to install that file.

Do not duplicate packages in both the requirements file and Dockerfile.

Current Dockerfile appears clean enough.

## Status

FIXED.


============================================================
# 9. AIRFLOW DEPENDENCIES — MINOR CLEANUP
============================================================

Current:

    airflow/requirements.txt

contains:

    apache-airflow-providers-databricks
    dbt-databricks
    databricks-sdk

This is correct.

The base Airflow image already contains Airflow itself.

Therefore you should not unnecessarily reinstall:

    apache-airflow==2.10.5

in the Dockerfile.

The current version no longer appears to do that, which is good.

## Status

Essentially fixed.


============================================================
# 10. `dbt clean` REMOVAL — FIXED
============================================================

The DAG now runs:

    dbt build --profiles-dir /opt/airflow/dbt

instead of:

    dbt clean && dbt build

This is correct for scheduled execution.

`dbt clean` can remain a manual development command.

## Status

FIXED.


============================================================
# 11. GITHUB USERNAME BUG — FIXED
============================================================

The KPI model now uses:

    u.user_login as github_username

instead of:

    u.user_id as github_username

Correct.

## Status

FIXED.


============================================================
# 12. MEDIAN KPI NAMING — FIXED
============================================================

The dashboard now displays:

    Avg Repo Median Close
    Avg Repo Median Merge

and the calculation is:

    mean(repository median values)

This is mathematically consistent.

It no longer falsely claims to be:

    global median

Good.

The PR Complexity tab also uses an actual median for:

    Median Merge Time

which is correct.

## Status

FIXED.


============================================================
# 13. EXTRACTOR DEAD `results` STRUCTURE — FIXED
============================================================

This was partially addressed.

`extract_repository_metadata()` now returns:

    list[int]

containing PR numbers.

The old:

    results

tuple containing temporary-file information is gone.

This is a real cleanup from the previous architecture.

Good.

## Remaining stale documentation

The function docstring and comments should be checked for old wording about
temporary files.

The implementation itself is now clean on this point.

## Status

FIXED / minor documentation cleanup only.


============================================================
# 14. TEMPFILE ARCHITECTURE — MOSTLY FIXED
============================================================

The actual implementation no longer uses temporary files.

The architecture documentation correctly says:

    records are streamed directly from GitHub into S3

and:

    no local temporary files are used.

That is correct.

There are still some references to "temp" in places, but these are mostly
generic words such as:

    temporary connection drops

rather than references to the old tempfile architecture.

The large stale tempfile issue from the previous audit has effectively been
resolved.

## Status

FIXED.


============================================================
# 15. NEW CODE-QUALITY ISSUE — `ponytail` COMMENTS
============================================================

There are multiple comments such as:

    # ponytail: ...

throughout the code.

Examples:

    # ponytail: generator instead of list accumulation
    # ponytail: handle temporary connection drops to S3
    # ponytail: delete existing objects
    # ponytail: skip repo_id
    # ponytail: hard deletes on GitHub won't trigger...

These comments are unusual and reduce the professional appearance of the
repository.

They look like internal implementation/audit markers rather than normal
engineering comments.

## Fix

Replace:

    # ponytail: generator instead of list accumulation

with:

    # Use a generator to avoid materializing large API responses in memory.

Replace:

    # ponytail: handle temporary connection drops to S3

with:

    # Configure botocore retries for transient S3 connection failures.

And so on.

Most importantly, remove the `ponytail` marker from:

    fact_releases.sql

because that comment describes a limitation that you should actually fix.

## Status

P2 — cleanup recommended.


============================================================
# 16. README STILL CALLS THE PROJECT "PRODUCTION-GRADE"
============================================================

README currently says:

    "This project implements a production-grade data pipeline..."

I would still change this.

The architecture is strong, but you do not yet have:

- Python unit tests
- CI
- complete release dimensional modeling
- fully resolved historical SCD semantics
- production secrets management
- full operational validation

Therefore:

    production-grade

is too strong.

## Better wording

Use:

    "This project implements an end-to-end production-oriented Data
    Engineering pipeline..."

or:

    "This project demonstrates production-style Data Engineering practices..."

That is accurate without overselling.

## Status

P2 — portfolio credibility.


============================================================
# 17. README "REAL-TIME" CLAIM IS INACCURATE
============================================================

The dashboard header says:

    "Real-time insights..."

but also says:

    "Data refreshed daily"

This is contradictory.

The pipeline is:

    daily batch

not:

    real-time / streaming

DuckDB can provide real-time INTERACTION against the exported data, but the
data itself is not real-time.

## Fix

Change:

    Real-time insights from X open-source repositories

to:

    Daily analytics from X open-source repositories

or:

    Interactive analytics from X open-source repositories


============================================================
# 18. README PROJECT STRUCTURE IS SLIGHTLY STALE
============================================================

README says:

    s3_writer.py # S3 upload (single file + chunked streaming)

The current implementation is fundamentally streaming/chunked.

"single file + chunked streaming" is not particularly clear.

Change to:

    s3_writer.py # Chunked streaming NDJSON uploads to S3

Similarly, the documentation should describe:

    immutable logical resource partition

rather than implying a single output file.


============================================================
# 19. DEVELOPMENT CREDENTIAL DEFAULTS STILL EXIST
============================================================

Docker Compose still contains defaults such as:

    POSTGRES_PASSWORD:-airflow
    AIRFLOW_ADMIN_USERNAME:-admin
    AIRFLOW_ADMIN_PASSWORD:-admin
    AIRFLOW_SECRET_KEY:-'a_very_secret_key_for_airflow_logs'

These are acceptable for a local demo.

They should not be described as production configuration.

## Recommended fix

Use:

    AIRFLOW_ADMIN_USERNAME=${AIRFLOW_ADMIN_USERNAME}
    AIRFLOW_ADMIN_PASSWORD=${AIRFLOW_ADMIN_PASSWORD}
    AIRFLOW_SECRET_KEY=${AIRFLOW_SECRET_KEY}

with:

    .env.example

The README should explain that local credentials are development-only.

This is not a portfolio blocker.

## Status

P2.


============================================================
# 20. README CREDENTIAL EXAMPLES SHOULD BE CLEARER
============================================================

README contains examples resembling:

    AKIAXXXXXXXXXXXX
    dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

These are clearly placeholders, so there is no credential leak.

However, clearer placeholders would be:

    <AWS_ACCESS_KEY_ID>
    <AWS_SECRET_ACCESS_KEY>
    <DATABRICKS_TOKEN>

This makes accidental copy/paste less likely.

P2 only.


============================================================
# 21. GITHUB API CLIENT — ADD 429 HANDLING
============================================================

I found one additional robustness issue in the HTTP client.

The client explicitly handles:

    HTTP 403
    rate limit

but generic:

    HTTP 429

falls through to:

    4xx → fail fast

GitHub commonly uses 403 for rate-limit conditions, but handling 429 is still
a worthwhile defensive improvement.

## Fix

Treat:

    429

as retryable/rate-limit behavior.

Use:

    Retry-After

when available.

Otherwise fall back to:

    X-RateLimit-Reset

or exponential backoff.

Conceptually:

    if status_code == 429:
        wait according to Retry-After
        retry

This is a small improvement but makes the API client more robust.

## Status

P2/P1 depending on how broadly you want to claim API resilience.


============================================================
# 22. S3 WRITER DOCSTRING HAS A SMALL SEMANTIC MISMATCH
============================================================

The S3 writer says:

    "if a long-running extraction crashes midway, Airflow can retry without
    losing the successfully uploaded parts"

But the retry path deliberately purges the old parts before rewriting them.

Therefore the successfully uploaded parts are NOT retained across the retry.

What actually happens is:

    partial upload
        ↓
    task fails
        ↓
    retry
        ↓
    old parts purged
        ↓
    extraction restarted
        ↓
    data rewritten

This is still valid.

But the docstring is misleading.

## Fix

Say:

    "Chunked writes limit memory usage and allow failed extractions to be
    safely retried by purging the partial output before rewriting the
    repository partition."


============================================================
# 23. S3 DESIGN — EMPTY RESOURCE CASE
============================================================

The writer purges the existing prefix before writing.

If an incremental run returns:

    0 records

then the old data is deleted and no new part files are written.

For some resources, that may be correct.

For example, if:

    no new releases

you could accidentally remove the previous raw release dataset for that
ingestion partition.

However, your release resource is described as a full-population extraction,
so this needs careful distinction.

The larger issue is that raw data is partitioned by:

    ingestion_date

and each daily partition is independently loaded into Bronze.

If a full-population resource legitimately returns zero records, an empty
partition may be correct.

For incremental resources, however, zero records means:

    no changes

not:

    source has zero records.

The current writer treats both cases identically.

## Recommendation

Do not create an immediate redesign.

Instead, make the semantic distinction explicit:

    full resource:
        empty output = valid empty dataset

    incremental resource:
        empty output = no new records

For incremental resources, consider writing an explicit run marker/manifest
even when zero records are returned.

This becomes especially useful with immutable run architecture.

P2.


============================================================
# 24. SCD2 + FACT RELATIONSHIP NEEDS A FINAL DESIGN DECISION
============================================================

At this point, I would stop making small patches and make one deliberate
decision.

You currently have:

    dim_repositories
        SCD2

and:

    fact_issues
        repository_sk

    fact_pull_requests
        repository_sk


The intended relationship is:

    fact event
        ↓
    repository state at event time


That is only possible if historical repository states are actually available.

They aren't.

Therefore choose one:

### Design A — Current repository dimension

    dim_repositories_current
        one row per repo

Facts reference this.

SCD2 history remains available separately.

### Design B — Observation-time SCD

Facts reference the repository state observed by the pipeline.

Documentation explicitly states that historical source state is approximate.

### Design C — True historical repository reconstruction

Would require historical source snapshots.

This is unnecessary for the portfolio.

## Recommendation

Use Design A.

It is simpler and more semantically honest.


============================================================
# 25. TESTING IS STILL THE BIGGEST MISSING ENGINEERING PRACTICE
============================================================

There are still no Python tests.

Current dbt tests are useful, but they don't replace Python unit tests.

Minimum:

    tests/
        test_github_client.py
        test_extractor.py
        test_s3_writer.py
        test_normalizer.py
        test_repo_config.py

Target:

    15–25 meaningful tests


Highest-value cases:

GitHub:

    [ ] pagination
    [ ] 403 rate limit
    [ ] 429 rate limit
    [ ] 500 retry
    [ ] network failure
    [ ] retry exhaustion

Extractor:

    [ ] full extraction
    [ ] incremental extraction
    [ ] empty result
    [ ] PR extraction
    [ ] GraphQL enrichment

S3:

    [ ] chunking
    [ ] empty input
    [ ] upload retry
    [ ] deletion failure
    [ ] retry cleanup

Watermarks:

    [ ] first run
    [ ] existing watermark
    [ ] overlap window
    [ ] Bronze failure
    [ ] successful watermark commit

Do not chase 100% coverage.

Test failure behavior and critical boundaries.


============================================================
# 26. CI/CD IS STILL MISSING
============================================================

There is still no:

    .github/workflows/

workflow.

Add:

    .github/
        workflows/
            ci.yml

Minimum:

    checkout
        ↓
    setup Python
        ↓
    install dependencies
        ↓
    ruff
        ↓
    pytest
        ↓
    compileall
        ↓
    YAML validation

Optional:

    dbt parse


You do NOT need:

- Kubernetes
- Terraform
- deployment pipelines
- cloud infrastructure automation

A simple CI workflow is enough for this portfolio.


============================================================
# 27. FINAL STATUS OF THE PREVIOUS AUDIT ITEMS
============================================================

| Issue | Status |
|---|---|
| Airflow Docker requirements | FIXED |
| S3 IAM/delete contradiction | FIXED |
| S3 deletion failure swallowed | FIXED |
| Watermark advanced before Bronze | FIXED |
| Watermark timestamp timing | FIXED |
| GitHub username as user ID | FIXED |
| Median KPI naming | FIXED |
| Artificial 1900 SCD date | FIXED |
| Dynamic mapping documentation | MOSTLY FIXED |
| Temporary-file architecture | FIXED |
| Extractor dead results structure | FIXED |
| `dbt clean` every run | FIXED |
| fact_releases repository FK | NOT FIXED |
| SCD2 historical semantics | NEEDS FINAL DESIGN |
| SCD2 → fact FK nullability | NEW IMPORTANT ISSUE |
| Python tests | NOT IMPLEMENTED |
| CI/CD | NOT IMPLEMENTED |
| Hardcoded development defaults | STILL PRESENT |
| README production-grade claim | STILL TOO STRONG |
| README real-time claim | STILL INACCURATE |
| 429 API handling | RECOMMENDED |
| S3 retry docstring | NEEDS CLEANUP |
| `ponytail` comments | NEEDS CLEANUP |


============================================================
# 28. WHAT I WOULD FIX BEFORE SUBMISSION
============================================================

Do these in this order.


## MUST FIX

### 1. Resolve repository dimension semantics

Choose:

    dim_repositories_current

for fact foreign keys.

Keep:

    dim_repositories

as SCD2 history.

This solves the historical fact → repository_sk null problem without inventing
1900-era repository states.


### 2. Add repository relationship to fact_releases

Carry:

    repo_full_name

through the release transformation and resolve it to:

    repository_sk
    repo_id


### 3. Fix SCD2 documentation

Explicitly distinguish:

    observed state

from:

    true historical source state.


============================================================
# SHOULD FIX
============================================================

### 4. Add 429 handling

### 5. Clean stale `ponytail` comments

### 6. Fix S3 writer docstring

### 7. Remove remaining misleading extractor failure-isolation wording

### 8. Change README "production-grade"

### 9. Change README "real-time"

### 10. Remove development credential defaults or label them clearly


============================================================
# THEN ADD TESTING
============================================================

Target:

    15–25 focused Python tests


============================================================
# THEN ADD CI
============================================================

One GitHub Actions workflow.


============================================================
# 29. FINAL ARCHITECTURE I WOULD SIGN OFF ON
============================================================

GitHub REST + GraphQL
        |
        v
Airflow Dynamic Task Mapping
        |
        v
Python Streaming Extractors
        |
        v
S3 Raw
        |
        v
Databricks Bronze
        |
        v
Watermark Commit
        |
        v
dbt Staging
        |
        v
dbt Intermediate + DQ
        |
        +-------------------+
        |                   |
        v                   v
   SCD2 History        Current State
        |                   |
        +---------+---------+
                  |
                  v
             Gold Facts
                  |
          +-------+-------+
          |       |       |
          v       v       v
       Issues    PRs   Releases
          |
          v
       KPI Models
          |
          v
      S3 Parquet
          |
          v
        DuckDB
          |
          v
      Streamlit


Repository modeling:

    dim_repositories
        = SCD2 historical observation history

    dim_repositories_current
        = one current row per repository

    facts
        = reference current repository identity

This is cleaner than pretending you have historical GitHub repository state
that the API extraction never captured.


============================================================
# 30. FINAL RATING
============================================================

Current:

    8.5 / 10

For a 4th-year Data Engineering student:

    Strong

Compared with typical student portfolio projects:

    Significantly above average in architecture and DE breadth


After fixing:

    repository SCD/fact semantics
    release repository relationship
    tests
    CI
    remaining documentation/code-quality issues

Expected:

    ~9 / 10


============================================================
# FINAL STAFF-LEVEL VERDICT
============================================================

The project has reached the point where I would NOT recommend adding any more
technologies.

You already have enough:

    Python
    REST
    GraphQL
    S3
    Airflow
    Databricks
    PySpark
    Delta
    dbt
    SCD2
    DQ
    DuckDB
    Streamlit


The remaining work is engineering maturity.

The most important unresolved problem is now:

    repository dimension semantics

because removing the artificial 1900 validity date was correct, but it means
historical issues/PRs can fail to find a valid repository SCD row.

The second important issue is:

    fact_releases missing repository relationship


After those are resolved, the remaining work is mostly:

    tests
    CI
    cleanup
    documentation precision


I would consider the project essentially architecturally complete after those
changes.

Do NOT add Kafka, Kubernetes, Terraform, Snowflake, another warehouse, another
orchestrator, or another serving layer just to make the stack larger.

At this stage:

    correctness > complexity
    reliability > technology count
    tests > another framework
    CI > another infrastructure component
    semantic precision > architectural buzzwords


Final target:

    A technically defensible, testable, reproducible 4th-year DE portfolio
    project rather than merely a project containing many DE technologies.