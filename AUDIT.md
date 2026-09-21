# GitHub Repository Analytics Pipeline
# Staff Data Engineer Audit — Issues & Remediation Plan

Project: GitHub Repository Analytics Data Pipeline
Target: 4th-Year Data Engineering Portfolio

============================================================
OVERALL ASSESSMENT
============================================================

Current Portfolio Rating: 7.8/10
Scope/Ambition for 4th-Year Student: 8.5/10
Current Engineering Quality: 6.8/10
Potential After Fixes: ~9/10

Overall verdict:

The project is significantly above the typical student Data Engineering
portfolio project. It demonstrates meaningful knowledge of:

- REST API ingestion
- GraphQL enrichment
- Pagination
- Retry handling
- Rate-limit handling
- Generator-based extraction
- S3 raw storage
- Databricks
- PySpark
- Delta Bronze
- dbt
- Staging/intermediate/Silver/Gold architecture
- Data-quality quarantine
- SCD Type 2
- Dimensional modeling
- Airflow Dynamic Task Mapping
- Parquet exports
- DuckDB
- Streamlit
- Data lineage
- Documentation and ADRs

However, several issues prevent the current implementation from being called
production-grade.

The main weakness is not the number of technologies used. The main weakness is
operational correctness:

    state management
    +
    idempotency
    +
    failure recovery
    +
    testing
    +
    deployment reproducibility

The project should prioritize reliability and correctness rather than adding
more technologies.


============================================================
PRIORITY CLASSIFICATION
============================================================

P0 — Critical / Must Fix

1. Airflow Docker image currently cannot build
2. S3 idempotency is not guaranteed
3. Watermark semantics can miss updates
4. Watermark advances before downstream processing succeeds

P1 — High Priority

5. GitHub username is incorrectly displayed as user ID
6. Dashboard "Median" KPI is not actually median
7. fact_releases lacks repository relationship
8. Repository SCD2 starts from artificial 1900 date
9. Automated testing is insufficient
10. No CI/CD validation

P2 — Medium Priority / Polish

11. Documentation is partially stale
12. Some parts of the architecture are over-engineered
13. Stale comments remain after refactoring
14. Duplicate Streamlit cache decorator
15. Unused imports / cleanup
16. Hardcoded development credentials
17. Hardcoded Airflow secret key
18. Dashboard repository count is stale


============================================================
P0 — ISSUE 001
AIRFLOW DOCKER IMAGE CANNOT CURRENTLY BUILD
============================================================

Severity:
P0 — Deployment Blocker

Location:
airflow/Dockerfile

Current Dockerfile:

FROM apache/airflow:2.10.5-python3.11

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

Docker Compose builds using:

build:
  context: ..
  dockerfile: airflow/Dockerfile

Therefore the build context is the repository root.

The repository does not contain:

requirements.txt

at its root.

Existing requirement files include:

ingestion/requirements.txt
streamlit_app/requirements.txt

but neither is the file referenced by the Airflow Dockerfile.

Problem:

COPY requirements.txt /requirements.txt

will fail because the required file does not exist in the build context.

Impact:

The documented:

cd airflow
docker compose up -d

deployment cannot successfully build the Airflow image from a clean
environment.

Why this matters:

A portfolio project should have a reproducible deployment path. A broken
Docker build undermines the claim that the orchestration environment is
actually deployable.

Recommended fix:

Create an explicit Airflow dependency file:

airflow/requirements.txt

and make the Docker build use that dependency file correctly.

Alternatively, create a repository-level requirements.txt if that is the
intended dependency strategy.

Verification:

Run:

docker compose build

Then:

docker compose up -d

The complete Airflow environment should start successfully from a clean
environment.


============================================================
P0 — ISSUE 002
S3 IDEMPOTENCY IS NOT GUARANTEED
============================================================

Severity:
P0/P1 — Data Correctness

Location:

ingestion/s3_writer.py

The S3 writer attempts to delete existing objects using:

s3.delete_objects(...)

However, the documented IAM design does not grant:

s3:DeleteObject

The code also catches deletion errors and logs a warning instead of failing.

Potential failure scenario:

Initial run:

part_001.json
part_002.json
part_003.json
part_004.json

Retry:

part_001.json
part_002.json

If old objects cannot be deleted, the bucket can contain:

part_001.json
part_002.json
part_003.json
part_004.json

The downstream Bronze loader may then read stale and/or duplicate records.

Potential consequences:

- Duplicate records
- Stale records
- Inflated counts
- Incorrect KPIs
- Incorrect SCD2 history
- Incorrect snapshots
- Incorrect dashboard results
- Non-deterministic reruns

This directly conflicts with the project's idempotency claim.

Recommended solution A:

Allow the ingestion role to delete objects and make deletion failures fatal.

Required permissions would include:

s3:ListBucket
s3:PutObject
s3:DeleteObject

Recommended solution B:

Prefer immutable run-based raw storage.

Example:

raw/
  issues/
    ingestion_date=2026-09-21/
      run_id=<run_id>/
        part_001.json
        part_002.json

Then track successful ingestion runs explicitly.

This avoids destructive replacement semantics and gives the pipeline a
replayable raw history.

Preferred design:

Immutable raw data + explicit run metadata + downstream deduplication.


============================================================
P0 — ISSUE 003
WATERMARK SEMANTICS ARE NOT SUFFICIENTLY SAFE
============================================================

Severity:
P1 — Incremental Data Correctness

Location:

Airflow DAG / GitHub extraction logic

The DAG captures a current run timestamp and uses it as the extraction
boundary.

The problem is that API extraction is not instantaneous.

Example:

10:00 — extraction starts
10:05 — first API endpoint is read
11:30 — GitHub record is updated
12:00 — extraction finishes

The pipeline needs a clearly defined extraction boundary and safe overlap
strategy.

The current implementation relies heavily on source updated_at semantics
without an explicit safety window.

Potential problem:

A record can change during a long-running extraction window and fall into a
boundary where it is not captured by the next incremental run.

Recommended approach:

Use:

previous_successful_watermark

as the logical lower boundary.

Apply an overlap window:

new_since =
    previous_successful_watermark - safety_window

Then deduplicate downstream using the natural/business key.

Example:

previous watermark:
2026-09-21 10:00

safety window:
10 minutes

next extraction starts from:

2026-09-21 09:50

This provides protection against:

- clock differences
- API ordering
- extraction duration
- delayed source updates
- boundary timestamps


============================================================
P0 — ISSUE 004
WATERMARK CAN ADVANCE BEFORE DOWNSTREAM PROCESSING SUCCEEDS
============================================================

Severity:
P0/P1 — Potential Data Loss

Current flow is approximately:

extract_repos
    ↓
run_bronze
    ↓
dbt_build
    ↓
export_kpis

However, the watermark is updated inside the extraction task.

This creates the following failure scenario:

Extraction succeeds
    ↓
Watermark advances
    ↓
Bronze fails
    ↓
Pipeline run fails

On the next run, the extractor may use the already-advanced watermark.

The previous data may therefore not be extracted again even though it never
successfully reached Bronze.

This can create:

S3 raw:
    data exists

Airflow state:
    watermark says processed

Bronze:
    data missing

This is a classic pipeline state-consistency problem.

Recommended fix:

Do not define:

watermark = extraction completed

Instead define:

watermark = successfully processed source boundary

Possible implementation:

1. Extract data
2. Write raw data
3. Validate raw write
4. Load Bronze
5. Validate Bronze success
6. Only then advance watermark

Better architecture:

extracted_until
bronze_loaded_until
transformed_until

or a simpler implementation where the watermark is advanced only after the
Bronze task succeeds.

The raw layer should also be replayable so a failed downstream stage can
process the exact previous ingestion run.


============================================================
P1 — ISSUE 005
GITHUB USERNAME IS ACTUALLY USER ID
============================================================

Severity:
P1 — Visible Analytical Bug

Location:

dbt/models/marts/gold/kpis/kpi_user_contributions.sql

Current logic:

u.user_id as github_username

However, dim_users contains:

user_login

The dashboard therefore has the potential to display:

10230594

instead of:

some_github_username

The comment suggests the model intended to use username if available, but the
actual SQL selects the ID.

Recommended fix:

Use:

u.user_login as github_username

if the intended dashboard field is the GitHub username.

Also verify downstream references in Streamlit.


============================================================
P1 — ISSUE 006
DASHBOARD "MEDIAN" KPI IS NOT ACTUALLY MEDIAN
============================================================

Severity:
P1 — Analytical Semantics Bug

Location:

streamlit_app/app.py

Current logic is approximately:

avg_close = df_health["median_time_to_close_hours"].mean()
avg_merge = df_health["median_time_to_merge_hours"].mean()

These values are then displayed as:

Median Close Time
Median Merge Time

This is mathematically incorrect.

The code calculates:

mean(repository_medians)

It does not calculate:

median(all observations)

Example:

Repository A median = 10 hours
Repository B median = 100 hours

Mean of repository medians:

55 hours

55 hours is not the global median.

Recommended fix:

Option A:

Rename the KPI:

Average Repository Median Close Time

and:

Average Repository Median Merge Time

Option B:

Calculate the actual global median from fact-level observations.

Option B is preferable if the KPI is intended to represent the global
population.


============================================================
P1 — ISSUE 007
FACT_RELEASES LACKS REPOSITORY RELATIONSHIP
============================================================

Severity:
P1 — Data Modeling Limitation

Location:

dbt fact_releases model

The model explicitly skips repository context because repo_id is not
available in the intermediate release model.

This weakens the dimensional model.

Current conceptual structure:

fact_releases
    ↓
release
author
date

Missing:

repository

This makes analytical questions such as:

- Releases per repository
- Release frequency by repository
- Release activity over time by repository

more difficult than necessary.

The extraction source already knows the repository because releases are
retrieved from:

/repos/{owner}/{repo}/releases

and the raw envelope contains repository information.

Recommended fix:

Carry repository identity through:

API extraction
    ↓
staging
    ↓
intermediate
    ↓
Silver
    ↓
fact_releases

Add:

repo_id

and/or the appropriate repository foreign key.

Then enforce the relationship with dbt tests.


============================================================
P1 — ISSUE 008
REPOSITORY SCD2 STARTS FROM ARTIFICIAL YEAR 1900
============================================================

Severity:
P1 — Historical Data Semantics

Location:

dim_repositories.sql

The model uses:

1900-01-01

as the initial valid_from date for the first observed repository state.

Example:

Repository actually created:
2012

Pipeline first observed repository:
2026

Current SCD semantics may imply:

repository state valid from 1900

This creates historical semantics that are not supported by the source data.

If a 2015 issue joins against the repository dimension, the dimension can
appear to provide a repository state that the pipeline never actually
observed in 2015.

Recommended fix:

Use:

valid_from = first_observed_timestamp

unless there is an actual source-level historical record.

Alternative:

Explicitly document the dimension as representing:

"first observed state"

rather than historical truth.

Do not manufacture historical validity periods unless there is a strong
business reason.


============================================================
P1 — ISSUE 009
AUTOMATED TESTING IS INSUFFICIENT
============================================================

Severity:
P1 — Engineering Quality

The repository contains custom dbt tests such as:

assert_one_current_row_per_issue.sql
assert_one_current_row_per_pr.sql
assert_one_current_row_per_repo.sql

This is good.

However, there are no meaningful Python unit tests covering the ingestion
layer.

Important untested areas include:

- GitHub pagination
- Retry logic
- 429 handling
- 403/rate-limit handling
- 5xx handling
- Network failures
- GraphQL batching
- GraphQL fallback
- Empty API responses
- S3 chunking
- S3 retry behavior
- Lineage envelope generation
- Watermark behavior
- Repository configuration
- Duplicate extraction
- Incremental extraction boundaries

Recommended test structure:

tests/
    test_github_client.py
    test_extractor.py
    test_s3_writer.py
    test_normalizer.py
    test_repo_config.py

At minimum, implement tests for:

1. Pagination
2. Empty response
3. 429 rate limit
4. 500 server error
5. Retry exhaustion
6. GraphQL partial failure
7. S3 upload failure
8. Duplicate records
9. Watermark boundary
10. Repository configuration


============================================================
P1 — ISSUE 010
NO CI/CD VALIDATION
============================================================

Severity:
P1 — Engineering Quality

There is no GitHub Actions CI workflow.

This means pull requests do not automatically validate:

- Python syntax
- Python tests
- Linting
- YAML
- dbt parsing
- SQL structure
- configuration integrity

Recommended addition:

.github/
  workflows/
    ci.yml

Example CI stages:

1. Checkout
2. Python setup
3. Install dependencies
4. ruff
5. pytest
6. compileall
7. YAML validation
8. dbt parse

For this portfolio project, CI provides significantly more value than adding
another infrastructure technology.


============================================================
P2 — ISSUE 011
DOCUMENTATION IS PARTIALLY STALE
============================================================

Severity:
P2 — Documentation / Maintainability

The project documentation is extensive and generally strong.

However, parts of:

docs/Architecture.md
README
DATA MODEL.md
CLAUDE.md
code comments

describe an older implementation.

For example, documentation/comments still reference tempfile-based extraction
while the implementation has been refactored to stream directly into S3.

There is also a stale DAG comment similar to:

save watermark BEFORE uploading temp files

even though the current architecture no longer follows that tempfile design.

This creates a mismatch:

Documentation:
    Architecture A

Actual code:
    Architecture B

Recommended fix:

After the code is stabilized, perform a documentation reconciliation pass.

Verify that:

README
Architecture.md
DATA MODEL.md
CLAUDE.md
code comments
Docker instructions

all describe the current implementation.


============================================================
P2 — ISSUE 012
SOME ARCHITECTURAL COMPONENTS ARE OVER-ENGINEERED
============================================================

Severity:
P2 — Architectural Complexity

Current architecture contains:

GitHub API
    ↓
Airflow
    ↓
Python
    ↓
S3
    ↓
Databricks/PySpark
    ↓
Delta Bronze
    ↓
dbt
    ↓
Gold
    ↓
S3 Parquet
    ↓
DuckDB
    ↓
Streamlit

This is technically impressive but introduces substantial complexity for a
GitHub analytics project.

The most questionable layer is:

Databricks Gold
    ↓
S3 Parquet
    ↓
DuckDB
    ↓
Streamlit

There are now multiple analytical/storage layers.

However, this is not necessarily something that must be removed.

For a portfolio project, the design can be justified if DuckDB is explicitly
positioned as a lightweight serving layer.

Recommended explanation:

"Databricks is used as the transformation and analytical platform, while
DuckDB provides a lightweight local/query serving layer for the public
Streamlit dashboard without requiring persistent Databricks connectivity."

That turns the extra component into a deliberate architectural choice.

Do not add more technologies merely to increase the stack.


============================================================
P2 — ISSUE 013
STALE COMMENTS AFTER REFACTORING
============================================================

Severity:
P2 — Code Quality

Several comments still refer to implementation details that no longer exist.

Example:

# save watermark BEFORE uploading temp files

The current implementation has moved away from the tempfile architecture.

Stale comments are dangerous because they make future maintenance harder.

Recommended action:

Search the repository for:

tempfile
temporary file
temp files
upload temp
watermark before upload

Then remove or update comments that no longer describe actual behavior.


============================================================
P2 — ISSUE 014
DUPLICATE STREAMLIT CACHE DECORATOR
============================================================

Severity:
P2 — Code Quality

Location:

streamlit_app/db.py

Current pattern:

@st.cache_resource
@st.cache_resource
def get_connection():

The decorator is duplicated.

Recommended fix:

@st.cache_resource
def get_connection():

This appears to be an accidental artifact from editing/refactoring.


============================================================
P2 — ISSUE 015
UNUSED IMPORTS / CLEANUP REQUIRED
============================================================

Severity:
P2 — Code Quality

Some imports remain from previous implementation versions.

This is especially noticeable around code that was refactored from tempfile
processing to direct S3 streaming.

Recommended actions:

Run a Python linter such as:

ruff

Remove:

- unused imports
- dead helper functions
- stale comments
- obsolete configuration
- unreachable branches

This is simple cleanup but increases reviewer confidence.


============================================================
P2 — ISSUE 016
HARDCODED DEVELOPMENT CREDENTIALS
============================================================

Severity:
P2 — Security

Docker Compose contains development-style credentials such as:

POSTGRES_PASSWORD: airflow

and:

--username admin
--password admin

For a local demo environment this is acceptable if clearly documented.

It should not be presented as production configuration.

Recommended approach:

Use environment variables:

AIRFLOW_DB_PASSWORD
AIRFLOW_ADMIN_PASSWORD

and provide:

.env.example

instead of real credentials.

Explicitly document:

"These credentials are development-only."


============================================================
P2 — ISSUE 017
HARDCODED AIRFLOW SECRET KEY
============================================================

Severity:
P2 — Security

Docker Compose contains a hardcoded value similar to:

AIRFLOW__WEBSERVER__SECRET_KEY:
  'a_very_secret_key_for_airflow_logs'

This should not be used as a production secret.

Recommended:

AIRFLOW__WEBSERVER__SECRET_KEY:
  ${AIRFLOW_SECRET_KEY}

Then provide the expected variable through:

.env

or an appropriate secrets mechanism.

For the portfolio repository, commit only:

.env.example

with placeholder values.


============================================================
P2 — ISSUE 018
DASHBOARD REPOSITORY COUNT IS STALE
============================================================

Severity:
P2 — Presentation / Documentation

The dashboard/documentation refers to approximately:

10 repositories

while the current repository configuration contains:

8 repositories

This is a small issue but visible to reviewers.

Recommended fixes:

Option A:
Update the static text.

Option B:
Calculate the repository count dynamically.

Preferred:

Calculate from the configured/loaded dataset so the dashboard cannot become
stale when repositories are added or removed.


============================================================
DATA MODEL REVIEW
============================================================

The dimensional model is one of the stronger parts of the project.

Current design demonstrates:

- Dimension tables
- Fact tables
- Surrogate keys
- Natural keys
- SCD Type 2
- Accumulating snapshots
- Point-in-time joins
- Date dimension
- Repository dimension
- User dimension

This is above-average for a 4th-year student project.

However, the following semantic issues should be addressed:

1. Repository SCD2 must not imply unsupported historical truth.
2. fact_releases should contain repository relationship.
3. Fact grain should be explicitly documented for every fact.
4. KPI definitions should be documented mathematically.
5. Historical joins should be validated with edge-case data.
6. Natural-key uniqueness should be tested.
7. SCD2 current-row uniqueness should be tested.
8. Foreign-key relationships should be tested where applicable.


============================================================
DATA QUALITY REVIEW
============================================================

The DQ architecture is a strong part of the project.

The pattern:

Bronze
  ↓
Staging
  ↓
DQ evaluation
  ├── clean
  │     ↓
  │   Snapshot
  │     ↓
  │   Silver
  │     ↓
  │   Gold
  │
  └── quarantine

is well designed.

The use of:

evaluate_dq_rules(...)

and:

dq_failed_rules
is_quarantined

shows better engineering maturity than simply failing a dbt model and
discarding invalid records.

Recommended improvements:

- Add more DQ tests
- Add severity levels
- Track quarantine counts
- Surface DQ metrics in Streamlit
- Track DQ failures by repository
- Track DQ failures over time
- Add alerts for abnormal quarantine rates


============================================================
INGESTION REVIEW
============================================================

The ingestion layer is one of the strongest components.

Positive characteristics:

1. Generator-based extraction
2. Pagination support
3. Retry handling
4. Rate-limit awareness
5. GraphQL batching
6. GraphQL fallback
7. S3 chunking
8. Lineage metadata
9. Repository-level task isolation
10. Incremental extraction concept

These demonstrate real API/data-engineering knowledge.

However, the following need stronger guarantees:

- Incremental watermark correctness
- Replayability
- Exactly-once/effectively-once semantics
- S3 idempotency
- Downstream failure recovery
- Duplicate handling
- Extraction run tracking


============================================================
AIRFLOW REVIEW
============================================================

Good:

- Dynamic Task Mapping
- Repository-level parallelism
- Dependency ordering
- Retry configuration
- Daily scheduling
- Separation of extraction and transformation

Problems:

- Docker build currently broken
- Watermark advancement is unsafe
- Failure recovery is not sufficiently explicit
- Run-state semantics should be stronger
- Configuration should be more environment-driven

Recommended conceptual flow:

extract
  ↓
write immutable raw
  ↓
validate raw
  ↓
bronze
  ↓
validate bronze
  ↓
advance watermark
  ↓
dbt
  ↓
export
  ↓
dashboard


============================================================
STREAMLIT REVIEW
============================================================

Strong aspects:

- Clear navigation
- Repository filtering
- KPI cards
- Time-series analysis
- PR complexity analysis
- Contributor analysis
- Plotly visualizations
- DuckDB integration
- Parquet serving

Problems:

1. Median KPI semantics are incorrect.
2. GitHub username currently maps to user ID.
3. Repository count is stale.
4. Dashboard data can become stale if export succeeds/fails independently.
5. No visible data freshness indicator.

Recommended addition:

Display:

Last Updated:
2026-09-21 13:00 UTC

Data Through:
2026-09-20

This makes the dashboard operationally transparent.


============================================================
SECURITY REVIEW
============================================================

Positive:

- API token is not hardcoded in source code.
- Environment-based secret handling is used.
- Credentials are not intended to be committed.

Issues:

1. Development credentials are hardcoded in Docker Compose.
2. Airflow secret key is hardcoded.
3. IAM design and S3 deletion behavior are inconsistent.
4. Production and development configurations are not clearly separated.

Recommended:

- .env.example
- environment variables
- secrets management
- least-privilege IAM
- separate dev/prod configurations
- explicit documentation of local-only credentials


============================================================
TESTING STRATEGY TO ADD
============================================================

Recommended minimum test suite:

tests/
├── test_github_client.py
├── test_extractor.py
├── test_s3_writer.py
├── test_normalizer.py
├── test_repo_config.py
└── fixtures/

Test cases:

GitHub Client:
- Pagination
- Empty page
- 200 response
- 404 response
- 429 response
- 500 response
- Network failure
- Retry exhaustion

GraphQL:
- Successful batch
- Partial batch failure
- Fallback to REST
- Missing node

Extractor:
- Incremental extraction
- Full extraction
- Empty repository
- Duplicate records
- Boundary timestamp

S3:
- Chunk creation
- Upload success
- Upload retry
- Upload failure
- Idempotent rerun

Watermark:
- Initial run
- Successful run
- Failed Bronze
- Failed dbt
- Replay
- Overlap window

Configuration:
- Valid repository
- Invalid repository
- Duplicate repository
- Missing required field


============================================================
CI/CD RECOMMENDATION
============================================================

Add:

.github/
  workflows/
    ci.yml

Recommended pipeline:

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
    ↓
dbt parse

Optional:

SQLFluff

Docker build validation:

docker compose build


============================================================
RECOMMENDED FINAL ARCHITECTURE
============================================================

GitHub API
    |
    | REST + GraphQL
    v
Airflow
    |
    | Dynamic Task Mapping
    v
Python Ingestion
    |
    | immutable raw run
    v
S3 RAW
    |
    v
Databricks / PySpark
    |
    v
Delta BRONZE
    |
    v
dbt STAGING
    |
    v
DQ / QUARANTINE
    |
    v
dbt SNAPSHOTS
    |
    v
SILVER
    |
    v
GOLD
    |
    +----------------------+
    |                      |
    v                      v
S3 Parquet              Databricks
    |
    v
DuckDB
    |
    v
Streamlit


============================================================
WATERMARK / FAILURE-RECOVERY DESIGN
============================================================

Recommended state model:

Run starts
    |
    v
Generate run_id
    |
    v
Read previous successful watermark
    |
    v
Apply safety overlap
    |
    v
Extract API data
    |
    v
Write immutable S3 raw
    |
    v
Validate raw write
    |
    v
Load Bronze
    |
    +---- failure ----> run remains replayable
    |
    v
Bronze success
    |
    v
Advance watermark
    |
    v
dbt transformations
    |
    v
Gold
    |
    v
Parquet export
    |
    v
Dashboard


Important rule:

NEVER advance the watermark merely because API extraction succeeded.

The watermark should represent a successfully processed source boundary.


============================================================
PORTFOLIO PRESENTATION RECOMMENDATION
============================================================

Do NOT add more technologies.

The project already has enough technologies.

Focus on demonstrating engineering maturity.

Resume/project description should emphasize:

- Incremental GitHub API ingestion
- REST + GraphQL
- Rate-limit-aware extraction
- S3 raw data lake
- Databricks Delta Bronze
- dbt transformations
- Data-quality quarantine
- SCD Type 2
- Dimensional modeling
- Airflow Dynamic Task Mapping
- DuckDB serving layer
- Streamlit analytics
- Automated testing
- CI/CD

Avoid claiming:

"Production-grade"

until the P0/P1 issues are fixed.

A more accurate current description is:

"Advanced end-to-end Data Engineering project demonstrating API ingestion,
orchestration, lakehouse processing, dimensional modeling, data quality,
analytics serving, and dashboarding."


============================================================
WHAT SHOULD NOT BE CHANGED
============================================================

Keep:

- Airflow
- Python ingestion
- S3
- Databricks
- PySpark
- dbt
- SCD2
- DQ quarantine
- DuckDB
- Streamlit
- GraphQL enrichment
- Dynamic Task Mapping
- Star schema

These provide strong interview material.

Do not replace the architecture with another technology simply to make the
stack look more impressive.


============================================================
RECOMMENDED IMPLEMENTATION ORDER
============================================================

PHASE 1 — Correctness

[ ] Fix Airflow Docker build
[ ] Fix S3 idempotency
[ ] Fix watermark advancement
[ ] Add watermark overlap/safety window
[ ] Make failed runs replayable
[ ] Verify raw → Bronze failure recovery

PHASE 2 — Data Model

[ ] Fix GitHub username
[ ] Fix median KPI semantics
[ ] Add repo_id to fact_releases
[ ] Fix repository SCD2 validity dates
[ ] Add/verify foreign-key tests
[ ] Document fact grain

PHASE 3 — Testing

[ ] Add GitHub client tests
[ ] Add extractor tests
[ ] Add S3 writer tests
[ ] Add GraphQL tests
[ ] Add watermark tests
[ ] Add configuration tests
[ ] Expand dbt tests

PHASE 4 — Engineering Quality

[ ] Add GitHub Actions CI
[ ] Run Ruff
[ ] Remove unused imports
[ ] Remove stale comments
[ ] Remove duplicate decorators
[ ] Validate Docker build automatically

PHASE 5 — Security

[ ] Remove hardcoded dev secrets where appropriate
[ ] Use .env.example
[ ] Externalize Airflow secret key
[ ] Reconcile S3 IAM permissions
[ ] Document dev vs production configuration

PHASE 6 — Documentation

[ ] Update README
[ ] Update Architecture.md
[ ] Update DATA MODEL.md
[ ] Update CLAUDE.md
[ ] Remove references to obsolete tempfile architecture
[ ] Document watermark semantics
[ ] Document failure recovery
[ ] Document KPI definitions

PHASE 7 — Dashboard

[ ] Fix median labels/calculations
[ ] Fix username display
[ ] Fix repository count
[ ] Add data freshness timestamp
[ ] Add DQ summary metrics


============================================================
FINAL STAFF-LEVEL ASSESSMENT
============================================================

Current project:

Portfolio value:
HIGH

Technical ambition:
HIGH

Data Engineering breadth:
HIGH

Architecture quality:
GOOD

Code quality:
GOOD

Data modeling:
GOOD

Data quality:
GOOD

Testing:
WEAK

Operational reliability:
NEEDS WORK

Deployment reproducibility:
NEEDS WORK

Security:
ACCEPTABLE FOR LOCAL DEMO, NOT PRODUCTION

Documentation:
STRONG BUT PARTIALLY STALE

Over-engineering:
MODERATE

Critical correctness issues:
PRESENT


Current rating:

7.8/10 overall

Expected rating after fixing P0/P1 issues:

~9/10 for a 4th-year Data Engineering portfolio.


MOST IMPORTANT PRINCIPLE:

Do not add another technology.

Fix:

1. Correctness
2. Idempotency
3. Watermark semantics
4. Failure recovery
5. Testing
6. CI
7. Documentation consistency

Those improvements will increase the engineering credibility of the project
far more than adding another tool to the stack.