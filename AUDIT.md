# GitHub Repository Analytics Pipeline
# Remaining Issues + Exact Fixes
# Final Engineering Remediation Checklist

Project Status:
- Current Portfolio Rating: ~8.5/10
- Target After Remaining Fixes: ~9/10
- Suitable for a 4th-year Data Engineering portfolio: YES
- Major architecture redesign required: NO
- Focus now: correctness, reliability, semantics, testing, and cleanup

============================================================
PRIORITY SUMMARY
============================================================

P0/P1 — MUST FIX

1. Watermark uses the wrong timestamp
2. Dynamic Task Mapping fault-isolation claim is incorrect
3. fact_releases is missing repository foreign key
4. SCD2 documentation overstates historical accuracy

P2 — SHOULD FIX

5. Remove stale tempfile documentation/comments
6. Remove obsolete extractor `results`
7. Remove `dbt clean` from scheduled production execution
8. Remove/parameterize development credentials
9. Clean duplicate/redundant dependency installation
10. Clarify S3 replacement semantics

P1 — ENGINEERING MATURITY

11. Add Python unit tests
12. Add GitHub Actions CI


============================================================
ISSUE 1
WATERMARK IS ADVANCED USING THE WRONG TIMESTAMP
============================================================

Priority:
P0/P1 — Data Correctness

Current improvement:

The watermark is now advanced AFTER Bronze processing.

Current flow:

extract
  ↓
S3
  ↓
Bronze
  ↓
advance_watermarks
  ↓
dbt

This is correct.

However, `_advance_watermarks()` currently generates a NEW timestamp:

current_run_ts = datetime.now(timezone.utc)

This timestamp represents the time when the watermark task executes.

It does NOT necessarily represent the source extraction boundary.

Example:

10:00 — extraction begins
10:30 — extraction finishes
11:00 — Bronze finishes
11:05 — watermark task executes

Current watermark:

11:05

But the extraction did not necessarily include source changes occurring after:

10:30

The next extraction uses:

watermark - 10 minutes

Therefore:

11:05 - 10 minutes
=
10:55

Data updated between:

10:30 and 10:55

could potentially be missed.

The overlap window does not completely solve this because the watermark itself
was moved beyond the actual extraction boundary.


FIX
---

Capture an extraction boundary BEFORE extraction begins.

Example:

extraction_boundary = current UTC timestamp

Then use:

previous_watermark - safety_window

as the extraction lower boundary.

After:

1. Extraction succeeds
2. Raw S3 write succeeds
3. Bronze succeeds

commit:

extraction_boundary

NOT:

datetime.now() at the watermark task.


Recommended flow:

previous_successful_watermark
        ↓
subtract safety window
        ↓
capture extraction_boundary
        ↓
extract GitHub
        ↓
write S3
        ↓
load Bronze
        ↓
commit extraction_boundary
        ↓
dbt


Example:

10:00
extraction_boundary = 10:00

Extraction finishes:
10:30

Bronze finishes:
11:00

Watermark committed:
10:00


Next run:

10:00 - 10 minutes
=
09:50


This provides a real overlap window around the actual extraction boundary.


RECOMMENDED IMPLEMENTATION
--------------------------

At the beginning of the repository extraction task:

extraction_boundary = datetime.now(timezone.utc)


Pass the boundary through the task result or run metadata.

After Bronze succeeds:

advance_watermark(repo, extraction_boundary)


Do NOT generate a fresh timestamp inside `_advance_watermarks()`.


BEST LONG-TERM DESIGN
---------------------

Track:

run_id
extraction_started_at
extraction_boundary
bronze_completed_at
watermark_committed_at


This gives the pipeline explicit operational lineage.


============================================================
ISSUE 2
DYNAMIC TASK MAPPING DOES NOT PROVIDE THE FAULT ISOLATION
CLAIMED BY THE DOCUMENTATION
============================================================

Priority:
P1 — Architecture / Documentation Correctness

Current architecture:

extract_repo[repo1] ─┐
extract_repo[repo2] ─┤
extract_repo[repo3] ─┤
extract_repo[repo4] ─┤
                     ↓
                 run_bronze


Suppose:

repo1 = SUCCESS
repo2 = SUCCESS
repo3 = SUCCESS
repo4 = FAILED


The mapped extraction tasks can execute independently.

However, the downstream Bronze task depends on the mapped extraction stage.

Therefore Bronze will not execute normally if the mapped extraction stage has a
failed task instance.

Consequently:

repo4 fails
    ↓
Bronze does not execute
    ↓
watermarks do not advance
    ↓
dbt does not execute
    ↓
entire pipeline run fails


The README currently implies:

"A failure in microsoft/vscode doesn't block duckdb/duckdb."

That is misleading.

The mapped extraction tasks are independently scheduled, but the downstream
pipeline is still effectively all-or-nothing.


RECOMMENDED FIX
---------------

For this project, DO NOT implement partial-success processing unless there
is a strong reason to do so.

Instead, change the documentation to explicitly state:

"Dynamic Task Mapping allows repositories to execute as independent Airflow
task instances. Extraction failures are isolated at the task-instance level,
but downstream Bronze processing intentionally requires the complete mapped
extraction stage to succeed."


This gives you a clear all-or-nothing daily batch design.


ALTERNATIVE — PARTIAL SUCCESS
-----------------------------

If true repository-level fault isolation is desired:

extract repositories
        ↓
collect successful repositories
        ↓
Bronze only successful repositories
        ↓
advance watermark only for successful repositories
        ↓
dbt


DO NOT simply change Bronze to:

trigger_rule="all_done"

That would be dangerous.

A failed repository must NEVER have its watermark advanced.


RECOMMENDATION
--------------

Use the simpler all-or-nothing design.

It is easier to reason about and perfectly acceptable for this portfolio.


============================================================
ISSUE 3
FACT_RELEASES IS MISSING REPOSITORY FOREIGN KEY
============================================================

Priority:
P1 — Data Modeling

Current fact:

fact_releases

contains information such as:

release_sk
author_user_sk
release_id
release_name
published_at
etc.

But it does not contain:

repo_id
repository_sk


The model itself acknowledges:

"repo_id is not present in releases intermediate model."


This weakens the star schema.

The release API endpoint is repository-specific:

/repos/{owner}/{repo}/releases


The raw extraction already knows the repository:

repo_full_name


Therefore repository identity is being lost during transformation.


WHY THIS MATTERS
----------------

Without repository context, questions such as:

- Which repository releases most frequently?
- Which repository has the most releases?
- Release frequency by repository?
- Releases by repository over time?

become unnecessarily difficult.


FIX
---

Carry repository identity through the entire transformation pipeline:

GitHub API
    ↓
raw
    ↓
Bronze
    ↓
staging
    ↓
intermediate
    ↓
Silver
    ↓
fact_releases


Recommended columns:

repo_id
or
repository_sk


Preferably resolve the repository through:

dim_repositories


Final fact should conceptually contain:

fact_releases
    release_sk
    repository_sk
    author_user_sk
    release_id
    release_name
    release_created_at
    release_published_at
    ...


Add dbt relationship testing:

release.repository_sk
    → dim_repositories.repository_sk


Also add uniqueness tests where appropriate.


============================================================
ISSUE 4
SCD2 DOCUMENTATION OVERSTATES HISTORICAL ACCURACY
============================================================

Priority:
P1 — Semantic Correctness

GOOD NEWS:

The artificial:

1900-01-01

valid_from value has been removed.

That issue is FIXED.


However, the documentation currently implies that the SCD2 model provides the
repository's exact metadata state at the time of an issue/PR event.

That is not strictly guaranteed.

Example:

Repository metadata actually changed:
January 2025

Pipeline observed the change:
January 2026

Issue created:
June 2025


The pipeline does not possess the actual June 2025 repository metadata unless
GitHub provided that historical state.

Therefore the model represents:

"repository metadata observed by the pipeline"

rather than:

"perfect historical business-time repository state."


FIX
---

Update documentation language.

Do NOT say:

"exact repository metadata state at the time of the event"


Use:

"repository metadata state represented by the latest available pipeline
observation covering the event timestamp."


Or explicitly define the dimension as:

Observation-Time SCD Type 2


Recommended explanation:

"The repository dimension tracks observed metadata states over successive
pipeline snapshots. Point-in-time joins associate facts with the latest
available observed repository state covering the event timestamp. This should
not be interpreted as a complete reconstruction of GitHub's historical
metadata."


This is technically honest and defensible in an interview.


============================================================
ISSUE 5
STALE TEMPFILE DOCUMENTATION
============================================================

Priority:
P2 — Documentation / Code Quality

The implementation has moved to direct streaming:

GitHub generator
    ↓
lineage envelope
    ↓
S3 streaming


However, parts of the documentation still refer to:

- temporary files
- tempfile paths
- uploading temp files
- temp-file encoding
- watermark before temp-file upload


This is leftover from the previous implementation.


FIX
---

Search the complete repository for:

tempfile
temporary file
temp files
temp file
temp_file_path
before uploading temp files


Remove obsolete references.


Specifically review:

README.md
docs/Architecture.md
docs/CLAUDE.md
extractor.py
DAG comments


The documentation must describe the current implementation, not the previous
architecture.


============================================================
ISSUE 6
REMOVE OBSOLETE `results` FROM EXTRACTOR
============================================================

Priority:
P2 — Code Quality

The extractor still has an old return structure similar to:

results, pr_numbers


and:

results = []


with entries such as:

(None, s3_key_prefix, resource_type)


The old docstring also describes:

temp_file_path


But temporary files no longer exist.

The DAG does not meaningfully use `results`.


FIX
---

Simplify the extractor API.

Current conceptual design:

results, pr_numbers = extract_repository_metadata(...)


Recommended:

pr_numbers = extract_repository_metadata(...)


The extractor should return only information actually required by the DAG.


Update the docstring accordingly.

Example:

Returns:
    List of PR numbers requiring GraphQL enrichment.


This removes dead architectural baggage from the old tempfile implementation.


============================================================
ISSUE 7
REMOVE `dbt clean` FROM EVERY SCHEDULED RUN
============================================================

Priority:
P2 — Operational Hygiene

Current command is approximately:

dbt clean && dbt build


`dbt clean` is normally a development/maintenance command.

Running it on every scheduled production execution:

- removes generated artifacts
- removes installed packages depending on configuration
- increases runtime
- introduces unnecessary failure points
- provides little value during normal scheduled execution


FIX
---

Scheduled Airflow task should run:

dbt build --profiles-dir /opt/airflow/dbt


Use manually when troubleshooting:

dbt clean


This makes scheduled execution simpler and more predictable.


============================================================
ISSUE 8
AIRFLOW DEPENDENCIES ARE DECLARED TWICE
============================================================

Priority:
P2 — Maintainability

The Airflow requirements file contains packages such as:

apache-airflow-providers-databricks
dbt-databricks
databricks-sdk


The Dockerfile also installs these packages explicitly.


This creates duplicate dependency declarations.


FIX
---

Keep dependency declarations in:

airflow/requirements.txt


Dockerfile should simply:

COPY airflow/requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt


Avoid maintaining the same dependency list in two places.


NOTE:

The Airflow base image already provides Airflow itself.

Avoid unnecessarily reinstalling:

apache-airflow==2.10.5

unless there is a specific reason to pin/reinstall it.


============================================================
ISSUE 9
DEVELOPMENT CREDENTIALS SHOULD BE EXTERNALIZED
============================================================

Priority:
P2 — Security

Current Docker configuration still contains development defaults such as:

AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin

and a hardcoded Airflow secret key.


These are acceptable for a local demo but should not look like production
configuration.


FIX
---

Use environment variables:

AIRFLOW_ADMIN_USERNAME=${AIRFLOW_ADMIN_USERNAME}
AIRFLOW_ADMIN_PASSWORD=${AIRFLOW_ADMIN_PASSWORD}
AIRFLOW__WEBSERVER__SECRET_KEY=${AIRFLOW_SECRET_KEY}


Provide:

.env.example


Example:

AIRFLOW_ADMIN_USERNAME=<your-admin-username>
AIRFLOW_ADMIN_PASSWORD=<your-admin-password>
AIRFLOW_SECRET_KEY=<generate-a-random-secret>


Do not commit `.env`.


Document clearly:

"These values are development-only and should be replaced in real
deployments."


============================================================
ISSUE 10
S3 REPLACEMENT DESIGN HAS A FAILURE WINDOW
============================================================

Priority:
P2 — Reliability Hardening

Current raw ingestion approach is approximately:

list existing objects
    ↓
delete objects
    ↓
upload new objects


This is much better than silently ignoring deletion failures because deletion
errors now fail the task.


However, consider:

delete succeeds
    ↓
upload begins
    ↓
upload fails halfway


The previous valid raw dataset has already been deleted.


This means a failed upload can leave the prefix incomplete.


CURRENT STATUS:

Acceptable for this portfolio.

Not an immediate blocker.


BEST LONG-TERM DESIGN
---------------------

Use immutable run-specific paths:

raw/
    issues/
        ingestion_date=2026-09-21/
            run_id=<run_id>/
                part_001.json
                part_002.json


Write the complete new run first.

Then mark the run as successful.

Bronze reads only successful runs.


This provides:

- replayability
- atomic publishing semantics
- historical raw data
- easier debugging
- safer retries


Do NOT implement this merely to increase technology complexity.

It is a future hardening improvement.


============================================================
ISSUE 11
TESTING IS STILL MISSING
============================================================

Priority:
P1 — Engineering Maturity

The project still needs Python unit tests.


Minimum recommended structure:

tests/
    test_github_client.py
    test_extractor.py
    test_s3_writer.py
    test_normalizer.py
    test_repo_config.py


Recommended test cases:


GitHub Client:

[ ] pagination
[ ] empty response
[ ] 429 rate limit
[ ] 403 rate limit
[ ] 500 server error
[ ] network failure
[ ] retry exhaustion


GraphQL:

[ ] successful batch
[ ] partial batch failure
[ ] fallback behavior
[ ] missing node


Extractor:

[ ] full extraction
[ ] incremental extraction
[ ] empty repository
[ ] PR detail extraction
[ ] duplicate handling


S3 Writer:

[ ] chunking
[ ] empty input
[ ] successful upload
[ ] retry
[ ] upload failure
[ ] deletion failure


Watermark:

[ ] first run
[ ] successful run
[ ] failed extraction
[ ] failed Bronze
[ ] replay
[ ] safety overlap


Configuration:

[ ] valid repository
[ ] invalid repository
[ ] duplicate repository
[ ] malformed configuration


TARGET:

Approximately 15–25 high-value tests.

Do NOT spend time trying to reach 100% code coverage.

Test the failure modes that matter.


============================================================
ISSUE 12
CI/CD IS STILL MISSING
============================================================

Priority:
P1 — Engineering Maturity

Add:

.github/
    workflows/
        ci.yml


Minimum CI:

checkout
    ↓
Python setup
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


Do NOT require a live Databricks environment for basic CI.


Recommended CI checks:

ruff check .
pytest
python -m compileall .
YAML parsing


Optional:

dbt parse --profiles-dir ...


CI should validate the repository on every:

push
pull request


Deployment automation is not necessary for this student portfolio.


============================================================
FINAL WATERMARK DESIGN
============================================================

The recommended final implementation is:

1. Read previous successful watermark.

2. Calculate:

lower_bound =
previous_watermark - safety_window


3. Capture:

extraction_boundary =
current UTC timestamp


4. Extract all records:

updated_at >= lower_bound
AND
updated_at < extraction_boundary


5. Write raw S3 data.

6. Load Bronze.

7. If Bronze succeeds:

commit:

watermark = extraction_boundary


8. Run dbt.

9. Export Gold data.


Important:

The watermark represents the SOURCE EXTRACTION BOUNDARY that has been
successfully processed.

It must NOT represent the time at which the watermark task happened.


============================================================
FINAL AIRFLOW DESIGN
============================================================

Recommended semantics:

extract_repo[repo1]
extract_repo[repo2]
extract_repo[repo3]
...
        ↓
ALL extraction tasks succeed
        ↓
Bronze
        ↓
watermark commit
        ↓
dbt
        ↓
Gold
        ↓
Parquet
        ↓
Streamlit


This is an intentional:

ALL-OR-NOTHING DAILY BATCH


Document it that way.

Dynamic Task Mapping provides:

- independent task instances
- repository-level execution
- parallelism
- retry isolation


It does NOT mean:

"one repository can fail while the rest of the downstream pipeline continues."


============================================================
FINAL DATA MODEL
============================================================

Recommended:

dim_repositories
    repository_sk
    repository_id
    repository_name
    ...


dim_users
    user_sk
    user_id
    user_login
    ...


dim_date
    date_key
    ...


fact_issues
    issue_sk
    repository_sk
    user_sk
    created_date_key
    ...


fact_pull_requests
    pr_sk
    repository_sk
    user_sk
    created_date_key
    ...


fact_releases
    release_sk
    repository_sk
    author_user_sk
    published_date_key
    ...


All foreign-key relationships should be validated using dbt tests.


============================================================
FINAL DOCUMENTATION RULES
============================================================

Documentation should explicitly state:

1. Raw data is streamed directly to S3.
2. There is no tempfile-based extraction architecture.
3. Bronze is loaded after all mapped repository extractions succeed.
4. Watermarks advance only after Bronze succeeds.
5. Watermarks represent the extraction boundary.
6. A safety overlap is used for incremental extraction.
7. Repository SCD2 represents observed repository states.
8. SCD2 does not reconstruct perfect GitHub historical metadata.
9. DuckDB is the lightweight serving layer for Streamlit.
10. Databricks remains the transformation/analytical platform.
11. Development credentials are not production credentials.


============================================================
FINAL REMEDIATION ORDER
============================================================

PHASE 1 — Correctness

[ ] Fix extraction boundary / watermark implementation
[ ] Update watermark tests
[ ] Correct Dynamic Task Mapping documentation
[ ] Add repository relationship to fact_releases
[ ] Correct SCD2 documentation


PHASE 2 — Cleanup

[ ] Remove tempfile references
[ ] Remove obsolete extractor `results`
[ ] Remove unused imports
[ ] Remove `dbt clean` from scheduled DAG
[ ] Consolidate Airflow dependencies
[ ] Externalize development credentials


PHASE 3 — Testing

[ ] Add GitHub client tests
[ ] Add extractor tests
[ ] Add S3 writer tests
[ ] Add GraphQL tests
[ ] Add watermark tests
[ ] Add configuration tests
[ ] Add dbt tests for new repository relationships


PHASE 4 — CI

[ ] Add GitHub Actions
[ ] Run Ruff
[ ] Run pytest
[ ] Run compileall
[ ] Validate YAML
[ ] Optionally run dbt parse


PHASE 5 — Final Validation

[ ] Build Airflow Docker image from clean checkout
[ ] Start Docker Compose
[ ] Validate Airflow DAG import
[ ] Run a full extraction
[ ] Verify S3 raw data
[ ] Verify Bronze
[ ] Verify dbt
[ ] Verify Gold
[ ] Verify Parquet export
[ ] Verify DuckDB
[ ] Verify Streamlit
[ ] Test an intentional extraction failure
[ ] Test an intentional Bronze failure
[ ] Verify watermark does not advance incorrectly
[ ] Verify retry behavior
[ ] Verify documentation matches implementation


============================================================
FINAL STAFF DATA ENGINEER SIGN-OFF CRITERIA
============================================================

Before calling the project finished, the following should be true:

[ ] Docker build works from a clean checkout
[ ] Airflow starts successfully
[ ] DAG imports successfully
[ ] GitHub extraction works
[ ] Rate limiting works
[ ] Retry behavior works
[ ] S3 writes are deterministic
[ ] Bronze is idempotent
[ ] Watermark represents a real extraction boundary
[ ] Watermark only advances after successful Bronze
[ ] Failed runs are safely replayable
[ ] fact_releases has repository relationship
[ ] SCD2 documentation accurately describes its semantics
[ ] Dashboard metrics have mathematically correct names
[ ] Python tests exist
[ ] CI exists
[ ] Documentation matches the current implementation
[ ] No obsolete tempfile architecture remains
[ ] No unnecessary production claims remain


============================================================
FINAL ASSESSMENT
============================================================

Current project:

Portfolio value:
HIGH

Data Engineering breadth:
HIGH

Architecture:
STRONG

Data modeling:
STRONG

Ingestion:
STRONG

Data quality:
STRONG

Documentation:
STRONG, but needs final consistency cleanup

Testing:
CURRENTLY WEAK

CI/CD:
CURRENTLY MISSING

Operational reliability:
GOOD, but watermark semantics need one final correction

Production readiness:
NOT YET

Student portfolio readiness:
STRONG


CURRENT RATING:

~8.5/10


EXPECTED RATING AFTER THESE FIXES:

~9/10


MOST IMPORTANT REMAINING TECHNICAL FIX:

Fix the watermark to commit the actual extraction boundary rather than the
timestamp generated by the later watermark task.


MOST IMPORTANT DOCUMENTATION FIX:

Stop claiming repository-level downstream fault isolation from Dynamic Task
Mapping. The current architecture is an intentional all-or-nothing daily
batch.


MOST IMPORTANT DATA-MODELING FIX:

Add repository relationship to fact_releases.


MOST IMPORTANT ENGINEERING-MATURITY ADDITION:

Add approximately 15–25 focused tests and a simple GitHub Actions CI workflow.


DO NOT ADD ANOTHER TECHNOLOGY.

The architecture is already sufficiently complex.

The remaining improvement should come from:

correctness
+
reliability
+
testing
+
CI
+
semantic precision
+
documentation consistency