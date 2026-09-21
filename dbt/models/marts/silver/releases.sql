-- silver_releases.sql
-- Type 1 incremental merge by release_id (no SCD2 snapshot).
-- Sources from intermediate (DQ-validated). Carries DQ columns through.
-- Releases are effectively immutable once published.

{{
    config(
        materialized='incremental',
        unique_key='release_id',
    )
}}

select
    release_id,
    tag_name,
    release_name,
    is_draft,
    is_prerelease,
    author_id,
    author_login,
    created_at,
    published_at,
    target_commitish,
    _bronze_ingest_ts,
    _extraction_run_id,
    _ingestion_date,
    _repo_full_name,
    repository_id,
    dq_failed_rules,
    is_quarantined
from {{ ref('int_releases_validated') }}
