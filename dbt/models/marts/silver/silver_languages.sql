-- silver_languages.sql
-- Type 1 full overwrite (no snapshot, no incremental).
-- Sources from intermediate (DQ-validated). Carries DQ columns through.
-- Languages are trivially small and fully re-fetched every run,
-- so a full table rebuild is simpler and correct. (Phase-2.md §3)

{{
    config(
        materialized='table',
    )
}}

select
    repo_slug,
    language,
    bytes,
    _bronze_ingest_ts,
    _ingestion_date,
    dq_failed_rules,
    is_quarantined
from {{ ref('int_languages_validated') }}
