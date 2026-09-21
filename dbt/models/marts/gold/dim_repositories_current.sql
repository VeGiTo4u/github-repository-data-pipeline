-- dim_repositories_current.sql
-- The current state of each repository. Facts join to this table for identity.

{{
    config(
        materialized='table'
    )
}}

select
    dbt_scd_id as repository_sk,
    repository_id as repo_id,
    full_name as repo_name,
    owner_login,
    primary_language,
    license as license_name,
    _bronze_ingest_ts,
    _extraction_run_id,
    current_timestamp() as _gold_update_ts
from {{ ref('snap_repositories') }}
where dbt_valid_to is null
