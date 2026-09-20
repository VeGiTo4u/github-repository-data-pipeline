-- fact_releases.sql
-- Transactional Fact Table

{{
    config(
        materialized='incremental',
        unique_key='release_id'
    )
}}

select
    md5(cast(release_id as string)) as release_sk,
    md5(cast(author_id as string)) as author_user_sk,
    release_id,
    -- ponytail: skip repo_id, it is not present in releases intermediate model.
    -- you must parse it out of url or pass it down from extraction to use it here.
    author_id as author_user_id,
    cast(date_format(to_date(published_at), 'yyyyMMdd') as int) as published_date_key,
    tag_name,
    is_prerelease,
    _bronze_ingest_ts,
    _extraction_run_id,
    current_timestamp() as _gold_update_ts
from {{ ref('releases') }}
where is_quarantined = false
