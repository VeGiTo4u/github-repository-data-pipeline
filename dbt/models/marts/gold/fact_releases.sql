-- fact_releases.sql
-- Transactional Fact Table

{{
    config(
        materialized='incremental',
        unique_key='release_id'
    )
}}

select
    release_id,
    -- ponytail: skip repo_id, it is not present in releases intermediate model.
    -- you must parse it out of url or pass it down from extraction to use it here.
    xxhash64(author_login) as author_user_id,
    cast(date_format(to_date(published_at), 'yyyyMMdd') as int) as published_date_key,
    tag_name,
    is_prerelease
from {{ ref('silver_releases') }}
where is_quarantined = false
