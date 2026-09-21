-- fact_releases.sql
-- Transactional Fact Table

{{
    config(
        materialized='incremental',
        unique_key='release_id'
    )
}}

select
    md5(cast(rel.release_id as string)) as release_sk,
    r.repository_sk as repository_sk,
    md5(cast(rel.author_id as string)) as author_user_sk,
    rel.release_id,
    rel.author_id as author_user_id,
    cast(date_format(to_date(rel.published_at), 'yyyyMMdd') as int) as published_date_key,
    rel.tag_name,
    rel.is_prerelease,
    rel._bronze_ingest_ts,
    rel._extraction_run_id,
    current_timestamp() as _gold_update_ts
from {{ ref('releases') }} rel
left join {{ ref('dim_repositories_current') }} r
  on rel._repo_full_name = r.repo_name
where rel.is_quarantined = false
