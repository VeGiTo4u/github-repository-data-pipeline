-- fact_issues.sql
-- Accumulating Snapshot Fact Table

{{
    config(
        materialized='incremental',
        unique_key='issue_id'
    )
}}

select
    md5(cast(i.issue_id as string)) as issue_sk,
    r.repository_sk as repository_sk,
    md5(cast(i.user_id as string)) as author_user_sk,
    i.issue_id,
    i.repository_id as repo_id,
    i.user_id as author_user_id,
    cast(date_format(to_date(i.created_at), 'yyyyMMdd') as int) as created_date_key,
    cast(date_format(to_date(i.closed_at), 'yyyyMMdd') as int) as closed_date_key,
    i.state,
    (unix_timestamp(i.closed_at) - unix_timestamp(i.created_at)) / 3600.0 as time_to_close_hours,
    i.comments_count as comment_count,
    i.updated_at,
    i._bronze_ingest_ts,
    i._extraction_run_id,
    current_timestamp() as _gold_update_ts
from {{ ref('snap_issues') }} i
left join {{ ref('dim_repositories_current') }} r
  on i.repository_id = r.repo_id
where i.dbt_valid_to is null
  and i.is_pull_request = false
{% if is_incremental() %}
  and i.updated_at >= (select coalesce(max(updated_at) - interval 3 days, cast('1900-01-01' as timestamp)) from {{ this }})
{% endif %}
