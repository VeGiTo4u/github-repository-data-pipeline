-- fact_pull_requests.sql
-- Accumulating Snapshot Fact Table

{{
    config(
        materialized='incremental',
        unique_key='pr_id'
    )
}}

select
    md5(cast(p.pull_request_id as string)) as pull_request_sk,
    r.repository_sk as repository_sk,
    md5(cast(p.user_id as string)) as author_user_sk,
    p.pull_request_id as pr_id,
    p.repository_id as repo_id,
    p.user_id as author_user_id,
    cast(date_format(to_date(p.created_at), 'yyyyMMdd') as int) as created_date_key,
    cast(date_format(to_date(p.merged_at), 'yyyyMMdd') as int) as merged_date_key,
    cast(date_format(to_date(p.closed_at), 'yyyyMMdd') as int) as closed_date_key,
    p.state,
    (unix_timestamp(p.merged_at) - unix_timestamp(p.created_at)) / 3600.0 as time_to_merge_hours,
    (unix_timestamp(p.closed_at) - unix_timestamp(p.created_at)) / 3600.0 as time_to_close_hours,
    p.review_comments_count as comment_count,
    p.additions,
    p.deletions,
    p.changed_files,
    p.commits_count,
    p.updated_at,
    p._bronze_ingest_ts,
    p._extraction_run_id,
    current_timestamp() as _gold_update_ts
from {{ ref('snap_pull_requests') }} p
left join {{ ref('dim_repositories_current') }} r
  on p.repository_id = r.repo_id
where p.dbt_valid_to is null
{% if is_incremental() %}
  and p.updated_at >= (select coalesce(max(updated_at) - interval 3 days, cast('1900-01-01' as timestamp)) from {{ this }})
{% endif %}
