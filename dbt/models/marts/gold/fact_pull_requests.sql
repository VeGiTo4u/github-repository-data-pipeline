-- fact_pull_requests.sql
-- Accumulating Snapshot Fact Table

{{
    config(
        materialized='incremental',
        unique_key='pr_id'
    )
}}

select
    pull_request_id as pr_id,
    repository_id as repo_id,
    user_id as author_user_id,
    cast(date_format(to_date(created_at), 'yyyyMMdd') as int) as created_date_key,
    cast(date_format(to_date(merged_at), 'yyyyMMdd') as int) as merged_date_key,
    cast(date_format(to_date(closed_at), 'yyyyMMdd') as int) as closed_date_key,
    state,
    (unix_timestamp(merged_at) - unix_timestamp(created_at)) / 3600.0 as time_to_merge_hours,
    (unix_timestamp(closed_at) - unix_timestamp(created_at)) / 3600.0 as time_to_close_hours,
    review_comments_count as comment_count,
    additions,
    deletions,
    changed_files,
    commits_count,
    updated_at
from {{ ref('snap_pull_requests') }}
where dbt_valid_to is null
{% if is_incremental() %}
  and updated_at >= (select max(updated_at) from {{ this }})
{% endif %}
