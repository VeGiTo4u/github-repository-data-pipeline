-- fact_issues.sql
-- Accumulating Snapshot Fact Table

{{
    config(
        materialized='incremental',
        unique_key='issue_id'
    )
}}

select
    issue_id,
    repository_id as repo_id,
    user_id as author_user_id,
    cast(date_format(to_date(created_at), 'yyyyMMdd') as int) as created_date_key,
    cast(date_format(to_date(closed_at), 'yyyyMMdd') as int) as closed_date_key,
    state,
    (unix_timestamp(closed_at) - unix_timestamp(created_at)) / 3600.0 as time_to_close_hours,
    comments_count as comment_count,
    updated_at
from {{ ref('snap_issues') }}
where dbt_valid_to is null
  and is_pull_request = false
{% if is_incremental() %}
  and updated_at >= (select max(updated_at) from {{ this }})
{% endif %}
