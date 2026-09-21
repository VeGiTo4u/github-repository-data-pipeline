-- dim_users.sql
-- Type 1 Overwrite. Deduplicated from issues, PRs, and releases.
-- Skipped assignee/reviewer/user_type extraction since not parsed in staging. Add when staging parser updated.

{{
    config(
        materialized='table'
    )
}}

with all_users as (
    select 
        user_id, 
        user_login as login,
        _bronze_ingest_ts,
        _extraction_run_id
    from {{ ref('issues_current') }} where is_quarantined = false
    union all
    select 
        user_id, 
        user_login as login,
        _bronze_ingest_ts,
        _extraction_run_id
    from {{ ref('pull_requests_current') }} where is_quarantined = false
    union all
    select 
        author_id as user_id, 
        author_login as login,
        _bronze_ingest_ts,
        _extraction_run_id
    from {{ ref('releases') }} where is_quarantined = false
),
ranked_users as (
    select 
        user_id,
        login,
        _bronze_ingest_ts,
        _extraction_run_id,
        row_number() over (partition by user_id order by _bronze_ingest_ts desc) as rn
    from all_users
    where login is not null
)
select 
    md5(cast(user_id as string)) as user_sk,
    user_id,
    login as user_login,
    'User' as user_type,
    _bronze_ingest_ts,
    _extraction_run_id,
    current_timestamp() as _gold_update_ts
from ranked_users
where rn = 1
