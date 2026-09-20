-- dim_users.sql
-- Type 1 Overwrite. Deduplicated from issues, PRs, and releases.
-- ponytail: skipped assignee/reviewer/user_type extraction since not parsed in staging. add when staging parser updated.

{{
    config(
        materialized='table',
        unique_key='user_id'
    )
}}

with all_users as (
    select user_id, user_login as login from {{ ref('issues_current') }} where is_quarantined = false
    union all
    select user_id, user_login as login from {{ ref('pull_requests_current') }} where is_quarantined = false
    union all
    select author_id as user_id, author_login as login from {{ ref('releases') }} where is_quarantined = false
)
select 
    user_id,
    login as user_login,
    'User' as user_type 
from all_users
where login is not null
group by user_id, login
