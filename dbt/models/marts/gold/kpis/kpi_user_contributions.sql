{{
    config(
        materialized='table'
    )
}}

with user_issues as (
    select
        repo_id,
        author_user_id as user_id,
        count(issue_id) as total_issues_opened,
        sum(comment_count) as total_issue_comments
    from {{ ref('fact_issues') }}
    group by 1, 2
),

user_prs as (
    select
        repo_id,
        author_user_id as user_id,
        count(pr_id) as total_prs_opened,
        sum(case when merged_date_key is not null then 1 else 0 end) as total_prs_merged,
        sum(comment_count) as total_pr_comments,
        sum(additions) as total_additions,
        sum(deletions) as total_deletions
    from {{ ref('fact_pull_requests') }}
    group by 1, 2
),

all_users_repos as (
    select repo_id, user_id from user_issues
    union
    select repo_id, user_id from user_prs
),

repos as (
    select repo_id, repo_name from {{ ref('dim_repositories') }} where is_current = true
)

select
    r.repo_name,
    ur.repo_id,
    ur.user_id,
    u.user_id as github_username, -- if dim_users has username, else just ID
    coalesce(ui.total_issues_opened, 0) as total_issues_opened,
    coalesce(up.total_prs_opened, 0) as total_prs_opened,
    coalesce(up.total_prs_merged, 0) as total_prs_merged,
    coalesce(ui.total_issue_comments, 0) + coalesce(up.total_pr_comments, 0) as total_comments,
    coalesce(up.total_additions, 0) as total_additions,
    coalesce(up.total_deletions, 0) as total_deletions
from all_users_repos ur
left join user_issues ui on ur.repo_id = ui.repo_id and ur.user_id = ui.user_id
left join user_prs up on ur.repo_id = up.repo_id and ur.user_id = up.user_id
left join repos r on ur.repo_id = r.repo_id
left join {{ ref('dim_users') }} u on ur.user_id = u.user_id
