{{
    config(
        materialized='table'
    )
}}

with repos as (
    select repo_id, repo_name 
    from {{ ref('dim_repositories') }}
    where is_current = true
),

issues as (
    select 
        repo_id,
        count(issue_id) as total_issues,
        sum(case when state = 'open' then 1 else 0 end) as open_issues,
        median(time_to_close_hours) as median_time_to_close_hours,
        count(distinct author_user_id) as issue_contributors
    from {{ ref('fact_issues') }}
    group by 1
),

prs as (
    select 
        repo_id,
        count(pr_id) as total_prs,
        sum(case when state = 'open' then 1 else 0 end) as open_prs,
        median(time_to_merge_hours) as median_time_to_merge_hours,
        count(distinct author_user_id) as pr_contributors
    from {{ ref('fact_pull_requests') }}
    group by 1
)

select
    r.repo_id,
    r.repo_name,
    coalesce(i.total_issues, 0) as total_issues,
    coalesce(i.open_issues, 0) as open_issues,
    i.median_time_to_close_hours,
    coalesce(p.total_prs, 0) as total_prs,
    coalesce(p.open_prs, 0) as open_prs,
    p.median_time_to_merge_hours,
    (coalesce(i.total_issues, 0) - coalesce(i.open_issues, 0)) / nullif(i.total_issues, 0) as closed_to_open_issue_ratio,
    (coalesce(p.total_prs, 0) - coalesce(p.open_prs, 0)) / nullif(p.total_prs, 0) as merged_pr_ratio,
    (coalesce(i.issue_contributors, 0) + coalesce(p.pr_contributors, 0)) as approximate_total_contributors
from repos r
left join issues i on r.repo_id = i.repo_id
left join prs p on r.repo_id = p.repo_id
