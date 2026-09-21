{{
    config(
        materialized='table'
    )
}}

with prs as (
    select
        pr_id,
        repo_id,
        author_user_id,
        created_date_key,
        merged_date_key,
        time_to_merge_hours,
        additions,
        deletions,
        changed_files,
        commits_count,
        (coalesce(additions, 0) + coalesce(deletions, 0)) as total_lines_changed
    from {{ ref('fact_pull_requests') }}
),

repos as (
    select repo_id, repo_name from {{ ref('dim_repositories') }} where is_current = true
)

select
    p.pr_id,
    r.repo_name,
    p.repo_id,
    p.author_user_id,
    p.created_date_key,
    p.merged_date_key,
    p.time_to_merge_hours,
    p.additions,
    p.deletions,
    p.changed_files,
    p.commits_count,
    p.total_lines_changed,
    p.additions / nullif(p.total_lines_changed, 0) as additions_ratio,
    p.deletions / nullif(p.total_lines_changed, 0) as deletions_ratio,
    case
        when p.total_lines_changed < 100 then 'Small'
        when p.total_lines_changed >= 100 and p.total_lines_changed < 500 then 'Medium'
        when p.total_lines_changed >= 500 and p.total_lines_changed < 1000 then 'Large'
        when p.total_lines_changed >= 1000 then 'XL'
        else 'Unknown'
    end as pr_size_bucket
from prs p
left join repos r on p.repo_id = r.repo_id
