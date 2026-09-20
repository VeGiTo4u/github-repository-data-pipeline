{{
    config(
        materialized='table'
    )
}}

with date_spine as (
    select distinct 
        year, 
        month, 
        date_format(date_actual, 'yyyy-MM') as month_year,
        date_key
    from {{ ref('dim_date') }}
),

issues_opened as (
    select
        i.repo_id,
        d.month_year,
        count(i.issue_id) as issues_opened
    from {{ ref('fact_issues') }} i
    join date_spine d on i.created_date_key = d.date_key
    group by 1, 2
),

issues_closed as (
    select
        i.repo_id,
        d.month_year,
        count(i.issue_id) as issues_closed,
        median(i.time_to_close_hours) as median_time_to_close_hours
    from {{ ref('fact_issues') }} i
    join date_spine d on i.closed_date_key = d.date_key
    where i.closed_date_key is not null
    group by 1, 2
),

prs_opened as (
    select
        p.repo_id,
        d.month_year,
        count(p.pr_id) as prs_opened
    from {{ ref('fact_pull_requests') }} p
    join date_spine d on p.created_date_key = d.date_key
    group by 1, 2
),

prs_merged as (
    select
        p.repo_id,
        d.month_year,
        count(p.pr_id) as prs_merged,
        median(p.time_to_merge_hours) as median_time_to_merge_hours
    from {{ ref('fact_pull_requests') }} p
    join date_spine d on p.merged_date_key = d.date_key
    where p.merged_date_key is not null
    group by 1, 2
),

all_months as (
    select distinct month_year from date_spine
),
all_repos as (
    select repo_id, repo_name from {{ ref('dim_repositories') }} where is_current = true
),
base as (
    select * from all_repos cross join all_months
)

select
    b.repo_id,
    b.repo_name,
    b.month_year,
    coalesce(io.issues_opened, 0) as issues_opened,
    coalesce(ic.issues_closed, 0) as issues_closed,
    ic.median_time_to_close_hours,
    coalesce(po.prs_opened, 0) as prs_opened,
    coalesce(pm.prs_merged, 0) as prs_merged,
    pm.median_time_to_merge_hours
from base b
left join issues_opened io on b.repo_id = io.repo_id and b.month_year = io.month_year
left join issues_closed ic on b.repo_id = ic.repo_id and b.month_year = ic.month_year
left join prs_opened po on b.repo_id = po.repo_id and b.month_year = po.month_year
left join prs_merged pm on b.repo_id = pm.repo_id and b.month_year = pm.month_year
