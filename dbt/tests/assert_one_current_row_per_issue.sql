-- assert_one_current_row_per_issue.sql
-- Singular test: ensures no duplicate issue_id with dbt_valid_to is null
-- in the issues snapshot. A duplicate means the dedup or snapshot config
-- has a bug that would propagate incorrect data to silver_issues_current.

select
    issue_id,
    count(*) as row_count
from {{ ref('snap_issues') }}
where dbt_valid_to is null
group by issue_id
having count(*) > 1
