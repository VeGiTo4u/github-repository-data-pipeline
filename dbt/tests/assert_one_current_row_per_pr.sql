-- assert_one_current_row_per_pr.sql
-- Singular test: ensures no duplicate pull_request_id with dbt_valid_to is null
-- in the pull requests snapshot.

select
    pull_request_id,
    count(*) as row_count
from {{ ref('snap_pull_requests') }}
where dbt_valid_to is null
group by pull_request_id
having count(*) > 1
