-- assert_one_current_row_per_repo.sql
-- Singular test: ensures no duplicate repository_id with dbt_valid_to is null
-- in the repositories snapshot.

select
    repository_id,
    count(*) as row_count
from {{ ref('snap_repositories') }}
where dbt_valid_to is null
group by repository_id
having count(*) > 1
