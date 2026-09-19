-- dim_repositories.sql
-- Type 2 SCD pulling from snap_repositories. 
-- snap_repositories only contains clean records (is_quarantined=false).

{{
    config(
        materialized='table'
    )
}}

select
    repository_id as repo_id,
    full_name as repo_name,
    owner_login,
    primary_language,
    license as license_name,
    case when dbt_valid_to is null then true else false end as is_current,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to
from {{ ref('snap_repositories') }}
