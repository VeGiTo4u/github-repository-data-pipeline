-- dim_repositories.sql
-- Type 2 SCD pulling from snap_repositories. 
-- snap_repositories only contains clean records (is_quarantined=false).

{{
    config(
        materialized='table'
    )
}}

select
    dbt_scd_id as repository_sk,
    repository_id as repo_id,
    full_name as repo_name,
    owner_login,
    primary_language,
    license as license_name,
    case when dbt_valid_to is null then true else false end as is_current,
    case 
        when row_number() over(partition by repository_id order by dbt_valid_from asc) = 1 
        then cast('1900-01-01' as timestamp)
        else dbt_valid_from
    end as valid_from,
    dbt_valid_to as valid_to,
    _bronze_ingest_ts,
    _extraction_run_id,
    current_timestamp() as _gold_update_ts
from {{ ref('snap_repositories') }}
