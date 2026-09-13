-- silver_repositories_current.sql
-- Current-state view of repositories with data quality tagging.
-- Clean records: from SCD2 snapshot (dbt_valid_to is null), is_quarantined = false.
-- Quarantined records: from intermediate layer, is_quarantined = true.
-- Downstream Gold can filter WHERE is_quarantined = false.

with clean_from_snapshot as (
    select
        repository_id,
        full_name,
        owner_login,
        repo_name,
        description,
        primary_language,
        stars,
        forks,
        watchers,
        open_issues,
        default_branch,
        is_fork,
        is_archived,
        created_at,
        updated_at,
        pushed_at,
        repo_size_kb,
        topics,
        license,
        _bronze_ingest_ts,
        _extraction_run_id,
        _ingestion_date,
        dbt_valid_from,
        dbt_valid_to,
        cast(array() as array<string>) as dq_failed_rules,
        false as is_quarantined
    from {{ ref('snap_repositories') }}
    where dbt_valid_to is null
),

quarantined_from_intermediate as (
    select
        repository_id,
        full_name,
        owner_login,
        repo_name,
        description,
        primary_language,
        stars,
        forks,
        watchers,
        open_issues,
        default_branch,
        is_fork,
        is_archived,
        created_at,
        updated_at,
        pushed_at,
        repo_size_kb,
        topics,
        license,
        _bronze_ingest_ts,
        _extraction_run_id,
        _ingestion_date,
        cast(null as timestamp) as dbt_valid_from,
        cast(null as timestamp) as dbt_valid_to,
        dq_failed_rules,
        is_quarantined
    from {{ ref('int_repositories_validated') }}
    where is_quarantined = true
)

select * from clean_from_snapshot
union all
select * from quarantined_from_intermediate
