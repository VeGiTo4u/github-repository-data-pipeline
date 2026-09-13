-- silver_issues_current.sql
-- Current-state view of issues with data quality tagging.
-- Clean records: from SCD2 snapshot (dbt_valid_to is null), is_quarantined = false.
-- Quarantined records: from intermediate layer, is_quarantined = true.
-- Excludes PR-issues (is_pull_request = false) per Master_Context.md §7.
-- Downstream Gold can filter WHERE is_quarantined = false.

with clean_from_snapshot as (
    select
        issue_id,
        issue_number,
        title,
        state,
        user_login,
        created_at,
        updated_at,
        closed_at,
        comments_count,
        repository_url,
        repository_id,
        labels,
        _bronze_ingest_ts,
        _extraction_run_id,
        dbt_valid_from,
        dbt_valid_to,
        cast(array() as array<string>) as dq_failed_rules,
        false as is_quarantined
    from {{ ref('snap_issues') }}
    where dbt_valid_to is null
      and is_pull_request = false
),

quarantined_from_intermediate as (
    select
        issue_id,
        issue_number,
        title,
        state,
        user_login,
        created_at,
        updated_at,
        closed_at,
        comments_count,
        repository_url,
        repository_id,
        labels,
        _bronze_ingest_ts,
        _extraction_run_id,
        cast(null as timestamp) as dbt_valid_from,
        cast(null as timestamp) as dbt_valid_to,
        dq_failed_rules,
        is_quarantined
    from {{ ref('int_issues_validated') }}
    where is_quarantined = true
      and is_pull_request = false
)

select * from clean_from_snapshot
union all
select * from quarantined_from_intermediate
