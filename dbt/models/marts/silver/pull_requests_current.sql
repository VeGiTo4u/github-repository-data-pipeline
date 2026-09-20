-- silver_pull_requests_current.sql
-- Current-state view of pull requests with data quality tagging.
-- Clean records: from SCD2 snapshot (dbt_valid_to is null), is_quarantined = false.
-- Quarantined records: from intermediate layer, is_quarantined = true.
-- Downstream Gold can filter WHERE is_quarantined = false.

with clean_from_snapshot as (
    select
        pull_request_id,
        pr_number,
        title,
        state,
        user_id,
        user_login,
        created_at,
        updated_at,
        closed_at,
        merged_at,
        merge_commit_sha,
        is_draft,
        head_branch,
        base_branch,
        additions,
        deletions,
        commits_count,
        changed_files,
        review_comments_count,
        _bronze_ingest_ts,
        _extraction_run_id,
        dbt_valid_from,
        dbt_valid_to,
        cast(array() as array<string>) as dq_failed_rules,
        false as is_quarantined
    from {{ ref('snap_pull_requests') }}
    where dbt_valid_to is null
),

quarantined_from_intermediate as (
    select
        pull_request_id,
        pr_number,
        title,
        state,
        user_id,
        user_login,
        created_at,
        updated_at,
        closed_at,
        merged_at,
        merge_commit_sha,
        is_draft,
        head_branch,
        base_branch,
        additions,
        deletions,
        commits_count,
        changed_files,
        review_comments_count,
        _bronze_ingest_ts,
        _extraction_run_id,
        cast(null as timestamp) as dbt_valid_from,
        cast(null as timestamp) as dbt_valid_to,
        dq_failed_rules,
        is_quarantined
    from {{ ref('int_pull_requests_validated') }}
    where is_quarantined = true
)

select * from clean_from_snapshot
union all
select * from quarantined_from_intermediate
