-- int_issues_validated.sql
-- Intermediate layer: high-water-mark incremental + DQ validation for issues.
-- Incrementality moved here from staging — self-healing watermark on _bronze_ingest_ts.
-- DQ rules run BEFORE the SCD2 snapshot; quarantined records never enter history.
-- Enriches with repository_id by joining on the repo full_name extracted from repository_url.

{{
    config(
        materialized='incremental',
        unique_key='issue_id',
    )
}}

with source as (
    select *
    from {{ ref('stg_github_issues') }}
    {% if is_incremental() %}
    where _bronze_ingest_ts > (
        select coalesce(max(_bronze_ingest_ts), cast('1900-01-01' as timestamp))
        from {{ this }}
    )
    {% endif %}
),

-- Extract repo full_name from repository_url and join to get repository_id
-- repository_url pattern: https://api.github.com/repos/{owner}/{repo}
enriched as (
    select
        s.*,
        r.repository_id as repository_id
    from source s
    left join {{ ref('stg_github_repositories') }} r
        on concat(r.owner_login, '/', r.repo_name) = replace(s.repository_url, 'https://api.github.com/repos/', '')
)

select
    issue_id,
    issue_number,
    title,
    state,
    user_id,
    user_login,
    created_at,
    updated_at,
    closed_at,
    comments_count,
    repository_url,
    repository_id,
    is_pull_request,
    labels,
    ingestion_timestamp,
    _bronze_ingest_ts,
    _extraction_run_id,

    -- Data Quality evaluation
    {{ evaluate_dq_rules([
        {'name': 'issue_id_not_null',              'expr': 'issue_id IS NOT NULL'},
        {'name': 'issue_number_positive',          'expr': 'issue_number > 0'},
        {'name': 'state_valid',                    'expr': "state IN ('open', 'closed')"},
        {'name': 'created_at_not_null',            'expr': 'created_at IS NOT NULL'},
        {'name': 'updated_at_not_null',            'expr': 'updated_at IS NOT NULL'},
        {'name': 'created_before_updated',         'expr': 'created_at <= updated_at'},
        {'name': 'closed_at_after_created',        'expr': 'closed_at IS NULL OR closed_at >= created_at'},
        {'name': 'comments_count_non_negative',    'expr': 'comments_count >= 0'},
    ]) }}
from enriched
