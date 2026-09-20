-- int_pull_requests_validated.sql
-- Intermediate layer: high-water-mark incremental + DQ validation for pull requests.
-- Incrementality moved here from staging — self-healing watermark on _bronze_ingest_ts.
-- DQ rules run BEFORE the SCD2 snapshot; quarantined records never enter history.

{{
    config(
        materialized='incremental',
        unique_key='pull_request_id',
    )
}}

with source as (
    select *
    from {{ ref('stg_github_pull_requests') }}
    {% if is_incremental() %}
    where _bronze_ingest_ts > (
        select coalesce(max(_bronze_ingest_ts), cast('1900-01-01' as timestamp))
        from {{ this }}
    )
    {% endif %}
),

enriched as (
    select
        s.*,
        r.repository_id as repository_id
    from source s
    left join {{ ref('stg_github_repositories') }} r
        on concat(r.owner_login, '/', r.repo_name) = s.canonical_repo_name
)

select
    *,
    -- Data Quality evaluation
    {{ evaluate_dq_rules([
        {'name': 'pr_id_not_null',             'expr': 'pull_request_id IS NOT NULL'},
        {'name': 'pr_number_positive',         'expr': 'pr_number > 0'},
        {'name': 'state_valid',                'expr': "state IN ('open', 'closed')"},
        {'name': 'created_at_not_null',        'expr': 'created_at IS NOT NULL'},
        {'name': 'updated_at_not_null',        'expr': 'updated_at IS NOT NULL'},
        {'name': 'created_before_updated',     'expr': 'created_at <= updated_at'},
        {'name': 'merged_at_after_created',    'expr': 'merged_at IS NULL OR merged_at >= created_at'},
        {'name': 'closed_at_after_created',    'expr': 'closed_at IS NULL OR closed_at >= created_at'},
        {'name': 'merged_implies_closed',      'expr': "merged_at IS NULL OR state = 'closed'"},
    ]) }}
from enriched
