-- int_languages_validated.sql
-- Intermediate layer: DQ validation for languages.
-- No incrementality — languages are trivially small, full-population every run.
-- No SCD2 snapshot (Type 1); DQ columns flow directly to silver_languages.

with source as (
    select * from {{ ref('stg_github_languages') }}
),

enriched as (
    select
        s.*,
        r.repository_id as repository_id
    from source s
    left join {{ ref('stg_github_repositories') }} r
        on r.full_name = s.repo_full_name
)

select
    *,
    -- Data Quality evaluation
    {{ evaluate_dq_rules([
        {'name': 'language_not_null',    'expr': 'language IS NOT NULL'},
        {'name': 'bytes_positive',       'expr': 'bytes > 0'},
        {'name': 'repo_id_not_null',     'expr': 'repository_id IS NOT NULL'},
    ]) }}
from enriched
