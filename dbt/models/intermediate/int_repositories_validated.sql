-- int_repositories_validated.sql
-- Intermediate layer: DQ validation + enrichment for repositories.
-- No incrementality needed — repositories are full-population (re-fetched every run).
-- DQ rules run here BEFORE the SCD2 snapshot; quarantined records never enter history.

with source as (
    select * from {{ ref('stg_github_repositories') }}
)

select
    *,
    -- Data Quality evaluation
    {{ evaluate_dq_rules([
        {'name': 'repo_id_not_null',          'expr': 'repository_id IS NOT NULL'},
        {'name': 'full_name_not_null',        'expr': 'full_name IS NOT NULL'},
        {'name': 'stars_non_negative',        'expr': 'stars >= 0'},
        {'name': 'forks_non_negative',        'expr': 'forks >= 0'},
        {'name': 'watchers_non_negative',     'expr': 'watchers >= 0'},
        {'name': 'open_issues_non_negative',  'expr': 'open_issues >= 0'},
        {'name': 'created_at_not_null',       'expr': 'created_at IS NOT NULL'},
        {'name': 'created_before_updated',    'expr': 'created_at <= updated_at'},
    ]) }}
from source
