-- int_releases_validated.sql
-- Intermediate layer: DQ validation for releases.
-- No incrementality needed — releases are full-population (re-fetched every run).
-- No SCD2 snapshot (Type 1); DQ columns flow directly to silver_releases.

with source as (
    select * from {{ ref('stg_github_releases') }}
)

select
    *,
    -- Data Quality evaluation
    {{ evaluate_dq_rules([
        {'name': 'release_id_not_null',        'expr': 'release_id IS NOT NULL'},
        {'name': 'tag_name_not_null',           'expr': 'tag_name IS NOT NULL'},
        {'name': 'created_at_not_null',         'expr': 'created_at IS NOT NULL'},
        {'name': 'published_after_created',     'expr': 'published_at IS NULL OR published_at >= created_at'},
    ]) }}
from source
