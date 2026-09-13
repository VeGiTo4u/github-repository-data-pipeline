-- snap_repositories.sql
-- SCD Type 2 via check strategy on business-relevant columns only.
-- Sources from intermediate (DQ-validated). Only clean records enter SCD2 history.
-- Do NOT use check_cols='all' — it triggers new rows on every
-- irrelevant field change (URLs, pushed_at). (Phase-2.md §3, §6.2)

{% snapshot snap_repositories %}
{{
    config(
        target_schema='silver_snapshots',
        unique_key='repository_id',
        strategy='check',
        check_cols=['stars', 'forks', 'watchers', 'open_issues', 'description', 'primary_language'],
        invalidate_hard_deletes=false,
    )
}}
select * except (dq_failed_rules, is_quarantined)
from {{ ref('int_repositories_validated') }}
where is_quarantined = false
{% endsnapshot %}

