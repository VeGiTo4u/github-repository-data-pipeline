-- snap_issues.sql
-- SCD Type 2 via timestamp strategy on updated_at.
-- Sources from intermediate (DQ-validated). Only clean records enter SCD2 history.
-- invalidate_hard_deletes=false because intermediate returns only fresh delta
-- rows (high-water-mark incremental), not the full population. (Phase-2.md §6.1)

{% snapshot snap_issues %}
{{
    config(
        target_schema='silver_snapshots',
        unique_key='issue_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=false,
    )
}}
select * except (dq_failed_rules, is_quarantined)
from {{ ref('int_issues_validated') }}
where is_quarantined = false
{% endsnapshot %}

