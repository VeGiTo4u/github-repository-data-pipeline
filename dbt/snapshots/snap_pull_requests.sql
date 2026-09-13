-- snap_pull_requests.sql
-- SCD Type 2 via timestamp strategy on updated_at.
-- Sources from intermediate (DQ-validated). Only clean records enter SCD2 history.
-- Same rationale as snap_issues for invalidate_hard_deletes=false.

{% snapshot snap_pull_requests %}
{{
    config(
        target_schema='silver_snapshots',
        unique_key='pull_request_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=false,
    )
}}
select * except (dq_failed_rules, is_quarantined)
from {{ ref('int_pull_requests_validated') }}
where is_quarantined = false
{% endsnapshot %}

