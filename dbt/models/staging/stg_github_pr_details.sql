-- stg_github_pr_details.sql
-- Lightweight view: flatten, typecast, dedup from Bronze pr_details.

with parsed as (
    select
        from_json(data, 'id bigint, number int, additions bigint, deletions bigint, changed_files bigint, commits bigint, review_comments bigint') as data,
        _ingestion_timestamp                as ingestion_timestamp,
        _bronze_ingest_ts,
        _extraction_run_id
    from {{ source('bronze', 'pr_details') }}
),

source as (
    select
        data.id                             as pull_request_id,
        data.number                         as pr_number,
        data.additions                      as additions,
        data.deletions                      as deletions,
        data.commits                        as commits_count,
        data.changed_files                  as changed_files,
        data.review_comments                as review_comments_count,
        ingestion_timestamp,
        _bronze_ingest_ts,
        _extraction_run_id
    from parsed
),

deduped as (
    select *,
        row_number() over (
            partition by pull_request_id
            order by _bronze_ingest_ts desc
        ) as rn
    from source
)

select * except (rn)
from deduped
where rn = 1
