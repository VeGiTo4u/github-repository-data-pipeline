-- stg_github_pull_requests.sql
-- Lightweight view: flatten, typecast, dedup from Bronze. No incrementality.
-- Watermark-based incremental logic lives in the intermediate layer.
--
-- Open item (Phase-2.md §12): PR detail fields (additions, deletions,
-- commits_count, changed_files, review_comments_count) are not available
-- from the /pulls list endpoint. They are carried as NULL here until
-- Phase 1 extraction is extended to fetch individual PR detail.

with parsed as (
    select
        from_json(data, 'id bigint, number int, title string, state string, user struct<login:string>, created_at string, updated_at string, closed_at string, merged_at string, merge_commit_sha string, draft boolean, head struct<ref:string>, base struct<ref:string>') as data,
        _ingestion_timestamp                as ingestion_timestamp,
        _bronze_ingest_ts,
        _extraction_run_id
    from {{ source('bronze', 'pull_requests') }}
),

source as (
    select
        data.id                             as pull_request_id,
        data.number                         as pr_number,
        data.title                          as title,
        data.state                          as state,
        data.user.login                     as user_login,
        cast(data.created_at as timestamp)  as created_at,
        cast(data.updated_at as timestamp)  as updated_at,
        cast(data.closed_at as timestamp)   as closed_at,
        cast(data.merged_at as timestamp)   as merged_at,
        data.merge_commit_sha               as merge_commit_sha,
        data.draft                          as is_draft,
        data.head.ref                       as head_branch,
        data.base.ref                       as base_branch,

        -- Open item: these fields are NULL from the /pulls list endpoint.
        -- Will be populated when Phase 1 adds single-PR detail fetching.
        cast(null as bigint)                as additions,
        cast(null as bigint)                as deletions,
        cast(null as bigint)                as commits_count,
        cast(null as bigint)                as changed_files,
        cast(null as bigint)                as review_comments_count,

        ingestion_timestamp,
        _bronze_ingest_ts,
        _extraction_run_id
    from parsed
),

deduped as (
    select *,
        row_number() over (
            partition by pull_request_id
            order by updated_at desc, _bronze_ingest_ts desc
        ) as rn
    from source
)

select * except (rn)
from deduped
where rn = 1
