-- stg_github_pull_requests.sql
-- Lightweight view: flatten, typecast, dedup from Bronze. No incrementality.
-- Watermark-based incremental logic lives in the intermediate layer.
--
-- Open item (Phase-2.md §12): PR detail fields (additions, deletions,
-- commits_count, changed_files, review_comments_count) are not available
-- from the /pulls list endpoint. We now join with the pr_details source
-- to populate these fields.

with parsed as (
    select
        from_json(data, 'id bigint, number int, title string, state string, user struct<id:bigint, login:string>, created_at string, updated_at string, closed_at string, merged_at string, merge_commit_sha string, draft boolean, head struct<ref:string>, base struct<ref:string>') as data,
        _ingestion_timestamp                as ingestion_timestamp,
        _bronze_ingest_ts,
        _extraction_run_id,
        _repo_full_name                     as repo_full_name
    from {{ source('bronze', 'pull_requests') }}
),

source as (
    select
        data.id                             as pull_request_id,
        data.number                         as pr_number,
        data.title                          as title,
        data.state                          as state,
        data.user.id                        as user_id,
        data.user.login                     as user_login,
        cast(data.created_at as timestamp)  as created_at,
        cast(data.updated_at as timestamp)  as updated_at,
        cast(data.closed_at as timestamp)   as closed_at,
        cast(data.merged_at as timestamp)   as merged_at,
        data.merge_commit_sha               as merge_commit_sha,
        data.draft                          as is_draft,
        data.head.ref                       as head_branch,
        data.base.ref                       as base_branch,

        -- Pulling PR details from a left join later
        repo_full_name,
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
),

latest_prs as (
    select * except (rn)
    from deduped
    where rn = 1
)

select 
    latest_prs.pull_request_id,
    latest_prs.pr_number,
    latest_prs.title,
    latest_prs.state,
    latest_prs.user_id,
    latest_prs.user_login,
    latest_prs.created_at,
    latest_prs.updated_at,
    latest_prs.closed_at,
    latest_prs.merged_at,
    latest_prs.merge_commit_sha,
    latest_prs.is_draft,
    latest_prs.head_branch,
    latest_prs.base_branch,
    details.additions,
    details.deletions,
    details.commits_count,
    details.changed_files,
    details.review_comments_count,
    latest_prs.ingestion_timestamp,
    latest_prs._bronze_ingest_ts,
    latest_prs._extraction_run_id,
    latest_prs.repo_full_name
from latest_prs
left join {{ ref('stg_github_pr_details') }} as details
    on latest_prs.pull_request_id = details.pull_request_id
