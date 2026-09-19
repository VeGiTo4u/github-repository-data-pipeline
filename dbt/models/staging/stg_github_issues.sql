-- stg_github_issues.sql
-- Lightweight view: flatten, typecast, dedup from Bronze. No incrementality.
-- Watermark-based incremental logic lives in the intermediate layer.
--
-- Derives is_pull_request from data.pull_request (Phase-2.md §4.2).
-- The downstream int_issues_validated applies DQ rules before snapshots.

with parsed as (
    select
        from_json(data, 'id bigint, number int, title string, state string, user struct<id:bigint, login:string>, created_at string, updated_at string, closed_at string, comments int, repository_url string, pull_request string, labels array<struct<name:string>>') as data,
        _ingestion_timestamp                as ingestion_timestamp,
        _bronze_ingest_ts,
        _extraction_run_id,
        _repo_full_name                     as repo_full_name
    from {{ source('bronze', 'issues') }}
),

source as (
    select
        data.id                             as issue_id,
        data.number                         as issue_number,
        data.title                          as title,
        data.state                          as state,
        data.user.id                        as user_id,
        data.user.login                     as user_login,
        cast(data.created_at as timestamp)  as created_at,
        cast(data.updated_at as timestamp)  as updated_at,
        cast(data.closed_at as timestamp)   as closed_at,
        data.comments                       as comments_count,
        data.repository_url                 as repository_url,
        (data.pull_request is not null)     as is_pull_request,
        data.labels                         as labels,
        repo_full_name,
        ingestion_timestamp,
        _bronze_ingest_ts,
        _extraction_run_id
    from parsed
),

deduped as (
    select *,
        row_number() over (
            partition by issue_id
            order by updated_at desc, _bronze_ingest_ts desc
        ) as rn
    from source
)

select * except (rn)
from deduped
where rn = 1
