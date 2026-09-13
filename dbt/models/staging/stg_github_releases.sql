-- stg_github_releases.sql
-- Full-population read: releases are fully re-fetched every run.
-- Read latest Bronze partition only, dedup by release_id.

with parsed as (
    select
        from_json(data, 'id bigint, tag_name string, name string, draft boolean, prerelease boolean, author struct<login:string>, created_at string, published_at string, target_commitish string') as data,
        _bronze_ingest_ts,
        _extraction_run_id,
        _ingestion_date
    from {{ source('bronze', 'releases') }}
    where _ingestion_date = (
        select max(_ingestion_date)
        from {{ source('bronze', 'releases') }}
    )
),

source as (
    select
        data.id                                 as release_id,
        data.tag_name                           as tag_name,
        data.name                               as release_name,
        data.draft                              as is_draft,
        data.prerelease                         as is_prerelease,
        data.author.login                       as author_login,
        cast(data.created_at as timestamp)      as created_at,
        cast(data.published_at as timestamp)    as published_at,
        data.target_commitish                   as target_commitish,
        _bronze_ingest_ts,
        _extraction_run_id,
        _ingestion_date
    from parsed
),

deduped as (
    select *,
        row_number() over (
            partition by release_id
            order by _bronze_ingest_ts desc
        ) as rn
    from source
)

select * except (rn)
from deduped
where rn = 1
