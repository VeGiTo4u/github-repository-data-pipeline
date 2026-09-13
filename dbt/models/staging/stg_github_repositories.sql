-- stg_github_repositories.sql
-- Full-population read: every Bronze partition represents the full current population.
-- Read latest partition only, dedup by repository_id.

with parsed as (
    select
        from_json(data, 'id bigint, full_name string, owner struct<login:string>, name string, description string, language string, stargazers_count bigint, forks_count bigint, watchers_count bigint, open_issues_count bigint, default_branch string, fork boolean, archived boolean, created_at string, updated_at string, pushed_at string, size bigint, topics array<string>, license struct<spdx_id:string>') as data,
        _bronze_ingest_ts,
        _extraction_run_id,
        _ingestion_date
    from {{ source('bronze', 'repositories') }}
    where _ingestion_date = (
        select max(_ingestion_date)
        from {{ source('bronze', 'repositories') }}
    )
),

source as (
    select
        data.id                             as repository_id,
        data.full_name                      as full_name,
        data.owner.login                    as owner_login,
        data.name                           as repo_name,
        data.description                    as description,
        data.language                       as primary_language,
        data.stargazers_count               as stars,
        data.forks_count                    as forks,
        data.watchers_count                 as watchers,
        data.open_issues_count              as open_issues,
        data.default_branch                 as default_branch,
        data.fork                           as is_fork,
        data.archived                       as is_archived,
        cast(data.created_at as timestamp)  as created_at,
        cast(data.updated_at as timestamp)  as updated_at,
        cast(data.pushed_at as timestamp)   as pushed_at,
        data.size                           as repo_size_kb,
        data.topics                         as topics,
        data.license.spdx_id               as license,
        _bronze_ingest_ts,
        _extraction_run_id,
        _ingestion_date
    from parsed
),

deduped as (
    select *,
        row_number() over (
            partition by repository_id
            order by _bronze_ingest_ts desc
        ) as rn
    from source
)

select * except (rn)
from deduped
where rn = 1
