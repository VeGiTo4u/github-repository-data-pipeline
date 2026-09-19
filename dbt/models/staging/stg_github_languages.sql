-- stg_github_languages.sql
-- Full-population read with map unpivot (Phase-2.md §4.1).
-- Bronze data.* is a map<string, bigint> e.g. {"C++": 52105464, "Python": 1770521}.
-- Explode into (repository, language, bytes) rows.

with source as (
    select
        _source_endpoint                    as source_endpoint,
        _bronze_ingest_ts,
        _extraction_run_id,
        _ingestion_date,
        _raw_s3_uri,
        _repo_full_name,
        data
    from {{ source('bronze', 'languages') }}
    where _ingestion_date = (
        select max(_ingestion_date)
        from {{ source('bronze', 'languages') }}
    )
)

select
    -- Extract repo full name from the lineage envelope
    _repo_full_name as repo_full_name,
    lang.key    as language,
    lang.value  as bytes,
    _bronze_ingest_ts,
    _ingestion_date
from source
lateral view explode(from_json(data, 'MAP<STRING, BIGINT>')) lang as key, value
