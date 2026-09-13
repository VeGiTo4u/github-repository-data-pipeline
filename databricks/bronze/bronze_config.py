"""Metadata-driven resource registry for the Bronze layer.

Adding a new resource type = one dict entry here, zero code changes elsewhere.
"""

BRONZE_REGISTRY = {
    "repositories": {
        "raw_prefix": "raw/repositories",
        "bronze_prefix": "bronze/repositories",
        "table_name": "{catalog}.{schema}.repositories",
    },
    "issues": {
        "raw_prefix": "raw/issues",
        "bronze_prefix": "bronze/issues",
        "table_name": "{catalog}.{schema}.issues",
    },
    "pull_requests": {
        "raw_prefix": "raw/pull_requests",
        "bronze_prefix": "bronze/pull_requests",
        "table_name": "{catalog}.{schema}.pull_requests",
    },
    "releases": {
        "raw_prefix": "raw/releases",
        "bronze_prefix": "bronze/releases",
        "table_name": "{catalog}.{schema}.releases",
    },
    "languages": {
        "raw_prefix": "raw/languages",
        "bronze_prefix": "bronze/languages",
        "table_name": "{catalog}.{schema}.languages",
    },
}
