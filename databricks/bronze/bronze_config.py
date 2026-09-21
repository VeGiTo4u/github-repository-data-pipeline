"""Metadata-driven resource registry for the Bronze layer.
We use a registry pattern instead of hardcoding resource types in the loader script. 
This allows us to add new GitHub endpoints (like 'commits' or 'comments') simply 
by adding a dictionary entry, isolating configuration from execution logic.
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
    "pr_details": {
        "raw_prefix": "raw/pr_details",
        "bronze_prefix": "bronze/pr_details",
        "table_name": "{catalog}.{schema}.pr_details",
    },
}
