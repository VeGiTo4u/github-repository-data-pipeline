"""Canonical repository configuration for the ingestion pipeline.

Centralizes repo metadata the pipeline needs: tier-based throttle
thresholds and estimated volumes for meaningful progress logging.
"""

REPO_REGISTRY = {
    "duckdb/duckdb":           {"tier": "medium",  "est_issues": 22_000,  "est_prs": 14_000},
    "apache/spark":            {"tier": "large",   "est_issues": 45_000,  "est_prs": 42_000},
    "dbt-labs/dbt-core":       {"tier": "small",   "est_issues": 7_000,   "est_prs": 6_000},
    "facebook/react":          {"tier": "medium",  "est_issues": 14_000,  "est_prs": 16_000},
    "langchain-ai/langchain":  {"tier": "medium",  "est_issues": 8_000,   "est_prs": 15_000},
    "kubernetes/kubernetes":   {"tier": "xlarge",  "est_issues": 60_000,  "est_prs": 100_000},
    "rust-lang/rust":          {"tier": "large",   "est_issues": 60_000,  "est_prs": 70_000},
    "microsoft/vscode":        {"tier": "xlarge",  "est_issues": 170_000, "est_prs": 25_000},
}

# Tier-based throttle thresholds (requests remaining before cooperative slowdown)
TIER_THROTTLE = {
    "small":  50,
    "medium": 100,
    "large":  200,
    "xlarge": 300,
}


def get_repo_tier(repo_full_name: str) -> str:
    """Returns the tier for a repo, defaulting to 'medium' for unknown repos."""
    entry = REPO_REGISTRY.get(repo_full_name, {})
    return entry.get("tier", "medium")


def get_throttle_threshold(repo_full_name: str) -> int:
    """Returns the rate-limit remaining threshold for cooperative slowdown."""
    tier = get_repo_tier(repo_full_name)
    return TIER_THROTTLE.get(tier, 100)


def get_estimated_volume(repo_full_name: str, resource_type: str) -> int | None:
    """Returns estimated record count for a resource, or None if unknown."""
    entry = REPO_REGISTRY.get(repo_full_name)
    if entry is None:
        return None
    if resource_type == "issues":
        return entry.get("est_issues")
    elif resource_type == "pull_requests":
        return entry.get("est_prs")
    return None
