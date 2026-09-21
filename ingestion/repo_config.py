"""Canonical repository configuration.
We use tier-based configurations instead of hard-coded stops so large repositories 
(like vscode) cooperatively throttle their API usage, ensuring they don't starve 
smaller repositories of the shared API budget.
"""

REPO_REGISTRY = {
    "duckdb/duckdb":           {"throttle": 100, "est_issues": 22_000,  "est_prs": 14_000},
    "apache/spark":            {"throttle": 200, "est_issues": 45_000,  "est_prs": 42_000},
    "dbt-labs/dbt-core":       {"throttle": 50,  "est_issues": 7_000,   "est_prs": 6_000},
    "facebook/react":          {"throttle": 100, "est_issues": 14_000,  "est_prs": 16_000},
    "langchain-ai/langchain":  {"throttle": 100, "est_issues": 8_000,   "est_prs": 15_000},
    "kubernetes/kubernetes":   {"throttle": 300, "est_issues": 60_000,  "est_prs": 100_000},
    "rust-lang/rust":          {"throttle": 200, "est_issues": 60_000,  "est_prs": 70_000},
    "microsoft/vscode":        {"throttle": 300, "est_issues": 170_000, "est_prs": 25_000},
}


def get_throttle_threshold(repo_full_name: str) -> int:
    """Returns the rate-limit remaining threshold for cooperative slowdown."""
    entry = REPO_REGISTRY.get(repo_full_name, {})
    return entry.get("throttle", 100)


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
