from datetime import datetime, timezone
from ingestion.github_client import GitHubClient
from ingestion.normalizer import build_lineage_envelope
from ingestion.repo_config import get_throttle_threshold, get_estimated_volume
from ingestion.logger import get_logger
import json
import tempfile

# GitHub API endpoints per resource type
RESOURCE_ENDPOINTS = {
    "repositories": "/repos/{repo}",
    "issues": "/repos/{repo}/issues",
    "pull_requests": "/repos/{repo}/pulls",
    "releases": "/repos/{repo}/releases",
    "languages": "/repos/{repo}/languages",
}

def extract_repository_metadata(
    repo_full_name: str,
    extraction_run_id: str,
    github_token: str,
    s3_bucket: str,
    since: str | None = None,
    ingestion_date: str | None = None,
) -> list[tuple[str, str, str]]:
    """Extracts all resource types for a single repo and returns tempfile paths.

    Args:
        repo_full_name: e.g. "apache/spark"
        extraction_run_id: Airflow run_id or a uuid for local runs.
        github_token: GitHub PAT.
        s3_bucket: Target S3 bucket name.
        since: Optional ISO 8601 UTC timestamp watermark for incremental extraction.
        ingestion_date: Partition date (YYYY-MM-DD). Defaults to UTC today for local runs.

    Returns:
        List of (temp_file_path, s3_key, resource_type) tuples.
    """
    log = get_logger(__name__, run_id=extraction_run_id)
    throttle = get_throttle_threshold(repo_full_name)
    client = GitHubClient(token=github_token, run_id=extraction_run_id, throttle_threshold=throttle)
    if not ingestion_date:
        ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    repo_slug = repo_full_name.replace("/", "_")
    results = []

    mode = "incremental" if since else "full backfill"
    log.info(f"Starting extraction for {repo_full_name} (mode={mode}, throttle={throttle})")

    for resource_type, endpoint_template in RESOURCE_ENDPOINTS.items():
        endpoint = endpoint_template.format(repo=repo_full_name)
        est = get_estimated_volume(repo_full_name, resource_type)
        est_str = f" (est ~{est:,})" if est else ""
        log.info(f"Extracting {resource_type} for {repo_full_name}{est_str}")

        params = {"per_page": 100}
        stop_predicate = None
        if resource_type == "issues":
            params["state"] = "all"
            if since:
                params["since"] = since
        elif resource_type == "pull_requests":
            params["state"] = "all"
            if since:
                params["sort"] = "updated"
                params["direction"] = "desc"
                stop_predicate = lambda pr: bool(pr.get("updated_at") and pr["updated_at"] < since)

        s3_key = f"raw/{resource_type}/ingestion_date={ingestion_date}/{repo_slug}.json"
        
        tf = tempfile.NamedTemporaryFile("w", delete=False)
        record_count = 0
        
        try:
            records = client.get(endpoint, params=params, stop_predicate=stop_predicate)
            for record in records:
                envelope = build_lineage_envelope(
                    record=record,
                    source_endpoint=endpoint,
                    http_status=client.last_status_code,
                    rate_limit_remaining=client.last_rate_limit_remaining,
                    extraction_run_id=extraction_run_id,
                    s3_bucket=s3_bucket,
                    s3_key=s3_key,
                    ingestion_date=ingestion_date,
                )
                tf.write(json.dumps(envelope) + "\n")
                record_count += 1
        finally:
            tf.close()

        if record_count == 0:
            log.info(f"0 records extracted for {resource_type} ({repo_full_name}); writing empty file to ensure S3 path exists")

        log.info(f"Extracted {resource_type} for {repo_full_name}: {record_count} records to {tf.name}")
        results.append((tf.name, s3_key, resource_type))

    return results
