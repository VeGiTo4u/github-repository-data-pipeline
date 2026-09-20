from datetime import datetime, timezone
from ingestion.github_client import GitHubClient
from ingestion.normalizer import build_lineage_envelope
from ingestion.repo_config import get_throttle_threshold, get_estimated_volume
from ingestion.logger import get_logger
from ingestion.s3_writer import stream_records_to_s3
import json
import tempfile
import os

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
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_region: str = "us-east-1",
) -> tuple[list[tuple[str | None, str, str]], list[int]]:
    """Extracts all resource types for a single repo.

    Args:
        repo_full_name: e.g. "apache/spark"
        extraction_run_id: Airflow run_id or a uuid for local runs.
        github_token: GitHub PAT.
        s3_bucket: Target S3 bucket name.
        since: Optional ISO 8601 UTC timestamp watermark for incremental extraction.
        ingestion_date: Partition date (YYYY-MM-DD). Defaults to UTC today for local runs.
        aws_access_key_id: Optional AWS credentials for streaming to S3.
        aws_secret_access_key: Optional AWS credentials.
        aws_region: AWS region.

    Returns:
        A tuple of (results, pr_numbers).
        results: List of (temp_file_path, s3_key, resource_type) tuples.
                 If the resource was streamed directly to S3 (part files), temp_file_path is None.
        pr_numbers: List of PR numbers extracted in this run.
    """
    log = get_logger(__name__, run_id=extraction_run_id)
    throttle = get_throttle_threshold(repo_full_name)
    client = GitHubClient(token=github_token, run_id=extraction_run_id, throttle_threshold=throttle)
    if not ingestion_date:
        ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    repo_slug = repo_full_name.replace("/", "_")
    results = []
    pr_numbers = []

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
                # ponytail: hard deletes on GitHub won't trigger `since`. 
                # Downstream SCD2 assumes they remain open. Rare enough to ignore.
                params["since"] = since
        elif resource_type == "pull_requests":
            params["state"] = "all"
            if since:
                params["sort"] = "updated"
                params["direction"] = "desc"
                stop_predicate = lambda pr: bool(pr.get("updated_at") and pr["updated_at"] < since)

        s3_key_prefix = f"raw/{resource_type}/ingestion_date={ingestion_date}/{repo_slug}"
        s3_key = f"{s3_key_prefix}.json"
        
        try:
            records = client.get(endpoint, params=params, stop_predicate=stop_predicate)
            
            def envelope_generator():
                for record in records:
                    if resource_type == "pull_requests" and "number" in record:
                        pr_numbers.append(record["number"])
                    yield build_lineage_envelope(
                        record=record,
                        source_endpoint=endpoint,
                        http_status=client.last_status_code,
                        rate_limit_remaining=client.last_rate_limit_remaining,
                        extraction_run_id=extraction_run_id,
                        s3_bucket=s3_bucket,
                        s3_key=s3_key_prefix,  # Will be appended with /part_NNN.json for streams
                        ingestion_date=ingestion_date,
                        repo_full_name=repo_full_name,
                    )
            
            if resource_type in {"issues", "pull_requests", "releases"}:
                # Stream directly to S3 for paginated resources
                record_count, _ = stream_records_to_s3(
                    records=envelope_generator(),
                    s3_key_prefix=s3_key_prefix,
                    bucket=s3_bucket,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    aws_region=aws_region,
                    run_id=extraction_run_id,
                )
                if record_count == 0:
                    log.info(f"0 records extracted for {resource_type} ({repo_full_name})")
                results.append((None, s3_key_prefix, resource_type))
            else:
                # Local tempfile for single-object resources
                with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as tf:
                    record_count = 0
                    for envelope in envelope_generator():
                        tf.write(json.dumps(envelope) + "\n")
                        record_count += 1
                    tf_name = tf.name
                if record_count == 0:
                    log.info(f"0 records extracted for {resource_type} ({repo_full_name})")
                log.info(f"Extracted {resource_type} for {repo_full_name}: {record_count} records to {tf_name}")
                results.append((tf_name, s3_key, resource_type))
                
        except Exception as e:
            log.error(f"Failed extracting {resource_type} for {repo_full_name}: {e}")
            raise

    return results, pr_numbers


def extract_pr_details(
    repo_full_name: str,
    pr_numbers: list[int],
    extraction_run_id: str,
    github_token: str,
    s3_bucket: str,
    ingestion_date: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_region: str = "us-east-1",
) -> tuple[int, list[str]]:
    """Extracts code-level details (additions, deletions, etc) for specific PRs."""
    log = get_logger(__name__, run_id=extraction_run_id)
    throttle = get_throttle_threshold(repo_full_name)
    client = GitHubClient(token=github_token, run_id=extraction_run_id, throttle_threshold=throttle)
    if not ingestion_date:
        ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    repo_slug = repo_full_name.replace("/", "_")
    s3_key_prefix = f"raw/pr_details/ingestion_date={ingestion_date}/{repo_slug}"

    log.info(f"Extracting details for {len(pr_numbers)} PRs in {repo_full_name}")

    def pr_detail_generator():
        chunk_size = 15
        owner, name = repo_full_name.split("/")
        
        for i in range(0, len(pr_numbers), chunk_size):
            chunk = pr_numbers[i:i + chunk_size]
            
            aliases = []
            for n in chunk:
                aliases.append(
                    f"pr_{n}: pullRequest(number: {n}) {{ databaseId number additions deletions changedFiles commits {{ totalCount }} reviewThreads {{ totalCount }} }}"
                )
            query_body = "\n".join(aliases)
            
            graphql_query = f"""
            query {{
              repository(owner: "{owner}", name: "{name}") {{
                {query_body}
              }}
            }}
            """
            
            response = client.post_graphql(query=graphql_query)
            data = response.json()
            
            repo_data = data.get("data", {}).get("repository") or {}
            if not repo_data and data.get("errors"):
                log.warning(f"GraphQL chunk failed (poisoned PR), falling back to individual queries for chunk")
                for n in chunk:
                    single_query = f"""query {{ repository(owner: "{owner}", name: "{name}") {{ pr_{n}: pullRequest(number: {n}) {{ databaseId number additions deletions changedFiles commits {{ totalCount }} reviewThreads {{ totalCount }} }} }} }}"""
                    try:
                        resp = client.post_graphql(query=single_query)
                        s_repo = resp.json().get("data", {}).get("repository") or {}
                        if s_repo.get(f"pr_{n}"):
                            repo_data[f"pr_{n}"] = s_repo[f"pr_{n}"]
                    except Exception as e:
                        log.warning(f"Failed to fetch PR {n} individually: {e}")
                
            for k, pr_node in repo_data.items():
                if pr_node:
                    # Map GraphQL node to REST-like structure
                    record = {
                        "id": pr_node.get("databaseId"),
                        "number": pr_node.get("number"),
                        "additions": pr_node.get("additions"),
                        "deletions": pr_node.get("deletions"),
                        "changed_files": pr_node.get("changedFiles"),
                        "commits": (pr_node.get("commits") or {}).get("totalCount"),
                        "review_comments": (pr_node.get("reviewThreads") or {}).get("totalCount")
                    }
                    
                    yield build_lineage_envelope(
                        record=record,
                        source_endpoint="graphql:/pullRequest",
                        http_status=client.last_status_code,
                        rate_limit_remaining=client.last_rate_limit_remaining,
                        extraction_run_id=extraction_run_id,
                        s3_bucket=s3_bucket,
                        s3_key=s3_key_prefix,
                        ingestion_date=ingestion_date,
                        repo_full_name=repo_full_name,
                    )

    return stream_records_to_s3(
        records=pr_detail_generator(),
        s3_key_prefix=s3_key_prefix,
        bucket=s3_bucket,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_region=aws_region,
        run_id=extraction_run_id,
    )
