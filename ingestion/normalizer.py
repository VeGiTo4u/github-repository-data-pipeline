import hashlib
import json
from datetime import datetime, timezone


def build_lineage_envelope(
    record: dict,
    source_endpoint: str,
    http_status: int,
    rate_limit_remaining: int | None,
    extraction_run_id: str,
    s3_bucket: str,
    s3_key: str,
    ingestion_date: str | None = None,
    repo_full_name: str | None = None,
) -> dict:
    """Wraps a raw record in a metadata envelope.
    We do this at the edge of ingestion so every single row carries its extraction context 
    (run ID, timestamp, API status), making it completely auditable throughout the pipeline.
    """
    now = datetime.now(timezone.utc)
    payload_bytes = json.dumps(record, sort_keys=True).encode("utf-8")
    date_str = ingestion_date or now.strftime("%Y-%m-%d")

    return {
        "data": record,
        "lineage": {
            "extraction_run_id": extraction_run_id,
            "ingestion_timestamp": now.isoformat(),
            "ingestion_date": date_str,
            "source_endpoint": source_endpoint,
            "http_status": http_status,
            "api_rate_limit_remaining": rate_limit_remaining,
            "record_count": 1,
            "payload_checksum": hashlib.sha256(payload_bytes).hexdigest(),
            "source_s3_uri": f"s3://{s3_bucket}/{s3_key}",
            "repo_full_name": repo_full_name,
        },
    }
