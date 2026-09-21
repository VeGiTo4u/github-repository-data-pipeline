import json
import boto3
from botocore.config import Config
from ingestion.logger import get_logger

# Chunk size trades off S3 PUT count vs resilience — 5000 records
# (~5-10 MB) is small enough for fast uploads, large enough to avoid thousands of PUTs
STREAM_CHUNK_SIZE = 5000

def stream_records_to_s3(
    records,
    s3_key_prefix: str,
    bucket: str,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_region: str = "us-east-1",
    run_id: str | None = None,
    chunk_size: int = STREAM_CHUNK_SIZE,
) -> tuple[int, list[str]]:
    """Streams records to S3 in manageable chunks.
    We write small part-files instead of holding everything in memory. This ensures 
    that if a long-running extraction crashes midway, Airflow can retry without 
    losing the successfully uploaded parts or OOMing the container.

    Args:
        records:    Generator/iterable of envelope dicts.
        s3_key_prefix: e.g. "raw/issues/ingestion_date=2026-09-19/apache_spark"
        bucket:     S3 bucket name.
        chunk_size: Records per part file (default 5000).

    Returns:
        (total_record_count, list_of_s3_keys_written)
    """
    log = get_logger(__name__, run_id=run_id)

    client_kwargs = {"region_name": aws_region}
    if aws_access_key_id and aws_secret_access_key:
        client_kwargs["aws_access_key_id"] = aws_access_key_id
        client_kwargs["aws_secret_access_key"] = aws_secret_access_key

    # Configure botocore retries for transient S3 connection failures
    retry_config = Config(retries={"max_attempts": 10, "mode": "standard"})
    s3 = boto3.client("s3", config=retry_config, **client_kwargs)

    # Delete existing objects in prefix to prevent dangling parts on retry
    try:
        s3_resource = boto3.resource("s3", config=retry_config, **client_kwargs)
        bucket_obj = s3_resource.Bucket(bucket)
        
        objects_to_delete = list(bucket_obj.objects.filter(Prefix=s3_key_prefix))
        if objects_to_delete:
            bucket_obj.objects.filter(Prefix=s3_key_prefix).delete()
            log.info(f"Purged {len(objects_to_delete)} old parts from s3://{bucket}/{s3_key_prefix}")
    except Exception as e:
        log.error(f"Failed to purge old parts in {s3_key_prefix}: {e}")
        raise e

    buffer = []
    part_num = 0
    total_count = 0
    keys_written = []

    def _flush():
        nonlocal part_num
        if not buffer:
            return
        part_num += 1
        part_key = f"{s3_key_prefix}/part_{part_num:03d}.json"
        body = ("\n".join(json.dumps(rec) for rec in buffer) + "\n").encode("utf-8")
        s3.put_object(Bucket=bucket, Key=part_key, Body=body, ContentType="application/x-ndjson")
        keys_written.append(part_key)
        log.info(f"Streamed part {part_num} ({len(buffer)} records) to s3://{bucket}/{part_key}")
        buffer.clear()

    for record in records:
        buffer.append(record)
        total_count += 1
        if len(buffer) >= chunk_size:
            _flush()

    _flush()  # remaining records

    log.info(f"Stream complete: {total_count} records in {part_num} parts to s3://{bucket}/{s3_key_prefix}/")
    return total_count, keys_written

